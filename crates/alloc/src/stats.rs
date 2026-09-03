//! Periodic jemalloc arena statistics, published as Prometheus gauges.
//!
//! Distinguishes a genuine leak from allocator retention/fragmentation:
//! `allocated` is live application heap, while `resident` is what the kernel
//! counts against the container memory limit. A growing `resident` with a flat
//! `allocated` is retention/fragmentation, not a leak.
//!
//! Note that jemalloc's own `resident` can *under-report* actual residency by
//! hundreds of `MiB`, because it tracks its logical 4 `KiB` view of memory it has
//! released while the kernel may still hold those pages inside transparent huge
//! pages. `lakekeeper_process_anon_hugepages_bytes`, read from
//! `/proc/self/smaps_rollup`, is the ground truth for that case.
use std::time::Duration;

use tikv_jemalloc_ctl::{arenas, epoch, stats, stats_print};

const METRIC_ALLOCATED: &str = "lakekeeper_jemalloc_allocated_bytes";
const METRIC_ACTIVE: &str = "lakekeeper_jemalloc_active_bytes";
const METRIC_METADATA: &str = "lakekeeper_jemalloc_metadata_bytes";
const METRIC_RESIDENT: &str = "lakekeeper_jemalloc_resident_bytes";
const METRIC_MAPPED: &str = "lakekeeper_jemalloc_mapped_bytes";
const METRIC_RETAINED: &str = "lakekeeper_jemalloc_retained_bytes";
const METRIC_DIRTY: &str = "lakekeeper_jemalloc_dirty_bytes";
const METRIC_MUZZY: &str = "lakekeeper_jemalloc_muzzy_bytes";
const METRIC_ARENAS: &str = "lakekeeper_jemalloc_arenas";
const METRIC_BG_THREADS: &str = "lakekeeper_jemalloc_background_threads_enabled";
const METRIC_BG_RUNS: &str = "lakekeeper_jemalloc_background_thread_runs";
/// Ground truth for THP-trapped memory; see [`anon_huge_pages_bytes`].
const METRIC_ANON_HUGEPAGES: &str = "lakekeeper_process_anon_hugepages_bytes";

/// Merged-arena statistics that `tikv_jemalloc_ctl` does not wrap as typed
/// keys. Read from `stats_print`'s JSON output, which is a safe API; the crate's
/// `raw` `mallctl` accessors are `unsafe`.
#[derive(Debug, Default)]
struct MergedArenaStats {
    /// Bytes in freed-but-unpurged pages. Fully resident.
    dirty: Option<f64>,
    /// Bytes in `MADV_FREE`d pages. On Linux these stay counted in RSS until
    /// the kernel reclaims them under pressure.
    muzzy: Option<f64>,
    /// Cumulative background purge thread runs. Flat means purging has stalled.
    background_runs: Option<f64>,
}

fn merged_arena_stats() -> MergedArenaStats {
    let mut buf = Vec::new();
    let mut options = stats_print::Options::default();
    options.json_format = true;
    options.skip_per_arena = true;
    options.skip_bin_size_classes = true;
    options.skip_large_size_classes = true;
    options.skip_mutex_statistics = true;
    if let Err(e) = stats_print::stats_print(&mut buf, options) {
        tracing::warn!("Could not render jemalloc stats: {e}");
        return MergedArenaStats::default();
    }
    let Ok(json) = serde_json::from_slice::<serde_json::Value>(&buf) else {
        tracing::warn!("Could not parse jemalloc stats as JSON");
        return MergedArenaStats::default();
    };
    let root = &json["jemalloc"];
    let page = root["arenas"]["page"].as_f64().unwrap_or(4096.0);
    // jemalloc keys the merged-arena block by the literal string
    // `"stats.arenas"`, dot included, directly under `jemalloc`. Releases have
    // also nested it under `stats`, so accept both shapes.
    let merged = ["stats.arenas", "stats"]
        .iter()
        .flat_map(|k| {
            [
                &root[*k]["merged"],
                &root[*k]["stats.arenas"]["merged"],
                &root[*k]["arenas"]["merged"],
            ]
        })
        .find(|v| !v["pdirty"].is_null());
    let Some(merged) = merged else {
        tracing::warn!(
            "jemalloc stats JSON has no merged-arena block; dirty/muzzy will not be reported"
        );
        return MergedArenaStats {
            background_runs: root["stats"]["background_thread"]["num_runs"].as_f64(),
            ..MergedArenaStats::default()
        };
    };
    MergedArenaStats {
        dirty: merged["pdirty"].as_f64().map(|p| p * page),
        muzzy: merged["pmuzzy"].as_f64().map(|p| p * page),
        background_runs: root["stats"]["background_thread"]["num_runs"].as_f64(),
    }
}

/// Anonymous memory the kernel holds in transparent huge pages, from
/// `/proc/self/smaps_rollup`.
///
/// This is the one number jemalloc cannot report. A 2 `MiB` huge page cannot be
/// partially freed, so memory jemalloc has correctly released stays resident if
/// any page inside the same huge page is still live. When that happens
/// `stats.resident` reads far *below* the process's real anonymous RSS, the gap
/// only grows, and a restart is the only way to recover it. If this gauge is a
/// large fraction of RSS, the fix is `thp:never` (see `lakekeeper-alloc`'s
/// `MALLOC_CONF`), not a hunt for a leak.
fn anon_huge_pages_bytes() -> Option<f64> {
    let rollup = std::fs::read_to_string("/proc/self/smaps_rollup").ok()?;
    for line in rollup.lines() {
        if let Some(rest) = line.strip_prefix("AnonHugePages:") {
            let kb: f64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024.0);
        }
    }
    None
}

/// Log the allocator options that actually took effect, and warn if transparent
/// huge pages are still enabled for our mappings.
///
/// Both mechanisms that set `thp:never` fail *silently*: the
/// `JEMALLOC_SYS_WITH_MALLOC_CONF` build variable is invisible in the source
/// tree and is lost by any build that does not set it, and the exported
/// `malloc_conf` symbol is renamed (without a link error) if
/// `tikv-jemalloc-sys`'s `unprefixed_malloc_on_supported_platforms` feature is
/// ever enabled. So this reads back jemalloc's own effective `opt.thp`.
pub fn log_effective_config() {
    let thp = opt_thp();
    match thp.as_deref() {
        Some("never") => tracing::info!(
            opt_thp = "never",
            "jemalloc: transparent huge pages disabled for allocator mappings"
        ),
        Some(other) => tracing::warn!(
            opt_thp = other,
            "jemalloc is allowed to use transparent huge pages. On a host with THP enabled, \
             freed memory can stay resident indefinitely (a 2 MiB huge page cannot be partially \
             freed), so container memory ratchets upward and only a restart recovers it. Expected \
             `never`; set _RJEM_MALLOC_CONF=thp:never or build with \
             JEMALLOC_SYS_WITH_MALLOC_CONF=thp:never."
        ),
        None => {
            tracing::warn!("Could not read jemalloc opt.thp; cannot confirm huge-page handling");
        }
    }
}

fn opt_thp() -> Option<String> {
    let mut buf = Vec::new();
    let mut options = stats_print::Options::default();
    options.json_format = true;
    options.skip_per_arena = true;
    options.skip_bin_size_classes = true;
    options.skip_large_size_classes = true;
    options.skip_mutex_statistics = true;
    stats_print::stats_print(&mut buf, options).ok()?;
    let json = serde_json::from_slice::<serde_json::Value>(&buf).ok()?;
    json["jemalloc"]["opt"]["thp"]
        .as_str()
        .map(ToOwned::to_owned)
}

/// Sample jemalloc's own accounting every `interval` and publish it as gauges.
///
/// Runs for the lifetime of the process.
pub async fn run(interval: Duration) {
    use axum_prometheus::metrics;

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        // Re-issued every tick. The global metrics recorder is installed while
        // the server starts, after this task is spawned, and a `describe_gauge!`
        // issued before that is dropped and never replayed. Registration is
        // idempotent, so describing on each tick is what gets `# HELP` onto
        // `/metrics` regardless of which side of startup wins the race.
        metrics::describe_gauge!(
            METRIC_ALLOCATED,
            "Bytes allocated by the application (live heap)"
        );
        metrics::describe_gauge!(METRIC_ACTIVE, "Bytes in active pages");
        metrics::describe_gauge!(METRIC_METADATA, "Bytes used by allocator metadata");
        metrics::describe_gauge!(METRIC_RESIDENT, "Bytes in physically resident pages");
        metrics::describe_gauge!(METRIC_MAPPED, "Bytes in mapped extents");
        metrics::describe_gauge!(
            METRIC_RETAINED,
            "Bytes retained (mapped but unused) by the allocator"
        );
        metrics::describe_gauge!(
            METRIC_DIRTY,
            "Bytes in freed-but-unpurged pages (fully resident)"
        );
        metrics::describe_gauge!(
            METRIC_MUZZY,
            "Bytes in MADV_FREE'd pages (still counted in RSS)"
        );
        metrics::describe_gauge!(
            METRIC_ARENAS,
            "Number of arenas (4 x the node's core count by default)"
        );
        metrics::describe_gauge!(
            METRIC_BG_THREADS,
            "1 if jemalloc background purge threads are running"
        );
        metrics::describe_gauge!(METRIC_BG_RUNS, "Cumulative background purge thread runs");
        metrics::describe_gauge!(
            METRIC_ANON_HUGEPAGES,
            "Anonymous bytes held in transparent huge pages (may include memory the allocator freed)"
        );

        // `epoch::advance` refreshes the cached statistics; without it every
        // read returns the values from process start.
        if let Err(e) = epoch::advance() {
            tracing::warn!("Could not advance jemalloc stats epoch: {e}");
            continue;
        }
        #[allow(clippy::cast_precision_loss)] // byte counts; f64 is exact to 2^53
        let read = |name: &str, f: fn() -> Result<usize, tikv_jemalloc_ctl::Error>| match f() {
            Ok(v) => Some(v as f64),
            Err(e) => {
                tracing::warn!("Could not read jemalloc stat {name}: {e}");
                None
            }
        };
        // Pages jemalloc has freed but not yet returned to the kernel. Dirty
        // pages are still fully resident; muzzy pages have been `MADV_FREE`d,
        // which on Linux leaves them counted in RSS until the kernel reclaims
        // them under pressure. Both therefore show up in
        // `container_memory_working_set_bytes` while being invisible to
        // `allocated` — so a container can look like it is leaking while the
        // application heap is flat. If these two climb, the fix is decay/purge
        // tuning (`dirty_decay_ms`, `muzzy_decay_ms`, `background_thread`),
        // not a hunt for a retained object.
        let merged = merged_arena_stats();
        let (dirty, muzzy, bg_runs) = (merged.dirty, merged.muzzy, merged.background_runs);
        // Arena count is `4 * ncpus` by default, and `ncpus` is the *node's*
        // core count: a CFS CPU limit does not reduce it. Each arena keeps its
        // own dirty/muzzy pools, so a small CPU limit on a large node multiplies
        // the resident-but-free memory above.
        let narenas = arenas::narenas::read().ok().map(f64::from);
        let bg_enabled = tikv_jemalloc_ctl::background_thread::read()
            .ok()
            .map(|on| f64::from(u8::from(on)));

        let allocated = read("allocated", stats::allocated::read);
        let active = read("active", stats::active::read);
        let metadata = read("metadata", stats::metadata::read);
        let resident = read("resident", stats::resident::read);
        let mapped = read("mapped", stats::mapped::read);
        let retained = read("retained", stats::retained::read);

        for (name, value) in [
            (METRIC_ALLOCATED, allocated),
            (METRIC_ACTIVE, active),
            (METRIC_METADATA, metadata),
            (METRIC_RESIDENT, resident),
            (METRIC_MAPPED, mapped),
            (METRIC_RETAINED, retained),
            (METRIC_DIRTY, dirty),
            (METRIC_MUZZY, muzzy),
            (METRIC_ARENAS, narenas),
            (METRIC_BG_THREADS, bg_enabled),
            (METRIC_BG_RUNS, bg_runs),
            (METRIC_ANON_HUGEPAGES, anon_huge_pages_bytes()),
        ] {
            if let Some(v) = value {
                metrics::gauge!(name).set(v);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    /// Both mechanisms that set `thp:never` fail silently, so assert the
    /// outcome. Catches a renamed export symbol, a dropped
    /// `JEMALLOC_SYS_WITH_MALLOC_CONF`, or a `.cargo/config.toml` that stopped
    /// being read — none of which produce a build error.
    #[test]
    fn transparent_huge_pages_are_disabled_for_allocator_mappings() {
        assert_eq!(
            super::opt_thp().as_deref(),
            Some("never"),
            "jemalloc opt.thp is not `never`; see crates/alloc/src/lib.rs"
        );
    }
}
