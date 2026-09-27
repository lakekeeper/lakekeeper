//! Allocator configuration and observability, shared by every Lakekeeper binary.
//!
//! This crate exists for one reason: it is the **only** crate permitted to use
//! `unsafe`, so that the binaries can keep `#![forbid(unsafe_code)]`. The single
//! use is [`MALLOC_CONF`], which defines a symbol jemalloc reads before `main`.
//!
//! Depend on it from binaries only, never from the `lakekeeper` library — a
//! library must not impose allocator behaviour on downstream consumers.
#![warn(missing_debug_implementations, rust_2018_idioms, clippy::pedantic)]
// See `MALLOC_CONF`. No `unsafe` blocks and no unsafe operations are used; the
// single exception is one `unsafe` *attribute*, which asserts that the exported
// symbol matches the type jemalloc declares for it.
#![allow(unsafe_code)]

#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
mod stats;
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
pub use stats::{log_effective_config, run};

/// jemalloc's `thp` option and `/proc/self/smaps_rollup` are Linux-only, so
/// these are no-ops elsewhere. Callers stay free of `cfg`.
#[cfg(not(all(target_os = "linux", not(target_env = "msvc"))))]
pub fn log_effective_config() {}

/// See [`log_effective_config`].
#[cfg(not(all(target_os = "linux", not(target_env = "msvc"))))]
// Stays `async` to match the Linux signature; callers `tokio::spawn` it.
#[allow(clippy::unused_async)]
pub async fn run(_interval: std::time::Duration) {}

/// jemalloc run-time options, compiled into the binary.
///
/// `thp:never` is the important one. Transparent huge pages defeat jemalloc's
/// page release: it frees at 4 `KiB` granularity with `MADV_DONTNEED`, but a 2 `MiB`
/// huge page cannot be *partially* freed, so a single live page anywhere inside
/// one keeps the whole 2 `MiB` resident. On a node with `THP=always` (the default
/// on common EKS AMIs) a replica's RSS therefore ratchets upward permanently and
/// only a restart recovers it, while jemalloc's own `stats.resident`
/// under-reports real residency because it tracks its logical 4 `KiB` view.
/// Measured on a commit-heavy workload: `AnonHugePages` 286 `MiB` -> 0, settled
/// RSS -68%, throughput +1.9% (i.e. no cost — the workload is I/O-bound with a
/// ~30 `MiB` live heap, so it gains nothing from huge pages).
///
/// jemalloc reads options from `--with-malloc-conf`, then this symbol, then
/// `/etc/malloc.conf`, then `MALLOC_CONF`, left to right with later winning. So
/// `.cargo/config.toml` sets `JEMALLOC_SYS_WITH_MALLOC_CONF`, which is safe code
/// and independent of the symbol name, and this symbol covers builds that read no
/// `.cargo/config.toml` (`cargo install`, or a vendored source tree). An operator
/// overrides either with `_RJEM_MALLOC_CONF`.
/// [`log_effective_config`] reports what actually took effect, because both
/// mechanisms fail *silently*.
///
/// # Symbol name
///
/// `tikv-jemalloc-sys` sets `cfg(prefixed)` unless the
/// `unprefixed_malloc_on_supported_platforms` feature is enabled, so the symbol
/// is `_rjem_malloc_conf`. Enabling that feature would rename it to
/// `malloc_conf` and silently disable this — no link error. That is what
/// [`log_effective_config`] guards against.
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
#[used]
#[unsafe(export_name = "_rjem_malloc_conf")]
static MALLOC_CONF: &[u8; 10] = b"thp:never\0";

// jemalloc reads the symbol as `const char *`. A *fat* pointer (`&[u8]`,
// `&CStr`) would make it read the length as part of the address and dereference
// garbage, silently and severely — so assert the shape is a thin pointer and the
// string is NUL-terminated. `&[u8; N]` is thin because `[u8; N]` is `Sized`.
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
const _: () = {
    // Deliberately the size of the *reference*: this asserts that
    // `MALLOC_CONF`'s own type is one word. Switching it to `&[u8]` or `&CStr`
    // makes it two and trips this.
    #[allow(clippy::size_of_ref)]
    {
        assert!(size_of_val(&MALLOC_CONF) == size_of::<*const u8>());
    }
    assert!(MALLOC_CONF[MALLOC_CONF.len() - 1] == 0);
};
