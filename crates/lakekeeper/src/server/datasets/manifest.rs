//! A snapshot's manifest read in logical-key order, a page at a time.
use std::collections::VecDeque;

use glob::{MatchOptions, Pattern};
use iceberg_ext::catalog::rest::ErrorModel;

use crate::{
    WarehouseId,
    api::iceberg::v1::Result,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetId, DatasetSnapshotId, ManifestEntry, Transaction,
    },
};

/// Rows read per page on either side of the merge.
pub(super) const MERGE_PAGE: i64 = 1_000;

/// Marker files and scratch directories job writers leave in their output, which no
/// dataset wants registered.
pub(super) const DEFAULT_EXCLUDES: [&str; 3] =
    ["**/_SUCCESS", "**/_temporary/**", "**/.checkpoint/**"];

/// `*` and `?` stay within a path segment; only `**` crosses one.
const KEY_MATCH: MatchOptions = MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: false,
};

/// Globs over logical keys. A key is admitted if it matches one of `include`, when
/// there is any, and none of `exclude`.
#[derive(Debug, Default)]
pub(super) struct KeyPatterns {
    include: Vec<Pattern>,
    exclude: Vec<Pattern>,
}

impl KeyPatterns {
    /// Refuses a pattern that does not parse, naming it.
    pub(super) fn new<'p>(
        include: impl IntoIterator<Item = &'p str>,
        exclude: impl IntoIterator<Item = &'p str>,
    ) -> Result<Self> {
        let compile = |patterns: Vec<&str>| {
            patterns
                .into_iter()
                .map(|p| {
                    Pattern::new(p).map_err(|e| {
                        ErrorModel::bad_request(
                            format!("Invalid glob '{p}': {e}"),
                            "InvalidGlob",
                            None,
                        )
                        .into()
                    })
                })
                .collect::<Result<Vec<_>>>()
        };
        Ok(Self {
            include: compile(include.into_iter().collect())?,
            exclude: compile(exclude.into_iter().collect())?,
        })
    }

    fn admits(&self, key: &str) -> bool {
        (self.include.is_empty() || self.include.iter().any(|p| p.matches_with(key, KEY_MATCH)))
            && !self.exclude.iter().any(|p| p.matches_with(key, KEY_MATCH))
    }
}

/// Which logical keys a scan covers. Only these can be judged absent: a key
/// outside the scan was never looked for.
pub(super) struct ScanScope<'a> {
    /// The sub-prefix as a key prefix, ending in `/`; empty covers every key.
    key_prefix: String,
    suffix: Option<&'a str>,
    patterns: KeyPatterns,
}

impl<'a> ScanScope<'a> {
    /// Splits `sub_prefix` into path segments as the import's scan root does, so the
    /// keys covered are exactly those under the listed root.
    pub(super) fn new(sub_prefix: Option<&str>, suffix: Option<&'a str>) -> Self {
        let mut key_prefix = String::new();
        for segment in sub_prefix
            .into_iter()
            .flat_map(|p| p.split('/'))
            .filter(|s| !s.is_empty())
        {
            key_prefix.push_str(segment);
            key_prefix.push('/');
        }
        Self {
            key_prefix,
            suffix,
            patterns: KeyPatterns::default(),
        }
    }

    /// Narrow the scope further to the keys `patterns` admit.
    pub(super) fn with_patterns(mut self, patterns: KeyPatterns) -> Self {
        self.patterns = patterns;
        self
    }

    pub(super) fn covers(&self, key: &str) -> bool {
        self.within_prefix(key)
            && self.suffix.is_none_or(|s| key.ends_with(s))
            && self.patterns.admits(key)
    }

    /// Whether `key` lies under the sub-prefix, whatever its suffix.
    pub(super) fn within_prefix(&self, key: &str) -> bool {
        key.starts_with(&self.key_prefix)
    }

    /// The lowest key the scope can cover: where a walk in key order starts.
    pub(super) fn first_key(&self) -> Option<&str> {
        (!self.key_prefix.is_empty()).then_some(self.key_prefix.as_str())
    }
}

/// `head`'s files within a scope, in key order a page at a time.
pub(super) struct ManifestCursor {
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    head: Option<DatasetSnapshotId>,
    after: Option<String>,
    pub(super) page: VecDeque<ManifestEntry>,
    done: bool,
    primary: bool,
    include_expired: bool,
}

impl ManifestCursor {
    pub(super) fn new(
        warehouse_id: WarehouseId,
        dataset_id: DatasetId,
        head: Option<DatasetSnapshotId>,
    ) -> Self {
        Self {
            warehouse_id,
            dataset_id,
            head,
            after: None,
            page: VecDeque::new(),
            // A branch with no commits has no files to compare against.
            done: head.is_none(),
            primary: false,
            include_expired: false,
        }
    }

    /// Read `head` though it expired since the caller read it: an import walks the
    /// head it began on, which retention may expire once the branch has moved on.
    pub(super) fn including_expired(mut self) -> Self {
        self.include_expired = true;
        self
    }

    /// Read on the primary: a snapshot just published may not be on a replica yet.
    pub(super) fn on_primary(mut self) -> Self {
        self.primary = true;
        self
    }

    /// Resume after `key`, where an earlier walk of the same snapshot stopped.
    pub(super) fn starting_after(mut self, key: Option<String>) -> Self {
        self.after = key;
        self
    }

    /// Read until a covered file is buffered or the scope is exhausted: a page
    /// can hold nothing the scope covers.
    pub(super) async fn fill<C: CatalogStore>(
        &mut self,
        scope: &ScanScope<'_>,
        catalog_state: C::State,
    ) -> Result<()> {
        while self.page.is_empty() && !self.done {
            let Some(head) = self.head else {
                self.done = true;
                break;
            };
            let mut t = if self.primary {
                C::Transaction::begin_write(catalog_state.clone()).await?
            } else {
                C::Transaction::begin_read(catalog_state.clone()).await?
            };
            let (files, last_key) = C::list_snapshot_files(
                self.warehouse_id,
                self.dataset_id,
                head,
                scope.first_key(),
                self.after.as_deref(),
                MERGE_PAGE,
                self.include_expired,
                t.transaction(),
            )
            .await?;
            t.commit().await?;
            for file in files {
                // Keys sort, so the first one past the sub-prefix ends the scope.
                if !scope.within_prefix(&file.logical_key) {
                    self.done = true;
                    break;
                }
                if scope.covers(&file.logical_key) {
                    self.page.push_back(file);
                }
            }
            match last_key {
                Some(key) if !self.done => self.after = Some(key),
                _ => self.done = true,
            }
        }
        Ok(())
    }
}
