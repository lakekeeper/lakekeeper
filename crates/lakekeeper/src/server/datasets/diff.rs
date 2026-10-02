//! Comparing two versions of a dataset: the review step before a promote.
//!
//! Both manifests are read in logical-key order and merged, so a request holds a
//! page per side whatever the size of the dataset.
use std::{cmp::Ordering, collections::BTreeSet, sync::Arc};

use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use iceberg::TableIdent;
use uuid::Uuid;

use super::manifest::{ManifestCursor, ScanScope};
use crate::{
    CONFIG, WarehouseId,
    api::{
        ApiContext, ErrorModel,
        data::v1::datasets::{
            DatasetFileChange, DatasetFileChangeKind, DatasetParameters, DiffDatasetQuery,
            DiffDatasetResponse,
        },
        iceberg::v1::Result,
    },
    request_metadata::RequestMetadata,
    server::require_warehouse_id,
    service::{
        CatalogDatasetOps, CatalogStore, DatasetId, DatasetSnapshotId, InvalidPaginationToken,
        SecretStore, State, TabularListFlags, Transaction,
        authz::{AuthZDatasetOps, Authorizer, CatalogDatasetAction},
        events::{APIEventContext, context::UserProvidedDataset},
    },
};

/// Keys compared in one request, ten manifest pages. A diff of two large, mostly
/// equal manifests returns a short page at this bound, never a scan of both.
const DIFF_SCAN_BUDGET: usize = 10_000;

/// One side of the comparison, as the request named it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Side {
    Ref(String),
    Snapshot(DatasetSnapshotId),
}

impl Side {
    fn from_query(
        name: Option<String>,
        snapshot_id: Option<DatasetSnapshotId>,
        field: &str,
    ) -> Result<Self> {
        match (name, snapshot_id) {
            (Some(name), None) => Ok(Self::Ref(name)),
            (None, Some(snapshot_id)) => Ok(Self::Snapshot(snapshot_id)),
            _ => Err(ErrorModel::bad_request(
                format!("Name exactly one of `{field}` and `{field}SnapshotId`"),
                "InvalidDiffRequest",
                None,
            )
            .into()),
        }
    }

    /// How the side appears in a page token, so a token continues only the diff it
    /// came from.
    fn spec(&self) -> String {
        match self {
            Self::Ref(name) => format!("ref:{name}"),
            Self::Snapshot(snapshot_id) => format!("snapshot:{snapshot_id}"),
        }
    }
}

/// Where a diff stopped. The resolved snapshots ride in the token, so a commit
/// landing mid-walk cannot shift or tear the sequence; the sides as named ride
/// too, base64-encoded because a ref name may contain `&`. The key is last so
/// `splitn` keeps it intact when it contains `&`.
struct DiffPageToken {
    from: Option<DatasetSnapshotId>,
    to: Option<DatasetSnapshotId>,
    sides: String,
    after_key: String,
}

impl DiffPageToken {
    fn encode(&self) -> String {
        let snapshot =
            |id: Option<DatasetSnapshotId>| id.map_or_else(String::new, |id| id.to_string());
        let raw = format!(
            "1&{}&{}&{}&{}",
            snapshot(self.from),
            snapshot(self.to),
            BASE64_URL_SAFE_NO_PAD.encode(&self.sides),
            self.after_key
        );
        BASE64_URL_SAFE_NO_PAD.encode(raw)
    }

    fn decode(token: &str) -> std::result::Result<Self, InvalidPaginationToken> {
        let invalid = |message: &str| InvalidPaginationToken::new(message, token);
        let decoded = BASE64_URL_SAFE_NO_PAD
            .decode(token)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .ok_or_else(|| invalid("Invalid dataset diff page token encoding"))?;
        let snapshot = |part: &str| {
            if part.is_empty() {
                return Ok(None);
            }
            part.parse::<Uuid>()
                .map(|id| Some(DatasetSnapshotId::from(id)))
                .map_err(|_| invalid("Invalid dataset diff page token snapshot"))
        };
        match decoded.splitn(5, '&').collect::<Vec<_>>().as_slice() {
            ["1", from, to, sides, key] => Ok(Self {
                from: snapshot(from)?,
                to: snapshot(to)?,
                sides: BASE64_URL_SAFE_NO_PAD
                    .decode(sides)
                    .ok()
                    .and_then(|b| String::from_utf8(b).ok())
                    .ok_or_else(|| invalid("Invalid dataset diff page token sides"))?,
                after_key: (*key).to_string(),
            }),
            _ => Err(invalid("Invalid dataset diff page token structure")),
        }
    }
}

pub(super) async fn diff_dataset<C: CatalogStore, A: Authorizer + Clone, S: SecretStore>(
    parameters: DatasetParameters,
    query: DiffDatasetQuery,
    state: ApiContext<State<A, C, S>>,
    request_metadata: RequestMetadata,
) -> Result<DiffDatasetResponse> {
    let DatasetParameters {
        prefix,
        namespace,
        dataset_name,
    } = parameters;
    let warehouse_id = require_warehouse_id(prefix.as_ref())?;
    let DiffDatasetQuery {
        from,
        from_snapshot_id,
        to,
        to_snapshot_id,
        page_token,
        page_size,
    } = query;
    let from = Side::from_query(from, from_snapshot_id, "from")?;
    let to = Side::from_query(to, to_snapshot_id, "to")?;
    let sides = format!("{}\n{}", from.spec(), to.spec());

    // Reading two versions is reading the dataset, through each ref named.
    let target_refs = [&from, &to]
        .into_iter()
        .filter_map(|side| match side {
            Side::Ref(name) => Some(name.clone()),
            Side::Snapshot(_) => None,
        })
        .collect::<BTreeSet<_>>();
    let action = CatalogDatasetAction::ReadData {
        target_refs: Arc::new(target_refs),
    };
    let dataset_ident = TableIdent::new(namespace, dataset_name);
    let event_ctx = APIEventContext::for_dataset(
        Arc::new(request_metadata.clone()),
        state.v1_state.events.clone(),
        warehouse_id,
        dataset_ident.clone(),
        action.clone(),
    );
    let (_event_ctx, (_warehouse, _ns, info)) = event_ctx.emit_authz(
        state
            .v1_state
            .authz
            .load_and_authorize_dataset_operation::<C>(
                &request_metadata,
                &UserProvidedDataset::new(warehouse_id, dataset_ident),
                TabularListFlags::active(),
                action,
                state.v1_state.catalog.clone(),
            )
            .await,
    )?;

    let (from_snapshot_id, to_snapshot_id, after_key) = if let Some(raw) = page_token {
        let token = DiffPageToken::decode(&raw).map_err(ErrorModel::from)?;
        if token.sides != sides {
            return Err(ErrorModel::from(InvalidPaginationToken::new(
                "The page token belongs to a diff of other versions",
                raw,
            ))
            .into());
        }
        (token.from, token.to, Some(token.after_key))
    } else {
        let mut t = C::Transaction::begin_read(state.v1_state.catalog.clone()).await?;
        let from_id = resolve::<C>(warehouse_id, info.tabular_id, &from, t.transaction()).await?;
        let to_id = resolve::<C>(warehouse_id, info.tabular_id, &to, t.transaction()).await?;
        t.commit().await?;
        (from_id, to_id, None)
    };

    let page_size =
        usize::try_from(CONFIG.page_size_or_pagination_default(page_size)).unwrap_or(usize::MAX);
    let (changes, last_key) = merge_changes::<C>(
        warehouse_id,
        info.tabular_id,
        from_snapshot_id,
        to_snapshot_id,
        after_key,
        page_size,
        state.v1_state.catalog,
    )
    .await?;

    Ok(DiffDatasetResponse {
        from_snapshot_id,
        to_snapshot_id,
        changes,
        next_page_token: last_key.map(|after_key| {
            DiffPageToken {
                from: from_snapshot_id,
                to: to_snapshot_id,
                sides,
                after_key,
            }
            .encode()
        }),
    })
}

/// The snapshot a side names now. A ref with no commits names none.
async fn resolve<C: CatalogStore>(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    side: &Side,
    transaction: <C::Transaction as Transaction<C::State>>::Transaction<'_>,
) -> Result<Option<DatasetSnapshotId>> {
    match side {
        Side::Ref(name) => Ok(
            C::get_dataset_ref(warehouse_id, dataset_id, name, transaction)
                .await?
                .snapshot_id,
        ),
        Side::Snapshot(snapshot_id) => Ok(Some(*snapshot_id)),
    }
}

/// Up to `page_size` changes after `after_key`, and the key to continue from when
/// the walk stopped short of both manifests' ends.
async fn merge_changes<C: CatalogStore>(
    warehouse_id: WarehouseId,
    dataset_id: DatasetId,
    from: Option<DatasetSnapshotId>,
    to: Option<DatasetSnapshotId>,
    after_key: Option<String>,
    page_size: usize,
    catalog_state: C::State,
) -> Result<(Vec<DatasetFileChange>, Option<String>)> {
    let scope = ScanScope::new(None, None);
    let mut before =
        ManifestCursor::new(warehouse_id, dataset_id, from).starting_after(after_key.clone());
    let mut after = ManifestCursor::new(warehouse_id, dataset_id, to).starting_after(after_key);
    let mut changes = Vec::new();
    let mut scanned = 0;
    let mut last_key = None;
    loop {
        before.fill::<C>(&scope, catalog_state.clone()).await?;
        after.fill::<C>(&scope, catalog_state.clone()).await?;
        if changes.len() >= page_size || scanned >= DIFF_SCAN_BUDGET {
            let more = !before.page.is_empty() || !after.page.is_empty();
            return Ok((changes, last_key.filter(|_| more)));
        }
        let order = match (before.page.front(), after.page.front()) {
            (None, None) => return Ok((changes, None)),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(old), Some(new)) => old.logical_key.cmp(&new.logical_key),
        };
        scanned += 1;
        let change = match order {
            Ordering::Less => before.page.pop_front().map(|old| DatasetFileChange {
                logical_key: old.logical_key.clone(),
                change: DatasetFileChangeKind::Removed,
                from: Some(old.into()),
                to: None,
            }),
            Ordering::Greater => after.page.pop_front().map(|new| DatasetFileChange {
                logical_key: new.logical_key.clone(),
                change: DatasetFileChangeKind::Added,
                from: None,
                to: Some(new.into()),
            }),
            Ordering::Equal => match (before.page.pop_front(), after.page.pop_front()) {
                (Some(old), Some(new)) => {
                    last_key = Some(new.logical_key.clone());
                    (old != new).then(|| DatasetFileChange {
                        logical_key: new.logical_key.clone(),
                        change: DatasetFileChangeKind::Modified,
                        from: Some(old.into()),
                        to: Some(new.into()),
                    })
                }
                _ => None,
            },
        };
        if let Some(change) = change {
            last_key = Some(change.logical_key.clone());
            changes.push(change);
        }
    }
}
