---
description: "Control how Lakekeeper lays out namespace and table directories under a Warehouse location: the default, full-hierarchy and tabular-only layouts, name and UUID templates, and namespace moves."
---

# Storage Layout

The storage layout controls how namespace and tabular directories are structured under the warehouse base location. It is configured via the `storage-layout` field inside the `storage-profile` when creating or updating a warehouse. The layout applies to new namespaces and to new tabulars in namespaces without a persisted `location`; a new tabular in a namespace with a `location` property is placed under that location (see [Namespace Location Property](#namespace-location-property)). Existing tabular locations are not changed.

## Layout Types

| Type                       | JSON `"type"` value            | Description    |
|----------------------------|--------------------------------|----------------|
| Default                    | `"default"`                    | Flat: no namespace directories; all tabulars are placed directly under the base location with a `{uuid}` segment. Used when `storage-layout` is omitted. **Changed in 0.13** — see [Default](#default). |
| Full hierarchy             | `"full-hierarchy"`             | One directory per namespace level in the full ancestry, one for the tabular. |
| Tabular-only (flat)          | `"tabular-only"`                 | No namespace directories; all tabulars are placed directly under the base location. |

!!! note "OneLake supports only the default layout"
    The [OneLake](storage-onelake.md) storage profile currently rejects `tabular-only` and `full-hierarchy` at warehouse-creation time because OneLake silently percent-decodes `%XX` in blob paths, which would alias `{name}` segments that differ only by URL-encoding. See [OneLake path and layout restrictions](storage-onelake.md#path-and-layout-restrictions) for details.

!!! warning "Some layouts prevent moving namespaces"
    A namespace's location is computed once when it is created and then frozen, so moving a namespace never relocates existing data. Under layouts that derive the location from the namespace hierarchy or from namespace *names*, a move would leave later-created child namespaces outside the moved namespace's location, fragmenting the layout. Lakekeeper therefore rejects the move with `StorageLayoutForbidsNamespaceMove` in those cases:

    | Layout | `namespace` template contains `{name}` | Rename | Re-parent |
    |--------|----------------------------------------|--------|-----------|
    | `default` / `tabular-only` | n/a — no namespace directories are emitted | allowed | allowed |
    | `full-hierarchy` | yes | **rejected** | **rejected** |
    | `full-hierarchy` | `{uuid}` only | allowed | **rejected** — the ancestor chain itself changes |

    The default layout emits no namespace directories, so this restriction only affects warehouses that explicitly configure `full-hierarchy`.

## Default

The default layout is **flat**: tabulars are placed directly under the warehouse base location with no namespace directories, using a `{uuid}` segment. This applies where Lakekeeper computes the location from the current layout; in a namespace with a persisted `location`, new tabulars use that location.

For a tabular `orders` in any namespace the path is:

```text
<base>/<orders-tabular>
```

With the default `{uuid}` template this looks like:

```text
s3://my-bucket/warehouse/<uuid-of-tabular>/
```

To use the default layout explicitly:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "default"
    }
  }
}
```

!!! warning "The default layout changed in 0.13"
    Before 0.13, the default layout emitted a directory for the **direct parent namespace** (`<base>/<parent-namespace-uuid>/<tabular-uuid>`). As of 0.13 the default is flat (`<base>/<tabular-uuid>`).

    The change is **not retroactive** — storage paths are assigned once, at creation time, and are never recomputed:

    - **Existing tabulars** keep their current locations.
    - **Existing namespaces** keep their persisted `location` property, so new tabulars created in them remain nested under the old `<parent-namespace-uuid>/` directory (see [Namespace Location Property](#namespace-location-property)).
    - Only **namespaces created on or after 0.13** use the flat default.

    A warehouse spanning the upgrade can therefore hold a mix of nested (pre-0.13 namespaces) and flat (new namespaces) tabular paths. If you want namespace directories in new tabular paths, set `storage-layout` to [`full-hierarchy`](#full-hierarchy) — note that this nests *every* ancestor level, whereas the pre-0.13 default nested only the direct parent.

## Full Hierarchy

The full hierarchy layout creates one path segment for **every namespace level** in the ancestry, followed by the tabular segment.

For a tabular `orders` in namespace `europe` / `production` the path is:

```text
<base>/<europe-namespace>/<production-namespace>/<orders-tabular>
```

With `{name}-{uuid}` templates:

```text
s3://my-bucket/warehouse/europe-<namespace-uuid>/production-<namespace-uuid>/orders-<tabular-uuid>/
```

Configuration:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "full-hierarchy",
      "namespace": "{name}-{uuid}",
      "tabular": "{name}-{uuid}"
    }
  }
}
```

## Tabular-Only (Flat)

The flat layout places all tabulars directly under the warehouse base location with no namespace directories.

For a tabular `orders` in any namespace the path is:

```text
<base>/<orders-tabular>
```

!!! note
    The `tabular` template **must** contain `{uuid}` in the flat layout. Without it, tabulars with the same name in different namespaces would map to the same storage path, causing data corruption.

Configuration:

```json
{
  "storage-profile": {
    "type": "s3",
    "storage-layout": {
      "type": "tabular-only",
      "tabular": "{name}-{uuid}"
    }
  }
}
```

## Template Placeholders

Namespace and tabular templates support two placeholders:

| Placeholder | Description                                                    |
|-------------|----------------------------------------------------------------|
| `{uuid}`    | UUID of the namespace or tabular, inserted without any encoding. |
| `{name}`    | Name of the namespace or tabular, URL percent-encoded. For example, `my tabular` becomes `my%20tabular` and `中文` becomes `%E4%B8%AD%E6%96%87`. |

Both placeholders can be combined with each other and with literal text, e.g. `{name}-{uuid}`. When `storage-layout` is omitted from the storage profile, the `default` layout is used.

!!! warning "Always include `{uuid}` in templates"
    **We strongly recommend including `{uuid}` in every namespace and tabular template.** Storage paths are assigned once at creation time and are never updated when a tabular or namespace is renamed. If a template relies solely on `{name}` and a tabular or namespace is later renamed and re-created with the same name, Lakekeeper will reject the creation because the path is already in use by the old (now-renamed) object. Because UUIDs are unique and stable for the lifetime of an object, using `{uuid}` (alone or combined with `{name}`) guarantees that each object always has a distinct, collision-free storage path. This is the reason Lakekeeper defaults to pure `{uuid}` templates.

## Namespace Location Property

Namespaces have a `location` property that determines where their tabulars are stored:

- **With location property**: New tabulars always use the namespace's persisted location, regardless of storage layout changes.
- **Without location property**: These namespaces compute locations from the current storage layout. Layout changes affect new tabular placement.

## Empty Namespace Directories

On [ADLS Gen2](storage-adls.md) with hierarchical namespace and on [OneLake](storage-onelake.md), a directory remains after the files in it are deleted. Dropping a namespace with `purge=true` removes the directories of the namespace and of the child namespaces dropped with it, if they are empty. Child directories are removed first, so a parent that only held them is removed too.

Only directories strictly below the warehouse base location are removed. With the `default` and `tabular-only` layouts a namespace's location is the base location itself, so this affects `full-hierarchy` warehouses, namespaces created before 0.13, and namespaces with an explicit `location`.

Removal runs once, during the drop request, and never fails the drop. A recursive drop queues table and view files for deletion in the background, so the directories of namespaces that contained tables usually remain. To have them removed, drop the tables first and drop the namespace with `purge=true` once their purge tasks have finished.
