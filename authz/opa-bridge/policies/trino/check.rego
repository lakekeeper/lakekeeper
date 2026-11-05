# Contains convenience wrappers around Lakekeeper functions and rules
# to reduce the number inputs required to call them
package trino

import data.configuration
import data.lakekeeper

catalog_config_by_name[catalog_name] := trino_catalog if {
	some trino_catalog in configuration.trino_catalog
	catalog_name := trino_catalog.name
}

# flatten properties by merging extra_properties into the main properties object
flatten_properties(properties) := flattened if {
	extra := object.get(properties, "extra_properties", {})
	base := object.remove(properties, ["extra_properties"])
	flattened := object.union(base, extra)
}

# Iceberg REST Namespaces are multi part identifiers (arrays).
# Trino schemas are strings separated by dots.
#
# Every check must name the namespace Trino's Iceberg REST catalog acts on, so
# `namespace_for_schema` is undefined when Trino or Lakekeeper would act on
# another name:
# - Trino drops empty parts and trims each part with Guava's
#   `CharMatcher.whitespace()`. A part must not be empty and must not begin or
#   end with a Unicode separator (`\p{Z}`), a control character (`\p{Cc}`) or
#   U+180E. This covers every character Guava trims and some it keeps.
# - The Iceberg REST client separates namespace parts with U+001F in the
#   request path, so a part must not contain U+001F.
# - Lakekeeper reads a '+' in a request path as a space, so a part must not
#   contain '+'.
namespace_for_schema(schema_name) := namespace_name if {
	namespace_name := split(schema_name, ".")
	every part in namespace_name {
		_is_canonical_namespace_part(part)
	}
}

_trimmed_part_pattern := `^[\p{Z}\p{Cc}\x{180E}]|[\p{Z}\p{Cc}\x{180E}]$`

_is_canonical_namespace_part(part) if {
	part != ""
	not regex.match(_trimmed_part_pattern, part)
	not contains(part, "\u001f")
	not contains(part, "+")
}

# Every check must name the table or view Trino acts on, so these table and view
# names are refused:
# - Lakekeeper reads a '+' in a request path as a space, so for a name with '+'
#   Trino would act on the name with a space.
# - Trino reads `$` and `@` in a name as part of a metadata table name (see
#   `is_metadata_table`). A metadata table is checked on its base table, whose
#   name has neither character.
# The rules for `CreateTable` and `CreateView` apply this to the name of the new
# table or view, and the rename rules to the new name. The name of a new child
# namespace is checked by `namespace_for_schema`.
is_canonical_tabular_name(name) if {
	name != ""
	not contains(name, "+")
	not contains(name, "$")
	not contains(name, "@")
}

parent_schema(schema_name) := parent_schema if {
	namespace_name := namespace_for_schema(schema_name)
	parent_namespace := array.slice(namespace_name, 0, count(namespace_name) - 1)
	parent_schema := concat(".", parent_namespace)
}

require_catalog_access_simple(catalog_name, action) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_warehouse_access_simple(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		lakekeeper_user_id,
		action,
	)
}

require_catalog_create_namespace_access(catalog_name, properties, name) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_warehouse_create_namespace_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		lakekeeper_user_id,
		properties,
		name,
	)
}

require_schema_access_simple(catalog_name, schema_name, action) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_namespace_access_simple(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		lakekeeper_user_id,
		action,
	)
}

# Trino's `DropSchema` request does not say whether the statement has
# `CASCADE`. With `CASCADE`, Trino drops and purges every table and view in the
# schema without checking each of them. The check describes this case for every
# drop: `recursive` and `purge`.
require_schema_access_drop(catalog_name, schema_name) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_namespace_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		lakekeeper_user_id,
		{"action": "delete", "recursive": true, "purge": true},
	)
}

require_schema_access_create(catalog_name, schema_name, action, properties, name) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_namespace_access_create(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		lakekeeper_user_id,
		action,
		properties,
		name,
	)
}

# Not used yet
require_schema_access_update_properties(catalog_name, schema_name, removed_properties, updated_properties) if {
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_namespace_access_update_properties(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		lakekeeper_user_id,
		removed_properties,
		updated_properties,
	)
}

require_table_access_simple(catalog_name, schema_name, table_name, action) if {
	is_canonical_tabular_name(table_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_table_access_simple(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		table_name,
		lakekeeper_user_id,
		action,
	)
}

# Trino drops a table through the Iceberg REST catalog with
# `purgeRequested=true`, so the check describes a drop with purge, as
# Lakekeeper authorizes it.
require_table_access_drop(catalog_name, schema_name, table_name) if {
	is_canonical_tabular_name(table_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_table_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		table_name,
		lakekeeper_user_id,
		{"action": "drop", "purge": true},
	)
}

require_table_access_commit(catalog_name, schema_name, table_name, updated_properties, removed_properties) if {
	is_canonical_tabular_name(table_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_table_access_commit(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		table_name,
		lakekeeper_user_id,
		updated_properties,
		removed_properties,
	)
}

require_view_access_simple(catalog_name, schema_name, view_name, action) if {
	is_canonical_tabular_name(view_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	namespace_name := namespace_for_schema(schema_name)
	lakekeeper.require_view_access_simple(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_name,
		view_name,
		lakekeeper_user_id,
		action,
	)
}

require_view_in_definer_view_access(catalog_name, schema_name, view_name) if {
	is_canonical_tabular_name(view_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_view_in_definer_view_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		view_name,
		lakekeeper_user_id,
	)
}

# Trino drops a view through the Iceberg REST catalog without
# `purgeRequested`, and Lakekeeper then drops with purge. The check describes
# the drop as Lakekeeper authorizes it.
require_view_access_drop(catalog_name, schema_name, view_name) if {
	is_canonical_tabular_name(view_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_view_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		view_name,
		lakekeeper_user_id,
		{"action": "drop", "purge": true},
	)
}

# A rename into another schema moves the table or view: it also needs `move` on
# the source and `accept_moved_tabular` on the target schema. `kind` is "table"
# or "view". A rename into another catalog is refused.
require_rename_target_access(_, source_catalog, source_schema, _, target_catalog, target_schema) if {
	source_catalog == target_catalog
	source_schema == target_schema
}

require_rename_target_access(kind, source_catalog, source_schema, name, target_catalog, target_schema) if {
	source_catalog == target_catalog
	source_schema != target_schema
	is_canonical_tabular_name(name)
	trino_catalog := catalog_config_by_name[source_catalog]
	lakekeeper.require_tabular_move_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		kind,
		namespace_for_schema(source_schema),
		name,
		namespace_for_schema(target_schema),
		lakekeeper_user_id,
	)
}

# Trino asks the same `CreateTable` for `CREATE TABLE` and for
# `CREATE OR REPLACE TABLE`, which replaces a table that already has the name.
# So creating a table also needs `commit` on a live table with that name.
require_table_replace_access(catalog_name, schema_name, table_name, updated_properties) if {
	is_canonical_tabular_name(table_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_table_replace_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		table_name,
		lakekeeper_user_id,
		updated_properties,
	)
}

# The same holds for `CreateView` and `CREATE OR REPLACE VIEW`: creating a view
# also needs Lakekeeper to allow replacing a live view with that name. That is
# `commit`, and for a view that runs as another user also
# `change_security_mode`.
require_view_replace_access(catalog_name, schema_name, view_name) if {
	is_canonical_tabular_name(view_name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_view_replace_access(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		view_name,
		lakekeeper_user_id,
	)
}

# Trino before 484 reads a view with a metadata table name, such as
# `orders$files`, if it does not find the base table while it plans the query,
# or if it does not know the type (`$all_manifests` before 467, `$entries` and
# `$all_entries` before 469). Trino asks for this check after it planned the
# query, so the base table can exist by then. On older or unknown Trino
# versions, reading a metadata table also needs `select` on a live view with
# this name, if one exists. Trino 484 and later never read such a view.
require_metadata_table_view_access(_, _, _) if {
	_trino_reads_metadata_tables_before_views
}

require_metadata_table_view_access(catalog_name, schema_name, name) if {
	not _trino_reads_metadata_tables_before_views
	_require_view_select_access_if_exists(catalog_name, schema_name, name)
}

_require_view_select_access_if_exists(catalog_name, schema_name, name) if {
	is_metadata_table(name)
	trino_catalog := catalog_config_by_name[catalog_name]
	lakekeeper.require_view_select_access_if_exists(
		trino_catalog.lakekeeper_id,
		trino_catalog.lakekeeper_warehouse,
		namespace_for_schema(schema_name),
		name,
		lakekeeper_user_id,
	)
}

# The Trino version is the leading number of `softwareStack.trinoVersion`, for
# example 484 for "484-SNAPSHOT" or 476 for "476-e.1". A version without a
# leading number counts as older than 484.
_trino_reads_metadata_tables_before_views if {
	version := regex.find_all_string_submatch_n(`^([0-9]+)`, input.context.softwareStack.trinoVersion, 1)
	to_number(version[0][1]) >= 484
}

child_schema_name(schema_name) := child_name if {
	parts := namespace_for_schema(schema_name)
	child_name := parts[count(parts) - 1]
}

# Trino's Iceberg connector reads `<table>$<type>` as the metadata table `<type>`
# of the table `<table>`. Trino before 484 can read a view with this name (see
# `require_metadata_table_view_access`). The pattern is
# `IcebergTableName.TABLE_PATTERN` of Trino 484: the base name has neither `$`
# nor `@`. The type is one of Trino's `TableType` except `DATA` and
# `MATERIALIZED_VIEW_STORAGE`, because Trino's REST catalog has no materialized
# views. Java's `(?i)` ignores only ASCII case, so the type must be ASCII before
# it is compared in lower case. `allow_table_metadata_read` checks the base
# table. Table and view rules refuse every name with `$` or `@` (see
# `is_canonical_tabular_name`), so a type that is not listed here, also one that
# a later Trino version adds, is denied, not checked on another object.
_metadata_table_types := {
	"history", "metadata_log_entries", "snapshots", "all_manifests", "manifests",
	"partitions", "files", "all_entries", "entries", "properties", "refs",
}

is_metadata_table(table_name) if {
	regex.match(`^[^$@]+\$[A-Za-z_]+$`, table_name)
	lower(split(table_name, "$")[1]) in _metadata_table_types
} else := false

split_metadata_table_name(table_name) := base_table_name if {
	is_metadata_table(table_name)
	base_table_name := split(table_name, "$")[0]
}

# The type of a metadata table in lower case, for example `files` for `orders$FILES`.
metadata_table_type(table_name) := lower(split(table_name, "$")[1]) if {
	is_metadata_table(table_name)
}
