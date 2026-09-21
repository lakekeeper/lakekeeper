"""Granting helpers: namespaces, generic tables, and why the difference matters.

Kept out of `mlib.py` because this is the file worth reading — the grants below are the
whole demonstration, and every line of it is one decision about who may see what.

This uses the **permissions** API, not the newer authorizer-independent Grants API: the
latter is preview and is not in the released server this example runs against (0.13.x).
The shapes differ — assignments take `{"type": <privilege>, "user": <id>}` — and the
generic-table path is nested under the warehouse while the namespace path is not.

A note on levels. In the OpenFGA model a namespace grant flows downward
(`define select: ... or select from parent`), so `select` on the `skills` namespace would
also expose every agent's proposal queue underneath it. Skills are therefore granted on
the **table**, memory on the **namespace** — the first because the boundary runs between
siblings, the second because it runs around a whole scope.

And one asymmetry that constrains the design: `can_read_data` resolves through
`select_effective`, which includes `modify_effective` — so **read can be granted without
write, but write cannot be granted without read**. There is no write-only grant. That is
why each agent files proposals into its own table rather than a shared queue: a shared one
would be readable by everyone who could write to it.

(The model splits *granted* from *effective* privileges — assignments name the bare
relation, `select`, while checks resolve `select_effective`. The implication above lives on
the effective side, so the behaviour is the same as before the split.)
"""

from __future__ import annotations

import requests

from mlib import CATALOG_URL, LAKEKEEPER_URL, MANAGEMENT_URL, OK, auth, warehouse_id

# The Iceberg REST multi-level namespace separator (unit separator, U+001F).
NS_SEP = "\x1f"


def create_namespace(token: str, name: str) -> None:
    """Create a namespace and its parents, tolerating re-runs."""
    parts = name.split(".")
    for depth in range(1, len(parts) + 1):
        r = requests.post(
            f"{CATALOG_URL}/v1/{warehouse_id(token)}/namespaces",
            headers=auth(token),
            json={"namespace": parts[:depth]},
            timeout=15,
        )
        if r.status_code not in (*OK, 409):
            raise RuntimeError(f"namespace {name}: {r.status_code} {r.text}")


def generic_table_id(token: str, namespace: str, name: str) -> str:
    """Resolve a generic table's UUID — needed to grant on the table itself."""
    encoded = namespace.replace(".", NS_SEP)
    r = requests.get(
        f"{LAKEKEEPER_URL}/lakekeeper/v1/{warehouse_id(token)}"
        f"/namespaces/{encoded}/generic-tables",
        headers=auth(token),
        timeout=15,
    )
    r.raise_for_status()
    for ident in r.json().get("identifiers", []):
        if ident.get("name") == name:
            return ident["id"]
    raise KeyError(f"no generic table {namespace}.{name}")


def _assign(admin_token: str, path: str, user_id: str, privilege: str, revoke: bool) -> int:
    """POST an assignment diff to the permissions API.

    One shape at every level: `{"writes"|"deletes": [{"type": <privilege>, "user": <id>}]}`.
    Valid privileges come from the authorizer and differ per resource type — a namespace
    has `create`, a generic table does not — so :func:`assignable_privileges` reads them
    off the server rather than hardcoding a list here.
    """
    entry = {"type": privilege, "user": user_id}
    body = {"deletes" if revoke else "writes": [entry]}
    r = requests.post(f"{MANAGEMENT_URL}/v1/permissions/{path}/assignments",
                      headers=auth(admin_token), json=body, timeout=15)
    if r.status_code not in OK:
        raise RuntimeError(f"assignment failed: {r.status_code} {r.text}")
    return r.status_code


def grant_namespace(admin_token: str, wh_id: str, ns_id: str, user_id: str,
                    privilege: str = "select", revoke: bool = False) -> int:
    """Grant or revoke on a namespace; it flows down to everything inside."""
    return _assign(admin_token, f"namespace/{ns_id}", user_id, privilege, revoke)


def grant_generic_table(admin_token: str, wh_id: str, table_id: str, user_id: str,
                        privilege: str = "select", revoke: bool = False) -> int:
    """Grant or revoke on one generic table, and nothing around it.

    Note the path: generic-table permissions are nested under the warehouse
    (`permissions/warehouse/{wh}/generic-table/{id}`), not flat like namespaces.
    """
    return _assign(admin_token, f"warehouse/{wh_id}/generic-table/{table_id}",
                   user_id, privilege, revoke)


def assignable_privileges(admin_token: str, resource: str = "warehouse", **ids: str) -> list[str]:
    """The privilege names this server accepts for a resource type.

    Read from the server's own OpenAPI rather than hardcoded: the set comes from the
    configured authorizer and differs per resource (a namespace has `create`, a generic
    table does not). Cheap, and it keeps the notebook honest about where these come from.
    """
    spec = requests.get(
        f"{LAKEKEEPER_URL}/api-docs/management/v1/openapi.json", timeout=15
    ).json()
    schema = {
        "warehouse": "WarehouseAssignment",
        "namespace": "NamespaceAssignment",
        "generic-table": "GenericTableAssignment",
    }[resource]
    variants = spec["components"]["schemas"][schema]["oneOf"]
    return [v["allOf"][1]["properties"]["type"]["enum"][0] for v in variants]
