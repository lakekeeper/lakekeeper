"""The decision log: one Iceberg row per approval or rejection.

Approving writes a `SKILL.md`; rejecting writes a note. Both are objects, and both answer
"what is the current state?" perfectly well. Neither answers the question an auditor
actually asks — *"show me every skill approved last quarter, by whom, and why"* — without
listing objects, parsing frontmatter and grepping container logs.

So decisions also land in a table. This is the one part of the example where Iceberg earns
its place, for reasons that did not hold for agent memory:

* **Decisions are central, not per-scope.** The skill library is shared, so there is one
  table to query. Memory is isolated per agent, which is why it has no shared index.
* **This is a reporting workload.** Auditors ask from SQL, not from an agent. Memory
  recall never was Trino-shaped; a compliance log is.
* **Append-only, multi-writer, time-ordered, with snapshots.** Iceberg's actual job, and
  the one thing generic tables explicitly do not offer — they have no commit coordination.

`rules_version` is the column worth having. It pins which rule set was in force when the
call was made, so a later review can tell *"we did not check for that then"* apart from
*"we checked and missed it"* — a distinction that matters rather a lot in a post-mortem.

The catalog's own audit log still records that each write happened and who made it. What
it cannot know is the *reason*, or which findings the reviewer was looking at. That is
application meaning, and this is where it goes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

TABLE = "decisions"

#: Table schema. Kept here rather than inferred from a dataframe so the columns an audit
#: depends on are declared in one readable place.
COLUMNS = (
    ("decided_at", "timestamptz", "when the human clicked"),
    ("decision", "string", "approved | rejected"),
    ("skill", "string", "skill name"),
    ("version", "string", "content hash of the proposed body"),
    ("proposer", "string", "principal that filed it"),
    ("decided_by", "string", "principal that signed"),
    ("severity", "string", "what triage said: ok | warning | critical"),
    ("findings", "string", "rules that fired, comma-separated; empty when none did"),
    ("reason", "string", "the reviewer's words; required to reject"),
    ("rules_version", "int", "which rule set was in force"),
)


def _schema():
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        IntegerType,
        NestedField,
        StringType,
        TimestamptzType,
    )

    kinds = {"string": StringType(), "int": IntegerType(), "timestamptz": TimestamptzType()}
    return Schema(
        *[
            NestedField(i, name, kinds[kind], required=False, doc=doc)
            for i, (name, kind, doc) in enumerate(COLUMNS, start=1)
        ]
    )


def ensure_table(catalog, namespace: str):  # noqa: ANN001, ANN201 - pyiceberg types
    """Create the decision log if absent. Needs `create` on the namespace."""
    identifier = f"{namespace}.{TABLE}"
    try:
        return catalog.load_table(identifier)
    except Exception:  # noqa: BLE001 - NoSuchTableError, spelled differently across versions
        return catalog.create_table(identifier, schema=_schema())


def record(
    catalog,  # noqa: ANN001
    namespace: str,
    *,
    decision: str,
    verdict,  # noqa: ANN001 - review.Verdict
    decided_by: str,
    rules_version: int,
    reason: str = "",
) -> None:
    """Append one decision. Iceberg arbitrates the commit, so concurrent reviewers are safe.

    `rules_version` is required rather than defaulted: a decision recorded against an
    assumed version is worse than one recorded against none, because it reads as an answer.
    Take it from `review.load_rules`, which returns it alongside the rules.
    """
    import pyarrow as pa

    table = catalog.load_table(f"{namespace}.{TABLE}")
    row = {
        "decided_at": datetime.now(tz=timezone.utc),
        "decision": decision,
        "skill": verdict.name,
        "version": verdict.version,
        "proposer": verdict.proposer,
        "decided_by": decided_by,
        "severity": verdict.severity.label,
        "findings": ",".join(f.rule for f in verdict.findings),
        "reason": reason,
        "rules_version": rules_version,
    }
    table.append(pa.Table.from_pylist([row], schema=table.schema().as_arrow()))


def history(catalog, namespace: str) -> list[dict[str, Any]]:
    """Every decision, newest first. The query an auditor would write in SQL."""
    table = catalog.load_table(f"{namespace}.{TABLE}")
    rows = table.scan().to_arrow().to_pylist()
    return sorted(rows, key=lambda r: r["decided_at"], reverse=True)


def summary(rows: list[dict[str, Any]]) -> str:
    """A readable rendering of the log, for the notebook."""
    if not rows:
        return "no decisions recorded yet"
    out = [f"{len(rows)} decisions, newest first:", ""]
    for r in rows:
        when = r["decided_at"].strftime("%Y-%m-%d %H:%M")
        mark = "APPROVED" if r["decision"] == "approved" else "REJECTED"
        who = str(r["decided_by"]).split("~")[-1][:18]
        out.append(f"  {when}  {mark:8s} {r['skill']}@{r['version']}  [{r['severity']}]  by {who}")
        if r["findings"]:
            out.append(f"                       flagged: {r['findings']}")
        if r["reason"]:
            out.append(f"                       reason:  {r['reason']}")
    return "\n".join(out)
