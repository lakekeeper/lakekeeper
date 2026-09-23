---
level: major
---

Replay records no longer carry `operation: "idempotent_replay"` and
`outcome: "replayed"`. They carry `record_type: "replay"` instead, and those two
values are gone from the vocabularies of `operation` and `outcome`.

Nothing is lost: both fields were constants on every replay record and existed only so
the record could be recognised. They were markers, not data.

They were also markers borrowed from the operational family, which meant a query
selecting records that have an `operation` also matched every replay. Select replays
with `record_type == "replay"`, and use `operation` for records that genuinely
describe something the system did.
