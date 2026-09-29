---
level: major
---

**Replay records carry `record_type: "replay"`.** They no longer carry
`operation: "idempotent_replay"` or `outcome: "replayed"`, and those two values are gone
from the `operation` and `outcome` vocabularies.

Both were constants borrowed from the operational family, so a query selecting records
that have an `operation` also matched every replay.

**What to do:** select replays with `record_type == "replay"`. Read `operation` only for
records that describe something the system did.
