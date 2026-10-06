---
level: none
---

**Audit records are emitted on the fixed `tracing` target `lakekeeper::audit`.**

A log filter naming `lakekeeper::service::events::backends::audit`, `lakekeeper::service::admission` or a prefix of either below `lakekeeper` matches no audit record. The server warns at start-up when it finds such a filter.

**What to do:** select audit records with `RUST_LOG=warn,lakekeeper::audit=info`, or suppress them with `RUST_LOG=info,lakekeeper::audit=warn`.
