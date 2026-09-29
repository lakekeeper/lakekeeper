---
level: none
---

**Audit records are emitted on the fixed `tracing` target `lakekeeper::audit`.** A log
filter that names a Rust module path matches none of them.

Retired: `lakekeeper::service::events::backends::audit`,
`lakekeeper::service::admission`, and any prefix of either below `lakekeeper`. A filter
naming `lakekeeper` itself still works.

**What to do:** select audit records with `RUST_LOG=warn,lakekeeper::audit=info`, or
suppress them with `RUST_LOG=info,lakekeeper::audit=warn`. The server warns on standard
error at start-up if it finds a retired filter. Routing records after they are emitted is
unaffected — match on `event_source`.
