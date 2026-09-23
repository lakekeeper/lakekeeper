---
level: none
---

**A log filter that selects audit records by their Rust module path now matches none of
them.** Audit records are emitted on the fixed `tracing` target `lakekeeper::audit`.

Previously the target was whichever module happened to hold the emitting code:
`lakekeeper::service::events::backends::audit` for authorization, replay and grant
records, and `lakekeeper::service::admission` for admission rejections. Both are
retired, as is any prefix of either below `lakekeeper`. A filter naming `lakekeeper`
itself still works.

Use `RUST_LOG=warn,lakekeeper::audit=info` to select audit records, or
`RUST_LOG=info,lakekeeper::audit=warn` to suppress them. The server prints a warning to
standard error at start-up if it finds a retired filter; it is written there rather
than to the log because the filter in question would suppress a logged warning.

The record body is unchanged by this, which is why it moves no version. Routing records
after they are emitted is unaffected: match on `event_source`, which has not changed.
