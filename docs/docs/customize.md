# Customize

Customizability is one of the core features that sets Lakekeeper apart from other Iceberg REST Catalog implementations. Almost every part of the server is a Rust trait or an injectable hook, so you can replace a backend or extend behaviour — for example to grant access to tables via your company's data-governance solution, persist secrets in a vault you already operate, react to every table change, expose custom endpoints, or publish events to your messaging system.

There are two ways to extend Lakekeeper, and the canonical list of everything you can supply is the [`ServeConfiguration`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/serve.rs) struct passed to [`serve()`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/serve.rs).

## Compile-time backends (generic parameters)

These are the type parameters of `serve<C: CatalogStore, S: SecretStore, A: Authorizer, N: Authenticator>` and [`CatalogServer<C, A, S>`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/server/mod.rs). Choosing an implementation is a build-time decision, so an integration is type-checked when you compile the server — there is no runtime plugin loading.

| Component | Trait | Responsibility | Ships with |
|-----------|-------|----------------|------------|
| `Catalog` | [`CatalogStore`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/catalog_store.rs) | Persistence for Warehouses, Namespaces, Tables, Views, and other entities. | Postgres (`lakekeeper-storage-postgres`) |
| `SecretStore` | [`SecretStore`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/secrets.rs) | Secure storage for storage credentials and other secrets. | Postgres, HashiCorp Vault-compatible KV v2 (`lakekeeper-secrets-kv2`) |
| `Authorizer` | [`Authorizer`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/authz/mod.rs) | The permission system. May expose its own management APIs. Default: `AllowAllAuthorizer`. | `AllowAll`, OpenFGA |
| `Authenticator` | [`limes::Authenticator`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/serve.rs) | Token authentication chain. Default: `AuthenticatorEnum`. | OIDC/OAuth2, Kubernetes |

## Runtime extension points (`ServeConfiguration`)

These are fields of `ServeConfiguration`, set with its builder when the server starts. Several accept a list, so multiple implementations can be active at once.

| Field | Trait / type | What it lets you do |
|-------|--------------|---------------------|
| `event_dispatcher` | [`EventListener`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/events/dispatch.rs) | React to strongly-typed domain events — table created/dropped/renamed/registered, transaction committed, view changes, and more. Override only the events you care about. |
| `cloud_event_sinks` | [`CloudEventBackend`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/events/publisher.rs) | Receive those changes as serialized [CloudEvents](https://cloudevents.io) and forward them somewhere. Ships with Kafka, NATS, and tracing sinks. |
| `contract_verification` | [`ContractVerification`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/contract_verification.rs) | Veto table/view changes — for example when a data contract would be violated. Implementations are collected into `ContractVerifiers`. |
| `stats` | [`EndpointStatisticsSink`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/service/endpoint_statistics.rs) | Consume per-project, per-endpoint API call statistics — for chargeback, quotas, or your own monitoring pipeline. |
| `modify_router_fn` | `fn(axum::Router) -> axum::Router` | Modify the HTTP router before serving: add custom routes, middleware, or layers. (The built-in UI is injected exactly this way.) |
| `register_additional_task_queues_fn` | [`RegisterTaskQueueFn`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/serve.rs) | Register custom background task queues alongside the built-in soft-delete expiration and purge queues. |
| `register_additional_background_services_fn` | [`RegisterBackgroundServiceFn`](https://github.com/lakekeeper/lakekeeper/blob/main/crates/lakekeeper/src/serve.rs) | Spawn arbitrary additional background services / futures that share the server's graceful-shutdown lifecycle. |
| `enable_built_in_task_queues` | `bool` | Disable the built-in task queues if you want to run them out-of-process. |

`ServeConfiguration` also accepts `license_status`, `build_info`, and `system_roles`, which downstream binaries (such as Lakekeeper Plus) use to inject build metadata and seed catalog-managed system roles.

### Two layers of event handling

Reacting to changes has two levels, depending on what you need:

* Implement an **`EventListener`** when you want to run typed Rust logic in-process on specific events (the default trait methods are no-ops, so you override only what you care about). Register it via the `EventDispatcher`.
* Provide a **`CloudEventBackend`** when you just want changes shipped out as CloudEvents. The built-in CloudEvents publisher is itself an `EventListener` that fans every event out to all configured `cloud_event_sinks`.

## What the hooks are for

A few common reasons to reach for each one:

* **Custom task queues** run persisted, scheduled background work on Lakekeeper's own task infrastructure — the tasks are stored in the database and picked up by workers across your Lakekeeper instances, so you don't have to stand up a separate scheduler or worry about a single worker dying mid-job. Beyond the built-in soft-delete expiration and purge queues, typical uses include triggering table maintenance/compaction, custom retention and cleanup policies, recomputing statistics, refreshing downstream materialized views, running data-quality or lineage scans, and pushing periodic digests to external systems.
* **Event listeners / cloud-event sinks** keep external systems in step with the catalog without polling — feeding data-discovery and lineage tools, audit pipelines, cache invalidation, or webhooks.
* **Contract verification** enforces organisational rules at write time, for example rejecting a schema change that would break a published data contract.
* **Endpoint-statistics sinks** route usage data into billing/chargeback, per-tenant quotas, or your own dashboards.
* **Router modifications** expose extra endpoints (custom health/readiness checks, internal admin APIs) or middleware alongside the catalog — this is how the built-in UI is mounted.

## Adding a custom implementation

Extending Lakekeeper always follows the same shape:

1. **Implement the trait.** Each extension point above links to its trait definition — that source is the authoritative, always-current signature.
2. **Wire it in.** For a runtime hook, pass your implementation to the matching `ServeConfiguration` builder field (for example `.cloud_event_sinks(...)`, `.stats(...)`, or `.modify_router_fn(...)`). For a compile-time backend, pass your type as the generic parameter to `serve()` (`CatalogStore`, `SecretStore`, `Authorizer`, `Authenticator`).

The best examples are the implementations Lakekeeper ships — they are compiled against the current traits and never drift out of date:

* **Event sinks** — the Kafka (`lakekeeper-events-kafka`) and NATS (`lakekeeper-events-nats`) crates, plus the in-tree `TracingPublisher`.
* **Catalog & secrets** — `lakekeeper-storage-postgres` and `lakekeeper-secrets-kv2`.
* **Authorizer** — the OpenFGA implementation in [`crates/authz-openfga`](https://github.com/lakekeeper/lakekeeper/tree/main/crates/authz-openfga).

See the [Developer Guide](./developer-guide.md) for building Lakekeeper from sources.
