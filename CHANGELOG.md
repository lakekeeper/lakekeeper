# Changelog

## [0.12.0](https://github.com/lakekeeper/lakekeeper/compare/v0.11.2...v0.12.0) (2026-03-01)


### ⚠ BREAKING CHANGES

* Use structured log format that uses objects as log values

### Features

* Add audit event handler ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* Add AuthorizerValidationFailed Error ([#1624](https://github.com/lakekeeper/lakekeeper/issues/1624)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Add in-memory Roles Cache ([#1623](https://github.com/lakekeeper/lakekeeper/issues/1623)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Add non-productive debug option to log authorization header ([#1613](https://github.com/lakekeeper/lakekeeper/issues/1613)) ([aff61ba](https://github.com/lakekeeper/lakekeeper/commit/aff61ba588a0623918046437d1ca3b2e7a26cbb7))
* add role lifecycle events and improve actor serialization ([#1622](https://github.com/lakekeeper/lakekeeper/issues/1622)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Adds max_request_body_size and max_request_time configuration variables ([#1583](https://github.com/lakekeeper/lakekeeper/issues/1583)) ([bbdf892](https://github.com/lakekeeper/lakekeeper/commit/bbdf89201bfa6a08ab1b24e0c48eae1206f319cc))
* Customization Option for Storage Layout ([#1615](https://github.com/lakekeeper/lakekeeper/issues/1615)) ([ed970ca](https://github.com/lakekeeper/lakekeeper/commit/ed970ca233144f7bb7718ee4454a2414a08951d4))
* **docs:** Add documentation for Logging ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* Improve action logs in audit ([#1610](https://github.com/lakekeeper/lakekeeper/issues/1610)) ([e3c1c97](https://github.com/lakekeeper/lakekeeper/commit/e3c1c97cdec6cc4ce531deaaa0df9a37aa98256e))
* Improve list namespaces, tables and views query performance ([#1618](https://github.com/lakekeeper/lakekeeper/issues/1618)) ([49efe9c](https://github.com/lakekeeper/lakekeeper/commit/49efe9c0c6eb1f50f20b0fc1930be821ada0f3c7))
* Improve StorageLayout and remove inner API from docs. Add validation for StorageLayouts ([#1628](https://github.com/lakekeeper/lakekeeper/issues/1628)) ([0f34f8d](https://github.com/lakekeeper/lakekeeper/commit/0f34f8dff03b3e81f24bd2c862ba0bd80c3f1ce6))
* Introduce Authorization events with exactly once per API-Call guarantee ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* Introduce string-based RoleIdent with provider-scoped role identifiers ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Parse roles from tokens into identifiers ([#1625](https://github.com/lakekeeper/lakekeeper/issues/1625)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Spark 4 Integration Tests ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c))
* Support V3 Variant Datatype ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c))
* Update Console to a78db64 ([#1611](https://github.com/lakekeeper/lakekeeper/issues/1611)) ([26040ce](https://github.com/lakekeeper/lakekeeper/commit/26040ce30e20ebe0548a22d56908ab97f1b66dc2))
* Update UI - Add Local Query Engine with memory Management ([#1621](https://github.com/lakekeeper/lakekeeper/issues/1621)) ([1aff402](https://github.com/lakekeeper/lakekeeper/commit/1aff402454b4095f06e4a1ab287d33f1ba6f2d21))
* Update UI - Add Storage Layout configuration ([#1634](https://github.com/lakekeeper/lakekeeper/issues/1634)) ([3c6e351](https://github.com/lakekeeper/lakekeeper/commit/3c6e351d8dab1eb4b5c1a9caad52785ab2aa70b8))
* Use structured log format that uses objects as log values ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))


### Bug Fixes

* **ci:** Pin botocore & s3fs Versions ([#1603](https://github.com/lakekeeper/lakekeeper/issues/1603)) ([735ee8c](https://github.com/lakekeeper/lakekeeper/commit/735ee8c24afc145124e98a2d82ab6f5118f236ec))
* **ui:** Better Safari support, Add Task Log Cleanup configuration ([#1593](https://github.com/lakekeeper/lakekeeper/issues/1593)) ([4738d4a](https://github.com/lakekeeper/lakekeeper/commit/4738d4a925f43fd7bcd51bb623e1d4c98e91c266))


### Miscellaneous Chores

* **deps:** Add cargo-deny with License checker ([#1580](https://github.com/lakekeeper/lakekeeper/issues/1580)) ([b1cdcd5](https://github.com/lakekeeper/lakekeeper/commit/b1cdcd51f715a24f8a258879e10f6e8a8c49ce41))
* **deps:** update rust crate bytes to v1.11.1 [security] ([#1597](https://github.com/lakekeeper/lakekeeper/issues/1597)) ([0ec6a00](https://github.com/lakekeeper/lakekeeper/commit/0ec6a008f674b8dca87a1ed813e345b9973e1a57))
* **deps:** update rust crate jsonwebtoken to v10.3.0 [security] ([#1598](https://github.com/lakekeeper/lakekeeper/issues/1598)) ([93bd887](https://github.com/lakekeeper/lakekeeper/commit/93bd887abf799fbc17d31721cb216ee3628780c0))
* **deps:** update rust crate time to v0.3.47 [security] ([#1600](https://github.com/lakekeeper/lakekeeper/issues/1600)) ([bc994e5](https://github.com/lakekeeper/lakekeeper/commit/bc994e542f108fc9a502ff92d7365d9723761c10))
* **docs:** Add Lakekeeper Plus license configuration ([#1595](https://github.com/lakekeeper/lakekeeper/issues/1595)) ([239dc5d](https://github.com/lakekeeper/lakekeeper/commit/239dc5d97b312dfedbf1d9eaef4288e5234e5625))
* **docs:** Improve cedar docs ([#1587](https://github.com/lakekeeper/lakekeeper/issues/1587)) ([30e04f7](https://github.com/lakekeeper/lakekeeper/commit/30e04f7c8646e0c9163c32d7ad5a1eb0f0262380))
* **events:** restructure event system service::events with clearer naming conventions ([#1602](https://github.com/lakekeeper/lakekeeper/issues/1602)) ([f6aaa45](https://github.com/lakekeeper/lakekeeper/commit/f6aaa4570cf62d54943581e7a00b9f882d9c992d))
* **hooks:** restructure EndpointHook to use event structs ([#1601](https://github.com/lakekeeper/lakekeeper/issues/1601)) ([7db8da6](https://github.com/lakekeeper/lakekeeper/commit/7db8da6eb001979f7bf4d3c1cf1e9e21ad2250e5))
* **opa-bridge:** Add CreateViewWithSelectFromColumns permissions based on "read_data" of underlying table ([#1594](https://github.com/lakekeeper/lakekeeper/issues/1594)) ([c378f6e](https://github.com/lakekeeper/lakekeeper/commit/c378f6e69f0e11e56d992c32b3939e73a4ed0ef0))

## [0.11.2](https://github.com/lakekeeper/lakekeeper/compare/v0.11.1...v0.11.2) (2026-01-29)


### Features

* Add configuration option to extract roles from tokens ([#1574](https://github.com/lakekeeper/lakekeeper/issues/1574)) ([bcc9103](https://github.com/lakekeeper/lakekeeper/commit/bcc9103655c1b971c8eb3a7fb3d6e580b3bba77f))
* Add DurationVisitors for serde for modular serialization and deserialization  ([#1575](https://github.com/lakekeeper/lakekeeper/issues/1575)) ([354f83f](https://github.com/lakekeeper/lakekeeper/commit/354f83fb11dad14a39335772fc7eed0d45456bdb))
* Add Task Log Cleanup Queue ([#1565](https://github.com/lakekeeper/lakekeeper/issues/1565)) ([d16d07c](https://github.com/lakekeeper/lakekeeper/commit/d16d07c06c6459c84e20d59bd7c04ab6a918640b))
* Update UI to 0.11.3 - Remember Tab on page reload ([ad1ec88](https://github.com/lakekeeper/lakekeeper/commit/ad1ec8801133814e935d9db363f7c8eba44ece4f))


### Bug Fixes

* Remove explicit schema qualification and update developer guide ([#1564](https://github.com/lakekeeper/lakekeeper/issues/1564)) ([b5f7cc9](https://github.com/lakekeeper/lakekeeper/commit/b5f7cc919c8102081453b2b627242647ad8a20c1))
* **ui:** enable_permissions should be true for all authorizers except AllowAll ([5abb3ba](https://github.com/lakekeeper/lakekeeper/commit/5abb3badf425aa8f516f007f11d05a2f961b9c12))
* Update UI to 0.11.2 - fix DuckDB Initialization ([#1578](https://github.com/lakekeeper/lakekeeper/issues/1578)) ([c4d3ad1](https://github.com/lakekeeper/lakekeeper/commit/c4d3ad16bf7c8dc84f17b503fab435095d463f0f))


### Miscellaneous Chores

* **docs:** Fix Cedar debug config table in docs ([26cf69d](https://github.com/lakekeeper/lakekeeper/commit/26cf69d0d2af606700e1580338b727ca380d8190))
* **docs:** Improve Cloudflare R2 region docs ([2e914f5](https://github.com/lakekeeper/lakekeeper/commit/2e914f5e04380e802c8d1c02d6b799daad466b69))
* **docs:** Update Management OpenAPI (plus) ([cdf8772](https://github.com/lakekeeper/lakekeeper/commit/cdf87722d80b66854764cc9e6eb4f88f34a9c7c9))
* Update lakekeeper.cedarschema ([db5303c](https://github.com/lakekeeper/lakekeeper/commit/db5303c2c68cbe5896d7c2405e3bdfa33adbf378))

## [0.11.1](https://github.com/lakekeeper/lakekeeper/compare/v0.11.0...v0.11.1) (2026-01-06)


### Features

* add configurable log formatting and database connection logging ([#1557](https://github.com/lakekeeper/lakekeeper/issues/1557)) ([1d638e8](https://github.com/lakekeeper/lakekeeper/commit/1d638e8dac18d68629ba35434db8e8205a181522))


### Bug Fixes

* Remove dependency on unmaintained libraries (paste, fxhash, derivative) ([f85128c](https://github.com/lakekeeper/lakekeeper/commit/f85128c347800505ad4c9e9f1084394085e0c608))
* Update OpenFGA Client to 0.5.1 (retry after ModelWrite) ([f85128c](https://github.com/lakekeeper/lakekeeper/commit/f85128c347800505ad4c9e9f1084394085e0c608))
* Update rsa to 0.9.10 to avoid potential panic ([836c2ec](https://github.com/lakekeeper/lakekeeper/commit/836c2ec56df03b60eba09287617fc652df9c2737))


### Miscellaneous Chores

* **dep:** Update UI to v0.11.1 ([836c2ec](https://github.com/lakekeeper/lakekeeper/commit/836c2ec56df03b60eba09287617fc652df9c2737))
* **docs:** Add 0.11 docs section ([02b421a](https://github.com/lakekeeper/lakekeeper/commit/02b421a4990bce5437af1527ecd6adc59f20a126))
* **docs:** Fix DuckDB WASM Bullets ([f74ef80](https://github.com/lakekeeper/lakekeeper/commit/f74ef808b6d3ffa81f6949e2b167781b8f807eaf))
* **docs:** Improve S3 CORS guide ([205ae0b](https://github.com/lakekeeper/lakekeeper/commit/205ae0be3a6bb20ad704ce5cdcbc7cadfee38507))
* **docs:** Update Odometer ([f85128c](https://github.com/lakekeeper/lakekeeper/commit/f85128c347800505ad4c9e9f1084394085e0c608))
* OPA Use catalog config Endpoint to get warehouse id ([#1558](https://github.com/lakekeeper/lakekeeper/issues/1558)) ([847ad13](https://github.com/lakekeeper/lakekeeper/commit/847ad13577643ed032be7d541ed695f02be1dc65))

## [0.11.0](https://github.com/lakekeeper/lakekeeper/compare/v0.10.3...v0.11.0) (2026-01-01)


### ⚠ BREAKING CHANGES

* Default to S3 `vended-credentials` instead of `remote-signing` if clients don't specify access delegation header
* **ci:** Use gnu instead of musl for ARM images ([#1508](https://github.com/lakekeeper/lakekeeper/issues/1508))
* Remove "name" filter in `ListRolesQuery`. Use more efficient search instead
* Deprecate Deprecate /permissions/.../actions Endpoints
* remove deprecated undrop_tabular and project_by_id endpoints
* Deprecate `id` in favor of `warehouse_id` in `GetWarehouseResponse`
* Require warehouse-id in the permissions/check API also for namespace-ids

### Features

* Add /authorizer-actions API ([53a0e4a](https://github.com/lakekeeper/lakekeeper/commit/53a0e4a6ee572ef8bf47cdc4be8067efede9a15c))
* add authorization-independent /actions endpoints ([25d777b](https://github.com/lakekeeper/lakekeeper/commit/25d777b74861046da04b186141db5ba762000738))
* Add etag method to `CommitTableResponse` ([#1530](https://github.com/lakekeeper/lakekeeper/issues/1530)) ([26353ec](https://github.com/lakekeeper/lakekeeper/commit/26353ecb8c57e6c8f8890557b876afd3186f590b))
* Add ETag to responses and evaluate If-None-Match Header ([#1509](https://github.com/lakekeeper/lakekeeper/issues/1509)) ([dcaac70](https://github.com/lakekeeper/lakekeeper/commit/dcaac70f9803983eea3d1924885a3529aecb4dc7))
* add GET /actions endpoints for all entity types (server, projects, warehouses, namespaces, tables, views, roles, users) ([25d777b](https://github.com/lakekeeper/lakekeeper/commit/25d777b74861046da04b186141db5ba762000738))
* Add properties & protection to AuthZ Info ([#1492](https://github.com/lakekeeper/lakekeeper/issues/1492)) ([e47a018](https://github.com/lakekeeper/lakekeeper/commit/e47a018c3c147e89108a0747a9f55d05f1e61aeb))
* Add separate permission to get Endpoint Statistics ([0b6ea38](https://github.com/lakekeeper/lakekeeper/commit/0b6ea38cc731203d7170b7eced99ffd15d8aa1ef))
* Add separate permission to set Warehouse Protection ([0b6ea38](https://github.com/lakekeeper/lakekeeper/commit/0b6ea38cc731203d7170b7eced99ffd15d8aa1ef))
* Add source system & source ID fields to Roles ([ca9153b](https://github.com/lakekeeper/lakekeeper/commit/ca9153b3a98ecf2d80dcd87ab19a6bdf96de48a4))
* Add storage profile-level credential control flags ([#1518](https://github.com/lakekeeper/lakekeeper/issues/1518)) ([1b69424](https://github.com/lakekeeper/lakekeeper/commit/1b694249fde87633f07279b653701aa74cf29764))
* add support for legacy md5 checksum ([#1551](https://github.com/lakekeeper/lakekeeper/issues/1551)) ([7dc818e](https://github.com/lakekeeper/lakekeeper/commit/7dc818eb40270c20dea0a0552d0b80a5364da03c))
* Add support for unmanaged Catalogs to trino OPA bridge ([#1542](https://github.com/lakekeeper/lakekeeper/issues/1542)) ([a67a87e](https://github.com/lakekeeper/lakekeeper/commit/a67a87e5355083bc072699e21e3a3819b3304ff6))
* Add Warehouse Cache to reduce DB requests ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Allow Authorizers to use Table & Namespace properties ([#1544](https://github.com/lakekeeper/lakekeeper/issues/1544)) ([8c00f6e](https://github.com/lakekeeper/lakekeeper/commit/8c00f6ef0ad81783e891bdbd67642ebe2fd8b125))
* Allow x-user-agent header ([#1453](https://github.com/lakekeeper/lakekeeper/issues/1453)) ([727f5b5](https://github.com/lakekeeper/lakekeeper/commit/727f5b554005fdf51ccb3e611bc2539cdcfef483))
* Apply sts tags also to Lakekeeper io operations ([77bb206](https://github.com/lakekeeper/lakekeeper/commit/77bb20691001f3cb3f383b0e928d517be1943d49))
* Authorizer independant permission batch-check endpoint ([#1529](https://github.com/lakekeeper/lakekeeper/issues/1529)) ([fed6b4a](https://github.com/lakekeeper/lakekeeper/commit/fed6b4ace7f81be65fcb37e95773068a8a6c95ec))
* AuthZ Server and Project Ops ([#1471](https://github.com/lakekeeper/lakekeeper/issues/1471)) ([9ef02d1](https://github.com/lakekeeper/lakekeeper/commit/9ef02d168c6c9937a34a60e2f91dbf6a19d97c54))
* Cache for Storage Secrets ([#1485](https://github.com/lakekeeper/lakekeeper/issues/1485)) ([505f8c4](https://github.com/lakekeeper/lakekeeper/commit/505f8c4ae7b81e3ac4de0623ae34a435b1b0bb10))
* Caching Short-Term-Credentials (STC) ([#1459](https://github.com/lakekeeper/lakekeeper/issues/1459)) ([c338372](https://github.com/lakekeeper/lakekeeper/commit/c3383720c5c3138c36d4f0391333ebc1fe4b5905))
* Catalog returns full Namespace Hierarchy for nested Namespaces ([#1472](https://github.com/lakekeeper/lakekeeper/issues/1472)) ([2fe38bf](https://github.com/lakekeeper/lakekeeper/commit/2fe38bf1cb6fc18da1efa3de29bf63625fff012c))
* Default to S3 `vended-credentials` instead of `remote-signing` if clients don't specify access delegation header ([2eaedf6](https://github.com/lakekeeper/lakekeeper/commit/2eaedf6e68b47f2f48591635ecabe4470eadd671))
* Deprecate `id` in favor of `warehouse_id` in `GetWarehouseResponse` ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Deprecate Deprecate /permissions/.../actions Endpoints ([53a0e4a](https://github.com/lakekeeper/lakekeeper/commit/53a0e4a6ee572ef8bf47cdc4be8067efede9a15c))
* Enrich Authorizer for Namespaces and Warehouses ([#1480](https://github.com/lakekeeper/lakekeeper/issues/1480)) ([f8fa500](https://github.com/lakekeeper/lakekeeper/commit/f8fa5007dfea19c5bf231c27e681ca81f2e88f85))
* Extend /actions endpoint with `for_user` parameter ([7dd4bb0](https://github.com/lakekeeper/lakekeeper/commit/7dd4bb0a14765661234ad1eb821bb8037c3e78d7))
* Extend Authorizer information for Tabulars ([#1484](https://github.com/lakekeeper/lakekeeper/issues/1484)) ([d5db102](https://github.com/lakekeeper/lakekeeper/commit/d5db10212b422b1dab41ec8b3c5e76fc258f2ddc))
* Extend ListRoles filter ([ca9153b](https://github.com/lakekeeper/lakekeeper/commit/ca9153b3a98ecf2d80dcd87ab19a6bdf96de48a4))
* Extend task system to support project-scoped tasks ([#1534](https://github.com/lakekeeper/lakekeeper/issues/1534)) ([58583a7](https://github.com/lakekeeper/lakekeeper/commit/58583a77fdfb396625531ee5cb2e9e2ceff1ae4e))
* implement action permission discovery API ([25d777b](https://github.com/lakekeeper/lakekeeper/commit/25d777b74861046da04b186141db5ba762000738))
* Improve compatibility for token refreshs ([#1546](https://github.com/lakekeeper/lakekeeper/issues/1546)) ([429f6b9](https://github.com/lakekeeper/lakekeeper/commit/429f6b9f5f87a22300672ef1facfd6c1b66c7b4f))
* Introduce AuthzWarehouseOps and AuthzNamespaceOps abstractions ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Introduce CatalogWarehouseOps & CatalogNamespaceOps abstractions ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Make Warehouse Cache case-insensitive ([#1473](https://github.com/lakekeeper/lakekeeper/issues/1473)) ([7d4c7d7](https://github.com/lakekeeper/lakekeeper/commit/7d4c7d7af44457b132937492fe7ecbec64dde228))
* Management endpoints now return full warehouse details after updates ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Namespace Cache ([#1478](https://github.com/lakekeeper/lakekeeper/issues/1478)) ([61b03a8](https://github.com/lakekeeper/lakekeeper/commit/61b03a8fdf792126e5fd9a05b426ec4175e1293f))
* New get role Metadata Endpoint (cross-project) ([#1516](https://github.com/lakekeeper/lakekeeper/issues/1516)) ([71efc01](https://github.com/lakekeeper/lakekeeper/commit/71efc0156918e34be7205eddf10825970964e2b1))
* Require warehouse-id in the permissions/check API also for namespace-ids ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Separate Namespace IncludeInList permission from CanGetMetadata ([#1491](https://github.com/lakekeeper/lakekeeper/issues/1491)) ([173ae32](https://github.com/lakekeeper/lakekeeper/commit/173ae32f5014f66fb1d93655cf88fb5889ddd669))
* Simplify Authorizer to only use `are_allowed_xxx` methods instead of `is_allowed_xxx` ([7dd4bb0](https://github.com/lakekeeper/lakekeeper/commit/7dd4bb0a14765661234ad1eb821bb8037c3e78d7))
* Support "+" as space in path identifiers, prohibit "+" in tabular & ns identifiers ([#1547](https://github.com/lakekeeper/lakekeeper/issues/1547)) ([a26b6f7](https://github.com/lakekeeper/lakekeeper/commit/a26b6f70317bf1c0c3ba30e73cd4dcd0c17b4739))
* Support Authorizers which cannot list projects ([#1481](https://github.com/lakekeeper/lakekeeper/issues/1481)) ([57663a2](https://github.com/lakekeeper/lakekeeper/commit/57663a276b4c3dd0b8f6b1367b4b0e0191c4e995))
* Table & View Ops Abstractions, Improved Error Handling ([#1454](https://github.com/lakekeeper/lakekeeper/issues/1454)) ([94996e4](https://github.com/lakekeeper/lakekeeper/commit/94996e4b17e87d510f18ae6c7e2f84b807079504))
* **ui:** Add DuckDB routes to UI router ([#1550](https://github.com/lakekeeper/lakekeeper/issues/1550)) ([dab8747](https://github.com/lakekeeper/lakekeeper/commit/dab8747398c5b52d563cd1311a582360039d6816))
* Update UI to Components ([#1549](https://github.com/lakekeeper/lakekeeper/issues/1549)) ([e4c57b1](https://github.com/lakekeeper/lakekeeper/commit/e4c57b17b8696a955b464b445d74c924df47a5d4))
* User & Role AuthZ Ops ([#1490](https://github.com/lakekeeper/lakekeeper/issues/1490)) ([1759290](https://github.com/lakekeeper/lakekeeper/commit/1759290e034ae9d997d56dc482f671e32edd34e6))
* Version based Warehouse Cache ([#1465](https://github.com/lakekeeper/lakekeeper/issues/1465)) ([c9c4b5e](https://github.com/lakekeeper/lakekeeper/commit/c9c4b5eec13d0c92167a372c57088b0cac501f91))


### Bug Fixes

* **ci:** Revert 0.10.3 release ([dfdbdcf](https://github.com/lakekeeper/lakekeeper/commit/dfdbdcf77923f36b1d2ea1a84cd494dad9f4bc9d))
* **ci:** Use gnu instead of musl for ARM images ([#1508](https://github.com/lakekeeper/lakekeeper/issues/1508)) ([621dfa4](https://github.com/lakekeeper/lakekeeper/commit/621dfa40d06473a2a36b1ac62977bda7f6bd61ed))
* Concurrent Table Update Error message & Server side retry ([#1527](https://github.com/lakekeeper/lakekeeper/issues/1527)) ([f283699](https://github.com/lakekeeper/lakekeeper/commit/f28369999097351241a6732754952945d7f8d029))
* CORS allow access delegation & etag headers ([#1455](https://github.com/lakekeeper/lakekeeper/issues/1455)) ([5f8c665](https://github.com/lakekeeper/lakekeeper/commit/5f8c66598cd59061c682b70b3e81c9c888fec1ba))
* Debug assertion table identifier mismatch for signer ([#1460](https://github.com/lakekeeper/lakekeeper/issues/1460)) ([684c690](https://github.com/lakekeeper/lakekeeper/commit/684c690244ce63a609d12a46d98b2e85d5df0ee1))
* Headers should be lowercase ([#1457](https://github.com/lakekeeper/lakekeeper/issues/1457)) ([06ad77e](https://github.com/lakekeeper/lakekeeper/commit/06ad77eb74c6b48520dcd5fafbbd99191d0b67ad))
* **linter:** fix linter errors for implicit cloning ([#1477](https://github.com/lakekeeper/lakekeeper/issues/1477)) ([4865d53](https://github.com/lakekeeper/lakekeeper/commit/4865d53da00e384e6cae587adbdacb37ffee2c8f))
* Make get_role_metadata work accross projects ([#1536](https://github.com/lakekeeper/lakekeeper/issues/1536)) ([ca344a1](https://github.com/lakekeeper/lakekeeper/commit/ca344a19343d782011e220de59b2d40513eb8c41))
* **openfga:** Delete user relations pagination ([#1507](https://github.com/lakekeeper/lakekeeper/issues/1507)) ([cb9908e](https://github.com/lakekeeper/lakekeeper/commit/cb9908e61c5b6cb31af06220a729e73bf9d7a72c))
* **openfga:** use url encode string when `Actor` is `Principal` in `to_openfga` ([#1521](https://github.com/lakekeeper/lakekeeper/issues/1521)) ([ca24218](https://github.com/lakekeeper/lakekeeper/commit/ca2421860aa8b7cd0bf504cafa12b2b9c90744e3))
* Remove explicit schema qualification to allow for dynamic override via: ([#1528](https://github.com/lakekeeper/lakekeeper/issues/1528)) ([69b960d](https://github.com/lakekeeper/lakekeeper/commit/69b960d3615a443c1bfffe98da571a9bbed5e601))
* remove redundant clone() before to_string() ([#1489](https://github.com/lakekeeper/lakekeeper/issues/1489)) ([fe9cf81](https://github.com/lakekeeper/lakekeeper/commit/fe9cf8165cab634d0a5e720f30e207af088744bd))
* Restrict cross-project Role Search, but allow cross-project Role Metadata get ([0b6ea38](https://github.com/lakekeeper/lakekeeper/commit/0b6ea38cc731203d7170b7eced99ffd15d8aa1ef))
* support view creation without "properties" (fix trino security invoker) ([#1545](https://github.com/lakekeeper/lakekeeper/issues/1545)) ([74bfb5c](https://github.com/lakekeeper/lakekeeper/commit/74bfb5ce9cbf974091b9811ccb350f12f24886a8))
* TaskQueueConfigFilter::WarehouseId should not require ProjectId ([#1554](https://github.com/lakekeeper/lakekeeper/issues/1554)) ([f32251c](https://github.com/lakekeeper/lakekeeper/commit/f32251c17e1e2ebfde033fb2d7d63bd71be7715b))
* Trino OPA bridge execute procedures ([#1541](https://github.com/lakekeeper/lakekeeper/issues/1541)) ([5d27125](https://github.com/lakekeeper/lakekeeper/commit/5d27125950e1124f5c55513028b5144bb2ff75b1))
* Use fallbacks instead of Err for vended cred. / remote-signing ([2eaedf6](https://github.com/lakekeeper/lakekeeper/commit/2eaedf6e68b47f2f48591635ecabe4470eadd671))
* Use read-pool for checks when flushing statistics ([d9ddda8](https://github.com/lakekeeper/lakekeeper/commit/d9ddda899d50bf19d97af66ef91d9c927d7ed087))
* Use read-pool for checks when flushing statistics ([#1526](https://github.com/lakekeeper/lakekeeper/issues/1526)) ([989bc8d](https://github.com/lakekeeper/lakekeeper/commit/989bc8d9589b939f087dfcf93dbf4fb1da0c566a))


### Documentation

* Add reference to Starburst query engine ([#1523](https://github.com/lakekeeper/lakekeeper/issues/1523)) ([32a17ea](https://github.com/lakekeeper/lakekeeper/commit/32a17ea73c9e2dde870763cf71ec48bdb63fd8d4))
* add RisingWave as a REST-compatible client ([#1496](https://github.com/lakekeeper/lakekeeper/issues/1496)) ([bd09d47](https://github.com/lakekeeper/lakekeeper/commit/bd09d4779ec6fb1046a88d8ed79d04799117ef05))


### Miscellaneous Chores

* Bump MSRV to 1.88 ([77bb206](https://github.com/lakekeeper/lakekeeper/commit/77bb20691001f3cb3f383b0e928d517be1943d49))
* Components UI ([2b188d5](https://github.com/lakekeeper/lakekeeper/commit/2b188d50194426f3e59fe23c8bcecad86a804c0d))
* **deps:** Iceberg 0.8, openfga 0.5, tonic 0.14, middle 0.4, limes 0.3 ([#1553](https://github.com/lakekeeper/lakekeeper/issues/1553)) ([a7abc10](https://github.com/lakekeeper/lakekeeper/commit/a7abc1039c91875c0f7ae3c01f8254053d471e8c))
* **deps:** update all non-major dependencies ([#1445](https://github.com/lakekeeper/lakekeeper/issues/1445)) ([4bf070c](https://github.com/lakekeeper/lakekeeper/commit/4bf070c0179bf79a738c1cfa5c64053c9dcb273f))
* **docs:** Cedar Authorizer ([#1512](https://github.com/lakekeeper/lakekeeper/issues/1512)) ([375ebdf](https://github.com/lakekeeper/lakekeeper/commit/375ebdf01f3a7fe26e33cb868ac67b2f9f441e2e))
* **docs:** Document Credential Vending ([#1531](https://github.com/lakekeeper/lakekeeper/issues/1531)) ([d3c41cf](https://github.com/lakekeeper/lakekeeper/commit/d3c41cf04e072d276b1bce8da1a9d54ab4f8df0d))
* **docs:** Update directory paths in getting-started.md ([#1548](https://github.com/lakekeeper/lakekeeper/issues/1548)) ([d97b32a](https://github.com/lakekeeper/lakekeeper/commit/d97b32abafb28c3beaa5acab07ad4af335ab347d))
* Improve Azure SAS generation logs ([#1522](https://github.com/lakekeeper/lakekeeper/issues/1522)) ([737d1ff](https://github.com/lakekeeper/lakekeeper/commit/737d1ffdd911a0ea543832a3fd3dd06ecf94e3c0))
* Improve IO Errors ([#1487](https://github.com/lakekeeper/lakekeeper/issues/1487)) ([510c551](https://github.com/lakekeeper/lakekeeper/commit/510c551e89fe81b25c7c3ea59a77a7c3785ab899))
* Introduce open-api feature to gate utoipa and swagger ([#1458](https://github.com/lakekeeper/lakekeeper/issues/1458)) ([c39d96e](https://github.com/lakekeeper/lakekeeper/commit/c39d96e9c39ed86f8c4357afe525c2794e2748ae))
* **main:** release 0.10.3 ([#1446](https://github.com/lakekeeper/lakekeeper/issues/1446)) ([b8fcf54](https://github.com/lakekeeper/lakekeeper/commit/b8fcf54c627d48a547ef0baf6863949b68579388))
* **permissions:** Restrict Endpoint Statistics access to Warehouse Assignee and above ([0b6ea38](https://github.com/lakekeeper/lakekeeper/commit/0b6ea38cc731203d7170b7eced99ffd15d8aa1ef))
* Remove "name" filter in `ListRolesQuery`. Use more efficient search instead ([ca9153b](https://github.com/lakekeeper/lakekeeper/commit/ca9153b3a98ecf2d80dcd87ab19a6bdf96de48a4))
* Remove `Can` prefix from rust Action types ([53a0e4a](https://github.com/lakekeeper/lakekeeper/commit/53a0e4a6ee572ef8bf47cdc4be8067efede9a15c))
* remove build-with-alpine ([#1514](https://github.com/lakekeeper/lakekeeper/issues/1514)) ([5d3436e](https://github.com/lakekeeper/lakekeeper/commit/5d3436e87c1756c7a94a89987ef2c6d13520c479))
* remove dependcy on forked rdkafka and remove unneccessaty build-dependency on openssl-sys ([#1515](https://github.com/lakekeeper/lakekeeper/issues/1515)) ([c53be9b](https://github.com/lakekeeper/lakekeeper/commit/c53be9b7184d479b2578e5718fc2357dd51634cb))
* remove deprecated undrop_tabular and project_by_id endpoints ([25d777b](https://github.com/lakekeeper/lakekeeper/commit/25d777b74861046da04b186141db5ba762000738))
* Rename `SecretIdent` to `SecretId` for consistency ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* **tests:** Add trino information_schema.tables test ([#1456](https://github.com/lakekeeper/lakekeeper/issues/1456)) ([665d8c9](https://github.com/lakekeeper/lakekeeper/commit/665d8c9e3b75c7140374b95e47b1d35e684c9b84))
* Update examples to use new /actions endpoints ([#1510](https://github.com/lakekeeper/lakekeeper/issues/1510)) ([3408454](https://github.com/lakekeeper/lakekeeper/commit/3408454b6323bca64710c8db2ab85d9bf0b47435))
* Update Plus API, UI v0.11 ([#1555](https://github.com/lakekeeper/lakekeeper/issues/1555)) ([790462b](https://github.com/lakekeeper/lakekeeper/commit/790462b802f30d320253801ec18a4e0092f28f1e))
* Update README.md ([cebc453](https://github.com/lakekeeper/lakekeeper/commit/cebc453b2fd5431a71d24dca95d4cfb5769d6728))
* Update to edition 2024 ([#1505](https://github.com/lakekeeper/lakekeeper/issues/1505)) ([203959a](https://github.com/lakekeeper/lakekeeper/commit/203959a46242ab38def9655a801ae9b09e6e3512))
* use rustls for gcloud storage ([#1506](https://github.com/lakekeeper/lakekeeper/issues/1506)) ([4233ca5](https://github.com/lakekeeper/lakekeeper/commit/4233ca5838842e5fbfe4deff353f73a9670746b4))
* Use structured error logs (requires tracing_unstable) ([#1504](https://github.com/lakekeeper/lakekeeper/issues/1504)) ([5c905df](https://github.com/lakekeeper/lakekeeper/commit/5c905df3d9679d9f990924064c7c7bee8f17f286))

## [0.10.3](https://github.com/lakekeeper/lakekeeper/compare/v0.10.2...v0.10.3) (2025-10-15)


### Features

* Add `debug_migrate_before_serve` env var ([#1426](https://github.com/lakekeeper/lakekeeper/issues/1426)) ([8022192](https://github.com/lakekeeper/lakekeeper/commit/8022192f1e0a8a9b71b8bbfaac340f19381cea3e))
* **examples:** add k8s iceberg sink connector (avro) ([#1336](https://github.com/lakekeeper/lakekeeper/issues/1336)) ([41ec6aa](https://github.com/lakekeeper/lakekeeper/commit/41ec6aab525e22edb1607e9f7899fc81b35c7f59))
* Expose License Information via API ([#1432](https://github.com/lakekeeper/lakekeeper/issues/1432)) ([612b2f9](https://github.com/lakekeeper/lakekeeper/commit/612b2f97f0f24e6afa6a501d7a1ab1589fb0a674))
* Move OpenFGA Authorizer to separate crate ([#1421](https://github.com/lakekeeper/lakekeeper/issues/1421)) ([7a9f235](https://github.com/lakekeeper/lakekeeper/commit/7a9f235158520ca25edd8bba26d340e99c521a2e))
* Validate Downscoping works on Warehouse creation ([#1437](https://github.com/lakekeeper/lakekeeper/issues/1437)) ([6e1d3f9](https://github.com/lakekeeper/lakekeeper/commit/6e1d3f97a2bbd14b93b6fea0f31875465c211719))


### Bug Fixes

* Better normalization for ARNs in S3 Profile ([#1441](https://github.com/lakekeeper/lakekeeper/issues/1441)) ([556b760](https://github.com/lakekeeper/lakekeeper/commit/556b7604399de3c7a656025fd5905d187a00214c))
* **ci:** Revert 0.10.3 release ([dfdbdcf](https://github.com/lakekeeper/lakekeeper/commit/dfdbdcf77923f36b1d2ea1a84cd494dad9f4bc9d))
* **deps:** update all non-major dependencies ([#1429](https://github.com/lakekeeper/lakekeeper/issues/1429)) ([e1865ab](https://github.com/lakekeeper/lakekeeper/commit/e1865abf65d6cfb1542bc2024faa9f694a00659e))


### Documentation

* Add info about installing sqlx to developer-guide.md ([#1438](https://github.com/lakekeeper/lakekeeper/issues/1438)) ([2b2f14d](https://github.com/lakekeeper/lakekeeper/commit/2b2f14d3cad3537f046675bb956f905eddd089fd))
* Update developer-guide.md ([#1439](https://github.com/lakekeeper/lakekeeper/issues/1439)) ([cfd6f56](https://github.com/lakekeeper/lakekeeper/commit/cfd6f56ee2e3d06e741d492cf3a74a1a3666e627))


### Miscellaneous Chores

* **ci:** Fix release please ([#1433](https://github.com/lakekeeper/lakekeeper/issues/1433)) ([7605a91](https://github.com/lakekeeper/lakekeeper/commit/7605a91d13ab75d485e2e4f9b84b26d257530609))
* **deps:** update all non-major dependencies ([#1436](https://github.com/lakekeeper/lakekeeper/issues/1436)) ([be514cf](https://github.com/lakekeeper/lakekeeper/commit/be514cf70791152d487e4486ce5f2c828d33124a))
* **deps:** update all non-major dependencies ([#1442](https://github.com/lakekeeper/lakekeeper/issues/1442)) ([af4a629](https://github.com/lakekeeper/lakekeeper/commit/af4a62980647a8a89be0b167babf5abb64c9a00e))
* **main:** release 0.10.3 ([#1427](https://github.com/lakekeeper/lakekeeper/issues/1427)) ([8722399](https://github.com/lakekeeper/lakekeeper/commit/8722399a0ffa4be9874e0f347a15557ad44b8443))
* Rename src/service -&gt; src/server, trait Catalog -&gt; CatalogStore ([#1428](https://github.com/lakekeeper/lakekeeper/issues/1428)) ([602191b](https://github.com/lakekeeper/lakekeeper/commit/602191b70507413df4415bcb1fce8f49195e5c5d))
* **renovate:** Group dependencies ([0475096](https://github.com/lakekeeper/lakekeeper/commit/047509686d2d71883f381f436ac54d8761770bbe))
* **renovate:** ignore additional deps ([c9a6149](https://github.com/lakekeeper/lakekeeper/commit/c9a61497325591aeff470b38da82ec4e60139aee))
* Restructure catalog_store into multiple modules ([#1434](https://github.com/lakekeeper/lakekeeper/issues/1434)) ([dde8c75](https://github.com/lakekeeper/lakekeeper/commit/dde8c7566183e088e97d7997c11408952dce8154))

## Changelog
