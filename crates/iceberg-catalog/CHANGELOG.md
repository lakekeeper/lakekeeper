# Changelog

## [0.4.2](https://github.com/lakekeeper/lakekeeper/compare/v0.4.1...v0.4.2) (2024-10-28)


### Features

* enable native-tls-root-certs ([af26004](https://github.com/lakekeeper/lakekeeper/commit/af26004c77a5013ad53a4718a1cd2e97654090ad))
* improve azure latency by reusing http clients ([af26004](https://github.com/lakekeeper/lakekeeper/commit/af26004c77a5013ad53a4718a1cd2e97654090ad))


### Miscellaneous Chores

* release 0.4.2 ([1d8c469](https://github.com/lakekeeper/lakekeeper/commit/1d8c469cd30121e17455b2c2a13e9f0a46f7f630))

## [0.4.1](https://github.com/lakekeeper/lakekeeper/compare/v0.4.0...v0.4.1) (2024-10-15)


### Bug Fixes

* bug in join for listing view representations ([d2f1d7a](https://github.com/lakekeeper/lakekeeper/commit/d2f1d7aad9497f8bf4fc04d8347949bf25ffc16a))
* gcs integration test are now running in ci ([d2f1d7a](https://github.com/lakekeeper/lakekeeper/commit/d2f1d7aad9497f8bf4fc04d8347949bf25ffc16a))
* increase keycloak timeout in integration tests ([d2f1d7a](https://github.com/lakekeeper/lakekeeper/commit/d2f1d7aad9497f8bf4fc04d8347949bf25ffc16a))
* purge tests are now properly executed in ci ([d2f1d7a](https://github.com/lakekeeper/lakekeeper/commit/d2f1d7aad9497f8bf4fc04d8347949bf25ffc16a))

## [0.4.0](https://github.com/lakekeeper/lakekeeper/compare/v0.3.0...v0.4.0) (2024-10-03)


### ⚠ BREAKING CHANGES

* Rename TIP to Lakekeeper ([#372](https://github.com/lakekeeper/lakekeeper/issues/372))

### Features

* **cache:** cache metadata location in signer ([#334](https://github.com/lakekeeper/lakekeeper/issues/334)) ([fa0863c](https://github.com/lakekeeper/lakekeeper/commit/fa0863cdbf5df626eec083499d76add4dade4e0b))
* **catalog:** expiration queue configuration ([#330](https://github.com/lakekeeper/lakekeeper/issues/330)) ([fd96861](https://github.com/lakekeeper/lakekeeper/commit/fd96861f6179296a554bacab47144838a0d352ab))
* **catalog:** Soft-deletions & tabular cleanup queues ([#310](https://github.com/lakekeeper/lakekeeper/issues/310)) ([1de63b3](https://github.com/lakekeeper/lakekeeper/commit/1de63b3886820ea219006fcc2c696328b44dfb0f))
* list soft deletions ([#302](https://github.com/lakekeeper/lakekeeper/issues/302)) ([0a01eaf](https://github.com/lakekeeper/lakekeeper/commit/0a01eaf87f32e7f393f0d8f0d104594171dccfce))
* make sure table locations are unique ([#335](https://github.com/lakekeeper/lakekeeper/issues/335)) ([543db50](https://github.com/lakekeeper/lakekeeper/commit/543db50319f757cb40f01600d36da2836cf49fb3))
* New TableMetadataBuilder with: ID Reassignments, Metadata expiry, safe binding... ([#387](https://github.com/lakekeeper/lakekeeper/issues/387)) ([e5c1c77](https://github.com/lakekeeper/lakekeeper/commit/e5c1c77fced957cd6703e1ae6ec77e151414a63e))
* Rename TIP to Lakekeeper ([#372](https://github.com/lakekeeper/lakekeeper/issues/372)) ([57df07e](https://github.com/lakekeeper/lakekeeper/commit/57df07e69a14fb74aa486cd185ae700c3040fe90))
* **storage:** support for google cloud storage (gcs) ([#361](https://github.com/lakekeeper/lakekeeper/issues/361)) ([ebb4e27](https://github.com/lakekeeper/lakekeeper/commit/ebb4e27f729e20e30f87e5ce4c2d2351c2422ca6))
* **tabular:** soft-delete & drop purge ([#287](https://github.com/lakekeeper/lakekeeper/issues/287)) ([475db44](https://github.com/lakekeeper/lakekeeper/commit/475db4438f3bb7f1246fb846d04843d4afe3782a))


### Bug Fixes

* make conditional compilation of tests depend on var content ([#311](https://github.com/lakekeeper/lakekeeper/issues/311)) ([79036db](https://github.com/lakekeeper/lakekeeper/commit/79036dba4739cc3a65d2fe706278ac81f64bc5f2))
* replace pretty debug prints with properly formatted errors ([#327](https://github.com/lakekeeper/lakekeeper/issues/327)) ([efe9fe9](https://github.com/lakekeeper/lakekeeper/commit/efe9fe9bd1953d59dc5e48d6901b70fbe8e24895))

## [0.3.0](https://github.com/lakekeeper/lakekeeper/compare/v0.2.1...v0.3.0) (2024-08-26)


### ⚠ BREAKING CHANGES

* dots can no longer be used in namespace names ([#257](https://github.com/lakekeeper/lakekeeper/issues/257))

### Features

* Add support for custom Locations for Namespaces & Tables ([1d2ac6f](https://github.com/lakekeeper/lakekeeper/commit/1d2ac6f4b3910bf161c47d0224689b6e611d15ab))
* **aws:** sts credentials for s3 ([#236](https://github.com/lakekeeper/lakekeeper/issues/236)) ([dbf775b](https://github.com/lakekeeper/lakekeeper/commit/dbf775b6e226a8b8822f2e725ec317b4230aa0c4))
* **compression-codec:** Support setting and altering write.metadata.compression-codec ([#235](https://github.com/lakekeeper/lakekeeper/issues/235)) ([f4fb4cb](https://github.com/lakekeeper/lakekeeper/commit/f4fb4cbb4ce7f357db8d4d37dce8b92173402777))
* **storage:** add ability to narrow token permissions ([#249](https://github.com/lakekeeper/lakekeeper/issues/249)) ([ba9f046](https://github.com/lakekeeper/lakekeeper/commit/ba9f046cf48a380b7d0b6ce01a7f2045a9e47bea))
* **storage:** azdls ([#223](https://github.com/lakekeeper/lakekeeper/issues/223)) ([fd11428](https://github.com/lakekeeper/lakekeeper/commit/fd1142852555d239e8ea8dac2cb9d5db76457ab1))


### Bug Fixes

* dots can no longer be used in namespace names ([#257](https://github.com/lakekeeper/lakekeeper/issues/257)) ([8ac52e0](https://github.com/lakekeeper/lakekeeper/commit/8ac52e0e998c1417f3cb19655aebb4b39f054374))
* **kv2:** extend docs & fix mismatch between docs and expected env values ([#224](https://github.com/lakekeeper/lakekeeper/issues/224)) ([be3e3e6](https://github.com/lakekeeper/lakekeeper/commit/be3e3e60181acdb501303b7fb4215d79e65dd79e))

## [0.2.1](https://github.com/lakekeeper/lakekeeper/compare/v0.2.0...v0.2.1) (2024-07-29)


### Features

* **db:** Add Encryption Secret for postgres SecretStore to README & warn on startup ([#217](https://github.com/lakekeeper/lakekeeper/issues/217)) ([933409d](https://github.com/lakekeeper/lakekeeper/commit/933409da47aefb7b1fb9668386da35adab43477e))
* **secrets:** Secret Backend configuration is now case insensitive ([#215](https://github.com/lakekeeper/lakekeeper/issues/215)) ([99b19ab](https://github.com/lakekeeper/lakekeeper/commit/99b19ab3072fc4d9e2648a81cbca7b87b3b193b0))


### Bug Fixes

* **examples:** Fix `ICEBERG_REST__BASE_URI` ([33f213b](https://github.com/lakekeeper/lakekeeper/commit/33f213bf2592c958ac299a89ddae1a72e3446ed6))
* **s3signing:** Add S3 remote signing "content-md5" for pyiceberg compatability ([33f213b](https://github.com/lakekeeper/lakekeeper/commit/33f213bf2592c958ac299a89ddae1a72e3446ed6))


### Miscellaneous Chores

* release 0.2.1 ([587ea12](https://github.com/lakekeeper/lakekeeper/commit/587ea129780c21a3cd0fa8dd371b6901dede4c20))

## [0.2.0](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0...v0.2.0) (2024-07-26)


### ⚠ BREAKING CHANGES

* Catalog base URL should not contain /catalog suffix ([#208](https://github.com/lakekeeper/lakekeeper/issues/208))
* **views:** split off tabular from table to prepare for views

### Features

* **health:** Service health checks ([#181](https://github.com/lakekeeper/lakekeeper/issues/181)) ([3bf4d4c](https://github.com/lakekeeper/lakekeeper/commit/3bf4d4c99e09b3ae90ea1b4a9aba5136300df514))
* **pagination:** add pagination for namespaces & tables & views ([#186](https://github.com/lakekeeper/lakekeeper/issues/186)) ([37b1dbd](https://github.com/lakekeeper/lakekeeper/commit/37b1dbd3fdd16c79e9f981d29c3842d7d7140564))
* **prometheus:** add prometheus axum metrics ([#185](https://github.com/lakekeeper/lakekeeper/issues/185)) ([d60d84a](https://github.com/lakekeeper/lakekeeper/commit/d60d84aebf26052a72e26ff6350d9636d4865009))
* **secrets:** add support for kv2 secret storage ([#192](https://github.com/lakekeeper/lakekeeper/issues/192)) ([a86b13c](https://github.com/lakekeeper/lakekeeper/commit/a86b13c5020cd52073608c74dacc86eff7e1bb60))
* **server:** make listenport configurable ([#183](https://github.com/lakekeeper/lakekeeper/issues/183)) ([9ffe0c2](https://github.com/lakekeeper/lakekeeper/commit/9ffe0c2e2c78b178bcb3900ed4d6a246e4eaeacb))
* **views:** authz interface for views & view-ident resolve ([#141](https://github.com/lakekeeper/lakekeeper/issues/141)) ([c5e1f99](https://github.com/lakekeeper/lakekeeper/commit/c5e1f99eba7244bdca9c37a42c3fe36f47c117a0))
* **views:** commit views ([#146](https://github.com/lakekeeper/lakekeeper/issues/146)) ([0f6310b](https://github.com/lakekeeper/lakekeeper/commit/0f6310b2486cc608af6844c35be7a45ebeb998cd))
* **views:** create + load view ([#142](https://github.com/lakekeeper/lakekeeper/issues/142)) ([328cf33](https://github.com/lakekeeper/lakekeeper/commit/328cf33cf268cdbb7df2f185ed228291e509d6ab))
* **views:** exists ([#149](https://github.com/lakekeeper/lakekeeper/issues/149)) ([fdb5013](https://github.com/lakekeeper/lakekeeper/commit/fdb501326f72734a7faafc685402ef7d12e1189c))
* **views:** list-views ([5917a5e](https://github.com/lakekeeper/lakekeeper/commit/5917a5e853e1a3c03f47cbad9152b74f9b88e9fa))
* **views:** rename views ([#148](https://github.com/lakekeeper/lakekeeper/issues/148)) ([4aaaa7d](https://github.com/lakekeeper/lakekeeper/commit/4aaaa7d6f727388c43a8ecc6f307a261b74abbef))
* **views:** split off tabular from table to prepare for views ([f62b329](https://github.com/lakekeeper/lakekeeper/commit/f62b3292e5fd9951dd20c6a48432e16c337db7a5))


### Bug Fixes

* Catalog base URL should not contain /catalog suffix ([#208](https://github.com/lakekeeper/lakekeeper/issues/208)) ([6aabaa9](https://github.com/lakekeeper/lakekeeper/commit/6aabaa97b1f8531830dd512c9a61c461c3f05b7f))
* **db:** add wait-for-db command ([#196](https://github.com/lakekeeper/lakekeeper/issues/196)) ([c1cd069](https://github.com/lakekeeper/lakekeeper/commit/c1cd069d773906a4c647dcc007c50b0aa6929c29))
* remove unused cfg-attributes ([#203](https://github.com/lakekeeper/lakekeeper/issues/203)) ([b6d17c4](https://github.com/lakekeeper/lakekeeper/commit/b6d17c4bbdef073962fd220faf4a632f4a64e541))
* **tables:** deny "write.metadata" & "write.data.path" table properties  ([#197](https://github.com/lakekeeper/lakekeeper/issues/197)) ([4b2191e](https://github.com/lakekeeper/lakekeeper/commit/4b2191e58439ce99a5420f411a121a2ba89a0698))

## [0.1.0](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc3...v0.1.0) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0 ([a5def9a](https://github.com/lakekeeper/lakekeeper/commit/a5def9a527aa615779b60fe8fc5a18aaa47f33ee))

## [0.1.0-rc3](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc2...v0.1.0-rc3) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc3 ([9b0d219](https://github.com/lakekeeper/lakekeeper/commit/9b0d219e865dce85803fc93da7233e92d3e8b4b8))

## [0.1.0-rc2](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc1...v0.1.0-rc2) (2024-06-17)


### Bug Fixes

* add view router ([#116](https://github.com/lakekeeper/lakekeeper/issues/116)) ([0745cc8](https://github.com/lakekeeper/lakekeeper/commit/0745cc85e16974c05adc3b158f5cb04c9dd54ac4))


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc2 ([9bc25ef](https://github.com/lakekeeper/lakekeeper/commit/9bc25ef2b44d6c29556a5d0913c076904b1cb010))

## [0.1.0-rc1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.2-rc1...v0.1.0-rc1) (2024-06-16)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc1 ([ba6e5d5](https://github.com/lakekeeper/lakekeeper/commit/ba6e5d5c8a59cb1da5b61dd559c783998559debf))

## [0.0.2-rc1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.1...v0.0.2-rc1) (2024-06-16)


### Miscellaneous Chores

* 🚀 Release 0.0.2-rc1 ([eb34b9c](https://github.com/lakekeeper/lakekeeper/commit/eb34b9cd613bb2d72d4a9b33b103d36c7649bd57))

## [0.0.1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.0...v0.0.1) (2024-06-15)


### Miscellaneous Chores

* 🚀 Release 0.0.1 ([c52ddec](https://github.com/lakekeeper/lakekeeper/commit/c52ddec7520ec16ed0b6f70c5e3108a7d8a35665))
