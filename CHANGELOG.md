# Changelog

## [0.11.0](https://github.com/lakekeeper/lakekeeper/compare/v0.10.3...v0.11.0) (2025-10-30)


### ⚠ BREAKING CHANGES

* Deprecate `id` in favor of `warehouse_id` in `GetWarehouseResponse`
* Require warehouse-id in the permissions/check API also for namespace-ids

### Features

* Add Warehouse Cache to reduce DB requests ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Allow x-user-agent header ([#1453](https://github.com/lakekeeper/lakekeeper/issues/1453)) ([727f5b5](https://github.com/lakekeeper/lakekeeper/commit/727f5b554005fdf51ccb3e611bc2539cdcfef483))
* AuthZ Server and Project Ops ([#1471](https://github.com/lakekeeper/lakekeeper/issues/1471)) ([9ef02d1](https://github.com/lakekeeper/lakekeeper/commit/9ef02d168c6c9937a34a60e2f91dbf6a19d97c54))
* Caching Short-Term-Credentials (STC) ([#1459](https://github.com/lakekeeper/lakekeeper/issues/1459)) ([c338372](https://github.com/lakekeeper/lakekeeper/commit/c3383720c5c3138c36d4f0391333ebc1fe4b5905))
* Catalog returns full Namespace Hierarchy for nested Namespaces ([#1472](https://github.com/lakekeeper/lakekeeper/issues/1472)) ([2fe38bf](https://github.com/lakekeeper/lakekeeper/commit/2fe38bf1cb6fc18da1efa3de29bf63625fff012c))
* Deprecate `id` in favor of `warehouse_id` in `GetWarehouseResponse` ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Introduce AuthzWarehouseOps and AuthzNamespaceOps abstractions ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Introduce CatalogWarehouseOps & CatalogNamespaceOps abstractions ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Management endpoints now return full warehouse details after updates ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* Require warehouse-id in the permissions/check API also for namespace-ids ([e2da40f](https://github.com/lakekeeper/lakekeeper/commit/e2da40f0251bd161c69a35effb3decc8f67b8aaa))
* Table & View Ops Abstractions, Improved Error Handling ([#1454](https://github.com/lakekeeper/lakekeeper/issues/1454)) ([94996e4](https://github.com/lakekeeper/lakekeeper/commit/94996e4b17e87d510f18ae6c7e2f84b807079504))
* Version based Warehouse Cache ([#1465](https://github.com/lakekeeper/lakekeeper/issues/1465)) ([c9c4b5e](https://github.com/lakekeeper/lakekeeper/commit/c9c4b5eec13d0c92167a372c57088b0cac501f91))


### Bug Fixes

* **ci:** Revert 0.10.3 release ([dfdbdcf](https://github.com/lakekeeper/lakekeeper/commit/dfdbdcf77923f36b1d2ea1a84cd494dad9f4bc9d))
* CORS allow access delegation & etag headers ([#1455](https://github.com/lakekeeper/lakekeeper/issues/1455)) ([5f8c665](https://github.com/lakekeeper/lakekeeper/commit/5f8c66598cd59061c682b70b3e81c9c888fec1ba))
* Debug assertion table identifier mismatch for signer ([#1460](https://github.com/lakekeeper/lakekeeper/issues/1460)) ([684c690](https://github.com/lakekeeper/lakekeeper/commit/684c690244ce63a609d12a46d98b2e85d5df0ee1))
* Headers should be lowercase ([#1457](https://github.com/lakekeeper/lakekeeper/issues/1457)) ([06ad77e](https://github.com/lakekeeper/lakekeeper/commit/06ad77eb74c6b48520dcd5fafbbd99191d0b67ad))


### Miscellaneous Chores

* Introduce open-api feature to gate utoipa and swagger ([#1458](https://github.com/lakekeeper/lakekeeper/issues/1458)) ([c39d96e](https://github.com/lakekeeper/lakekeeper/commit/c39d96e9c39ed86f8c4357afe525c2794e2748ae))
* **main:** release 0.10.3 ([#1446](https://github.com/lakekeeper/lakekeeper/issues/1446)) ([b8fcf54](https://github.com/lakekeeper/lakekeeper/commit/b8fcf54c627d48a547ef0baf6863949b68579388))
* Rename `SecretIdent` to `SecretId` for consistency ([443548a](https://github.com/lakekeeper/lakekeeper/commit/443548a518165c6d175a5f929531e4fe49e9ed5b))
* **tests:** Add trino information_schema.tables test ([#1456](https://github.com/lakekeeper/lakekeeper/issues/1456)) ([665d8c9](https://github.com/lakekeeper/lakekeeper/commit/665d8c9e3b75c7140374b95e47b1d35e684c9b84))

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
