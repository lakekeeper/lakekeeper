# Changelog

## [0.7.0](https://github.com/lakekeeper/lakekeeper/compare/v0.6.2...v0.7.0) (2025-02-24)


### Features

* Add Opt-In to S3 Variant prefixes (s3a, s3n) ([#821](https://github.com/lakekeeper/lakekeeper/issues/821)) ([b85b724](https://github.com/lakekeeper/lakekeeper/commit/b85b7245376cedb18d5131e24cff671d18045dff))
* collect warehouse statistics ([#811](https://github.com/lakekeeper/lakekeeper/issues/811)) ([063066c](https://github.com/lakekeeper/lakekeeper/commit/063066ca9561e3ab01b7599c79a5ae76828f8ef1))
* emit CloudEvent on undropTabulars [#572](https://github.com/lakekeeper/lakekeeper/issues/572) ([#803](https://github.com/lakekeeper/lakekeeper/issues/803)) ([9dd431a](https://github.com/lakekeeper/lakekeeper/commit/9dd431ab9713e425e848337adaeb464ddda87321))
* Migrate Authentication to Limes, Support Unlimited Authenticators, Customizable Authentication ([b72852d](https://github.com/lakekeeper/lakekeeper/commit/b72852de125a23691f75a291ecdf3c452b8e1c14))
* **tasks:** add unit to poll interval config ([#829](https://github.com/lakekeeper/lakekeeper/issues/829)) ([c0edafa](https://github.com/lakekeeper/lakekeeper/commit/c0edafa403f28cf578a7db81fd6ada36774cd749))
* use x-forwarded-for/host headers to generate links ([#834](https://github.com/lakekeeper/lakekeeper/issues/834)) ([89c0f8a](https://github.com/lakekeeper/lakekeeper/commit/89c0f8adacc2dc26b30d400563aeed716b5bdf62))


### Bug Fixes

* **deps:** update rust crate rand to 0.9.0 ([#785](https://github.com/lakekeeper/lakekeeper/issues/785)) ([b9952de](https://github.com/lakekeeper/lakekeeper/commit/b9952decd4ecbf959452170b89a5127e4d57cfb9))
* HEAD Namespace missing in supported endpoints ([#847](https://github.com/lakekeeper/lakekeeper/issues/847)) ([f3e43fe](https://github.com/lakekeeper/lakekeeper/commit/f3e43fe206f32402316daf546130ef197a52ca03))
* parsing of pg sslmode should be case-insensitive ([#802](https://github.com/lakekeeper/lakekeeper/issues/802)) ([1e3d001](https://github.com/lakekeeper/lakekeeper/commit/1e3d00177f952d0431ddcdb516d4f8e1e1413149))
* **s3:** set path style access in s3 file_io ([#796](https://github.com/lakekeeper/lakekeeper/issues/796)) ([33e690f](https://github.com/lakekeeper/lakekeeper/commit/33e690f77737ce3f7def54d4913164161aa60505))


### Miscellaneous Chores

* release 0.7.0 ([491940b](https://github.com/lakekeeper/lakekeeper/commit/491940b864dbf564f711adfa58be03f45f06f9e3))

## [0.6.2](https://github.com/lakekeeper/lakekeeper/compare/v0.6.1...v0.6.2) (2025-01-30)


### Features

* Scope validation ([#790](https://github.com/lakekeeper/lakekeeper/issues/790)) ([65e664f](https://github.com/lakekeeper/lakekeeper/commit/65e664fb804fc6657dcfab655344d9bfd0224b5a))


### Miscellaneous Chores

* release 0.6.2 ([0c7e181](https://github.com/lakekeeper/lakekeeper/commit/0c7e1814eef9c039f6d05dbb3256c70caed1c36f))

## [0.6.1](https://github.com/lakekeeper/lakekeeper/compare/v0.6.0...v0.6.1) (2025-01-27)


### Features

* expose cloud-events tracing publisher on cli ([#747](https://github.com/lakekeeper/lakekeeper/issues/747)) ([798e85d](https://github.com/lakekeeper/lakekeeper/commit/798e85d39c034b2351443d7b1f9983160b820ed7))
* Register table endpoint ([#775](https://github.com/lakekeeper/lakekeeper/issues/775)) ([4b88f73](https://github.com/lakekeeper/lakekeeper/commit/4b88f730f3d54e7d905f66a0b32ca93f88e67f69))


### Bug Fixes

* clippy & pin rust version ([#758](https://github.com/lakekeeper/lakekeeper/issues/758)) ([0899d4c](https://github.com/lakekeeper/lakekeeper/commit/0899d4cf91a1a056337ee00bcba0827b2e0a9792))
* Table locations with same prefix ([#780](https://github.com/lakekeeper/lakekeeper/issues/780)) ([39eb3d2](https://github.com/lakekeeper/lakekeeper/commit/39eb3d2a169ff41c9b644a435fa53dadd449925a))
* **test:** use 0.3.0 for kube-auth test & fix pyiceberg aws tests ([#767](https://github.com/lakekeeper/lakekeeper/issues/767)) ([e6b7b9c](https://github.com/lakekeeper/lakekeeper/commit/e6b7b9ca23d3054c5b95aa09f2bb590ad188d2e2))


### Performance Improvements

* optimize load_table by refine SQL ([#784](https://github.com/lakekeeper/lakekeeper/issues/784)) ([2b87915](https://github.com/lakekeeper/lakekeeper/commit/2b87915b7436ed8ec752046a687140fb18fbdf3e))


### Miscellaneous Chores

* release 0.6.1 ([a17f5c4](https://github.com/lakekeeper/lakekeeper/commit/a17f5c4919bbe5797099dcbf45cf8a6becf0b3c1))

## [0.6.0](https://github.com/lakekeeper/lakekeeper/compare/v0.5.2...v0.6.0) (2025-01-07)


### Features

* Check Endpoint for single permissions ([#706](https://github.com/lakekeeper/lakekeeper/issues/706)) ([6a149a6](https://github.com/lakekeeper/lakekeeper/commit/6a149a64a20728464e1a385a1a788ad9a49bfbe6))
* Lakekeeper Open Policy Agent Bridge with trino support ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))
* **tests:** run integration tests with iceberg versions: 1.5.2, 1.6.1, 1.7.1 ([3f3b5ad](https://github.com/lakekeeper/lakekeeper/commit/3f3b5ad671cae04725474dee9cfd3f2ce7e0cae7))
* Update Lakekeeper UI to 0.4.0 ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))


### Bug Fixes

* credentials configs are never empty but are either null or an empty list ([3f3b5ad](https://github.com/lakekeeper/lakekeeper/commit/3f3b5ad671cae04725474dee9cfd3f2ce7e0cae7))
* Default to purge drop for managed tables ([#712](https://github.com/lakekeeper/lakekeeper/issues/712)) ([676d995](https://github.com/lakekeeper/lakekeeper/commit/676d995c559a6ee6b08a6e030cd9eeac60931b93))
* Enable openfga integration tests ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))
* files of deleted tables not deleted for ADLS ([#715](https://github.com/lakekeeper/lakekeeper/issues/715)) ([d81677f](https://github.com/lakekeeper/lakekeeper/commit/d81677f32df5aadedd83f439afa845c454f6af9a))
* return proper error codes for invalid writes and reads of permission tuples ([#727](https://github.com/lakekeeper/lakekeeper/issues/727)) ([96c2d5e](https://github.com/lakekeeper/lakekeeper/commit/96c2d5e0f9a5971610ced65138c5405c1a9702ad))
* use correct list of supported endpoints ([3f3b5ad](https://github.com/lakekeeper/lakekeeper/commit/3f3b5ad671cae04725474dee9cfd3f2ce7e0cae7))

## [0.5.2](https://github.com/lakekeeper/lakekeeper/compare/v0.5.1...v0.5.2) (2024-12-17)


### Features

* Support for Statistic Files ([1e4fa38](https://github.com/lakekeeper/lakekeeper/commit/1e4fa3876919bf5b926ea73f370ff62efd9081b9))
* **tables:** load table credentials ([#675](https://github.com/lakekeeper/lakekeeper/issues/675)) ([9fd272e](https://github.com/lakekeeper/lakekeeper/commit/9fd272ee831b3347c3d4e790fcaacba6f4ded273))


### Bug Fixes

* Make BASE_URI trailing slash insensitive ([#681](https://github.com/lakekeeper/lakekeeper/issues/681)) ([4799ea7](https://github.com/lakekeeper/lakekeeper/commit/4799ea78e60e5c908547f0cf1421384da919bfe3))
* Snapshots without schema ([1e4fa38](https://github.com/lakekeeper/lakekeeper/commit/1e4fa3876919bf5b926ea73f370ff62efd9081b9))


### Miscellaneous Chores

* release 0.5.2 ([c5774b2](https://github.com/lakekeeper/lakekeeper/commit/c5774b26183dc41e938af130005e8df9230b3b82))

## [0.5.1](https://github.com/lakekeeper/lakekeeper/compare/v0.5.0...v0.5.1) (2024-12-12)


### Features

* **openapi:** document error models in openapi ([#658](https://github.com/lakekeeper/lakekeeper/issues/658)) ([2a67196](https://github.com/lakekeeper/lakekeeper/commit/2a67196a9f9844db0f846cb2e9016c4d4620b0b5))
* undrop  ([#517](https://github.com/lakekeeper/lakekeeper/issues/517)) ([658e757](https://github.com/lakekeeper/lakekeeper/commit/658e757663cd35a87ecccb2173e9e87247239c8e))


### Bug Fixes

* allow mixed-case properties ([#660](https://github.com/lakekeeper/lakekeeper/issues/660)) ([f435573](https://github.com/lakekeeper/lakekeeper/commit/f4355732583d396822ed7bbb55cc81537153b29e))
* potential deadlock for views through uncommitted transactions ([#638](https://github.com/lakekeeper/lakekeeper/issues/638)) ([0dda8e3](https://github.com/lakekeeper/lakekeeper/commit/0dda8e3d9bc7cdb1a36c7fc23d57c0e7e7e1389a))
* potential deadlock in load-table ([#636](https://github.com/lakekeeper/lakekeeper/issues/636)) ([c22b0e0](https://github.com/lakekeeper/lakekeeper/commit/c22b0e00ae76ad3458c91ed860c8410446f52173))
* remove unused relation from openfga schema ([#659](https://github.com/lakekeeper/lakekeeper/issues/659)) ([764ca5b](https://github.com/lakekeeper/lakekeeper/commit/764ca5b2667de591a4c6bc2ba798c41eb6b5c59e))
* tokens of humans are wrongly identified as applications if "appid" claim is present ([#647](https://github.com/lakekeeper/lakekeeper/issues/647)) ([bc6b475](https://github.com/lakekeeper/lakekeeper/commit/bc6b475075354a66ae80b771ac7de3287b467841))


### Miscellaneous Chores

* release 0.5.1 ([f8aa87c](https://github.com/lakekeeper/lakekeeper/commit/f8aa87ca8b7a8074389cd43a39007b2652a46494))

## [0.5.0](https://github.com/lakekeeper/lakekeeper/compare/v0.4.3...v0.5.0) (2024-12-06)


### ⚠ BREAKING CHANGES

* Rename S3 minio flavor to s3-compat ([#630](https://github.com/lakekeeper/lakekeeper/issues/630))
* Change default port from 8080 to 8181
* Default to single-tenant / single-project with NIL Project-ID

### Features

* Add iceberg openapi to swagger ([#431](https://github.com/lakekeeper/lakekeeper/issues/431)) ([bb3d12f](https://github.com/lakekeeper/lakekeeper/commit/bb3d12f2075e704eaab58b5a8292422fee3784fb))
* Add Iceberg REST Spec to swagger ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* add kafka support [#271](https://github.com/lakekeeper/lakekeeper/issues/271) ([#340](https://github.com/lakekeeper/lakekeeper/issues/340)) ([7973586](https://github.com/lakekeeper/lakekeeper/commit/7973586282b0f09074f00bf455ea9fc1a9fe1cf3))
* Add namespace_id filter to list deleted tabulars ([#443](https://github.com/lakekeeper/lakekeeper/issues/443)) ([cc82736](https://github.com/lakekeeper/lakekeeper/commit/cc82736e9507f69e33f68c54d21bd4638126ef72))
* Add operator role ([#543](https://github.com/lakekeeper/lakekeeper/issues/543)) ([bcddb60](https://github.com/lakekeeper/lakekeeper/commit/bcddb60e458872f4ae88d0cd8f2ea1cc2d146445))
* Allow configuration of additional Issuer URLs ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Allow configuration of multiple Audiences ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Change default port from 8080 to 8181 ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Create default Project on Bootstrap ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Default to hard deletion ([#507](https://github.com/lakekeeper/lakekeeper/issues/507)) ([5d794aa](https://github.com/lakekeeper/lakekeeper/commit/5d794aa8c620157c8e437a9683f5c508a7659d86))
* Default to single-tenant / single-project with NIL Project-ID ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* docs ([#605](https://github.com/lakekeeper/lakekeeper/issues/605)) ([c1d2348](https://github.com/lakekeeper/lakekeeper/commit/c1d23488c40a43af7f303ed78cadef76d9ccc06b))
* Embedded UI ([#622](https://github.com/lakekeeper/lakekeeper/issues/622)) ([332f3b8](https://github.com/lakekeeper/lakekeeper/commit/332f3b87db9ffbc6fdaf1d48855b8e3cdcf3c017))
* Enable K8s Auth explicitly ([#594](https://github.com/lakekeeper/lakekeeper/issues/594)) ([3773141](https://github.com/lakekeeper/lakekeeper/commit/3773141690cb2225571f4708509df90103bd3226))
* Extend user search to email field ([#477](https://github.com/lakekeeper/lakekeeper/issues/477)) ([9f9f42b](https://github.com/lakekeeper/lakekeeper/commit/9f9f42b5ac3dfb6b978f4db20a698cbc7163dec7))
* Fine Grained Access Controls with OpenFGA ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Generated TS Client ([#453](https://github.com/lakekeeper/lakekeeper/issues/453)) ([24bfccf](https://github.com/lakekeeper/lakekeeper/commit/24bfccf80fb1166f5a52f72ac0e8d2effb5b0bc0))
* Hierarchical Namespaces ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* improve latency against aws by reusing http clients ([#540](https://github.com/lakekeeper/lakekeeper/issues/540)) ([8c384f7](https://github.com/lakekeeper/lakekeeper/commit/8c384f7aabc6f1ae0445f0c6c7fe4a4bab4ad147))
* OIDC Audience validation ([#607](https://github.com/lakekeeper/lakekeeper/issues/607)) ([052bb3f](https://github.com/lakekeeper/lakekeeper/commit/052bb3f539e9247fb5d4312447b9ab1823f20d8b))
* Optionally return uuids for Iceberg APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* pagination without empty pages ([#450](https://github.com/lakekeeper/lakekeeper/issues/450)) ([c88a59d](https://github.com/lakekeeper/lakekeeper/commit/c88a59db4e99a01118a281a51c97aa21f69cdb0f))
* Project Management APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Provide inherited managed access via API ([#619](https://github.com/lakekeeper/lakekeeper/issues/619)) ([e7b0394](https://github.com/lakekeeper/lakekeeper/commit/e7b039488e605bf64e7dca2df137565eda7ed8c2))
* Rename S3 minio flavor to s3-compat ([#630](https://github.com/lakekeeper/lakekeeper/issues/630)) ([acb7419](https://github.com/lakekeeper/lakekeeper/commit/acb7419f08301fa3e1d84a96dc3da0014ee0bb65))
* Server Info Endpoint ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* split table metadata into tables ([#478](https://github.com/lakekeeper/lakekeeper/issues/478)) ([942fa97](https://github.com/lakekeeper/lakekeeper/commit/942fa97c98049d15a50168ce7d7a9e711d9de3d1))
* support kubernetes service-accounts ([#538](https://github.com/lakekeeper/lakekeeper/issues/538)) ([2982210](https://github.com/lakekeeper/lakekeeper/commit/298221063ad61b0301c019dee2094aae64dd5447))


### Bug Fixes

* aws s3 signer ([#493](https://github.com/lakekeeper/lakekeeper/issues/493)) ([b7ad8f4](https://github.com/lakekeeper/lakekeeper/commit/b7ad8f44eaba31c6ce181ba0844d2f0f6b8b1e76))
* **aws:** deal with closed connections via retries ([#569](https://github.com/lakekeeper/lakekeeper/issues/569)) ([bbda2c4](https://github.com/lakekeeper/lakekeeper/commit/bbda2c4dc2eb4d3d93443c86b48de6dfa606ad98))
* azure connection reset ([#553](https://github.com/lakekeeper/lakekeeper/issues/553)) ([5d4b041](https://github.com/lakekeeper/lakekeeper/commit/5d4b0413f79b9ba217bbe079555a0447f982eaa5))
* Bootstrap should return HTTP Code 204 ([#597](https://github.com/lakekeeper/lakekeeper/issues/597)) ([25d1d4e](https://github.com/lakekeeper/lakekeeper/commit/25d1d4e64902113173ecab7b86ebe82cefe0b580))
* Delete Namespaces with children should not be possible ([#482](https://github.com/lakekeeper/lakekeeper/issues/482)) ([7ffd864](https://github.com/lakekeeper/lakekeeper/commit/7ffd8648fd058be72126e76abfedb9d860e7c69d))
* flaky aws tests ([#545](https://github.com/lakekeeper/lakekeeper/issues/545)) ([f4d46b2](https://github.com/lakekeeper/lakekeeper/commit/f4d46b27770f2d62a41fab283d38098ef009d848))
* Include Deletion Profile in GetWarehouseResponse ([#514](https://github.com/lakekeeper/lakekeeper/issues/514)) ([54a6420](https://github.com/lakekeeper/lakekeeper/commit/54a6420580b24e3b7f329b00106b3a98e50da062))
* List Namespaces - Top level NS list should only contain top level Namespaces ([#512](https://github.com/lakekeeper/lakekeeper/issues/512)) ([795d4f0](https://github.com/lakekeeper/lakekeeper/commit/795d4f0be4f4432ce7e215b6885db92578ede748))
* list-projects for non admins ([#546](https://github.com/lakekeeper/lakekeeper/issues/546)) ([d0066b8](https://github.com/lakekeeper/lakekeeper/commit/d0066b86e0df2f6d5cef75a591906ad6e4d88e23))
* **management:** deleted tabulars endpoint should not contain underscore ([#556](https://github.com/lakekeeper/lakekeeper/issues/556)) ([b15a8fe](https://github.com/lakekeeper/lakekeeper/commit/b15a8fe9fd882c02e2c12691df6de849b80ff8cc))
* only log table load failed when it actually happened ([#626](https://github.com/lakekeeper/lakekeeper/issues/626)) ([be5f58c](https://github.com/lakekeeper/lakekeeper/commit/be5f58ca5e2f72a4352629304bff641d77f9f85a))
* **openapi:** Fix Soft-Deletion expiration seconds type ([#509](https://github.com/lakekeeper/lakekeeper/issues/509)) ([322a1a0](https://github.com/lakekeeper/lakekeeper/commit/322a1a01304f639211f168fa858141d0eb33e3c3))
* pagination ([#604](https://github.com/lakekeeper/lakekeeper/issues/604)) ([0be19ed](https://github.com/lakekeeper/lakekeeper/commit/0be19ed14e5eb1b4fb66db19bc242c65d989a5f3))
* permissions API Parameters ([#516](https://github.com/lakekeeper/lakekeeper/issues/516)) ([5133752](https://github.com/lakekeeper/lakekeeper/commit/51337524871622bdf4f04c26b7bdf993c152ba45))
* prepend a version count to metadata files ([#524](https://github.com/lakekeeper/lakekeeper/issues/524)) ([0d9d06f](https://github.com/lakekeeper/lakekeeper/commit/0d9d06f784f03c2bdb95312873643f00b8b8253a))
* recreate user ([#599](https://github.com/lakekeeper/lakekeeper/issues/599)) ([1194cb0](https://github.com/lakekeeper/lakekeeper/commit/1194cb05a1b2910f6535ac983a0ce53a69bebd85))
* run metrics router ([#628](https://github.com/lakekeeper/lakekeeper/issues/628)) ([f6b47e5](https://github.com/lakekeeper/lakekeeper/commit/f6b47e5d9c6ae6d884dcea2dcf4f2a6b9b3baefd))
* set pool idle timeout to &lt;20 not keepalive timeout ([#551](https://github.com/lakekeeper/lakekeeper/issues/551)) ([2ae5b8d](https://github.com/lakekeeper/lakekeeper/commit/2ae5b8de9d71d921dc46d53b337aa9f2d9388e17))
* **tests:** give openfga a bit of time to delete things ([#557](https://github.com/lakekeeper/lakekeeper/issues/557)) ([71daf6f](https://github.com/lakekeeper/lakekeeper/commit/71daf6ffc9a34c9e25b3b8df9c5343ecaa5d3ea7))
* **tests:** use a shared runtime for tests that share a static reqwest client ([#555](https://github.com/lakekeeper/lakekeeper/issues/555)) ([90c6880](https://github.com/lakekeeper/lakekeeper/commit/90c6880ae997bbaededb3c382e74e199088c5654))
* Warehouse managed-access in openapi spec ([#610](https://github.com/lakekeeper/lakekeeper/issues/610)) ([c860506](https://github.com/lakekeeper/lakekeeper/commit/c860506c387fa20860fd2dedd6d2eb91c48c690c))
* WarehouseAdmin renamed to DataAdmin ([#515](https://github.com/lakekeeper/lakekeeper/issues/515)) ([7ec4c01](https://github.com/lakekeeper/lakekeeper/commit/7ec4c01508ee108da30b3fd070e39d309df001ec))


### Miscellaneous Chores

* release 0.5.0 ([b1b2ee6](https://github.com/lakekeeper/lakekeeper/commit/b1b2ee6d0f068adf9a60719c1cfb88201825d389))

## [0.4.3](https://github.com/lakekeeper/lakekeeper/compare/v0.4.2...v0.4.3) (2024-11-13)


### Bug Fixes

* aws s3 signer ([#493](https://github.com/lakekeeper/lakekeeper/issues/493)) ([dea4a57](https://github.com/lakekeeper/lakekeeper/commit/dea4a5774f34e92aa510df5a7e628c8e410a7085))


### Miscellaneous Chores

* release 0.4.3 ([e577ab2](https://github.com/lakekeeper/lakekeeper/commit/e577ab2e4da78d612e87bd4844307c28098e2c31))

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
* **storage:** adls ([#223](https://github.com/lakekeeper/lakekeeper/issues/223)) ([fd11428](https://github.com/lakekeeper/lakekeeper/commit/fd1142852555d239e8ea8dac2cb9d5db76457ab1))


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
