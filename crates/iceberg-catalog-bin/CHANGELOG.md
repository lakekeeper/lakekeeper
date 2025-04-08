# Changelog



## [0.8.0](https://github.com/lakekeeper/lakekeeper/compare/v0.7.4...v0.8.0) (2025-04-08)


### Features

* add kafka support [#271](https://github.com/lakekeeper/lakekeeper/issues/271) ([#937](https://github.com/lakekeeper/lakekeeper/issues/937)) ([1fa2f09](https://github.com/lakekeeper/lakekeeper/commit/1fa2f09ce00f906515eeec98338394c2f2bc7b52))
* Add support for legacy Kubernetes tokens (no audience) ([#940](https://github.com/lakekeeper/lakekeeper/issues/940)) ([e7daf01](https://github.com/lakekeeper/lakekeeper/commit/e7daf010a74b53ac2c6fee0833e8ef1308c1d02a))
* **s3:** make url-style detection configurable ([#905](https://github.com/lakekeeper/lakekeeper/issues/905)) ([69234fe](https://github.com/lakekeeper/lakekeeper/commit/69234fe4e340485af9bd52be13adb64de7dbd9f8))
* **stats:** Endpoint call statistics ([#818](https://github.com/lakekeeper/lakekeeper/issues/818)) ([bf7bcde](https://github.com/lakekeeper/lakekeeper/commit/bf7bcde7e8b475e92a03dc92f53a2a9d1190293a))
* Update UI to Version 0.6.0, Add Stats, Fix Favicon ([#980](https://github.com/lakekeeper/lakekeeper/issues/980)) ([3746ef3](https://github.com/lakekeeper/lakekeeper/commit/3746ef38a72249d3452b7cd38aecbc80491f7b44))


### Bug Fixes

* Consistency checks for READ Committed transaction level ([#975](https://github.com/lakekeeper/lakekeeper/issues/975)) ([6b852dd](https://github.com/lakekeeper/lakekeeper/commit/6b852dddd32a0e6165ef9c8eed308169a3477587))


### Miscellaneous Chores

* release 0.8.0 ([f1b8083](https://github.com/lakekeeper/lakekeeper/commit/f1b80837ed6e296c48f2dd4491ded384c3658546))

## [0.7.4](https://github.com/lakekeeper/lakekeeper/compare/v0.7.3...v0.7.4) (2025-03-20)


### Features

* configurable bind ip address ([#922](https://github.com/lakekeeper/lakekeeper/issues/922)) ([e545a26](https://github.com/lakekeeper/lakekeeper/commit/e545a26503af1458ded9c3875563340da91c165b))
* **ui:** ui Azure Shared Key Authentication([#918](https://github.com/lakekeeper/lakekeeper/issues/918)) ([71e7856](https://github.com/lakekeeper/lakekeeper/commit/71e7856cc97553a9dd98c77d8d9173949cc5dbcb))


### Miscellaneous Chores

* release 0.7.4 ([e51010a](https://github.com/lakekeeper/lakekeeper/commit/e51010a8d3ceefdad118d3424b81259ab02188b5))

## [0.7.3](https://github.com/lakekeeper/lakekeeper/compare/v0.7.2...v0.7.3) (2025-03-04)


### Miscellaneous Chores

* release 0.7.3 ([630ee3a](https://github.com/lakekeeper/lakekeeper/commit/630ee3adc67533e499e8d656fdb1ba49900b87b3))

## [0.7.2](https://github.com/lakekeeper/lakekeeper/compare/v0.7.1...v0.7.2) (2025-02-27)


### Bug Fixes

* Authenticator order (OIDC before K8s), add K8s Authenticator Audiences ([#864](https://github.com/lakekeeper/lakekeeper/issues/864)) ([b894ba5](https://github.com/lakekeeper/lakekeeper/commit/b894ba564b7629bf5ec42e7e7e961cd833bd6de2))
* **ui:** Update UI to 0.5.0 ([fc81b7b](https://github.com/lakekeeper/lakekeeper/commit/fc81b7bd967dc47f9b30fb511bfa0eee7a10ed43))


### Miscellaneous Chores

* release 0.7.2 ([b4c77ac](https://github.com/lakekeeper/lakekeeper/commit/b4c77ac57ed1b463f4a89a10e380da7b9d7960f3))

## [0.7.1](https://github.com/lakekeeper/lakekeeper/compare/v0.7.0...v0.7.1) (2025-02-26)


### Bug Fixes

* improve authn logging ([#854](https://github.com/lakekeeper/lakekeeper/issues/854)) ([2f2b3a2](https://github.com/lakekeeper/lakekeeper/commit/2f2b3a26292665260f59de745137f78ba9389b30))


### Miscellaneous Chores

* release 0.7.1 ([05938cd](https://github.com/lakekeeper/lakekeeper/commit/05938cd499e15ea648c58de05ef1b866d6395036))

## [0.7.0](https://github.com/lakekeeper/lakekeeper/compare/v0.6.2...v0.7.0) (2025-02-24)


### Features

* Add Opt-In to S3 Variant prefixes (s3a, s3n) ([#821](https://github.com/lakekeeper/lakekeeper/issues/821)) ([b85b724](https://github.com/lakekeeper/lakekeeper/commit/b85b7245376cedb18d5131e24cff671d18045dff))
* Migrate Authentication to Limes, Support Unlimited Authenticators, Customizable Authentication ([b72852d](https://github.com/lakekeeper/lakekeeper/commit/b72852de125a23691f75a291ecdf3c452b8e1c14))
* Update Storage Profile alternative S3 protocols in UI ([#848](https://github.com/lakekeeper/lakekeeper/issues/848)) ([55a5334](https://github.com/lakekeeper/lakekeeper/commit/55a5334c35cab649a72cb218aa781d2b379614be))
* use x-forwarded-for/host headers to generate links ([#834](https://github.com/lakekeeper/lakekeeper/issues/834)) ([89c0f8a](https://github.com/lakekeeper/lakekeeper/commit/89c0f8adacc2dc26b30d400563aeed716b5bdf62))


### Miscellaneous Chores

* release 0.7.0 ([491940b](https://github.com/lakekeeper/lakekeeper/commit/491940b864dbf564f711adfa58be03f45f06f9e3))

## [0.6.2](https://github.com/lakekeeper/lakekeeper/compare/v0.6.1...v0.6.2) (2025-01-30)


### Features

* Scope validation ([#790](https://github.com/lakekeeper/lakekeeper/issues/790)) ([65e664f](https://github.com/lakekeeper/lakekeeper/commit/65e664fb804fc6657dcfab655344d9bfd0224b5a))


### Bug Fixes

* Add S3 path style access to UI ([#787](https://github.com/lakekeeper/lakekeeper/issues/787)) ([f393881](https://github.com/lakekeeper/lakekeeper/commit/f393881acf60b8bd3910c17eca6f7325aedbf8a7))


### Miscellaneous Chores

* release 0.6.2 ([0c7e181](https://github.com/lakekeeper/lakekeeper/commit/0c7e1814eef9c039f6d05dbb3256c70caed1c36f))

## [0.6.2](https://github.com/lakekeeper/lakekeeper/compare/v0.6.1...v0.6.2) (2025-01-29)


### Bug Fixes

* Add S3 path style access to UI ([#787](https://github.com/lakekeeper/lakekeeper/issues/787)) ([f393881](https://github.com/lakekeeper/lakekeeper/commit/f393881acf60b8bd3910c17eca6f7325aedbf8a7))

## [0.6.1](https://github.com/lakekeeper/lakekeeper/compare/v0.6.0...v0.6.1) (2025-01-27)


### Features

* expose cloud-events tracing publisher on cli ([#747](https://github.com/lakekeeper/lakekeeper/issues/747)) ([798e85d](https://github.com/lakekeeper/lakekeeper/commit/798e85d39c034b2351443d7b1f9983160b820ed7))


### Miscellaneous Chores

* release 0.6.1 ([a17f5c4](https://github.com/lakekeeper/lakekeeper/commit/a17f5c4919bbe5797099dcbf45cf8a6becf0b3c1))

## [0.6.0](https://github.com/lakekeeper/lakekeeper/compare/v0.5.2...v0.6.0) (2025-01-07)


### Features

* Lakekeeper Open Policy Agent Bridge with trino support ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))
* Update Lakekeeper UI to 0.4.0 ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))


### Bug Fixes

* Enable openfga integration tests ([3735742](https://github.com/lakekeeper/lakekeeper/commit/3735742e5c8efb05894f02208afdc2b03e321093))

## [0.5.2](https://github.com/lakekeeper/lakekeeper/compare/v0.5.1...v0.5.2) (2024-12-17)


### Features

* Update UI to 0.3.0 ([#684](https://github.com/lakekeeper/lakekeeper/issues/684)) ([2f9da51](https://github.com/lakekeeper/lakekeeper/commit/2f9da5148f80293eefda25ac9bc53cac30d92b78))


### Miscellaneous Chores

* release 0.5.2 ([c5774b2](https://github.com/lakekeeper/lakekeeper/commit/c5774b26183dc41e938af130005e8df9230b3b82))

## [0.5.1](https://github.com/lakekeeper/lakekeeper/compare/v0.5.0...v0.5.1) (2024-12-12)


### Features

* Update UI to 0.2.0 ([#661](https://github.com/lakekeeper/lakekeeper/issues/661)) ([fb7e2f5](https://github.com/lakekeeper/lakekeeper/commit/fb7e2f51d122ddf54a405684f5076e63d68d3cf6))


### Miscellaneous Chores

* release 0.5.1 ([f8aa87c](https://github.com/lakekeeper/lakekeeper/commit/f8aa87ca8b7a8074389cd43a39007b2652a46494))

## [0.5.0](https://github.com/lakekeeper/lakekeeper/compare/v0.4.3...v0.5.0) (2024-12-06)


### ⚠ BREAKING CHANGES

* Change default port from 8080 to 8181
* Default to single-tenant / single-project with NIL Project-ID

### Features

* Add Iceberg REST Spec to swagger ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* add kafka support [#271](https://github.com/lakekeeper/lakekeeper/issues/271) ([#340](https://github.com/lakekeeper/lakekeeper/issues/340)) ([7973586](https://github.com/lakekeeper/lakekeeper/commit/7973586282b0f09074f00bf455ea9fc1a9fe1cf3))
* Allow configuration of additional Issuer URLs ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Allow configuration of multiple Audiences ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Change default port from 8080 to 8181 ([b712cf0](https://github.com/lakekeeper/lakekeeper/commit/b712cf062ae6ecfc6904123672f25515304f65b1))
* Create default Project on Bootstrap ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Default to single-tenant / single-project with NIL Project-ID ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* docs ([#605](https://github.com/lakekeeper/lakekeeper/issues/605)) ([c1d2348](https://github.com/lakekeeper/lakekeeper/commit/c1d23488c40a43af7f303ed78cadef76d9ccc06b))
* Embedded UI ([#622](https://github.com/lakekeeper/lakekeeper/issues/622)) ([332f3b8](https://github.com/lakekeeper/lakekeeper/commit/332f3b87db9ffbc6fdaf1d48855b8e3cdcf3c017))
* Enable K8s Auth explicitly ([#594](https://github.com/lakekeeper/lakekeeper/issues/594)) ([3773141](https://github.com/lakekeeper/lakekeeper/commit/3773141690cb2225571f4708509df90103bd3226))
* Fine Grained Access Controls with OpenFGA ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Hierarchical Namespaces ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* OIDC Audience validation ([#607](https://github.com/lakekeeper/lakekeeper/issues/607)) ([052bb3f](https://github.com/lakekeeper/lakekeeper/commit/052bb3f539e9247fb5d4312447b9ab1823f20d8b))
* Optionally return uuids for Iceberg APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Project Management APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Server Info Endpoint ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* server now refuses to start if migrations are missing ([#531](https://github.com/lakekeeper/lakekeeper/issues/531)) ([5f34fca](https://github.com/lakekeeper/lakekeeper/commit/5f34fcad18cf58c7ed08e3f7b11c87415c8cb55c))
* split table metadata into tables ([#478](https://github.com/lakekeeper/lakekeeper/issues/478)) ([942fa97](https://github.com/lakekeeper/lakekeeper/commit/942fa97c98049d15a50168ce7d7a9e711d9de3d1))
* support kubernetes service-accounts ([#538](https://github.com/lakekeeper/lakekeeper/issues/538)) ([2982210](https://github.com/lakekeeper/lakekeeper/commit/298221063ad61b0301c019dee2094aae64dd5447))


### Bug Fixes

* Respect PG pool sizes ([#624](https://github.com/lakekeeper/lakekeeper/issues/624)) ([4fa0e2c](https://github.com/lakekeeper/lakekeeper/commit/4fa0e2cc29646c738f6075dba092f05159882b09))
* run metrics router ([#628](https://github.com/lakekeeper/lakekeeper/issues/628)) ([f6b47e5](https://github.com/lakekeeper/lakekeeper/commit/f6b47e5d9c6ae6d884dcea2dcf4f2a6b9b3baefd))


### Miscellaneous Chores

* release 0.5.0 ([b1b2ee6](https://github.com/lakekeeper/lakekeeper/commit/b1b2ee6d0f068adf9a60719c1cfb88201825d389))

## [0.4.3](https://github.com/lakekeeper/lakekeeper/compare/v0.4.2...v0.4.3) (2024-11-13)


### Miscellaneous Chores

* release 0.4.3 ([e577ab2](https://github.com/lakekeeper/lakekeeper/commit/e577ab2e4da78d612e87bd4844307c28098e2c31))

## [0.4.2](https://github.com/lakekeeper/lakekeeper/compare/v0.4.1...v0.4.2) (2024-10-28)


### Miscellaneous Chores

* release 0.4.2 ([1d8c469](https://github.com/lakekeeper/lakekeeper/commit/1d8c469cd30121e17455b2c2a13e9f0a46f7f630))

## [0.4.0](https://github.com/lakekeeper/lakekeeper/compare/v0.3.0...v0.4.0) (2024-10-03)


### Features

* Add Lakekeeper startup banner ([#389](https://github.com/lakekeeper/lakekeeper/issues/389)) ([8ead450](https://github.com/lakekeeper/lakekeeper/commit/8ead4508580d5a3e0c82aa40fceb5e688c263556))
* **catalog:** Soft-deletions & tabular cleanup queues ([#310](https://github.com/lakekeeper/lakekeeper/issues/310)) ([1de63b3](https://github.com/lakekeeper/lakekeeper/commit/1de63b3886820ea219006fcc2c696328b44dfb0f))

## [0.3.0](https://github.com/lakekeeper/lakekeeper/compare/v0.2.1...v0.3.0) (2024-08-26)


### Features

* Add support for custom Locations for Namespaces & Tables ([1d2ac6f](https://github.com/lakekeeper/lakekeeper/commit/1d2ac6f4b3910bf161c47d0224689b6e611d15ab))


### Bug Fixes

* **kv2:** extend docs & fix mismatch between docs and expected env values ([#224](https://github.com/lakekeeper/lakekeeper/issues/224)) ([be3e3e6](https://github.com/lakekeeper/lakekeeper/commit/be3e3e60181acdb501303b7fb4215d79e65dd79e))

## [0.2.1](https://github.com/lakekeeper/lakekeeper/compare/v0.2.0...v0.2.1) (2024-07-29)


### Miscellaneous Chores

* release 0.2.1 ([587ea12](https://github.com/lakekeeper/lakekeeper/commit/587ea129780c21a3cd0fa8dd371b6901dede4c20))

## [0.2.0](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0...v0.2.0) (2024-07-26)


### Features

* **health:** Service health checks ([#181](https://github.com/lakekeeper/lakekeeper/issues/181)) ([3bf4d4c](https://github.com/lakekeeper/lakekeeper/commit/3bf4d4c99e09b3ae90ea1b4a9aba5136300df514))
* **openapi:** management openapi spec ([#184](https://github.com/lakekeeper/lakekeeper/issues/184)) ([5f2f8e3](https://github.com/lakekeeper/lakekeeper/commit/5f2f8e30c154dbc31607d4b629070dc207b47652))
* **prometheus:** add prometheus axum metrics ([#185](https://github.com/lakekeeper/lakekeeper/issues/185)) ([d60d84a](https://github.com/lakekeeper/lakekeeper/commit/d60d84aebf26052a72e26ff6350d9636d4865009))
* **secrets:** add support for kv2 secret storage ([#192](https://github.com/lakekeeper/lakekeeper/issues/192)) ([a86b13c](https://github.com/lakekeeper/lakekeeper/commit/a86b13c5020cd52073608c74dacc86eff7e1bb60))
* **server:** make listenport configurable ([#183](https://github.com/lakekeeper/lakekeeper/issues/183)) ([9ffe0c2](https://github.com/lakekeeper/lakekeeper/commit/9ffe0c2e2c78b178bcb3900ed4d6a246e4eaeacb))


### Bug Fixes

* **db:** add wait-for-db command ([#196](https://github.com/lakekeeper/lakekeeper/issues/196)) ([c1cd069](https://github.com/lakekeeper/lakekeeper/commit/c1cd069d773906a4c647dcc007c50b0aa6929c29))

## [0.1.0](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc3...v0.1.0) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0 ([a5def9a](https://github.com/lakekeeper/lakekeeper/commit/a5def9a527aa615779b60fe8fc5a18aaa47f33ee))

## [0.1.0-rc3](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc2...v0.1.0-rc3) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc3 ([9b0d219](https://github.com/lakekeeper/lakekeeper/commit/9b0d219e865dce85803fc93da7233e92d3e8b4b8))

## [0.1.0-rc2](https://github.com/lakekeeper/lakekeeper/compare/v0.1.0-rc1...v0.1.0-rc2) (2024-06-17)


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc2 ([9bc25ef](https://github.com/lakekeeper/lakekeeper/commit/9bc25ef2b44d6c29556a5d0913c076904b1cb010))

## [0.1.0-rc1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.2-rc1...v0.1.0-rc1) (2024-06-16)


### Features

* CLI version command ([#112](https://github.com/lakekeeper/lakekeeper/issues/112)) ([b8d9129](https://github.com/lakekeeper/lakekeeper/commit/b8d9129f576249aa9a9673f901a9f5903f1bb4a7))


### Miscellaneous Chores

* 🚀 Release 0.1.0-rc1 ([ba6e5d5](https://github.com/lakekeeper/lakekeeper/commit/ba6e5d5c8a59cb1da5b61dd559c783998559debf))

## [0.0.2-rc1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.1...v0.0.2-rc1) (2024-06-16)


### Miscellaneous Chores

* 🚀 Release 0.0.2-rc1 ([eb34b9c](https://github.com/lakekeeper/lakekeeper/commit/eb34b9cd613bb2d72d4a9b33b103d36c7649bd57))

## [0.0.1](https://github.com/lakekeeper/lakekeeper/compare/v0.0.0...v0.0.1) (2024-06-15)


### Miscellaneous Chores

* 🚀 Release 0.0.1 ([c52ddec](https://github.com/lakekeeper/lakekeeper/commit/c52ddec7520ec16ed0b6f70c5e3108a7d8a35665))
