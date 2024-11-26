# Changelog



## [0.5.0](https://github.com/lakekeeper/lakekeeper/compare/v0.4.3...v0.5.0) (2024-11-26)


### ⚠ BREAKING CHANGES

* Default to single-tenant / single-project with NIL Project-ID
* Management API ([#99](https://github.com/lakekeeper/lakekeeper/issues/99))

### Features

* Add Iceberg REST Spec to swagger ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* add kafka support [#271](https://github.com/lakekeeper/lakekeeper/issues/271) ([#340](https://github.com/lakekeeper/lakekeeper/issues/340)) ([7973586](https://github.com/lakekeeper/lakekeeper/commit/7973586282b0f09074f00bf455ea9fc1a9fe1cf3))
* Add Lakekeeper startup banner ([#389](https://github.com/lakekeeper/lakekeeper/issues/389)) ([8ead450](https://github.com/lakekeeper/lakekeeper/commit/8ead4508580d5a3e0c82aa40fceb5e688c263556))
* Add support for custom Locations for Namespaces & Tables ([1d2ac6f](https://github.com/lakekeeper/lakekeeper/commit/1d2ac6f4b3910bf161c47d0224689b6e611d15ab))
* **catalog:** Soft-deletions & tabular cleanup queues ([#310](https://github.com/lakekeeper/lakekeeper/issues/310)) ([1de63b3](https://github.com/lakekeeper/lakekeeper/commit/1de63b3886820ea219006fcc2c696328b44dfb0f))
* CLI version command ([#112](https://github.com/lakekeeper/lakekeeper/issues/112)) ([b8d9129](https://github.com/lakekeeper/lakekeeper/commit/b8d9129f576249aa9a9673f901a9f5903f1bb4a7))
* Create default Project on Bootstrap ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Default to single-tenant / single-project with NIL Project-ID ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Fine Grained Access Controls with OpenFGA ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* **health:** Service health checks ([#181](https://github.com/lakekeeper/lakekeeper/issues/181)) ([3bf4d4c](https://github.com/lakekeeper/lakekeeper/commit/3bf4d4c99e09b3ae90ea1b4a9aba5136300df514))
* Hierarchical Namespaces ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Management API ([#99](https://github.com/lakekeeper/lakekeeper/issues/99)) ([bf23319](https://github.com/lakekeeper/lakekeeper/commit/bf233198e22c75d476fc7531834757127a2804ba))
* **nats:** add optional auth to nats ([#94](https://github.com/lakekeeper/lakekeeper/issues/94)) ([caf350d](https://github.com/lakekeeper/lakekeeper/commit/caf350d566acf9a451f12630d52831ad97c58f65))
* **openapi:** management openapi spec ([#184](https://github.com/lakekeeper/lakekeeper/issues/184)) ([5f2f8e3](https://github.com/lakekeeper/lakekeeper/commit/5f2f8e30c154dbc31607d4b629070dc207b47652))
* Optionally return uuids for Iceberg APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* Project Management APIs ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* **prometheus:** add prometheus axum metrics ([#185](https://github.com/lakekeeper/lakekeeper/issues/185)) ([d60d84a](https://github.com/lakekeeper/lakekeeper/commit/d60d84aebf26052a72e26ff6350d9636d4865009))
* **secrets:** add support for kv2 secret storage ([#192](https://github.com/lakekeeper/lakekeeper/issues/192)) ([a86b13c](https://github.com/lakekeeper/lakekeeper/commit/a86b13c5020cd52073608c74dacc86eff7e1bb60))
* Secure routes via openid provider ([#89](https://github.com/lakekeeper/lakekeeper/issues/89)) ([7d68206](https://github.com/lakekeeper/lakekeeper/commit/7d6820602d963e0da0a7a60e58e52cc427adca50))
* Server Info Endpoint ([2eaa10e](https://github.com/lakekeeper/lakekeeper/commit/2eaa10e7cb233282fe4452bf526deee7c07a5fb5))
* server now refuses to start if migrations are missing ([#531](https://github.com/lakekeeper/lakekeeper/issues/531)) ([5f34fca](https://github.com/lakekeeper/lakekeeper/commit/5f34fcad18cf58c7ed08e3f7b11c87415c8cb55c))
* **server:** make listenport configurable ([#183](https://github.com/lakekeeper/lakekeeper/issues/183)) ([9ffe0c2](https://github.com/lakekeeper/lakekeeper/commit/9ffe0c2e2c78b178bcb3900ed4d6a246e4eaeacb))
* split table metadata into tables ([#478](https://github.com/lakekeeper/lakekeeper/issues/478)) ([942fa97](https://github.com/lakekeeper/lakekeeper/commit/942fa97c98049d15a50168ce7d7a9e711d9de3d1))
* support kubernetes service-accounts ([#538](https://github.com/lakekeeper/lakekeeper/issues/538)) ([2982210](https://github.com/lakekeeper/lakekeeper/commit/298221063ad61b0301c019dee2094aae64dd5447))


### Bug Fixes

* **db:** add wait-for-db command ([#196](https://github.com/lakekeeper/lakekeeper/issues/196)) ([c1cd069](https://github.com/lakekeeper/lakekeeper/commit/c1cd069d773906a4c647dcc007c50b0aa6929c29))
* **kv2:** extend docs & fix mismatch between docs and expected env values ([#224](https://github.com/lakekeeper/lakekeeper/issues/224)) ([be3e3e6](https://github.com/lakekeeper/lakekeeper/commit/be3e3e60181acdb501303b7fb4215d79e65dd79e))
* release-please pipeline filter ([f73590c](https://github.com/lakekeeper/lakekeeper/commit/f73590cf187d8efdaf0ba4e96828285a0386d13b))


### Miscellaneous Chores

* 🚀 Release 0.0.1 ([c52ddec](https://github.com/lakekeeper/lakekeeper/commit/c52ddec7520ec16ed0b6f70c5e3108a7d8a35665))
* 🚀 Release 0.0.2-rc1 ([eb34b9c](https://github.com/lakekeeper/lakekeeper/commit/eb34b9cd613bb2d72d4a9b33b103d36c7649bd57))
* 🚀 Release 0.1.0 ([a5def9a](https://github.com/lakekeeper/lakekeeper/commit/a5def9a527aa615779b60fe8fc5a18aaa47f33ee))
* 🚀 Release 0.1.0-rc1 ([ba6e5d5](https://github.com/lakekeeper/lakekeeper/commit/ba6e5d5c8a59cb1da5b61dd559c783998559debf))
* 🚀 Release 0.1.0-rc2 ([9bc25ef](https://github.com/lakekeeper/lakekeeper/commit/9bc25ef2b44d6c29556a5d0913c076904b1cb010))
* 🚀 Release 0.1.0-rc3 ([9b0d219](https://github.com/lakekeeper/lakekeeper/commit/9b0d219e865dce85803fc93da7233e92d3e8b4b8))
* release 0.0.1 ([dc8f5a1](https://github.com/lakekeeper/lakekeeper/commit/dc8f5a148938930346de43c3223eb7332ef34e5f))
* release 0.1.0 ([f94579a](https://github.com/lakekeeper/lakekeeper/commit/f94579a2232b780c98fa2061c967b76a78a24c35))
* release 0.1.0 ([640e8c3](https://github.com/lakekeeper/lakekeeper/commit/640e8c3462ee2c7e8c8bdf223f13f2320c56a61a))
* release 0.1.1 ([01d1aab](https://github.com/lakekeeper/lakekeeper/commit/01d1aabc52b33d0969f5cda4eb9a72130ac08d68))
* release 0.2.1 ([587ea12](https://github.com/lakekeeper/lakekeeper/commit/587ea129780c21a3cd0fa8dd371b6901dede4c20))
* release 0.4.2 ([1d8c469](https://github.com/lakekeeper/lakekeeper/commit/1d8c469cd30121e17455b2c2a13e9f0a46f7f630))
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
