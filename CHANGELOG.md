# Changelog

## [0.13.0](https://github.com/lakekeeper/lakekeeper/compare/v0.12.3...v0.13.0) (2026-06-30)


### ⚠ BREAKING CHANGES

* the default storage layout for newly created namespaces changed from <base>/<parent-namespace-uuid>/<tabular-uuid> to <base>/<tabular-uuid>.
* **authz:** `CatalogRoleAction` gains a non-unit variant and `RoleAction` is no longer `Copy`; downstream authorizer implementations must handle `UpdateSourceSystem`, and the rebind now requires membership-control permission rather than plain update.
* **authz:** `Authorizer::are_allowed_*_actions_impl` return `Result<Vec<AuthorizationDecision>, _>` instead of `Result<Vec<bool>, _>`. Implementors map their per-decision diagnostics into `AuthorizationDecision` (or `vec![AuthorizationDecision::allow(); n]` / `.map(AuthorizationDecision::from)` for backends without diagnostics).
* **events:** split nats + kafka backends into separate crates ([#1814](https://github.com/lakekeeper/lakekeeper/issues/1814))
* **storage:** split lakekeeper-storage-postgres into separate crate ([#1812](https://github.com/lakekeeper/lakekeeper/issues/1812))

### Features

* **authn:** add pluggable post-authentication admission gates ([#1865](https://github.com/lakekeeper/lakekeeper/issues/1865)) ([a00d597](https://github.com/lakekeeper/lakekeeper/commit/a00d59766fd09043d556af2ba1d4af9eae5e39b0))
* **authn:** let admission gates contribute resolved roles ([#1866](https://github.com/lakekeeper/lakekeeper/issues/1866)) ([b92c902](https://github.com/lakekeeper/lakekeeper/commit/b92c9020710da87447957c11c72caf95d6c223e5))
* **authn:** pass the caller's bearer token to admission gates ([#1869](https://github.com/lakekeeper/lakekeeper/issues/1869)) ([9147ec0](https://github.com/lakekeeper/lakekeeper/commit/9147ec00c86f50109dbba03a031490538f87f5a8))
* **authn:** support multiple OIDC providers via LAKEKEEPER__OPENID_PROVIDERS ([#1760](https://github.com/lakekeeper/lakekeeper/issues/1760)) ([c54a991](https://github.com/lakekeeper/lakekeeper/commit/c54a991776613711812b93566560d08dd59efe33))
* **authz:** add load_and_authorize_generic_table_operation ([#1813](https://github.com/lakekeeper/lakekeeper/issues/1813)) ([f8c8007](https://github.com/lakekeeper/lakekeeper/commit/f8c800747d0067dfeb898867b998dae3696df5f4))
* **authz:** carry force/purge/recursive context on delete actions ([#1860](https://github.com/lakekeeper/lakekeeper/issues/1860)) ([9af0a3f](https://github.com/lakekeeper/lakekeeper/commit/9af0a3f141dd27e6de7067a4117c76a6651f34dd))
* **authz:** gate role source-system rebind as a destination-aware action ([#1848](https://github.com/lakekeeper/lakekeeper/issues/1848)) ([e02fbc0](https://github.com/lakekeeper/lakekeeper/commit/e02fbc05eae37d52404a261014c88541c8012b3d))
* **authz:** surface contributing policies in authorization audit events ([#1844](https://github.com/lakekeeper/lakekeeper/issues/1844)) ([cca7bce](https://github.com/lakekeeper/lakekeeper/commit/cca7bce586e1685df23fc45a104c78af2ef23a01))
* **cache:** dedup role/project identity in the user-assignments cache ([#1841](https://github.com/lakekeeper/lakekeeper/issues/1841)) ([211d1a1](https://github.com/lakekeeper/lakekeeper/commit/211d1a1382db53ba50826156ab1e1d19b559816f))
* **cache:** jitter cache TTLs and add role-membership fan-out metric ([#1837](https://github.com/lakekeeper/lakekeeper/issues/1837)) ([1b97d53](https://github.com/lakekeeper/lakekeeper/commit/1b97d536c0f8c47cbcee57bf35cd282f12cd04c9))
* **cache:** single-flight the hot cache read-throughs ([#1833](https://github.com/lakekeeper/lakekeeper/issues/1833)) ([a39e606](https://github.com/lakekeeper/lakekeeper/commit/a39e606924eeb63c69c453882892d279cda85c9a))
* **deps:** bump console to v0.18.1 ([#1871](https://github.com/lakekeeper/lakekeeper/issues/1871)) ([228ebe1](https://github.com/lakekeeper/lakekeeper/commit/228ebe1f9cf43fb5fdaecb4616ea525cf5a0679e))
* **deps:** bump console to v0.18.2 ([#1872](https://github.com/lakekeeper/lakekeeper/issues/1872)) ([e732aeb](https://github.com/lakekeeper/lakekeeper/commit/e732aebaaea1fa3be4feea033222cd919525d559))
* **events:** add timing instrumentation to event listener dispatch ([#1863](https://github.com/lakekeeper/lakekeeper/issues/1863)) ([f146ada](https://github.com/lakekeeper/lakekeeper/commit/f146ada3de150a4108f8ad771a3102b3b873622f))
* **examples:** use SeaweedFS instead of MinIO in docker-compose examples ([#1811](https://github.com/lakekeeper/lakekeeper/issues/1811)) ([3e4084a](https://github.com/lakekeeper/lakekeeper/commit/3e4084a7714b661508c80ce82e8e90a775b78f36))
* flat ({uuid}-only) layout is the new default storage layout ([#1853](https://github.com/lakekeeper/lakekeeper/issues/1853)) ([acd19e4](https://github.com/lakekeeper/lakekeeper/commit/acd19e4dad991446ec1f9eeb8aa62f15a44e3843))
* Generic Table API with Lance ([#1673](https://github.com/lakekeeper/lakekeeper/issues/1673)) ([a474476](https://github.com/lakekeeper/lakekeeper/commit/a47447670f45b1b5315bd036bfc140dbc998d994))
* **metrics:** add client-side Postgres connection-pool metrics ([#1838](https://github.com/lakekeeper/lakekeeper/issues/1838)) ([7916505](https://github.com/lakekeeper/lakekeeper/commit/791650571da769679a2037102a9737eb4fc84cfa))
* **migrations:** refuse to start against a newer (downgraded) database ([#1861](https://github.com/lakekeeper/lakekeeper/issues/1861)) ([d8de0db](https://github.com/lakekeeper/lakekeeper/commit/d8de0db84e2427a15b66ba45867b782d5be40092))
* **role:** capability-gated system-role seeder + fatal backfill ([#1784](https://github.com/lakekeeper/lakekeeper/issues/1784)) ([5b74867](https://github.com/lakekeeper/lakekeeper/commit/5b748678d88cf8a6fa03b44641a9af9ddb9de2d1))
* **roles:** backend support for role-in-role membership ([#1816](https://github.com/lakekeeper/lakekeeper/issues/1816)) ([47489d8](https://github.com/lakekeeper/lakekeeper/commit/47489d8a7f1ef6c086c0ae997da86d696fa3a71e))
* **roles:** bound role-in-role nesting depth at write time ([#1819](https://github.com/lakekeeper/lakekeeper/issues/1819)) ([1440e1d](https://github.com/lakekeeper/lakekeeper/commit/1440e1d1a372ce5a5bb1f7e93352a4e913c19420))
* **roles:** role-membership backend foundation + consistency hardening ([#1824](https://github.com/lakekeeper/lakekeeper/issues/1824)) ([c6f1327](https://github.com/lakekeeper/lakekeeper/commit/c6f13277a7f49b8a4c6c078367c3cc64f072bace))
* **roles:** role-membership management API ([#1829](https://github.com/lakekeeper/lakekeeper/issues/1829)) ([8c5da57](https://github.com/lakekeeper/lakekeeper/commit/8c5da57323fca8503ebe8aa2a4b4cadbc23a6507))
* **roles:** role-membership management API (users + nested roles) ([8c5da57](https://github.com/lakekeeper/lakekeeper/commit/8c5da57323fca8503ebe8aa2a4b4cadbc23a6507))
* **s3:** vend SSE-KMS config to clients for KMS-encrypted warehouses ([#1847](https://github.com/lakekeeper/lakekeeper/issues/1847)) ([de8fc08](https://github.com/lakekeeper/lakekeeper/commit/de8fc08fd1544a17e38d8787de0d1a277a62c24d))
* **storage:** emit Iceberg 1.11 signer.uri/signer.endpoint properties ([#1820](https://github.com/lakekeeper/lakekeeper/issues/1820)) ([dc7eede](https://github.com/lakekeeper/lakekeeper/commit/dc7eede89585ef91e008be92d62d08c3ea303a9e))
* Support Private Endpoints and Onelake/Fabric Storage Accounts ([#1840](https://github.com/lakekeeper/lakekeeper/issues/1840)) ([c439334](https://github.com/lakekeeper/lakekeeper/commit/c43933421f1ee84a90ebcaa576aec27cf7d8e3b9))
* **tasks:** add POST .../task-queue/{queue_name}/schedule endpoint ([#1783](https://github.com/lakekeeper/lakekeeper/issues/1783)) ([cd4f820](https://github.com/lakekeeper/lakekeeper/commit/cd4f82089d5f87425025352d96248f6027cea464))
* **tasks:** surface failure message in task details ([#1873](https://github.com/lakekeeper/lakekeeper/issues/1873)) ([7ba4d44](https://github.com/lakekeeper/lakekeeper/commit/7ba4d4438492f799d9bdd7a5d72ebe592cc546ee))
* **ui:** bump console to v0.15.1 ([#1855](https://github.com/lakekeeper/lakekeeper/issues/1855)) ([01e62ef](https://github.com/lakekeeper/lakekeeper/commit/01e62ef943483efa1c98d3b80afe7376eb07c5d1))
* **warehouse:** add managed-by ownership to lock spec to instance admins ([#1828](https://github.com/lakekeeper/lakekeeper/issues/1828)) ([eb6983f](https://github.com/lakekeeper/lakekeeper/commit/eb6983fc6ae45442ed7b15f9e3764ae981139cfc))
* **warehouse:** per-warehouse Iceberg table format-version policy ([#1786](https://github.com/lakekeeper/lakekeeper/issues/1786)) ([f5e77df](https://github.com/lakekeeper/lakekeeper/commit/f5e77df519cee5ecc15764a226fb95f7f1586bd2))


### Bug Fixes

* **authz:** evict role-assignment caches when deleting a role ([#1830](https://github.com/lakekeeper/lakekeeper/issues/1830)) ([8862549](https://github.com/lakekeeper/lakekeeper/commit/886254954d8de533111656e9c9cfe84bc6e32348))
* **cache:** serialize invalidation with the read-through loader (by-id resurrection race) ([#1843](https://github.com/lakekeeper/lakekeeper/issues/1843)) ([119db74](https://github.com/lakekeeper/lakekeeper/commit/119db7460a11ac5e4186f202cc29ecb3c9f8c510))
* **config:** resolve GET /config warehouse directly, drop project-level authz ([#1788](https://github.com/lakekeeper/lakekeeper/issues/1788)) ([608e847](https://github.com/lakekeeper/lakekeeper/commit/608e847a35ad3807cf13c552dbc9c04aca8fdd06))
* **deps:** update anyhow to 1.0.103 (RUSTSEC-2026-0190) ([16b57fd](https://github.com/lakekeeper/lakekeeper/commit/16b57fd3b15f9218b1b54397b73c40cf4822f741))
* **io:** bound batch-delete concurrency to avoid connection exhaustion ([45c8db0](https://github.com/lakekeeper/lakekeeper/commit/45c8db0e657615cd64394a3fb9de3cdd125f4524))
* **io:** make ADLS retry budget span connect timeouts ([#1815](https://github.com/lakekeeper/lakekeeper/issues/1815)) ([5e89416](https://github.com/lakekeeper/lakekeeper/commit/5e89416dd7fd26bdc7008f347ee05e2b53b58080))
* **io:** retry transient failures when acquiring storage OAuth tokens ([#1827](https://github.com/lakekeeper/lakekeeper/issues/1827)) ([3ff4036](https://github.com/lakekeeper/lakekeeper/commit/3ff40360c203842e15b5483bfe924a1e8a193f5c))
* **postgres:** switch migration lock to pg_advisory_xact_lock ([#1790](https://github.com/lakekeeper/lakekeeper/issues/1790)) ([83fe080](https://github.com/lakekeeper/lakekeeper/commit/83fe080f81fbb54e5ee1af0eeeee1a233d3e52f4))
* return 503 for unhealthy health endpoint ([#1802](https://github.com/lakekeeper/lakekeeper/issues/1802)) ([aca51e7](https://github.com/lakekeeper/lakekeeper/commit/aca51e72b1e5326685f996445f948c5f466c3e35))
* **roles:** evict membership caches on role source-system rebind ([#1849](https://github.com/lakekeeper/lakekeeper/issues/1849)) ([5cc1433](https://github.com/lakekeeper/lakekeeper/commit/5cc1433324b0017b099279f6769eeba210768997))
* **s3:** keep AWS STS vended-credential policy within the packed-size limit ([#1857](https://github.com/lakekeeper/lakekeeper/issues/1857)) ([b86ccd5](https://github.com/lakekeeper/lakekeeper/commit/b86ccd5340d536ee443e89b60494c55dc4cf3114))
* **tables:** don't return 304 on loadTable once vended credentials expired ([#1862](https://github.com/lakekeeper/lakekeeper/issues/1862)) ([c492020](https://github.com/lakekeeper/lakekeeper/commit/c492020154fdf8e18933b349ffbb2ba0835c8d45))
* **tasks:** stop post-migration cleanup hook spinning to the pagination cap ([#1876](https://github.com/lakekeeper/lakekeeper/issues/1876)) ([87fc710](https://github.com/lakekeeper/lakekeeper/commit/87fc710240f8ae8f05f5611fe110d7006c3197d6))


### Documentation

* Docs:  ([a39e606](https://github.com/lakekeeper/lakekeeper/commit/a39e606924eeb63c69c453882892d279cda85c9a))
* **authorization-cedar:** map Cedar actions to audit log action_names ([7ad1cd9](https://github.com/lakekeeper/lakekeeper/commit/7ad1cd91c40739e3619086f8116f0ac31ba1a0c7))
* **authz:** document persist_token_roles for definer views ([89277d7](https://github.com/lakekeeper/lakekeeper/commit/89277d7b625d0183e383c886a361d9b0708f7eec))
* **maintenance:** note encrypted tables are skipped by expire/orphan-file jobs ([d2ff38f](https://github.com/lakekeeper/lakekeeper/commit/d2ff38feae0abccde9cd780ee20eb27ba8884590))
* **openapi:** clarify purgeRequested default vs. Lakekeeper behaviour ([#1836](https://github.com/lakekeeper/lakekeeper/issues/1836)) ([5e9da34](https://github.com/lakekeeper/lakekeeper/commit/5e9da34b131d2f78c6c3f8d7cee301d788c7be36)), closes [#1832](https://github.com/lakekeeper/lakekeeper/issues/1832)
* publish enterprise release notes; fix Cedar global_role_ids requirement ([#1821](https://github.com/lakekeeper/lakekeeper/issues/1821)) ([eb154b1](https://github.com/lakekeeper/lakekeeper/commit/eb154b14ba0e51b10451088e7daaefb6e8676d11))
* refresh landing page, restructure API docs, lighten per-page assets ([#1826](https://github.com/lakekeeper/lakekeeper/issues/1826)) ([8c7b4c0](https://github.com/lakekeeper/lakekeeper/commit/8c7b4c021518160aae5c5fbd73240199fad2e1e4))
* **release-notes:** curate site Release Notes page + add release runbook ([#1817](https://github.com/lakekeeper/lakekeeper/issues/1817)) ([92435c1](https://github.com/lakekeeper/lakekeeper/commit/92435c174ae4316d6975a1030f3c8546320d632c))
* **release:** fix v0.13.0 role-membership note; add changelog-curation guidance ([9c3e50d](https://github.com/lakekeeper/lakekeeper/commit/9c3e50dd774b48d90e85ef6b484cc8ff98c3116f))
* **roles:** correct role-membership API doc inaccuracies ([#1877](https://github.com/lakekeeper/lakekeeper/issues/1877)) ([d12dfba](https://github.com/lakekeeper/lakekeeper/commit/d12dfba0c20634d914abfd60c15586949fe138cc))


### Miscellaneous Chores

* **authz-openfga:** refactor / decouple reconcile from Postgres/sqlx ([#1789](https://github.com/lakekeeper/lakekeeper/issues/1789)) ([27a0b24](https://github.com/lakekeeper/lakekeeper/commit/27a0b24289a0438623c74ac4d98d8e4a1696ea2e))
* **authz:** remove unused intermediate OpenFGA model versions ([#1787](https://github.com/lakekeeper/lakekeeper/issues/1787)) ([c7bcddd](https://github.com/lakekeeper/lakekeeper/commit/c7bcddd2017331fd76cb0fd553c473d9eeb0e940))
* **authz:** rename role-rebind context keys to requested_provider_id/requested_source_id ([b6a4b1d](https://github.com/lakekeeper/lakekeeper/commit/b6a4b1d58a86995d99717b84f11dab8144a55118))
* **ci:** free disk space before Docker image builds ([#1842](https://github.com/lakekeeper/lakekeeper/issues/1842)) ([dc137bc](https://github.com/lakekeeper/lakekeeper/commit/dc137bc1074c2361476d87a7544045e56688f231))
* **deps:** batch GitHub Actions updates ([#1803](https://github.com/lakekeeper/lakekeeper/issues/1803)) ([7cc3128](https://github.com/lakekeeper/lakekeeper/commit/7cc3128c6242a1cbdc8d6ca32b0d80027202ae75))
* **deps:** switch openfga-client from git rev to crates.io v0.6.0 ([058ddec](https://github.com/lakekeeper/lakekeeper/commit/058ddec3d5d783f7c8e5ab5a94c1abd730e546af))
* **deps:** update busybox docker tag to v1.38.0 ([#1808](https://github.com/lakekeeper/lakekeeper/issues/1808)) ([2b77200](https://github.com/lakekeeper/lakekeeper/commit/2b7720036db53795a615fa236705181c3236bc60))
* **deps:** update Cargo.lock ([#1785](https://github.com/lakekeeper/lakekeeper/issues/1785)) ([f8c876b](https://github.com/lakekeeper/lakekeeper/commit/f8c876bb4cc6f0b1749a72fdbd04baac6e2709dd))
* **deps:** update dependency mkdocs-material to v9.7.6 ([#1804](https://github.com/lakekeeper/lakekeeper/issues/1804)) ([eac8949](https://github.com/lakekeeper/lakekeeper/commit/eac89493f0a8297aa7490f5df8beb2d054ec73fc))
* **deps:** update rust crate similar to v3 ([#1694](https://github.com/lakekeeper/lakekeeper/issues/1694)) ([ee1c262](https://github.com/lakekeeper/lakekeeper/commit/ee1c26298aaeaa8dc8df53ab0764ed49d48fe52d))
* **docker:** migrate base images from Debian bookworm to trixie ([#1794](https://github.com/lakekeeper/lakekeeper/issues/1794)) ([4105808](https://github.com/lakekeeper/lakekeeper/commit/4105808fff2d52da47b6929e541ef026c7cc9dff))
* **docs:** add a per-PR docs link-check gate ([#1823](https://github.com/lakekeeper/lakekeeper/issues/1823)) ([cc0d455](https://github.com/lakekeeper/lakekeeper/commit/cc0d455b25c45676dbfe7e44eeddca64599428f0))
* **docs:** Add EntraID-Graph Role Provider docs ([#1870](https://github.com/lakekeeper/lakekeeper/issues/1870)) ([656bb90](https://github.com/lakekeeper/lakekeeper/commit/656bb901fdef217682b24a5e0ab94a1ca2473202))
* **docs:** Add OneLake availablilty note ([5b02770](https://github.com/lakekeeper/lakekeeper/commit/5b02770cb17fb3b782a61102bb52e55b0fec7c0f))
* **docs:** Admission Gate kebab-case ([ef9ff3a](https://github.com/lakekeeper/lakekeeper/commit/ef9ff3a8c25ce5d85e8a2b5e570df27148bddde1))
* **docs:** Mention support for Azure & GCS vended credentials for Trino & Starburst ([#1864](https://github.com/lakekeeper/lakekeeper/issues/1864)) ([a58e401](https://github.com/lakekeeper/lakekeeper/commit/a58e40171f9572647973b7b270f84deaa7e7cef1))
* **docs:** Sync documentation from enterprise repo ([#1818](https://github.com/lakekeeper/lakekeeper/issues/1818)) ([699d43a](https://github.com/lakekeeper/lakekeeper/commit/699d43af122add45e1baef11d3803287d404c3ad))
* **docs:** Sync documentation from enterprise repo ([#1825](https://github.com/lakekeeper/lakekeeper/issues/1825)) ([18339d4](https://github.com/lakekeeper/lakekeeper/commit/18339d44d1cd7aecde0d4d7dce36ed82df2a9230))
* **docs:** Sync documentation from enterprise repo ([#1845](https://github.com/lakekeeper/lakekeeper/issues/1845)) ([c1189d4](https://github.com/lakekeeper/lakekeeper/commit/c1189d4eb09c3569d8cbc5bf3328884e09964b3d))
* **docs:** Update Admission Gates JSON body docs ([8e971e3](https://github.com/lakekeeper/lakekeeper/commit/8e971e37eecaa3c16655c116b4713a37ea42bd0c))
* **events:** split nats + kafka backends into separate crates ([#1814](https://github.com/lakekeeper/lakekeeper/issues/1814)) ([600c3fa](https://github.com/lakekeeper/lakekeeper/commit/600c3fab9d11181813a4ae5eed7511636b9edce3))
* **examples:** use SeaweedFS 4.36 and drop the Admin action ([#1867](https://github.com/lakekeeper/lakekeeper/issues/1867)) ([047807e](https://github.com/lakekeeper/lakekeeper/commit/047807e9446ea873503c7584ce4676c1809bd098))
* **readme:** Update Overview Image ([#1868](https://github.com/lakekeeper/lakekeeper/issues/1868)) ([198efeb](https://github.com/lakekeeper/lakekeeper/commit/198efeb786d9a3064863c678c0f184b32789b7fa))
* **storage:** split lakekeeper-storage-postgres into separate crate ([#1812](https://github.com/lakekeeper/lakekeeper/issues/1812)) ([72f0ebd](https://github.com/lakekeeper/lakekeeper/commit/72f0ebde98561227bb9130197cdd82eedf677432))
* **ui:** Update console 0.18.3 ([#1875](https://github.com/lakekeeper/lakekeeper/issues/1875)) ([5526be4](https://github.com/lakekeeper/lakekeeper/commit/5526be42d4884d7045a2d2c3fb1bfa5dcd723061))

## [0.12.3](https://github.com/lakekeeper/lakekeeper/compare/v0.12.2...v0.12.3) (2026-05-26)


### Features

* add LAKEKEEPER__UI__ENABLE_SURVEYS opt-out flag ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **migrations:** atomic core + extension migrations ([aa734bf](https://github.com/lakekeeper/lakekeeper/commit/aa734bffcacd98566aa670f62341a4455833496c))
* **role:** accept Into&lt;Arc&lt;RoleProviderId&gt;&gt; in RoleIdent::new ([#1758](https://github.com/lakekeeper/lakekeeper/issues/1758)) ([60ac97d](https://github.com/lakekeeper/lakekeeper/commit/60ac97d652d105be8a4fca99f2ecc84bdebf5430))
* RoleIdent cheap cloning / inner Arc for provider ([#1757](https://github.com/lakekeeper/lakekeeper/issues/1757)) ([78b3009](https://github.com/lakekeeper/lakekeeper/commit/78b3009fefab629d151c0ffd25a5baa5275fcd31))
* **role:** reserve `system` provider for catalog-managed roles ([#1776](https://github.com/lakekeeper/lakekeeper/issues/1776)) ([0441112](https://github.com/lakekeeper/lakekeeper/commit/044111241dd1c15fd955323575b692756f7c1ae9))
* **server:** add read-only maintenance mode ([#1765](https://github.com/lakekeeper/lakekeeper/issues/1765)) ([5279d69](https://github.com/lakekeeper/lakekeeper/commit/5279d69dcaa29e78a8b435a8fb9b36d109c47978))
* **ui:** "Export for GitHub" button and dialog in Server Overview — copies / downloads a JSON support bundle (server info + UI config, no tokens) ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **ui:** "Export for GitHub" item in AppBar help menu ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **ui:** Feedback button at the right of the AppBar with 1-10 rating, discovery source select (incl. "Other"), and 2000-char improvements field; pre-fills a mailto draft to info@vakamo.com ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **ui:** Show role provider id in overfiew (v0.14.3) ([c7c2c1b](https://github.com/lakekeeper/lakekeeper/commit/c7c2c1b836f60937e8788df4bab7b3dc2e88e14e))
* **ui:** two-column Server Information + Console layout in Server Settings ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **ui:** User Surveys row in UI Configuration (toggle from VITE_ENABLE_USER_SURVEYS) ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **users:** drop unique-email constraint ([#1755](https://github.com/lakekeeper/lakekeeper/issues/1755)) ([bfd2812](https://github.com/lakekeeper/lakekeeper/commit/bfd2812fa3b9f8ec30824340d465a15c9f5cc546))


### Bug Fixes

* **ci:** hashicorp/vault non-root entrypoint ([0ea1f47](https://github.com/lakekeeper/lakekeeper/commit/0ea1f4735f45de1b425063f8c401895a99f35614))
* inconsistency between link text and target ([#1753](https://github.com/lakekeeper/lakekeeper/issues/1753)) ([d01dd94](https://github.com/lakekeeper/lakekeeper/commit/d01dd94720fe6bff9ac73efba763896dab710be5))
* **kv2:** replace stale vault health status ([#1773](https://github.com/lakekeeper/lakekeeper/issues/1773)) ([4176ad5](https://github.com/lakekeeper/lakekeeper/commit/4176ad5d2368a630d44fb5313ceba8c3fdb2245f))
* **postgres/migrations:** rewrite namespace trigger so pg_restore can replay it ([#1781](https://github.com/lakekeeper/lakekeeper/issues/1781)) ([14f224d](https://github.com/lakekeeper/lakekeeper/commit/14f224d63181725483184ee21be7aa15926abd3e))
* **stats:** assert relative deltas to tolerate bucket straddling ([0ea1f47](https://github.com/lakekeeper/lakekeeper/commit/0ea1f4735f45de1b425063f8c401895a99f35614))
* table property removal lost when no properties remain ([#1767](https://github.com/lakekeeper/lakekeeper/issues/1767)) ([3055b0a](https://github.com/lakekeeper/lakekeeper/commit/3055b0a6c888b752b2b6c014d6e8bbc743acd96f))
* **ui:** show Lakekeeper Version (drop Enterprise Version row when running OSS edition) ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **view:** preserve protection during view commits ([#1770](https://github.com/lakekeeper/lakekeeper/issues/1770)) ([bf4c409](https://github.com/lakekeeper/lakekeeper/commit/bf4c40907bfddefe6590bbd52af5bb8bc5d5c3ef))
* **view:** respect force=true in soft-delete warehouses on drop ([#1779](https://github.com/lakekeeper/lakekeeper/issues/1779)) ([c1b4357](https://github.com/lakekeeper/lakekeeper/commit/c1b4357b0efd659ea9a4920d5c31964b4ef62aae))


### Documentation

* **example:** add Firebolt section to documented engines ([#1759](https://github.com/lakekeeper/lakekeeper/issues/1759)) ([ab030b1](https://github.com/lakekeeper/lakekeeper/commit/ab030b10cf74ce47f3bab66bc18a8f1ec0cc9d2b))
* **role-provider:** document Search and Branching LDAP modes ([05cce57](https://github.com/lakekeeper/lakekeeper/commit/05cce5797d3321ce30c59e034f19a3861c39cd88))


### Miscellaneous Chores

* Bridge lakekeeper-io to Iceberg Storage trait ([#1734](https://github.com/lakekeeper/lakekeeper/issues/1734)) ([a152365](https://github.com/lakekeeper/lakekeeper/commit/a15236583a61f0ef50ccb0228d0b3b7833df3ab9))
* Bump MSRV to 1.94 ([aa734bf](https://github.com/lakekeeper/lakekeeper/commit/aa734bffcacd98566aa670f62341a4455833496c))
* **deps:** bump @lakekeeper/console-components to v0.6.2 (Renovate batches: eslint, vue-router / vue-tsc / eslint-plugin-vue, sql-formatter, @codemirror/view; README typos; @hey-api/openapi-ts 0.97 SDK regen) ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* **deps:** sqlx v0.9.0 ([aa734bf](https://github.com/lakekeeper/lakekeeper/commit/aa734bffcacd98566aa670f62341a4455833496c))
* **docs:** Improve remove orphan file docs ([79b9d46](https://github.com/lakekeeper/lakekeeper/commit/79b9d4630cc8804cc483c2d85ba2ed0430ac110a))
* **docs:** Update plus OpenAPI (remove orphan files) ([bf77a59](https://github.com/lakekeeper/lakekeeper/commit/bf77a590f4fe27f07d42d44534a1518c8f87fd65))
* **refactor:** split TabularRow into core and properties variants ([#1777](https://github.com/lakekeeper/lakekeeper/issues/1777)) ([30a9259](https://github.com/lakekeeper/lakekeeper/commit/30a925901f79e8e8b6b8520999c674f79a20a785))
* **ui:** dependencies page reads installed lib version from node_modules (no longer needs a sibling repo checkout) ([719150b](https://github.com/lakekeeper/lakekeeper/commit/719150bed3b700308ff5954217cfad8aac5ba9cf))
* Update dependencies ([#1754](https://github.com/lakekeeper/lakekeeper/issues/1754)) ([bd2c57c](https://github.com/lakekeeper/lakekeeper/commit/bd2c57cac9b36ff6f036c590c5a7e39197ec966d))

## [0.12.2](https://github.com/lakekeeper/lakekeeper/compare/v0.12.1...v0.12.2) (2026-05-10)


### Features

* **io:** canonicalise Location at parse time to avoid aliases ([#1743](https://github.com/lakekeeper/lakekeeper/issues/1743)) ([7e6f257](https://github.com/lakekeeper/lakekeeper/commit/7e6f257cacf1fb95a9f89bc04b6bd0b3ff1f85ed))
* **io:** expose object size on FileInfo ([#1741](https://github.com/lakekeeper/lakekeeper/issues/1741)) ([3ec87dd](https://github.com/lakekeeper/lakekeeper/commit/3ec87dd56f707aadd7f1ae26e8a22aa23dfd1343))


### Bug Fixes

* **adls:** pre-encode `%` in blob name to defeat SDK alias collapse ([#1746](https://github.com/lakekeeper/lakekeeper/issues/1746)) ([2e8aea5](https://github.com/lakekeeper/lakekeeper/commit/2e8aea51a438b38c7369d21f35279bd8ccdb9ea8))
* **postgres:** apply pg_acquire_timeout to all pool initializations ([#1744](https://github.com/lakekeeper/lakekeeper/issues/1744)) ([3af2055](https://github.com/lakekeeper/lakekeeper/commit/3af2055c19f4e54db4bd90ec852dcef27d63eb64))
* **storage:** harden STS/CEL credential policies against path injection ([#1740](https://github.com/lakekeeper/lakekeeper/issues/1740)) ([8531e7b](https://github.com/lakekeeper/lakekeeper/commit/8531e7b210a7e953749ff180f780a767efba6fbb))


### Miscellaneous Chores

* **docs:** Update Cedarschema ([cf796f6](https://github.com/lakekeeper/lakekeeper/commit/cf796f6de006fcd2f110b5bc384befa6e27f0a93))
* **main:** release 0.12.1 ([#1742](https://github.com/lakekeeper/lakekeeper/issues/1742)) ([b5c98a3](https://github.com/lakekeeper/lakekeeper/commit/b5c98a3de56396c51d81590f92e957fec27f48d3))
* Release-as 0.12.2 ([db043b3](https://github.com/lakekeeper/lakekeeper/commit/db043b3e089f11324a8fac1a5c3bfba475497bc6))

## [0.12.1](https://github.com/lakekeeper/lakekeeper/compare/v0.12.1...v0.12.1) (2026-05-09)


### Features

* **io:** canonicalise Location at parse time to avoid aliases ([#1743](https://github.com/lakekeeper/lakekeeper/issues/1743)) ([7e6f257](https://github.com/lakekeeper/lakekeeper/commit/7e6f257cacf1fb95a9f89bc04b6bd0b3ff1f85ed))
* **io:** expose object size on FileInfo ([#1741](https://github.com/lakekeeper/lakekeeper/issues/1741)) ([3ec87dd](https://github.com/lakekeeper/lakekeeper/commit/3ec87dd56f707aadd7f1ae26e8a22aa23dfd1343))


### Bug Fixes

* **adls:** pre-encode `%` in blob name to defeat SDK alias collapse ([#1746](https://github.com/lakekeeper/lakekeeper/issues/1746)) ([2e8aea5](https://github.com/lakekeeper/lakekeeper/commit/2e8aea51a438b38c7369d21f35279bd8ccdb9ea8))
* **postgres:** apply pg_acquire_timeout to all pool initializations ([#1744](https://github.com/lakekeeper/lakekeeper/issues/1744)) ([3af2055](https://github.com/lakekeeper/lakekeeper/commit/3af2055c19f4e54db4bd90ec852dcef27d63eb64))
* **storage:** harden STS/CEL credential policies against path injection ([#1740](https://github.com/lakekeeper/lakekeeper/issues/1740)) ([8531e7b](https://github.com/lakekeeper/lakekeeper/commit/8531e7b210a7e953749ff180f780a767efba6fbb))


### Miscellaneous Chores

* **docs:** Update Cedarschema ([cf796f6](https://github.com/lakekeeper/lakekeeper/commit/cf796f6de006fcd2f110b5bc384befa6e27f0a93))

## [0.12.1](https://github.com/lakekeeper/lakekeeper/compare/v0.12.0...v0.12.1) (2026-05-04)


### Features

* **audit:** include inner check tuples and decisions in introspect_permission audit logs ([#1697](https://github.com/lakekeeper/lakekeeper/issues/1697)) ([154be89](https://github.com/lakekeeper/lakekeeper/commit/154be89e85cc8dbee67182ab686ff6fcd757790b))
* **authz:** add data-plane Select action for views ([#1721](https://github.com/lakekeeper/lakekeeper/issues/1721)) ([58aaaca](https://github.com/lakekeeper/lakekeeper/commit/58aaacad3b67a71fe07b1c0cd8f19d8a606e2c91))
* **authz:** add RequireServerActionError variant to AuthZError ([#1719](https://github.com/lakekeeper/lakekeeper/issues/1719)) ([55eb67e](https://github.com/lakekeeper/lakekeeper/commit/55eb67ef3c0dbc043d81bc2a0c6b5dc5be71b909))
* **authz:** expose resolve_principal for downstream API-to-authz UserOrRole conversion ([#1703](https://github.com/lakekeeper/lakekeeper/issues/1703)) ([511fdf2](https://github.com/lakekeeper/lakekeeper/commit/511fdf2b47cba311162e5f0a84d609921d13fe3a))
* **authz:** rebuild and reconcile OpenFGA hierarchy tuples from catalog ([#1731](https://github.com/lakekeeper/lakekeeper/issues/1731)) ([551949f](https://github.com/lakekeeper/lakekeeper/commit/551949f01bd1c9ba8a34b8ed265a92e798d7738f))
* **authz:** support switching to OpenFGA via reconcile + reopen-bootstrap ([#1733](https://github.com/lakekeeper/lakekeeper/issues/1733)) ([c5352a1](https://github.com/lakekeeper/lakekeeper/commit/c5352a187391a498bee83429feb16f72c3153682))
* Extend Server Info with console information and commit SHAs ([#1725](https://github.com/lakekeeper/lakekeeper/issues/1725)) ([d47fd55](https://github.com/lakekeeper/lakekeeper/commit/d47fd55dc1cbe25e878a018aa92e1592b12102d7))
* Instance Admins ([#1716](https://github.com/lakekeeper/lakekeeper/issues/1716)) ([22d7602](https://github.com/lakekeeper/lakekeeper/commit/22d7602f7940babf1386f6abecaffd7aadd63ee6))
* **opa-bridge:** allow CreateViewWithSelectFromColumns on views so view-on-view queries pass the run-as-owner check ([#1712](https://github.com/lakekeeper/lakekeeper/issues/1712)) ([63d4020](https://github.com/lakekeeper/lakekeeper/commit/63d4020b783006f153f4d27d21655ae908093af5))
* **opa-bridge:** Support "ADD_FILES" trino operation ([199e7a2](https://github.com/lakekeeper/lakekeeper/commit/199e7a2d3bdcbc7bdfa61e1d07ec866d643a0882))
* **perf:** add warehouse/namespace broad-access fast path to Trino OPA batch ([#1727](https://github.com/lakekeeper/lakekeeper/issues/1727)) ([5483b69](https://github.com/lakekeeper/lakekeeper/commit/5483b69b1158789cddbebb9dfb876e75458796fd))
* protect immutable table properties (encryption.key-id) during commits ([#1700](https://github.com/lakekeeper/lakekeeper/issues/1700)) ([d2e7787](https://github.com/lakekeeper/lakekeeper/commit/d2e778760e5bfd6ba3e0d69e49adb01a2f47d987))
* reject case variants of protected view properties ([#1724](https://github.com/lakekeeper/lakekeeper/issues/1724)) ([9edf577](https://github.com/lakekeeper/lakekeeper/commit/9edf5777f8a1970d3e62da8774794f1a3e888d34))
* **storage:** drop opendal, validate vended creds via lakekeeper_io ([#1737](https://github.com/lakekeeper/lakekeeper/issues/1737)) ([62add79](https://github.com/lakekeeper/lakekeeper/commit/62add79da1b115c4f4a7aface08ca304ceb0f399))
* Validation for referenced-by parameter for trusted engines merged ([#1647](https://github.com/lakekeeper/lakekeeper/issues/1647)) ([ac0d70e](https://github.com/lakekeeper/lakekeeper/commit/ac0d70e1488e170a79f7dc9f150bacbec6ef9171))


### Bug Fixes

* Add webpki_root_certs to S3 Client, ubi native certs ([#1720](https://github.com/lakekeeper/lakekeeper/issues/1720)) ([8fec273](https://github.com/lakekeeper/lakekeeper/commit/8fec27375cdddbc725ffa13e73f549239ba05594))
* allow renaming tables to a different case of their own name ([7c26309](https://github.com/lakekeeper/lakekeeper/commit/7c263091f255b75ed5d66024b5bc6b29ef553508))
* **authz:** collapse user == actor to None in authz batch wrapper ([#1715](https://github.com/lakekeeper/lakekeeper/issues/1715)) ([bd6507d](https://github.com/lakekeeper/lakekeeper/commit/bd6507dfd20d4441f84228251ee54aec6227922f))
* **opa-bridge:** add ReadSystemInformation rule and trim admin user entries ([e251740](https://github.com/lakekeeper/lakekeeper/commit/e25174065b41846968c24b30ca3d448a57f23bed))
* pin gcloud-storage/gcloud-auth to ~1.2 to prevent reqwest-middleware version conflict ([#1701](https://github.com/lakekeeper/lakekeeper/issues/1701)) ([a3cfa87](https://github.com/lakekeeper/lakekeeper/commit/a3cfa87bf2f411b235a11e40c6494660e450585f))
* return caller's case from namespace lookups, canonical from id lookups ([7c26309](https://github.com/lakekeeper/lakekeeper/commit/7c263091f255b75ed5d66024b5bc6b29ef553508))
* **storage/adls:** remove the actual matched SAS token key, not the prefix ([76a091b](https://github.com/lakekeeper/lakekeeper/commit/76a091b9b01ba507cc448a56241d54f526c19a14))
* **ui:** Fix base url trailing slash - 0.13.15 ([#1729](https://github.com/lakekeeper/lakekeeper/issues/1729)) ([a5cc2c6](https://github.com/lakekeeper/lakekeeper/commit/a5cc2c6f16372fef9f856509af1bc28bdf0e39fd))
* **ui:** Fix UI Authentication due to Vite 8 changes ([#1723](https://github.com/lakekeeper/lakekeeper/issues/1723)) ([e2e8502](https://github.com/lakekeeper/lakekeeper/commit/e2e8502ad000dc3b042751fca5d7cad7d189bff2))
* **ui:** pass warehouse-name to TablePreview to fix stale name after rename ([d2c7cef](https://github.com/lakekeeper/lakekeeper/commit/d2c7cef5fe95acd0d179b88a2740c82a8db6201c))
* **ui:** wipe persisted state on fresh bootstrap, detach LoQE catalog on warehouse rename ([e12ae4e](https://github.com/lakekeeper/lakekeeper/commit/e12ae4eef2b75576329322977b224ace1306f4ac))


### Documentation

* document case sensitivity design and fix case-only rename conflict ([7c26309](https://github.com/lakekeeper/lakekeeper/commit/7c263091f255b75ed5d66024b5bc6b29ef553508))
* Fluss example ([#1688](https://github.com/lakekeeper/lakekeeper/issues/1688)) ([6f5a52a](https://github.com/lakekeeper/lakekeeper/commit/6f5a52aa2f4edf15af027e97db9e41bb2678d8ce))
* support deploying into a custom Postgres schema ([#1714](https://github.com/lakekeeper/lakekeeper/issues/1714)) ([fcab053](https://github.com/lakekeeper/lakekeeper/commit/fcab053eef0edb1799b5b25ea8721c84c33bb457))


### Miscellaneous Chores

* Add AGENTS.md ([46870c5](https://github.com/lakekeeper/lakekeeper/commit/46870c5569ef3cac6c1588875238110430290f50))
* **ci:** pin actions to SHA, add cargo-machete, nightly toolchain, MSRV 1.92 ([#1705](https://github.com/lakekeeper/lakekeeper/issues/1705)) ([87a11d5](https://github.com/lakekeeper/lakekeeper/commit/87a11d5045c32ddd291b22af88794e7b9290784a))
* **deps:** bump iceberg-rust, adopt iceberg-storage-opendal split ([#1736](https://github.com/lakekeeper/lakekeeper/issues/1736)) ([ed96701](https://github.com/lakekeeper/lakekeeper/commit/ed967015b9471f1e2a6a604cccb07db02cfbcf9d))
* **deps:** bump rustls-webpki 0.103 to 0.103.12 for RUSTSEC-2026-0098 ([#1713](https://github.com/lakekeeper/lakekeeper/issues/1713)) ([d1f797d](https://github.com/lakekeeper/lakekeeper/commit/d1f797db28c396eb42fe9f412e2d71a545428f74))
* **docs:** 0.12 docs ([63bbcff](https://github.com/lakekeeper/lakekeeper/commit/63bbcffa7d2596a5308dcae8f1fa92d1a9654550))
* **docs:** Improve `get_metadata` vs `select` docs ([7b84288](https://github.com/lakekeeper/lakekeeper/commit/7b8428896404cbd7a2ac83d667b601f7ac4e500b))
* **docs:** Improve Instance Admin docs ([8674abb](https://github.com/lakekeeper/lakekeeper/commit/8674abbb3634e1501af561804aa7ce35d0e6f78d))
* **docs:** Update numbers ([973d7a9](https://github.com/lakekeeper/lakekeeper/commit/973d7a97ab46383f995342d78440e145a4196a05))
* Make LakekeeperStorage dyn-Compatible ([#1728](https://github.com/lakekeeper/lakekeeper/issues/1728)) ([966675d](https://github.com/lakekeeper/lakekeeper/commit/966675d29b7152f815d0f19f7d417e9f0049a77e))
* return standartized NoSuchWarehouseException for warehouse-not-found errors ([#1702](https://github.com/lakekeeper/lakekeeper/issues/1702)) ([3ac0fa8](https://github.com/lakekeeper/lakekeeper/commit/3ac0fa80c18586ba62abae50991fa5f24bb4df5f))
* **ui:** Compress files ([#1738](https://github.com/lakekeeper/lakekeeper/issues/1738)) ([0368cdc](https://github.com/lakekeeper/lakekeeper/commit/0368cdc19d79ea0713fb086f835bf0b64128f002))

## [0.12.0](https://github.com/lakekeeper/lakekeeper/compare/v0.11.2...v0.12.0) (2026-04-01)


### ⚠ BREAKING CHANGES

* unify cache metrics under shared names with cache_type label ([#1641](https://github.com/lakekeeper/lakekeeper/issues/1641))
* Use structured log format that uses objects as log values

### Features

* Add audit event handler ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* Add AuthorizerValidationFailed Error ([#1624](https://github.com/lakekeeper/lakekeeper/issues/1624)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Add Configuration of TrustedEngines and add to Request Metadata ([#1629](https://github.com/lakekeeper/lakekeeper/issues/1629)) ([83317c2](https://github.com/lakekeeper/lakekeeper/commit/83317c2ec9f7632bf81e64da4fa41720d17027e2))
* Add FileInfo to `LakekeeperStorage::list` ([#1605](https://github.com/lakekeeper/lakekeeper/issues/1605)) ([e96afc0](https://github.com/lakekeeper/lakekeeper/commit/e96afc004a1e72ec0f3ffff50ba4176a084763de))
* Add in-memory Roles Cache ([#1623](https://github.com/lakekeeper/lakekeeper/issues/1623)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Add non-productive debug option to log authorization header ([#1613](https://github.com/lakekeeper/lakekeeper/issues/1613)) ([aff61ba](https://github.com/lakekeeper/lakekeeper/commit/aff61ba588a0623918046437d1ca3b2e7a26cbb7))
* add OPA batch optimization for Lakekeeper-managed catalogs ([#1674](https://github.com/lakekeeper/lakekeeper/issues/1674)) ([a8045ed](https://github.com/lakekeeper/lakekeeper/commit/a8045edde7c26fa182f1d8c3fef8d9f36e89e08a))
* add resource name to Create* authorizer action contexts ([#1657](https://github.com/lakekeeper/lakekeeper/issues/1657)) ([0de3544](https://github.com/lakekeeper/lakekeeper/commit/0de354421581e9f8f29b99f7a557b6e84c9e515d))
* add role lifecycle events and improve actor serialization ([#1622](https://github.com/lakekeeper/lakekeeper/issues/1622)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Adds max_request_body_size and max_request_time configuration variables ([#1583](https://github.com/lakekeeper/lakekeeper/issues/1583)) ([bbdf892](https://github.com/lakekeeper/lakekeeper/commit/bbdf89201bfa6a08ab1b24e0c48eae1206f319cc))
* **authz:** add Trino custom rule extension point and configurable admin users ([47fcb6a](https://github.com/lakekeeper/lakekeeper/commit/47fcb6ad2482f545a9ecb20931fcb25f8774ee88))
* Configurable STS Endpoint ([#1653](https://github.com/lakekeeper/lakekeeper/issues/1653)) ([40b41d4](https://github.com/lakekeeper/lakekeeper/commit/40b41d4a397268f9ae9c9931a175768fb939e6a5))
* Customization Option for Storage Layout ([#1615](https://github.com/lakekeeper/lakekeeper/issues/1615)) ([ed970ca](https://github.com/lakekeeper/lakekeeper/commit/ed970ca233144f7bb7718ee4454a2414a08951d4))
* **docs:** Add documentation for Logging ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* extend OPA bridge with admin users, system schema handling, and context forwarding ([#1662](https://github.com/lakekeeper/lakekeeper/issues/1662)) ([40ef2e9](https://github.com/lakekeeper/lakekeeper/commit/40ef2e968eacbc81242accd3c512311b6822b738))
* Fallback subject claims ([#1646](https://github.com/lakekeeper/lakekeeper/issues/1646)) ([09ff52e](https://github.com/lakekeeper/lakekeeper/commit/09ff52e3fa3eb412920336f5c24a62079a420b64))
* Idempotency Keys ([#1671](https://github.com/lakekeeper/lakekeeper/issues/1671)) ([6bba368](https://github.com/lakekeeper/lakekeeper/commit/6bba368793eb3c8fcf2a61ba9ddd1bba2d1268a6))
* Improve action logs in audit ([#1610](https://github.com/lakekeeper/lakekeeper/issues/1610)) ([e3c1c97](https://github.com/lakekeeper/lakekeeper/commit/e3c1c97cdec6cc4ce531deaaa0df9a37aa98256e))
* Improve list namespaces, tables and views query performance ([#1618](https://github.com/lakekeeper/lakekeeper/issues/1618)) ([49efe9c](https://github.com/lakekeeper/lakekeeper/commit/49efe9c0c6eb1f50f20b0fc1930be821ada0f3c7))
* Improve StorageLayout and remove inner API from docs. Add validation for StorageLayouts ([#1628](https://github.com/lakekeeper/lakekeeper/issues/1628)) ([0f34f8d](https://github.com/lakekeeper/lakekeeper/commit/0f34f8dff03b3e81f24bd2c862ba0bd80c3f1ce6))
* include S3 multipart upload actions in STS session policy ([#1652](https://github.com/lakekeeper/lakekeeper/issues/1652)) ([0664778](https://github.com/lakekeeper/lakekeeper/commit/06647780b75d181be726d0b6d39836d2bcd84ab9))
* include storage credential type in GetWarehouseResponse ([#1668](https://github.com/lakekeeper/lakekeeper/issues/1668)) ([7ca148f](https://github.com/lakekeeper/lakekeeper/commit/7ca148ff140cb62eb2fd2dd4a8ccc9d890f0dec8))
* Interior Mutability for EventDispatcher ([#1649](https://github.com/lakekeeper/lakekeeper/issues/1649)) ([7589b17](https://github.com/lakekeeper/lakekeeper/commit/7589b1783690bb2c62c02b610dc5b6a355ce62d5))
* Introduce Authorization events with exactly once per API-Call guarantee ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))
* Introduce string-based RoleIdent with provider-scoped role identifiers ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Parse roles from tokens into identifiers ([#1625](https://github.com/lakekeeper/lakekeeper/issues/1625)) ([add0654](https://github.com/lakekeeper/lakekeeper/commit/add06545a0215f3786195a386252447c6c5a9a90))
* Parsing referenced-by query parameter ([#1627](https://github.com/lakekeeper/lakekeeper/issues/1627)) ([87c078f](https://github.com/lakekeeper/lakekeeper/commit/87c078f638d5db6c4b69f17aba74e375f0b97b79))
* Reduce memory footprint by switching to jemalloc from ptmalloc ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23))
* Role Assignment Store & Cache ([#1638](https://github.com/lakekeeper/lakekeeper/issues/1638)) ([cc0d6f8](https://github.com/lakekeeper/lakekeeper/commit/cc0d6f8b2e1aa36676cc41580c30319b7d63b20e))
* Spark 4 Integration Tests ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c))
* Support V3 Variant Datatype ([daa7947](https://github.com/lakekeeper/lakekeeper/commit/daa7947333097b25e09a91281a6057d334db599c))
* Tokio Metrics ([#1664](https://github.com/lakekeeper/lakekeeper/issues/1664)) ([294dbbf](https://github.com/lakekeeper/lakekeeper/commit/294dbbf066bb335c6552220e64aa4598dc14cbb7))
* **ui:** Add branch operations: create, rename, delete, rollback, and fast-forward ([66e768e](https://github.com/lakekeeper/lakekeeper/commit/66e768ee630804d04f4274922985d8a2ee32aad2))
* **ui:** Add branch operations: create, rename, delete, rollback, and fast-forward ([66e768e](https://github.com/lakekeeper/lakekeeper/commit/66e768ee630804d04f4274922985d8a2ee32aad2))
* **ui:** add community action cards (Star, Contribute, Share on LinkedIn) to Home page ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* **ui:** add HomeStatistics dashboard with project/warehouse/table/view counts and API calls area chart ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* **ui:** Add Properties Dialog for editing table/view/namespace properties ([66e768e](https://github.com/lakekeeper/lakekeeper/commit/66e768ee630804d04f4274922985d8a2ee32aad2))
* **ui:** add WarehouseStatistics tab with D3 stacked area charts and server-side filtering ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* **ui:** move GitHub stars to AppBar linked to repository ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* **ui:** pass storageLayout prop to NamespaceTables and NamespaceViews ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* **ui:** Redesign branch visualization snapshot detail panel with card layout ([66e768e](https://github.com/lakekeeper/lakekeeper/commit/66e768ee630804d04f4274922985d8a2ee32aad2))
* **ui:** update Contributing section in README to point to shared CONTRIBUTING.md ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* unify cache metrics under shared names with cache_type label ([#1641](https://github.com/lakekeeper/lakekeeper/issues/1641)) ([894c01e](https://github.com/lakekeeper/lakekeeper/commit/894c01eb445ac9fc6759e021487f49f849fd4b36))
* Update Console to a78db64 ([#1611](https://github.com/lakekeeper/lakekeeper/issues/1611)) ([26040ce](https://github.com/lakekeeper/lakekeeper/commit/26040ce30e20ebe0548a22d56908ab97f1b66dc2))
* Update UI - Add Local Query Engine with memory Management ([#1621](https://github.com/lakekeeper/lakekeeper/issues/1621)) ([1aff402](https://github.com/lakekeeper/lakekeeper/commit/1aff402454b4095f06e4a1ab287d33f1ba6f2d21))
* Update UI - Add Storage Layout configuration ([#1634](https://github.com/lakekeeper/lakekeeper/issues/1634)) ([3c6e351](https://github.com/lakekeeper/lakekeeper/commit/3c6e351d8dab1eb4b5c1a9caad52785ab2aa70b8))
* Use structured log format that uses objects as log values ([b77c687](https://github.com/lakekeeper/lakekeeper/commit/b77c68740a67221669acaa122742b3912d48aeb5))


### Bug Fixes

* Allow Storage Profile region update if S3 endpoint is set ([#1678](https://github.com/lakekeeper/lakekeeper/issues/1678)) ([4e7db35](https://github.com/lakekeeper/lakekeeper/commit/4e7db3534092b6c2a6d271f561ede390c1cbecc9))
* bump aws-lc-sys to 0.38.0 to patch GHSA-vw5v-4f2q-w9xf ([#1644](https://github.com/lakekeeper/lakekeeper/issues/1644)) ([e5e2d64](https://github.com/lakekeeper/lakekeeper/commit/e5e2d649a5a3c8cfdd67337efd226d0b5e8d11b0))
* **ci:** Pin botocore & s3fs Versions ([#1603](https://github.com/lakekeeper/lakekeeper/issues/1603)) ([735ee8c](https://github.com/lakekeeper/lakekeeper/commit/735ee8c24afc145124e98a2d82ab6f5118f236ec))
* **docs:** Add missing `TOKEN_TYPE` config for Google IdP ([de24e13](https://github.com/lakekeeper/lakekeeper/commit/de24e13e26edf0887e9d93f3e4e9f9d2d89b4de9))
* **docs:** Cedarschema download link ([0808a66](https://github.com/lakekeeper/lakekeeper/commit/0808a6682eedc2082b0d7361270bd3d2a91b63f5))
* **docs:** linebreaks for Cedar configuration ([aef6020](https://github.com/lakekeeper/lakekeeper/commit/aef6020492df4df9886f24b96b0f8dea332d5029))
* list_tabulars returning duplicate pagination results ([#1682](https://github.com/lakekeeper/lakekeeper/issues/1682)) ([#1684](https://github.com/lakekeeper/lakekeeper/issues/1684)) ([379a0f7](https://github.com/lakekeeper/lakekeeper/commit/379a0f7b8a62f6a4d1f18dd927720168b6419b7a))
* rename S3 credential fields to drop aws_ prefix, add alias for backwards compatibility ([#1685](https://github.com/lakekeeper/lakekeeper/issues/1685)) ([c803c0d](https://github.com/lakekeeper/lakekeeper/commit/c803c0d13ca4f459c0afd16963615a7b2ec22a9b))
* S3 identity cache memory leak ([0eaeedc](https://github.com/lakekeeper/lakekeeper/commit/0eaeedc8411120f18ec9229b4dd08c36dd294d23))
* **security:** update crypto dependencies to patch CVE in aws-lc-sys, rustls-webpki ([#1672](https://github.com/lakekeeper/lakekeeper/issues/1672)) ([8f8e4de](https://github.com/lakekeeper/lakekeeper/commit/8f8e4de24654dd9935cd06ac178b88e3b71126d9))
* **ui:** Better Safari support, Add Task Log Cleanup configuration ([#1593](https://github.com/lakekeeper/lakekeeper/issues/1593)) ([4738d4a](https://github.com/lakekeeper/lakekeeper/commit/4738d4a925f43fd7bcd51bb623e1d4c98e91c266))
* **ui:** remove deprecated GitHub stats chips, quick access cards, and routeToRoles from Home ([ba5c0b1](https://github.com/lakekeeper/lakekeeper/commit/ba5c0b187d2e888daf183197dd4964159e82dcf4))
* update lz4_flex to 0.12.1 to patch memory leak vulnerability (GHSA-vvp9-7p8x-rfvv) ([#1665](https://github.com/lakekeeper/lakekeeper/issues/1665)) ([913ebd2](https://github.com/lakekeeper/lakekeeper/commit/913ebd23b47da231e0ebe47f831cc4c53884dd91))


### Miscellaneous Chores

* add monitoring documentation ([#1648](https://github.com/lakekeeper/lakekeeper/issues/1648)) ([c7174ad](https://github.com/lakekeeper/lakekeeper/commit/c7174adfb604f6b9d35ae6094e197f5b2333260f))
* Bump node to 24 (LTS), UI 1c6a3bb ([#1660](https://github.com/lakekeeper/lakekeeper/issues/1660)) ([3e91f6e](https://github.com/lakekeeper/lakekeeper/commit/3e91f6ed3649effc5acaf547937ead096b554bda))
* **ci:** Use LAKEKEEPER_TEST prefix for AWS variables ([#1679](https://github.com/lakekeeper/lakekeeper/issues/1679)) ([31ef80a](https://github.com/lakekeeper/lakekeeper/commit/31ef80a501dd13afbbe43967f5eb9cc16cc573ec))
* **deps:** Add cargo-deny with License checker ([#1580](https://github.com/lakekeeper/lakekeeper/issues/1580)) ([b1cdcd5](https://github.com/lakekeeper/lakekeeper/commit/b1cdcd51f715a24f8a258879e10f6e8a8c49ce41))
* **deps:** update rust crate bytes to v1.11.1 [security] ([#1597](https://github.com/lakekeeper/lakekeeper/issues/1597)) ([0ec6a00](https://github.com/lakekeeper/lakekeeper/commit/0ec6a008f674b8dca87a1ed813e345b9973e1a57))
* **deps:** update rust crate jsonwebtoken to v10.3.0 [security] ([#1598](https://github.com/lakekeeper/lakekeeper/issues/1598)) ([93bd887](https://github.com/lakekeeper/lakekeeper/commit/93bd887abf799fbc17d31721cb216ee3628780c0))
* **deps:** update rust crate time to v0.3.47 [security] ([#1600](https://github.com/lakekeeper/lakekeeper/issues/1600)) ([bc994e5](https://github.com/lakekeeper/lakekeeper/commit/bc994e542f108fc9a502ff92d7365d9723761c10))
* **docs:** Add Lakekeeper Plus license configuration ([#1595](https://github.com/lakekeeper/lakekeeper/issues/1595)) ([239dc5d](https://github.com/lakekeeper/lakekeeper/commit/239dc5d97b312dfedbf1d9eaef4288e5234e5625))
* **docs:** add opt-in audit event for resolved role assignments ([20044c2](https://github.com/lakekeeper/lakekeeper/commit/20044c2c059b67f9a4d0be48655fd11f16ee4f35))
* **docs:** Cedar & Role Provider docs ([#1651](https://github.com/lakekeeper/lakekeeper/issues/1651)) ([5bf1312](https://github.com/lakekeeper/lakekeeper/commit/5bf13128dd6e3dc34fba7572caec8af3092bd0d2))
* **docs:** Cedar User Derivations Transformations ([8ba8862](https://github.com/lakekeeper/lakekeeper/commit/8ba886296666f4140ff19d30902d530cff30e429))
* **docs:** Cedar User Identity Derivations ([a86301f](https://github.com/lakekeeper/lakekeeper/commit/a86301f5c65085fead6095fe1da6c5214e18389e))
* **docs:** Clarify Cedar property value ([0a257d0](https://github.com/lakekeeper/lakekeeper/commit/0a257d0c34a418e8ec28aa88cdf3ca0193917131))
* **docs:** document Azure token v1/v2 issuer differences, fix S3 CORS headers ([527b748](https://github.com/lakekeeper/lakekeeper/commit/527b7484950bacd53c45478a1fd0a8afe42dc1f0))
* **docs:** Document Cedar global_role_ids for ParsedProperties ([6a6a5d6](https://github.com/lakekeeper/lakekeeper/commit/6a6a5d6301589e2726d0ab15478aaf8aaedb5059))
* **docs:** Fix Role-Provider heading style ([85520d1](https://github.com/lakekeeper/lakekeeper/commit/85520d1435efa599941dc71a47d5fca422621205))
* **docs:** Group Provider TOML configuration ([624abf9](https://github.com/lakekeeper/lakekeeper/commit/624abf9935dbeae5e553950fef4d1d19d167c235))
* **docs:** Improve cedar docs ([#1587](https://github.com/lakekeeper/lakekeeper/issues/1587)) ([30e04f7](https://github.com/lakekeeper/lakekeeper/commit/30e04f7c8646e0c9163c32d7ad5a1eb0f0262380))
* **events:** restructure event system service::events with clearer naming conventions ([#1602](https://github.com/lakekeeper/lakekeeper/issues/1602)) ([f6aaa45](https://github.com/lakekeeper/lakekeeper/commit/f6aaa4570cf62d54943581e7a00b9f882d9c992d))
* **hooks:** restructure EndpointHook to use event structs ([#1601](https://github.com/lakekeeper/lakekeeper/issues/1601)) ([7db8da6](https://github.com/lakekeeper/lakekeeper/commit/7db8da6eb001979f7bf4d3c1cf1e9e21ad2250e5))
* **opa-bridge:** Add CreateViewWithSelectFromColumns permissions based on "read_data" of underlying table ([#1594](https://github.com/lakekeeper/lakekeeper/issues/1594)) ([c378f6e](https://github.com/lakekeeper/lakekeeper/commit/c378f6e69f0e11e56d992c32b3939e73a4ed0ef0))
* **opa:** Format policies, strict checking in CI ([#1666](https://github.com/lakekeeper/lakekeeper/issues/1666)) ([155b4ae](https://github.com/lakekeeper/lakekeeper/commit/155b4ae90dc00a0102bb3f355371a358b97f7aad))
* **test:** Extend tests for v2 to v3 migration with data ([#1691](https://github.com/lakekeeper/lakekeeper/issues/1691)) ([d926bbf](https://github.com/lakekeeper/lakekeeper/commit/d926bbfc7351934f15a96f370a0d0c1b3b9f631b))
* **ui:** Update UI to 0.13.2 ([#1686](https://github.com/lakekeeper/lakekeeper/issues/1686)) ([1d79d98](https://github.com/lakekeeper/lakekeeper/commit/1d79d98a8079e7573945ff522acedcfb0ab6cd2b))

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
