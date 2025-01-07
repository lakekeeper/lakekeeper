# Production Checklist

Lakekeeper is the heart of your data platform and needs to integrate deeply with your existing infrastructure such as IdPs. The easiest way to get Lakekeeper to production is our enterprise support. Please find more information on our commercial offerings at [lakekeeper.io](https://lakekeeper.io)

Please find following some general recommendations for productive setups:

* Use an external high-available database as a catalog backend. We recommend using a managed service in your preferred Cloud or host a high available cluster on Kubernetes yourself using your preferred operator. We are using the amazing [CloudNativePG](https://cloudnative-pg.io) internally. Make sure the Database is backed-up regularly.
* Ensure sure both `LAKEKEEPER__PG_DATABASE_URL_READ` and `LAKEKEEPER__PG_DATABASE_URL_WRITE` are set for ideal load distribution. Most postgres deployments specify separate URLs for reading and writing to channel writes to the master while distributing reads across replicas.
* For high-available setups, ensure that multiple Lakekeeper instances are running on different nodes. We recommend our [helm chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper) for production deployments.
* Ensure that Authentication is enabled, typically by setting `LAKEKEEPER__OPENID_PROVIDER_URI` and / or `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION`. Check our [Authentication Guide](./authentication.md) for more information.
* If `LAKEKEEPER__OPENID_PROVIDER_URI` is set, we recommend to set `LAKEKEEPER__OPENID_AUDIENCE` as well.
* If Authorization is desired, follow our [Authorization Guide](./authorization.md). Ensure that OpenFGA is hosted in close proximity to Lakekeeper - ideally on the same VM or Kubernetes node. In our Helm-Chart we use `PodAffinity` to achieve this.
* If the default Postgres secret backend is used, ensure that `LAKEKEEPER__PG_ENCRYPTION_KEY` is set to a long random string.
* Ensure that all Warehouses use distinct storage locations / prefixes and distinct credentials that only grant access to the prefix used for a Warehouse.
* Ensure that SSL / TLS is enabled. Lakekeeper does not terminate connections natively. Please use a reverse proxy like Nginx or Envoy to secure the connection to Lakekeeper. On Kubernetes, any Ingress controller can be used. For high-availability, failover should be handled by the reverse proxy. Lakekeeper exposes a `/health` endpoint that can be used to determine its current status. If you are using our helm-chart, probes are already built-in.
* If a trusted query engine, such as a centrally managed trino, uses Lakekeeper's OPA bridge, ensure that no users have root access to trino or OPA as those contain credentials to Lakekeeper with very high permissions.
