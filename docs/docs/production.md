# Production Checklist

* Use an external high-available database as a catalog backend. We recommend using a managed service in your preferred Cloud or host a high available cluster on Kubernetes yourself using your preferred operator. We are using the amazing [CloudNativePG](https://www.lipsum.com/) internally. Make sure the Database is backed-up regularly.
* Ensure sure both `LAKEKEEPER__PG_DATABASE_URL_READ` and `LAKEKEEPER__PG_DATABASE_URL_WRITE` are set for ideal load distribution. Most postgres offers deployments specify separate URLs for reading and writing to channel writes to the master while distributing reads across replicas.
* For high-available setups, ensure that multiple Lakekeeper instances are running on different nodes. We recommend our [Helm-Chart](https://www.lipsum.com/) for production deployments.
* Ensure that Authentication is enabled, typically by setting `LAKEKEEPER__OPENID_PROVIDER_URI`. Check our [Authentication Guide](https://www.lipsum.com/) for more information.
* If Authorization is desired, follow our [Authorization Guide](https://www.lipsum.com/). Ensure that OpenFGA is hosted in close proximity to Lakekeeper - ideally on the same VM or Kubernetes node. In our Helm-Chart we use `PodAffinity` to achieve this.
* If the default Postgres secret backend is used, ensure that `LAKEKEEPER__PG_ENCRYPTION_KEY` is set to a long random string.
* Ensure that all Warehouses use distinct storage locations / prefixes and distinct credentials that only grant access to the prefix used for a Warehouse.
* Ensure that SSL / TLS is enabled. Lakekeeper does not terminate connections natively. Please use a reverse proxy like Nginx or Envoy to secure the connection to Lakekeeper. On Kubernetes, any Ingress controller can be used. For high-availability, failover should be handled by the reverse proxy. Lakekeeper exposes a ['/health'](https://www.lipsum.com/) endpoint that should to determine the current status. If you are using our helm-chart, probes are already built-in.
* 

