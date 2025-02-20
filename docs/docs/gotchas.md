# Gotchas

## I got permissions but am still getting 403s

Lakekeeper does not always return 404s for missing objects. If you are getting 403s while having correct grants, it is likely that the object you are trying to access does not exist. This is a security feature to prevent information leakage.

## I'm using Helm and the UI seems to hang forever

Check out [our routing guide](./configuration.md#routing-and-base-url), both the catalog and UI create links pointing at the Lakekeeper instance. We use some heuristics by default and also offer a configuration escape hatch (`catalog.config.ICEBERG_REST__BASE_URI`).

### Examples

#### Local

```ssh
k port-forward services/my-lakekeeper 7777:8181
```

```yaml
catalog:
   # omitting the rest of the values
  config:
    # assuming that the catalog is forwarded to localhost:7777
    ICEBERG_REST__BASE_URI: "http://localhost:7777"
```

#### Public

```yaml
catalog:
   # omitting the rest of the values
  config:
    # assuming that the catalog is reachable at https://lakekeeper.example.com
    ICEBERG_REST__BASE_URI: "https://lakekeeper.example.com"
```