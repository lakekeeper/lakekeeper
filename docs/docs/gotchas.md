# Gotchas

## I got permissions but am still getting 403s

Lakekeeper does not always return 404s for missing objects. If you are getting 403s while having correct grants, it is likely that the object you are trying to access does not exist. This is a security feature to prevent information leakage.

## I'm using Helm and the UI seems to hang forever

Both the catalog, as well as the UI create links based on `catalog.config.ICEBERG_REST__BASE_URI`. You'll have to set it in your Helm values.yaml to a URL that your clients can reach.

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