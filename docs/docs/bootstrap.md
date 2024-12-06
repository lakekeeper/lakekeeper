# Bootstrap / Initialize
After the initial deployment, Lakekeeper needs to be bootstrapped. This can be done via the UI or the `/management/v1/bootstrap` endpoint. A typical POST request to bootstrap Lakekeeper looks like this:

```bash
curl --location 'https://<lakekeeper-url>/management/v1/bootstrap' \
--header 'Content-Type: application/json' \
--header 'Authorization: Bearer <my-bearer-token>' \
--data '{
    "accept-terms-of-use": true
}'
```

`<my-bearer-token>` is obtained by logging into the IdP before bootstrapping Lakekeeper. If authentication is disabled, no token is required. Lakekeeper can only be bootstrapped once.

During bootstrapping, Lakekeeper performs the following actions:

* Grants the server's `admin` role to the user performing the POST request. The user is identified by their token. If authentication is disabled, the `Authorization` header is not required, and no `admin` is set, as permissions are disabled in this case.
* Stores the current [Server ID](./concepts.md#server) to prevent unwanted future changes that would break permissions.
* Accepts terms of use as defined by our [License](../../about/license.md).

If the initial user is a technical user (e.g., a Kubernetes Operator) managing the Lakekeeper deployment, the `admin` role might not be sufficient as it limits access to projects until the `admin` grants themselves permission. For technical users, the `operator` role grants full access to all APIs and can be obtained by adding `"is-operator": true` to the JSON body of the bootstrap request.
