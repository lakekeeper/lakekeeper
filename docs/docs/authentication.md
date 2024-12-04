# Authentication
Authentication is crucial for securing access to Lakekeeper. By enabling authentication, you ensure that only authorized users can access and interact with your data. Lakekeeper supports authentication via any OpenID (or OAuth 2) capable identity provider as well as authentication for Kubernetes service accounts, allowing you to integrate with your existing identity providers.

Authentication and Authorization are distinct processes in Lakekeeper. Authentication verifies the identity of users, ensuring that only authorized individuals can access the system. This is performed via an Identity Provider (IdP) such as OpenID or Kubernetes. Authorization, on the other hand, determines what authenticated users are allowed to do within the system. Lakekeeper uses OpenFGA to manage and evaluate permissions, providing a robust and flexible authorization model. For more details, see the [Authorization guide](./authorization.md).

Lakekeeper does not issue API-Keys or Client-Credentials itself, as this can introduce multiple security risks. Instead, it relies on external IdPs for authentication, ensuring a secure and centralized management of user identities. This approach minimizes the risk of credential leakage and simplifies the integration with existing security infrastructures.

## OpenID Provider
Lakekeeper can be configured to integrate with all common identity providers. For best performance, tokens are validated locally against the server keys (`jwks_uri`). This requires all incoming tokens to be JWT tokens. If you require support for opaque tokens, please upvote the corresponding [Github Issue](https://github.com/lakekeeper/lakekeeper/issues/620).

If `LAKEKEEPER__OPENID_PROVIDER_URI` is specified, Lakekeeper will  verify access tokens against this provider. The provider must provide the `.well-known/openid-configuration` endpoint and the openid-configuration needs to have `jwks_uri` and `issuer` defined. Optionally, if `LAKEKEEPER__OPENID_AUDIENCE` is specified, Lakekeeper validates the `aud` field of the provided token to match the specified value. We recommend to specify the audience in all deployments, so that tokens leaked for other applications in the same IdP cannot be used to access data in Lakekeeper.

In the following section we describe common setups for popular IdPs. Please refer to the documentation of your IdP for further information.

### Entra-ID (Azure)
We are creating two App-Registrations: One for Lakekeeper and a second one for a machine client (Spark) to access Lakekeeper.

#### 1. Lakekeeper Application
1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Lakekeeper`
    - **Redirect URI**:

ToDo

## Kubernetes
If `LAKEKEEPER__ENABLE_KUBERNETES_AUTHENTICATION` is set to true, Lakekeeper validates incoming tokens against the default kubernetes context of the system. Lakekeeper uses the [`TokenReview`](https://kubernetes.io/docs/reference/kubernetes-api/authentication-resources/token-review-v1/) to determine the validity of a token. By default the `TokenReview` resource is protected. When deploying Lakekeeper on Kubernetes, make sure to grant the `system:auth-delegator` Cluster Role to the service account used by Lakekeeper:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: allow-token-review
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: system:auth-delegator
subjects:
- kind: ServiceAccount
  name: <lakekeeper-serviceaccount>
  namespace: <lakekeeper-namespace>
```
The [Lakekeeper Helm Chart](https://github.com/lakekeeper/lakekeeper-charts/tree/main/charts/lakekeeper) creates the required binding by default.
