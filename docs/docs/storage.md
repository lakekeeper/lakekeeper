# Storage

Storage in Lakekeeper is bound to a Warehouse. Each Warehouse stores data in a location defined by a `StorageProfile` attached to it.

Currently, we support the following storages:

- S3 (tested with AWS & Minio)
- Azure Data Lake Storage Gen 2
- Google Cloud Storage

When creating a Warehouse or updating storage information, Lakekeeper validates the configuration.

## S3

We support remote signing and vended-credentials with Minio & AWS. Both provide a secure way to access data on S3:

* **Remote Signing**: The client prepares an S3 request and sends its headers to the sign endpoint of Lakekeeper. Lakekeeper checks if the request is allowed, if so, it signs the request with its own credentials, creating additional headers during the process. These additional signing headers are returned to the client, which then contacts S3 directly to perform the operation on files.
* **Vended Credentials**: Lakekeeper uses the "STS" Endpoint of S3 to generate temporary credentials which are then returned to clients.

Remote signing works natively with all S3 storages that support the default `AWS Signature Version 4`. This includes almost all S3 solutions on the market today, including Minio, Rook Ceph and others. Vended credentials in turn depend on an additional "STS" Endpoint, that is not supported by all S3 implementations. We run our integration tests for vended credentials against Minio and AWS. We recommend to setup vended credentials for all supported stores, remote signing is not supported by all clients.

Remote signing relies on identifying a table by its location in the storage. Since there are multiple canonical ways to specify S3 resources (virtual-host & path), Lakekeeper warehouses by default use a heuristic to determine which style is used. For some setups these heuristics may not work, or you may want to enforce a specific style. In this case, you can set the `remote-signing-url-style` field to either `path` or `virtual-host` in your storage profile. `path` will always use the first path segment as the bucket name. `virtual-host` will use the first subdomain if it is followed by `.s3` or `.s3-`. The default mode is `auto` which first tries `virtual-host` and falls back to `path` if it fails.

### AWS

#### Direct File-Access with Access Key
First create a new S3 bucket for the warehouse. Buckets can be re-used for multiple Warehouses as long as the `key-prefix` is different. We recommend to block all public access.

Secondly we need to create an AWS role that can access and delegate access to the bucket. We start by creating a new Policy that allows access to data in the bucket. We call this policy `LakekeeperWarehouseDev`:

```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "ListBuckets",
			"Action": [
				"s3:ListAllMyBuckets",
				"s3:GetBucketLocation"
			],
			"Effect": "Allow",
			"Resource": [
				"arn:aws:s3:::*"
			]
		},
		{
			"Sid": "ListBucketContent",
			"Action": [
				"s3:ListBucket"
			],
			"Effect": "Allow",
			"Resource": "arn:aws:s3:::lakekeeper-aws-demo"
		},
		{
			"Sid": "DataAccess",
			"Effect": "Allow",
			"Action": [
				"s3:*"
			],
			"Resource": [
				"arn:aws:s3:::lakekeeper-aws-demo/*"
			]
		}
	]
}
```

Now create a new user, we call the user `LakekeeperWarehouseDev`, and attach the previously created policy. When the user is created, click on "Security credentials" and "Create access key". Note down the access key and secret key for later use.

We are done if we only rely on remote signing. For vended credentials, we need to perform one more step. Create a new role that we call `LakekeeperWarehouseDevRole`. This role needs to be trusted by the user, which is achieved via with the following trust policy:
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "TrustLakekeeperWarehouseDev",
			"Effect": "Allow",
			"Principal": {
				"AWS": "arn:aws:iam::<aws-account-id>:user/LakekeeperWarehouseDev"
			},
			"Action": "sts:AssumeRole"
		}
	]
}
```

Also attach the `LakekeeperWarehouseDev` policy created earlier.

We are now ready to create the Warehouse via the UI or REST-API using the following values (make sure to replace everything in `<>`):

```json
{
    "warehouse-name": "aws_docs",
    "storage-credential": {
        "type": "s3",
        "aws-access-key-id": "<Access Key of the created user>",
        "aws-secret-access-key": "<Secret Key of the created user>",
        "credential-type": "access-key"
    },
    "storage-profile": {
        "type": "s3",
        "bucket": "<name of the bucket>",
        "region": "<region of the bucket>",
        "sts-enabled": true,
        "flavor": "aws",
        "key-prefix": "lakekeeper-dev-warehouse",
        "sts-role-arn": "arn:aws:iam::<aws account id>:role/LakekeeperWarehouseDevRole"
    },
    "delete-profile": {
        "type": "hard"
    }
}
```
As part of the `storage-profile`, the field `assume-role-arn` can optionally be specified. If it is specified, this role is assumed for every IO Operation of Lakekeeper. It is also used as `sts-role-arn`, unless `sts-role-arn` is specified explicitly. If no `assume-role-arn` is specified, whatever authentication method / user os configured via the `storage-credential` is used directly for IO Operations, so needs to have S3 access policies attached directly (as shown in the example above).

#### System Identities / Managed Identities
Since Lakekeeper version 0.8, credentials for S3 access can also be loaded directly from the environment. Lakekeeper integrates with the AWS SDK to support standard environment-based authentication, including all common configuration options through AWS_* environment variables.

!!! note

    When using system identities, we **strongly recommend** configuring external-id values. This prevents unauthorized cross-account role access and ensures roles can only be assumed by authorized Lakekeeper warehouses.

Without external IDs, any user with warehouse creation permissions in Lakekeeper could potentially access any role the system identity is allowed to assume. For more information, see [AWS's documentation on external IDs](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_third-party.html).

Below is a step-by-step guide for setting up a secure system identity configuration:

Firstly, create a dedicated AWS user to serve as your system identity. Do not attach any direct permissions or trust policies to this user. This user will only have the ability to assume specific roles with the proper external ID

Secondly, configure Lakekeeper with this identity by setting the following environment variables.

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=...
# Required for System Credentials to work:
LAKEKEEPER__S3_REQUIRE_EXTERNAL_ID_FOR_SYSTEM_CREDENTIALS=true
```

In addition to the standard `AWS_*` environment variables, Lakekeeper supports all authentication methods available in the AWS SDK, including instance profiles, container credentials, and SSO configurations.

For enhanced security, Lakekeeper enforces that warehouses using system identities must specify both an `external-id` and an `assume-role-arn` when configured. This implementation follows AWS security best practices by preventing unauthorized role assumption. These default requirements can be adjusted through settings described in the [Configuration Guide](./configuration.md#storage).

For this example, assume the system identity has the ARN `arn:aws:iam::123:user/lakekeeper-system-identity`.

When creating a warehouse, users must configure an IAM role with an appropriate trust policy. The following trust policy template enables the Lakekeeper system identity to assume the role, while enforcing external ID validation:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::123:user/lakekeeper-system-identity"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "<Use a secure random string that cannot be guessed. Treat it like a password.>"
                }
            }
        }
    ]
}
```

The role also needs S3 access, so attach a policy like this:
```json
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "AllowAllAccessInWarehouseFolder",
			"Action": [
				"s3:*"
			],
			"Resource": [
				"arn:aws:s3:::<bucket-name>/<key-prefix if used>/*"
			],
			"Effect": "Allow"
		},
		{
			"Sid": "AllowRootAndHomeListing",
			"Action": [
				"s3:ListBucket"
			],
			"Effect": "Allow",
			"Resource": [
				"arn:aws:s3:::<bucket-name>",
				"arn:aws:s3:::<bucket-name>/"
			]
		}
	]
}
```

We are now ready to create the Warehouse using the system identity:
```json
{
    "warehouse-name": "aws_docs_managed_identity",
    "storage-credential": {
        "type": "s3",
        "credential-type": "aws-system-identity",
        "external-id": "<external id configured in the trust policy of the role>"
    },
    "storage-profile": {
        "type": "s3",
        "assume-role-arn": "<arn of the role that was created>",
        "bucket": "<name of the bucket>",
        "region": "<region of the bucket>",
        "sts-enabled": true,
        "flavor": "aws",
        "key-prefix": "<path to warehouse in bucket>"
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

The specified `assume-role-arn` is used for Lakekeeper's reads and writes of the object store. It is also used as a default for `sts-role-arn`, which is the role that is assumed when generating vended credentials for clients (with an attached policy for the accessed table). 

### S3 Compatible

Unlike for AWS, we do not need any special trust-setup for vended credentials / STS with most S3 compatible solutions like Minio. Instead, we just need a bucket and an access key / secret key combination that is able to read and write from it. If `sts-role-arn` is provided, it will be sent as part of the request to the STS service. Keep in mind that the specific S3 compatible solution may ignore the parameter. Conversely, if `sts-role-arn` is not specified, the request to the STS service will not contain it. Make sure to select `flavor` to have the value `s3-compat`! This setting should work for most self-hosted S3 solutions.

An warehouse create call could look like this:

```json

{
    "warehouse-name": "minio_dev",
    "storage-credential": {
        "type": "s3",
        "aws-access-key-id": "<Access Key of the created user>",
        "aws-secret-access-key": "<Secret Key of the created user>",
        "credential-type": "access-key"
    },
    "storage-profile": {
        "type": "s3",
        "bucket": "<name of the bucket>",
        "region": "local-01",
        "sts-enabled": true,
        "flavor": "s3-compat",
        "key-prefix": "lakekeeper-dev-warehouse",
    },
    "delete-profile": {
        "type": "hard"
    }
}
```

## Azure Data Lake Storage Gen 2
To add a Warehouse backed by ADLS, we need two Azure objects: The Storage Account itself and an App Registration which Lakekeeper can use to access it and delegate access to compute engines.

Lets start by creating a new "App Registration":

1. Create a new "App Registration"
    - **Name**: choose any, for this example we choose `Lakekeeper Warehouse (Development)`
    - **Redirect URI**: Leave empty
2. When the App Registration is created, select "Manage" -> "Certificates & secrets" and create a "New client secret". Note down the secrets "Value".
3. In the "Overview" page of the "App Registration" note down the `Application (client) ID` and the `Directory (tenant) ID`.

Next, we create a new Storage Account. Make sure to select "Enable hierarchical namespace" in the "Advanced" section. For existing Storage Accounts make sure "Hierarchical namespace: Enabled" is shown in the "Overview" page. There are no specific requirements otherwise. Note down the name of the storage account. When the storage account is created, we need to grant the correct permissions to the "App Registration" and create the filesystem / container where the data is stored:

1. Open the Storage Account and select "Data storage" -> Containers. Add a new Container, we call it `warehouse-dev`.
2. Next, select "Access Control (IAM)" in the left menu and "Add role assignment". Grant the `Storage Blob Data Contributor` and `Storage Blob Delegator` roles to the `Lakekeeper Warehouse (Development)` App Registration that we previously created.

We are now ready to create the Warehouse via the UI or the REST API. Use the following information:

* **client-id**: The `Application (client) ID` of the `Lakekeeper Warehouse (Development)` App Registration.
* **client-secret**: The "Value" of the client secret that we noted down previously.
* **tenant-id**: The `Directory (tenant) ID` from the Applications Overview page.
* **account-name**: Name of the Storage Account
* **filesystem**: Name of the container (that Azure also calls filesystem) previously created. In our example its `warehouse-dev`.

A POST request to `/management/v1/warehouse` would expects the following body:

```json
{
  "warehouse-name": "azure_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential":
    {
      "client-id": "...",
      "client-secret": "...",
      "credential-type": "client-credentials",
      "tenant-id": "...",
      "type": "az",
    },
  "storage-profile":
    {
      "account-name": "...",
      "filesystem": "warehouse-dev",
      "type": "adls",
    },
}
```

## GCS

For GCS, the used bucket needs to disable hierarchical namespaces and should have the storage admin role.

A sample storage profile could look like this.

```json
{
  "warehouse-name": "gcs_dev",
  "storage-profile": {
    "type": "gcs",
    "bucket": "...",
    "key-prefix": "..."
  },
  "storage-credential": {
    "type": "gcs",
    "credential-type": "service-account-key",
    "key": {
      "type": "service_account",
      "project_id": "example-project-1234",
      "private_key_id": "....",
      "private_key": "-----BEGIN PRIVATE KEY-----\n.....\n-----END PRIVATE KEY-----\n",
      "client_email": "abc@example-project-1234.iam.gserviceaccount.com",
      "client_id": "123456789012345678901",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/abc%example-project-1234.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
  }
}
```
