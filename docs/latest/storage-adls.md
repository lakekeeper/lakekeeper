---
title: "Azure Data Lake Storage"
description: "Configure Lakekeeper Warehouses on Azure Data Lake Storage Gen2: parameters, App Registration and storage account setup, SAS vended credentials and Azure system identities."
source: "https://docs.lakekeeper.io/docs/latest/storage-adls/"
---

# Azure Data Lake Storage Gen2

The `adls` storage profile backs Warehouses with an Azure Data Lake Storage Gen2 storage account. Table locations use `abfss://`. Lakekeeper vends SAS tokens to clients; ADLS has no remote signing. [LoQE](engines.md#loqe) reads ADLS but does not write it.

## Configuration Parameters

| Parameter                     | Type    | Required | Default                             | Description |
|-------------------------------|---------|----------|-------------------------------------|-----|
| `account-name`                | String  | Yes      | -                                   | Name of the Azure storage account. |
| `filesystem`                  | String  | Yes      | -                                   | Name of the ADLS filesystem, in blob storage also known as container. |
| `sas-enabled`                 | Boolean | No       | `true`                              | Whether to enable SAS (Shared Access Signature) token generation for Azure Data Lake Storage. When disabled, clients cannot use vended credentials for this storage profile. |
| `key-prefix`                  | String  | No       | None                                | Subpath in the filesystem to use. |
| `allow-alternative-protocols` | Boolean | No       | `false`                             | Whether to allow `wasbs://` in locations in addition to `abfss://`. This is disabled by default and should only be enabled for migrating legacy Hadoop-based tables via the register endpoint. |
| `host`                        | String  | No       | `dfs.core.windows.net`              | The host to use for the storage account. |
| `authority-host`              | URL     | No       | `https://login.microsoftonline.com` | The authority host to use for authentication. |
| `sas-token-validity-seconds`  | Integer | No       | `3600`                              | The validity period of the SAS token in seconds. |
| `storage-layout`              | Object  | No       | `{"type": "default"}`               | Controls how namespace and tabular directories are structured under the warehouse base location. See [Storage Layout](storage-layout.md). |

## Credentials

The `storage-credential` of an ADLS Warehouse has `"type": "az"` and one of these `credential-type` values:

| `credential-type` | Use for |
|---|---|
| `client-credentials` | An App Registration (service principal) with `client-id`, `client-secret` and `tenant-id`, see [Setup](#setup) |
| `shared-access-key` | The storage account's access key, in `key` |
| `azure-system-identity` | The managed identity of the Lakekeeper process, see [Azure System Identity](#azure-system-identity) |

## Setup

An ADLS Warehouse needs two Azure objects: the storage account, and an App Registration that Lakekeeper uses to access it and to delegate access to query engines.

First, create the App Registration:

1. Create a new "App Registration".
    - **Name**: any; in this example `Lakekeeper Warehouse (Development)`
    - **Redirect URI**: leave empty
2. Once it is created, select "Manage" → "Certificates & secrets" and create a "New client secret". Note down the secret's "Value".
3. On the App Registration's "Overview" page, note down the `Application (client) ID` and the `Directory (tenant) ID`.

Next, create a storage account with "Enable hierarchical namespace" selected in the "Advanced" section. For an existing storage account, check that its "Overview" page shows "Hierarchical namespace: Enabled". There are no other requirements. Note down the storage account's name. Then create the container that holds the data, and grant the App Registration access:

1. Open the storage account and select "Data storage" → "Containers". Add a new container; we call it `warehouse-dev`.
2. Select "Access Control (IAM)" in the left menu and "Add role assignment". Grant the `Storage Blob Data Contributor` and `Storage Blob Delegator` roles to the `Lakekeeper Warehouse (Development)` App Registration.

Now create the Warehouse through the UI or with a POST request to `/management/v1/warehouse`:

- **client-id**: the `Application (client) ID` of the App Registration
- **client-secret**: the "Value" of its client secret
- **tenant-id**: the `Directory (tenant) ID`
- **account-name**: the name of the storage account
- **filesystem**: the name of the container (Azure also calls it filesystem), `warehouse-dev` in our example

```json
{
  "warehouse-name": "azure_dev",
  "delete-profile": { "type": "hard" },
  "storage-credential": {
    "type": "az",
    "credential-type": "client-credentials",
    "client-id": "...",
    "client-secret": "...",
    "tenant-id": "..."
  },
  "storage-profile": {
    "type": "adls",
    "account-name": "...",
    "filesystem": "warehouse-dev"
  }
}
```

## Azure System Identity

!!! warning
    Enabling Azure system identities allows Lakekeeper to access any storage location that the managed identity has permissions for. To minimize security risks, ensure the managed identity is restricted to only the necessary resources. Additionally, limit Warehouse creation permission in Lakekeeper to users who are authorized to access all locations that the system identity can access.

With a system identity, Lakekeeper authenticates to ADLS with the managed identity of the virtual machine or application it runs on, and Warehouses are created without explicit credentials. The feature is disabled by default and must be enabled server-wide:

```bash
LAKEKEEPER__ENABLE_AZURE_SYSTEM_CREDENTIALS=true
```

Grant the managed identity access to the storage account and container, for example the `Storage Blob Data Contributor` and `Storage Blob Delegator` roles as in [Setup](#setup). Then create the Warehouse with:

```json
{
  "storage-credential": {
    "type": "az",
    "credential-type": "azure-system-identity"
  }
}
```

## CORS

[LoQE](engines.md#loqe) needs a [CORS policy](storage.md#cors) on the storage account to read from the browser. Azure applies the Blob service's CORS rules to the Data Lake endpoint too, so set them there: in the Azure portal, open the storage account, select "Settings" → "Resource sharing (CORS)", and add a rule on the **Blob service** tab:

| Field | Value |
|---|---|
| Allowed origins | The origin where your Lakekeeper instance is hosted, e.g. `https://lakekeeper.example.com` |
| Allowed methods | `GET`, `HEAD` |
| Allowed headers | `*` |
| Exposed headers | `*` |
| Max age | `3600` |

## Updating the Storage Profile

`filesystem`, `key-prefix`, `host` and `authority-host` cannot change on `update-storage-profile`. An update that omits `storage-layout` keeps the current layout.
