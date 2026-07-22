# Python Client (pylakekeeper)

`pylakekeeper` is the official Python client for Lakekeeper's [Generic Table API](generic-tables.md). It is a small, standalone library — **not** a general Iceberg REST client — that handles two things you otherwise hand-roll: catalog CRUD for generic tables, and OAuth2 authentication (static token or client-credentials with automatic token refresh).

Its main job is **credential vending**: you ask Lakekeeper to load a table with `vended=True`, and the client hands you short-lived, prefix-scoped storage credentials already translated into the shape your engine expects — Lance `storage_options`, `fsspec` kwargs, or plain AWS keys for `boto3`.

!!! note "Generic tables only"
    This client covers the generic-tables surface (Lance, `dataset`, Parquet, CSV, any format). For Iceberg tables use a standard Iceberg REST client such as [PyIceberg](engines.md#pyiceberg) pointed at Lakekeeper's catalog endpoint.

## Install

```sh
pip install pylakekeeper

# with the Lance object-store helpers:
pip install 'pylakekeeper[lance]'
```

Requires Python 3.10+. The core install depends only on `httpx` and `pydantic`.

## Connect

Create a `Client` with your Lakekeeper URL, a **warehouse UUID** (it becomes the URL path prefix — use the UUID, not the warehouse name), and an **auth strategy**. Use it as a context manager to release the connection pool.

```python
from pylakekeeper import Client, StaticToken

with Client(base_url="http://localhost:8181",
            warehouse="my-warehouse-uuid",
            auth=StaticToken("dev")) as client:      # a no-auth dev stack accepts any token
    ...
```

The `auth=` argument is where you choose *how* you authenticate — a static token, a service account, or an interactive human login. Those strategies are shared across all clients and documented once in **[Client Authentication](generic-tables-auth.md)** (`StaticToken`, `ClientCredentials`, `DeviceCodeFlow`, `AuthorizationCodeFlow`, with automatic token refresh). Build whichever one you need and pass it to `Client(...)` as above.

!!! info "Warehouse and namespace must already exist"
    The client creates *tables*, not warehouses or namespaces — warehouse administration is intentionally out of scope. Create the warehouse via the UI or [Management API](api/management.md) and the namespace via the [Catalog API](api/catalog.md) first.

## Generic-tables API

All table operations hang off `client.generic_tables`:

| Method | Description |
|---|---|
| `create(namespace, name, format=..., base_location=..., doc=..., properties=...)` | Create a table; returns the load response. Raises `ConflictError` if it exists. |
| `load(namespace, name, vended=False)` | Load a table. With `vended=True`, the response carries inline STS credentials. |
| `list(namespace, page_size=100)` | Iterate every table identifier in a namespace, following pagination. |
| `drop(namespace, name)` | Drop a table. |

Namespaces are given as dotted strings (`"ai.test"`) or lists (`["ai", "test"]`); multi-level namespace encoding is handled for you.

The `load(..., vended=True)` response exposes ready-to-use credential shapes:

- `resp.location` — the table's storage URI (e.g. `s3://bucket/prefix/...`)
- `resp.lance_storage_options` — object-store option names for **Lance** (`aws_access_key_id`, …)
- `resp.fsspec_kwargs` — kwargs to unpack into `fsspec.filesystem("s3", ...)`

## Storage backends

`load(vended=True)` returns whatever short-lived credentials Lakekeeper mints for the warehouse's backend, under `t.storage_credentials` / `t.config`. The catalog flow is identical across backends — only the credential **keys** and the storage **library** differ. pylakekeeper ships ready-made credential shapes for **S3 only** (`lance_storage_options`, `fsspec_kwargs`); for Azure and GCS you read the raw vended keys off the response and hand them to your library.

=== "S3 & S3-compatible"

    Covers **AWS** and every S3-compatible store — **StackIT**, **MinIO**, **Cloudflare R2**, Ceph, … The client-side code is *identical* across them: Lakekeeper vends the correct `s3.endpoint`, and `lance_storage_options` applies it (plus `allow_http` for plaintext endpoints) automatically. There's no per-vendor branch in your code.

    ```python
    t = c.generic_tables.load(ns, name, vended=True)

    # Lance / object-store shape (ready-made):
    opts = t.lance_storage_options
    # → aws_access_key_id, aws_secret_access_key, aws_session_token, aws_region, aws_endpoint (+ allow_http)

    # ...or the raw vended keys for boto3 / another library:
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})
    # creds["s3.access-key-id"], creds["s3.secret-access-key"], creds.get("s3.session-token"),
    # creds.get("s3.region") or creds.get("client.region"), creds.get("s3.endpoint")
    ```

=== "Azure ADLS"

    Lakekeeper vends an **account-scoped SAS token**. pylakekeeper has no Azure credential helper, so read the keys yourself:

    ```python
    t = c.generic_tables.load(ns, name, vended=True)
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})

    # Iceberg-REST ADLS keys (<account> = storage account, <suffix> e.g. dfs.core.windows.net):
    #   adls.sas-token.<account>.<suffix>   → the SAS token
    #   adls.account-name / adls.account-host
    sas = next(v for k, v in creds.items() if k.startswith("adls.sas-token."))
    account = creds.get("adls.account-name")
    ```

    Hand these to your Azure library — e.g. `adlfs.AzureBlobFileSystem(account_name=account, sas_token=sas)`, or Lance / object-store `storage_options={"azure_storage_account_name": account, "azure_storage_sas_key": sas}`. Confirm the exact option names against your library's docs.

=== "GCS"

    Lakekeeper vends a short-lived **OAuth2 bearer token** (`gcs.oauth2.token`). Again there is no pylakekeeper helper:

    ```python
    t = c.generic_tables.load(ns, name, vended=True)
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})

    gcs_token = creds["gcs.oauth2.token"]        # also: gcs.oauth2.token-expires-at
    ```

    You can hand the bearer token to an fsspec-based tool (e.g. `gcsfs`) for direct file access. But the object-store writers (**Lance**, **`deltalake`**) expect a service-account key / ADC, not a raw bearer token — first-class GCS support for those is **coming soon**. Follow [lakekeeper-clients](https://github.com/lakekeeper/lakekeeper-clients).

!!! note "S3 has first-class helpers; Azure/GCS are manual today"
    The `lance_storage_options` / `fsspec_kwargs` helpers only translate the S3 (`s3.*`) keys. Azure/GCS work, but you map their vended keys yourself — first-class helpers for them are a planned client addition.

## Examples

Every format follows the **same catalog flow** — create → `load(vended=True)` → write → reload (STS creds are short-lived) → read. Only the **library** that does the I/O and the **shape** of the vended credentials change per backend.

The **Lance**, **Delta**, and **`dataset`** examples show a tab per storage backend. The other upload-based examples (**Vortex**, **Paimon**, **HDF5**) are shown for **S3**; they follow the same file-upload pattern as `dataset`, so swap the upload client + credentials per that example / [Storage backends](#storage-backends).

!!! warning "S3 is tested; Azure & GCS are illustrative"
    The **S3 & S3-compatible** tab is the tested path (it also covers StackIT, MinIO, R2 — the endpoint is auto-vended). The **Azure ADLS** and **GCS** tabs have **not** been run against a live warehouse — treat their credentials / `storage_options` / upload calls as best-effort and confirm them against your storage library. Note the split on **GCS**: the file-upload `dataset` path accepts the vended bearer token via `google-cloud-storage`, but the columnar writers (**Lance**, **Delta**) go through object-store, which wants a service-account key / ADC — so their GCS tabs are marked *coming soon*.

### Lance

The `lance` format stores a columnar dataset — here, vector embeddings. Create the table, load vended credentials, then write and read directly through Lance.

=== "S3 & S3-compatible"

    ```python
    import lance
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    with Client(base_url="http://localhost:8181",
                warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:

        # Create the table (idempotent — tolerate re-runs).
        try:
            c.generic_tables.create(
                "ai.test", "image_embeddings",
                format=GenericTableFormat.LANCE,   # or just "lance"
                properties={"embedding-dim": "768"},
            )
        except ConflictError:
            pass  # already exists

        # Load with vended credentials → base location + short-lived STS creds.
        t = c.generic_tables.load("ai.test", "image_embeddings", vended=True)

        # Write, then reload (STS creds are short-lived) and read back.
        lance.write_dataset(my_arrow_table, t.location,
                            storage_options=t.lance_storage_options, mode="overwrite")

        t = c.generic_tables.load("ai.test", "image_embeddings", vended=True)
        ds = lance.dataset(t.location, storage_options=t.lance_storage_options)
        print("rows:", ds.count_rows())
    ```

    `t.lance_storage_options` maps the vended `s3.*` keys to the `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_region`, `aws_endpoint` names Lance expects (and adds `allow_http=true` for plaintext endpoints like MinIO).

=== "Azure ADLS"

    ```python
    import lance
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    with Client(base_url="http://localhost:8181", warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:
        try:
            c.generic_tables.create("ai.test", "image_embeddings",
                                    format=GenericTableFormat.LANCE, properties={"embedding-dim": "768"})
        except ConflictError:
            pass
        # Rebuild storage options from the freshly-loaded table each time — the SAS token is short-lived.
        def adls_opts(t):   # map the vended SAS token to Lance/object-store Azure options (verify names)
            creds = {**{k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()},
                     **(t.config or {})}
            return {
                "azure_storage_account_name": creds.get("adls.account-name"),
                "azure_storage_sas_key": next(v for k, v in creds.items() if k.startswith("adls.sas-token.")),
            }

        t = c.generic_tables.load("ai.test", "image_embeddings", vended=True)  # location is abfss://...
        lance.write_dataset(my_arrow_table, t.location, storage_options=adls_opts(t), mode="overwrite")

        t = c.generic_tables.load("ai.test", "image_embeddings", vended=True)   # refresh SAS
        print("rows:", lance.dataset(t.location, storage_options=adls_opts(t)).count_rows())
    ```

=== "GCS"

    !!! info "GCS support is in progress"
        Lakekeeper vends a short-lived OAuth2 **bearer token** (`gcs.oauth2.token`), but Lance (via object-store) authenticates to GCS with a service-account key / ADC — so the vended token doesn't yet plug into `storage_options`. A first-class GCS path in `pylakekeeper` is **coming soon**; follow [lakekeeper-clients](https://github.com/lakekeeper/lakekeeper-clients).

### Delta

The `delta` format writes a Delta Lake table with [`deltalake`](https://pypi.org/project/deltalake/) (`pip install deltalake`). `deltalake` reads storage options under **`UPPER_SNAKE`** names rather than Lance's lower-case shape, so remap the vended keys.

=== "S3 & S3-compatible"

    ```python
    from deltalake import DeltaTable, write_deltalake
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    # deltalake wants UPPER_SNAKE storage-option names; the client only maps the Lance shape.
    ICEBERG_TO_DELTA = {
        "s3.access-key-id":     "AWS_ACCESS_KEY_ID",
        "s3.secret-access-key": "AWS_SECRET_ACCESS_KEY",
        "s3.session-token":     "AWS_SESSION_TOKEN",
        "s3.region":            "AWS_REGION",
        "client.region":        "AWS_REGION",
        "s3.endpoint":          "AWS_ENDPOINT_URL",
    }

    def delta_options(t):
        props = {**{k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()},
                 **(t.config or {})}
        opts = {ICEBERG_TO_DELTA[k]: v for k, v in props.items() if k in ICEBERG_TO_DELTA}
        opts.setdefault("AWS_S3_ALLOW_UNSAFE_RENAME", "true")   # plain S3 has no atomic rename
        if opts.get("AWS_ENDPOINT_URL", "").startswith("http://"):
            opts["AWS_ALLOW_HTTP"] = "true"                     # MinIO/SeaweedFS over http
        return opts

    with Client(base_url="http://localhost:8181",
                warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:

        try:
            c.generic_tables.create("ai.test", "events", format=GenericTableFormat.DELTA)
        except ConflictError:
            pass

        t = c.generic_tables.load("ai.test", "events", vended=True)
        write_deltalake(t.location, my_arrow_table,
                        storage_options=delta_options(t), mode="overwrite")

        t = c.generic_tables.load("ai.test", "events", vended=True)   # refresh STS creds
        dt = DeltaTable(t.location, storage_options=delta_options(t))
        print("rows:", dt.to_pyarrow_table().num_rows)
    ```

=== "Azure ADLS"

    ```python
    from deltalake import DeltaTable, write_deltalake
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    def delta_options(t):   # map the vended SAS token to deltalake's Azure options (verify names)
        creds = {**{k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()},
                 **(t.config or {})}
        sas = next(v for k, v in creds.items() if k.startswith("adls.sas-token."))
        return {
            "AZURE_STORAGE_ACCOUNT_NAME": creds.get("adls.account-name"),
            "AZURE_STORAGE_SAS_KEY": sas,
        }

    with Client(base_url="http://localhost:8181", warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:
        try:
            c.generic_tables.create("ai.test", "events", format=GenericTableFormat.DELTA)
        except ConflictError:
            pass
        t = c.generic_tables.load("ai.test", "events", vended=True)   # location is abfss://...
        write_deltalake(t.location, my_arrow_table, storage_options=delta_options(t), mode="overwrite")

        t = c.generic_tables.load("ai.test", "events", vended=True)   # refresh SAS
        print("rows:", DeltaTable(t.location, storage_options=delta_options(t)).to_pyarrow_table().num_rows)
    ```

=== "GCS"

    !!! info "GCS support is in progress"
        Lakekeeper vends a short-lived OAuth2 **bearer token** (`gcs.oauth2.token`), but `deltalake` (via object-store) authenticates to GCS with a service-account key / ADC — so the vended token doesn't yet plug into `storage_options`. A first-class GCS path in `pylakekeeper` is **coming soon**; follow [lakekeeper-clients](https://github.com/lakekeeper/lakekeeper-clients).

### Vortex

The [`vortex`](https://pypi.org/project/vortex-data/) writer (`pip install vortex-data`) targets a local path, so the pattern is **write-local-then-upload**: build a `boto3` client from the vended `s3.*` credentials, write the `.vortex` file to a temp dir, then upload it to the table location.

```python
import os, tempfile
from urllib.parse import urlparse
import boto3
import vortex as vx
from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

with Client(base_url="http://localhost:8181",
            warehouse="my-warehouse-uuid",
            auth=StaticToken("dev")) as c:

    try:
        c.generic_tables.create("ai.test", "signals", format=GenericTableFormat.VORTEX)
    except ConflictError:
        pass

    t = c.generic_tables.load("ai.test", "signals", vended=True)
    # Vortex isn't Lance — build the S3 client from the raw vended `s3.*` credentials.
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})
    s3 = boto3.client("s3",
        aws_access_key_id=creds["s3.access-key-id"],
        aws_secret_access_key=creds["s3.secret-access-key"],
        aws_session_token=creds.get("s3.session-token"),
        region_name=creds.get("s3.region") or creds.get("client.region"),
        endpoint_url=creds.get("s3.endpoint"))

    parsed = urlparse(t.location)
    bucket, key = parsed.netloc, f"{parsed.path.strip('/')}/data.vortex"

    with tempfile.TemporaryDirectory() as tmp:              # write
        local = os.path.join(tmp, "data.vortex")
        vx.io.write(vx.array(my_arrow_table), local)
        s3.upload_file(local, bucket, key)

    with tempfile.TemporaryDirectory() as tmp:              # read back
        local = os.path.join(tmp, "data.vortex")
        s3.download_file(bucket, key, local)
        table = vx.open(local).scan().read_all().to_arrow_table()
        print("rows:", table.num_rows)
```

### Paimon

[`pypaimon`](https://pypi.org/project/pypaimon/) (`pip install pypaimon`, needs a JVM on `PATH`) also writes through a local filesystem catalog, so it uses the same **write-local-then-upload** approach as Vortex — except a Paimon table is a *directory tree*, so the whole tree is walked and uploaded.

```python
import os, tempfile
from urllib.parse import urlparse
import boto3
from pypaimon import CatalogFactory, Schema
from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

with Client(base_url="http://localhost:8181",
            warehouse="my-warehouse-uuid",
            auth=StaticToken("dev")) as c:

    try:
        c.generic_tables.create("ai.test", "orders", format=GenericTableFormat.PAIMON)
    except ConflictError:
        pass

    t = c.generic_tables.load("ai.test", "orders", vended=True)
    # Paimon isn't Lance — build the S3 client from the raw vended `s3.*` credentials.
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})
    s3 = boto3.client("s3",
        aws_access_key_id=creds["s3.access-key-id"],
        aws_secret_access_key=creds["s3.secret-access-key"],
        aws_session_token=creds.get("s3.session-token"),
        region_name=creds.get("s3.region") or creds.get("client.region"),
        endpoint_url=creds.get("s3.endpoint"))

    parsed = urlparse(t.location)
    bucket, prefix = parsed.netloc, parsed.path.strip("/")

    with tempfile.TemporaryDirectory() as tmp:
        catalog = CatalogFactory.create({"warehouse": tmp})
        catalog.create_database("default", True)
        schema = Schema.from_pyarrow_schema(my_arrow_table.schema,
                                            primary_keys=["id"], options={"bucket": "1"})
        catalog.create_table("default.orders", schema, True)
        table = catalog.get_table("default.orders")

        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        writer.write_arrow(my_arrow_table)
        commit = wb.new_commit()
        commit.commit(writer.prepare_commit())
        writer.close(); commit.close()

        # Upload the local table tree to the vended location.
        table_dir = os.path.join(tmp, "default.db", "orders")
        for root, _dirs, files in os.walk(table_dir):
            for f in files:
                local = os.path.join(root, f)
                rel = os.path.relpath(local, table_dir).replace(os.sep, "/")
                s3.upload_file(local, bucket, f"{prefix}/{rel}")
        print("uploaded paimon table →", t.location)
```

Reading a Paimon table back is the mirror image: download the object tree under the prefix into a temp dir, then open it with a local `CatalogFactory(...).get_table("default.orders")` and a `new_read_builder()`.

### `dataset` — unstructured files

The `dataset` format catalogs unstructured data — raw files rather than a columnar table. The catalog flow is identical across backends; only the **upload client** changes (`boto3` / `azure-storage-blob` / `google-cloud-storage`). Because these are plain file-upload SDKs, each accepts the vended credential type directly — so unlike the columnar formats, **all three backends work here**, including GCS.

=== "S3 & S3-compatible"

    ```python
    import boto3, mimetypes
    from pathlib import Path
    from urllib.parse import urlparse
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    with Client(base_url="http://localhost:8181",
                warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:

        try:
            c.generic_tables.create("ai.test", "product_images",
                                    format=GenericTableFormat.DATASET, doc="product images")
        except ConflictError:
            pass

        t = c.generic_tables.load("ai.test", "product_images", vended=True)

        # boto3 client from the raw vended s3.* credentials.
        creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
        creds.update(t.config or {})
        s3 = boto3.client("s3",
            aws_access_key_id=creds["s3.access-key-id"],
            aws_secret_access_key=creds["s3.secret-access-key"],
            aws_session_token=creds.get("s3.session-token"),
            region_name=creds.get("s3.region") or creds.get("client.region"),
            endpoint_url=creds.get("s3.endpoint"),  # None on real AWS; set for MinIO/SeaweedFS
        )

        parsed = urlparse(t.location)          # s3://<bucket>/<key-prefix>
        bucket, prefix = parsed.netloc, parsed.path.strip("/")

        for p in Path("images").iterdir():
            s3.put_object(Bucket=bucket, Key=f"{prefix}/{p.name}", Body=p.read_bytes(),
                          ContentType=mimetypes.guess_type(p.name)[0] or "application/octet-stream")
    ```

=== "Azure ADLS"

    ```python
    from azure.storage.blob import BlobServiceClient
    from pathlib import Path
    from urllib.parse import urlparse
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    with Client(base_url="http://localhost:8181", warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:
        try:
            c.generic_tables.create("ai.test", "product_images",
                                    format=GenericTableFormat.DATASET, doc="product images")
        except ConflictError:
            pass
        t = c.generic_tables.load("ai.test", "product_images", vended=True)

        # Vended SAS token is accepted directly as the BlobServiceClient credential.
        creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
        creds.update(t.config or {})
        sas = next(v for k, v in creds.items() if k.startswith("adls.sas-token."))
        account = creds.get("adls.account-name")

        parsed = urlparse(t.location)          # abfss://<container>@<account>.dfs.core.windows.net/<prefix>
        container, prefix = parsed.username, parsed.path.strip("/")

        bsc = BlobServiceClient(f"https://{account}.blob.core.windows.net", credential=sas)
        for p in Path("images").iterdir():
            bsc.get_blob_client(container=container, blob=f"{prefix}/{p.name}") \
               .upload_blob(p.read_bytes(), overwrite=True)
    ```

=== "GCS"

    ```python
    from google.cloud import storage
    from google.oauth2.credentials import Credentials
    from pathlib import Path
    from urllib.parse import urlparse
    from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

    with Client(base_url="http://localhost:8181", warehouse="my-warehouse-uuid",
                auth=StaticToken("dev")) as c:
        try:
            c.generic_tables.create("ai.test", "product_images",
                                    format=GenericTableFormat.DATASET, doc="product images")
        except ConflictError:
            pass
        t = c.generic_tables.load("ai.test", "product_images", vended=True)

        # The vended OAuth2 bearer token works via google.oauth2.credentials.Credentials.
        creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
        creds.update(t.config or {})
        gcs = storage.Client(credentials=Credentials(token=creds["gcs.oauth2.token"]))

        parsed = urlparse(t.location)          # gs://<bucket>/<prefix>
        bucket, prefix = gcs.bucket(parsed.netloc), parsed.path.strip("/")

        for p in Path("images").iterdir():
            bucket.blob(f"{prefix}/{p.name}").upload_from_filename(str(p))
    ```

### HDF5

HDF5 is another `dataset`-format case — an opaque binary file that Lakekeeper catalogs and vends credentials for, read and written with [`h5py`](https://pypi.org/project/h5py/) (`pip install h5py`). Write the `.h5` locally, upload it, then read it back from the downloaded bytes:

```python
import io, tempfile
from pathlib import Path
from urllib.parse import urlparse
import boto3, h5py, numpy as np
from pylakekeeper import Client, StaticToken, GenericTableFormat, ConflictError

with Client(base_url="http://localhost:8181",
            warehouse="my-warehouse-uuid",
            auth=StaticToken("dev")) as c:

    try:
        c.generic_tables.create("ai.test", "sensor_hdf5",
                                format=GenericTableFormat.DATASET, doc="HDF5 sensor data")
    except ConflictError:
        pass

    t = c.generic_tables.load("ai.test", "sensor_hdf5", vended=True)

    # boto3 client from the raw vended s3.* credentials (same as the dataset example).
    creds = {k: v for cred in (t.storage_credentials or []) for k, v in cred.config.items()}
    creds.update(t.config or {})
    s3 = boto3.client("s3",
        aws_access_key_id=creds["s3.access-key-id"],
        aws_secret_access_key=creds["s3.secret-access-key"],
        aws_session_token=creds.get("s3.session-token"),
        region_name=creds.get("s3.region") or creds.get("client.region"),
        endpoint_url=creds.get("s3.endpoint"))

    parsed = urlparse(t.location)
    bucket, key = parsed.netloc, f"{parsed.path.strip('/')}/data.h5"

    # Write an HDF5 file locally, then upload it.
    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "data.h5"
        with h5py.File(local, "w") as f:
            f.create_dataset("embeddings",
                             data=np.random.default_rng(0).standard_normal((100, 8)),
                             compression="gzip")
            f.attrs["rows"] = 100
        s3.upload_file(str(local), bucket, key)

    # Read it back: download the object and open the bytes with h5py.
    raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    with h5py.File(io.BytesIO(raw), "r") as f:
        print("datasets:", list(f.keys()), "rows:", int(f.attrs["rows"]))
```

## Related

- [Client Authentication](generic-tables-auth.md) — all auth strategies (static / client-credentials / device-code / PKCE)
- [Generic Tables](generic-tables.md) — the catalog concept this client operates on
- [Apache Flink](generic-tables-flink.md) — the same vending flow from Java/Flink
- [Query Engines](engines.md) — Iceberg-native engines against Lakekeeper
- Source & examples: [`lakekeeper-clients` on GitHub](https://github.com/lakekeeper/lakekeeper-clients/tree/main/python)
