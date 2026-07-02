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

Create a `Client` with your Lakekeeper URL, a **warehouse UUID** (it becomes the URL path prefix — use the UUID, not the warehouse name), and an auth strategy. Use it as a context manager to release the connection pool.

```python
from pylakekeeper import Client, StaticToken, ClientCredentials

# Static bearer token (e.g. the no-auth `minimal` stack accepts TOKEN=dev):
client = Client(
    base_url="http://localhost:8181",
    warehouse="my-warehouse-uuid",
    auth=StaticToken("my-token"),
)

# ...or OAuth2 client credentials (the client refreshes the token automatically):
client = Client(
    base_url="http://localhost:8181",
    warehouse="my-warehouse-uuid",
    auth=ClientCredentials(
        token_url="http://keycloak/realms/iceberg/protocol/openid-connect/token",
        client_id="...",
        client_secret="...",
        scope="lakekeeper",
    ),
)
```

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
- `resp.lance_storage_options` — a dict of AWS-style keys for Lance / `boto3`
- `resp.fsspec_kwargs` — kwargs to unpack into `fsspec.filesystem("s3", ...)`

## Example: Lance table

The `lance` format stores a columnar dataset — here, vector embeddings. Create the table, load vended credentials, then write and read directly through Lance:

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

`t.lance_storage_options` maps Lakekeeper's Iceberg-style credential keys (`s3.access-key-id`, …) to the `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`, `aws_region`, and `aws_endpoint` names Lance expects (and adds `allow_http=true` for plaintext endpoints like MinIO).

!!! tip "Every format shares the same catalog flow"
    Create → `load(vended=True)` → write → reload (STS creds are short-lived) → read. Only two things change per format: the **library** that does the I/O, and the **shape** the vended credentials must take. The client maps the Lance/`boto3` shape for you (`t.lance_storage_options`); Delta wants `UPPER_SNAKE` names, so you remap those yourself.

## Example: Delta table

The `delta` format writes a Delta Lake table with [`deltalake`](https://pypi.org/project/deltalake/) (`pip install deltalake`). Delta reads storage options under **`UPPER_SNAKE`** names (`AWS_ACCESS_KEY_ID`, …) rather than Lance's lower-case shape, so remap the vended keys:

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

## Example: Vortex table

The [`vortex`](https://pypi.org/project/vortex-data/) writer (`pip install vortex-data`) targets a local path, so the pattern is **write-local-then-upload**: build a `boto3` client from the vended credentials (the Lance option names double as `boto3` kwargs), write the `.vortex` file to a temp dir, then upload it to the table location.

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
    opts = t.lance_storage_options            # lower_snake keys double as boto3 kwargs
    s3 = boto3.client("s3",
        aws_access_key_id=opts["aws_access_key_id"],
        aws_secret_access_key=opts["aws_secret_access_key"],
        aws_session_token=opts.get("aws_session_token"),
        region_name=opts.get("aws_region"),
        endpoint_url=opts.get("aws_endpoint"))

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

## Example: Paimon table

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
    opts = t.lance_storage_options
    s3 = boto3.client("s3",
        aws_access_key_id=opts["aws_access_key_id"],
        aws_secret_access_key=opts["aws_secret_access_key"],
        aws_session_token=opts.get("aws_session_token"),
        region_name=opts.get("aws_region"),
        endpoint_url=opts.get("aws_endpoint"))

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

## Example: dataset format (unstructured files)

The `dataset` format catalogs unstructured data — raw files rather than a columnar table. Lakekeeper vends credentials; the actual upload is done with a backend-specific client (`boto3` for S3, `azure-storage-blob` for ADLS, `google-cloud-storage` for GCS). Only the upload step changes — the catalog flow is identical.

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

    # Build a boto3 client straight from the vended credentials.
    opts = t.lance_storage_options
    s3 = boto3.client("s3",
        aws_access_key_id=opts["aws_access_key_id"],
        aws_secret_access_key=opts["aws_secret_access_key"],
        aws_session_token=opts.get("aws_session_token"),
        region_name=opts.get("aws_region"),
        endpoint_url=opts.get("aws_endpoint"),  # None on real AWS; set for MinIO/SeaweedFS
    )

    parsed = urlparse(t.location)          # s3://<bucket>/<key-prefix>
    bucket, prefix = parsed.netloc, parsed.path.strip("/")

    for p in Path("images").iterdir():
        s3.put_object(Bucket=bucket, Key=f"{prefix}/{p.name}", Body=p.read_bytes(),
                      ContentType=mimetypes.guess_type(p.name)[0] or "application/octet-stream")
```

## Authentication

| Strategy | Class | Use when |
|---|---|---|
| Static token | `StaticToken(token)` | You already have a bearer token, or the target is a no-auth dev stack. |
| Client credentials | `ClientCredentials(token_url=, client_id=, client_secret=, scope=)` | Production OAuth2 — the client fetches and refreshes the token for you. |

See [Authentication](authentication.md) for how Lakekeeper validates tokens and maps them to identities.

## Related

- [Generic Tables](generic-tables.md) — the catalog concept this client operates on
- [Apache Flink](generic-tables-flink.md) — the same vending flow from Java/Flink
- [Query Engines](engines.md) — Iceberg-native engines against Lakekeeper
- Source & examples: [`lakekeeper-clients` on GitHub](https://github.com/lakekeeper/lakekeeper-clients/tree/main/python)
