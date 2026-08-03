"""Thin plumbing for generic-table datasets — the notebooks call **pylakekeeper**
directly (so you can see it); these helpers only build the pieces around it.

The medallion keeps two non-Iceberg datasets, both catalogued by Lakekeeper as
**generic tables** (governance + credential vending), with the engine writing
straight to the vended location:

* **`raw.images`** — `format="dataset"`: the raw image *objects* (`<loc>/<id>.jpg`).
  Lakekeeper catalogs the location and vends scoped creds; we write the objects with
  `pyarrow.fs`. Bronze (Iceberg) links each by its `s3://` URI.
* **`gold.image_embeddings`** — `format="lance"`: a Lance vector dataset.

Bronze and Silver are tabular metadata and live in Iceberg (see `icehelp.py`).
"""
from __future__ import annotations

from urllib.parse import urlsplit

import pyarrow.fs as pafs
from pylakekeeper import Client, ClientCredentials

from mlib import KEYCLOAK_TOKEN_URL, LAKEKEEPER_URL, get_token, warehouse_id


def client(creds: tuple) -> Client:
    """A pylakekeeper Client for a service account (warehouse auto-resolved).

    Use it directly in the notebooks:
        with gt.client(PIPELINE) as c:
            c.generic_tables.create(ns, name, format="dataset")
            t = c.generic_tables.load(ns, name, vended=True)   # <- vends scoped STS creds
    """
    wh = warehouse_id(get_token(*creds))
    return Client(
        base_url=LAKEKEEPER_URL,
        warehouse=wh,
        auth=ClientCredentials(
            token_url=KEYCLOAK_TOKEN_URL,
            client_id=creds[0],
            client_secret=creds[1],
            scope="lakekeeper",
        ),
    )


def s3fs(vended) -> "pafs.S3FileSystem":
    """Build a `pyarrow.fs.S3FileSystem` from a loaded generic table's vended
    credentials (`t.lance_storage_options`), so you can read/write objects at its
    location with short-lived, scoped STS creds."""
    o = vended.lance_storage_options
    ep = urlsplit(o["aws_endpoint"])
    return pafs.S3FileSystem(
        access_key=o["aws_access_key_id"],
        secret_key=o["aws_secret_access_key"],
        session_token=o.get("aws_session_token"),
        region=o.get("aws_region", "local-01"),
        endpoint_override=ep.netloc,
        scheme=ep.scheme,
    )


def read_object(fs, uri: str) -> bytes:
    """Read an object by its `s3://…` URI using an `fs` from `s3fs()`."""
    with fs.open_input_stream(uri.replace("s3://", "")) as f:
        return f.read()
