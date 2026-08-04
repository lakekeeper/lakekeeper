"""PyIceberg RestCatalog pointed at Lakekeeper. ML-image only (pyiceberg).

Lakekeeper vends scoped S3 credentials to PyIceberg automatically as part of the
Iceberg REST loadTable response — the same governance path as everything else.
"""
from __future__ import annotations

from pyiceberg.catalog.rest import RestCatalog

from mlib import LAKEKEEPER_URL, WAREHOUSE_NAME


def catalog(token: str) -> RestCatalog:
    return RestCatalog(
        name=WAREHOUSE_NAME,
        warehouse=WAREHOUSE_NAME,
        uri=f"{LAKEKEEPER_URL}/catalog/",
        token=token,
    )
