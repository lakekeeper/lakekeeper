import json
from pathlib import Path

import pyarrow as pa
import requests
from datafusion import SessionContext
from pyiceberg.catalog import load_catalog
from pyiceberg_core.datafusion import IcebergDataFusionTable

LAKEKEEPER_URL = "http://lakekeeper:8181"


def check_response(response: requests.Response, already_exists: tuple[int, str] | None = None) -> None:
    if 200 <= response.status_code < 300:
        return
    if already_exists is not None and response.status_code == already_exists[0]:
        try:
            error_type = response.json()["error"]["type"]
        except (KeyError, TypeError, ValueError):
            pass
        else:
            if error_type == already_exists[1]:
                return
    raise RuntimeError(
        f"{response.request.method} {response.request.path_url} returned HTTP {response.status_code}: {response.text}"
    )


bootstrap = requests.post(
    f"{LAKEKEEPER_URL}/management/v1/bootstrap",
    json={"accept-terms-of-use": True},
    timeout=30,
    allow_redirects=False,
)
check_response(bootstrap, (400, "CatalogAlreadyBootstrapped"))

warehouse = json.loads(Path(__file__).with_name("warehouse.json").read_text(encoding="utf-8"))
warehouse_url = f"{LAKEKEEPER_URL}/management/v1/warehouse"
warehouses = requests.get(
    warehouse_url,
    headers={"x-project-id": warehouse["project-id"]},
    timeout=30,
    allow_redirects=False,
)
check_response(warehouses)
existing = next(
    (item for item in warehouses.json()["warehouses"] if item["name"] == warehouse["warehouse-name"]),
    None,
)
if existing is None:
    check_response(requests.post(warehouse_url, json=warehouse, timeout=30, allow_redirects=False))
else:
    expected = warehouse["storage-profile"]
    actual = existing["storage-profile"]
    same_bucket = all(actual.get(key) == expected[key] for key in ("type", "bucket", "key-prefix"))
    same_endpoint = (actual.get("endpoint") or "").rstrip("/") == expected["endpoint"].rstrip("/")
    if not (same_bucket and same_endpoint):
        raise RuntimeError(f"Warehouse {warehouse['warehouse-name']} uses a different storage location.")

catalog = load_catalog(
    "lakekeeper",
    type="rest",
    uri=f"{LAKEKEEPER_URL}/catalog",
    warehouse="demo",
)

namespace = ("datafusion_demo",)
identifier = (*namespace, "trips")
catalog.create_namespace_if_not_exists(namespace)

data = pa.table({"id": [1, 2, 3], "destination": ["Oslo", "Berlin", "Lisbon"]})
table = catalog.create_table_if_not_exists(identifier, schema=data.schema)
if table.current_snapshot() is None:
    table.append(data)

# Load the table and its vended storage credentials from the REST catalog.
table = catalog.load_table(identifier)

# The native provider accepts string-valued FileIO settings, including S3 credentials.
provider = IcebergDataFusionTable(
    identifier=table.name(),
    metadata_location=table.metadata_location,
    file_io_properties={key: value for key, value in table.io.properties.items() if isinstance(value, str)},
)

context = SessionContext()
context.register_table("trips", provider)
context.sql("SELECT id, destination FROM trips WHERE id > 1 ORDER BY id").show()
