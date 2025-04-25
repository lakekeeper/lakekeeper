import conftest
import pandas as pd
import pyarrow as pa
import pytest
import time
import pyiceberg.io as io
import requests
from urllib.parse import quote_plus, quote
import uuid


def create_user(warehouse: conftest.Warehouse):
    user_email = f"foo~bar+:\\/.!?*👾 -{uuid.uuid4().hex}@lakekeeper.io"
    user_id = f"oidc~{user_email}"

    requests.post(
        warehouse.server.user_url,
        headers={"Authorization": f"Bearer {warehouse.access_token}"},
        json={
            "email": user_email,
            "id": user_id,
            "name": "Peter Cold",
            "update-if-exists": True,
            "user-type": "human",
        },
    ).raise_for_status()

    return user_email, user_id


def test_create_user_with_email_id(warehouse: conftest.Warehouse):
    user_email, user_id = create_user(warehouse)
    # Get this user
    response = requests.get(
        warehouse.server.user_url + f"/{quote(user_id, safe='')}",
        headers={"Authorization": f"Bearer {warehouse.access_token}"},
    )
    response.raise_for_status()
    user = response.json()
    assert user["email"] == user_email
    assert user["id"] == user_id


def test_user_permissions_with_email_id(warehouse: conftest.Warehouse):
    _, user_id = create_user(warehouse)

    # Make user admin of the warehouse
    requests.post(
        warehouse.server.openfga_permissions_url
        + f"/warehouse/{warehouse.warehouse_id}/assignments",
        headers={"Authorization": f"Bearer {warehouse.access_token}"},
        json={
            "deletes": [],
            "writes": [{"user": user_id, "type": "ownership"}],
        },
    ).raise_for_status()

    # Check if user is admin
    response = requests.get(
        warehouse.server.openfga_permissions_url
        + f"/warehouse/{warehouse.warehouse_id}/assignments",
        headers={"Authorization": f"Bearer {warehouse.access_token}"},
    )
    response.raise_for_status()
    assignments = response.json()["assignments"]
    assignment = [
        a for a in assignments if a["user"] == user_id and a["type"] == "ownership"
    ]
    assert len(assignment) == 1


def test_create_namespace(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_create_namespace",)
    catalog.create_namespace(namespace)
    assert namespace in catalog.list_namespaces()


def test_list_namespaces(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    catalog.create_namespace(("test_list_namespaces_1",))
    catalog.create_namespace(("test_list_namespaces_2"))
    namespaces = catalog.list_namespaces()
    assert ("test_list_namespaces_1",) in namespaces
    assert ("test_list_namespaces_2",) in namespaces


def test_list_hierarchical_namespaces(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    catalog.create_namespace(("test_list_hierarchical_namespaces_1",))
    catalog.create_namespace(
        ("test_list_hierarchical_namespaces_1", "test_list_hierarchical_namespaces_2")
    )
    namespaces = catalog.list_namespaces()
    assert ("test_list_hierarchical_namespaces_1",) in namespaces
    assert all([len(namespace) == 1 for namespace in namespaces])
    namespaces = catalog.list_namespaces(
        namespace=("test_list_hierarchical_namespaces_1",)
    )
    print(namespaces)
    assert (
        "test_list_hierarchical_namespaces_1",
        "test_list_hierarchical_namespaces_2",
    ) in namespaces
    assert len(namespaces) == 1


def test_default_location_for_namespace_is_set(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_default_location_for_namespace",)
    catalog.create_namespace(namespace)
    loaded_properties = catalog.load_namespace_properties(namespace)
    assert "location" in loaded_properties


def test_namespace_properties(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_namespace_properties",)
    properties = {"key-1": "value-1", "key2": "value2"}
    catalog.create_namespace(namespace, properties=properties)
    loaded_properties = catalog.load_namespace_properties(namespace)
    for key, value in properties.items():
        assert loaded_properties[key] == value


def test_drop_namespace(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_drop_namespace",)
    catalog.create_namespace(namespace)
    assert namespace in catalog.list_namespaces()
    catalog.drop_namespace(namespace)
    assert namespace not in catalog.list_namespaces()


def test_create_table(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_create_table",)
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    # Namespace is required:
    with pytest.raises(Exception) as e:
        catalog.create_table(table_name, schema=schema)
        assert "NamespaceNotFound" in str(e)

    catalog.create_namespace(namespace)
    catalog.create_table((*namespace, table_name), schema=schema)
    loaded_table = catalog.load_table((*namespace, table_name))
    assert len(loaded_table.schema().fields) == 3


def test_drop_table(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    assert catalog.load_table((*namespace.name, table_name))
    catalog.drop_table((*namespace.name, table_name))
    with pytest.raises(Exception) as e:
        catalog.load_table((*namespace.name, table_name))
        assert "NoSuchTableError" in str(e)


def test_drop_purge_table(namespace: conftest.Namespace, storage_config):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    tab = catalog.load_table((*namespace.name, table_name))

    properties = tab.io.properties
    if storage_config["storage-profile"]["type"] == "s3":
        # Gotta use the s3 creds here since the prefix no longer exists after deletion & at least minio will not allow
        # listing a location that doesn't exist with our downscoped cred
        properties = dict()
        properties["s3.access-key-id"] = storage_config["storage-credential"][
            "aws-access-key-id"
        ]
        properties["s3.secret-access-key"] = storage_config["storage-credential"][
            "aws-secret-access-key"
        ]
        properties["s3.endpoint"] = storage_config["storage-profile"]["endpoint"]

    file_io = io._infer_file_io_from_scheme(tab.location(), properties)

    catalog.drop_table((*namespace.name, table_name), purge_requested=True)

    with pytest.raises(Exception) as e:
        catalog.load_table((*namespace.name, table_name))
        assert "NoSuchTableError" in str(e)

    location = tab.location().rstrip("/") + "/"

    inp = file_io.new_input(location)
    assert inp.exists(), f"Table location {location} still exists"
    # sleep to give time for the table to be gone
    time.sleep(5)

    inp = file_io.new_input(location)
    assert not inp.exists(), f"Table location {location} still exists"

    with pytest.raises(Exception) as e:
        catalog.load_table((*namespace.name, table_name))
        assert "NoSuchTableError" in str(e)


def test_table_properties(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    properties = {"key-1": "value-1", "key2": "value2"}
    catalog.create_table(
        (*namespace.name, table_name), schema=schema, properties=properties
    )
    table = catalog.load_table((*namespace.name, table_name))
    assert table.properties == properties


def test_list_tables(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    assert len(catalog.list_tables(namespace.name)) == 0
    table_name_1 = "my_table_1"
    table_name_2 = "my_table_2"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name_1), schema=schema)
    catalog.create_table((*namespace.name, table_name_2), schema=schema)
    tables = catalog.list_tables(namespace.name)
    assert len(tables) == 2
    assert (*namespace.name, table_name_1) in tables
    assert (*namespace.name, table_name_2) in tables


def test_write_read(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "my_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    table = catalog.load_table((*namespace.name, table_name))

    df = pd.DataFrame(
        {
            "my_ints": [1, 2, 3],
            "my_floats": [1.1, 2.2, 3.3],
            "strings": ["a", "b", "c"],
        }
    )
    data = pa.Table.from_pandas(df)
    table.append(data)

    read_table = table.scan().to_arrow()
    read_df = read_table.to_pandas()

    assert read_df.equals(df)
