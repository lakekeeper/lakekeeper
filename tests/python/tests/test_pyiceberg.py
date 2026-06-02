import conftest
import pandas as pd
import pyarrow as pa
import pytest
import time
import pyiceberg.io as io
from pyiceberg import exceptions as exc
import requests
from urllib.parse import quote
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


def test_create_namespace_already_exists(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("test_namespace_already_exists",)
    catalog.create_namespace(namespace)
    with pytest.raises(exc.NamespaceAlreadyExistsError):
        catalog.create_namespace(namespace)


def test_namespace_case_insensitivity(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    namespace = ("Test_Case_Ns",)
    catalog.create_namespace(namespace)

    # Creating with different case should fail
    with pytest.raises(exc.NamespaceAlreadyExistsError):
        catalog.create_namespace(("test_case_ns",))

    # Loading properties with different case should succeed
    props = catalog.load_namespace_properties(("test_case_ns",))
    assert "location" in props

    props_upper = catalog.load_namespace_properties(("TEST_CASE_NS",))
    assert "location" in props_upper


def test_table_case_insensitivity(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog

    schema = pa.schema([pa.field("id", pa.int64())])
    catalog.create_table(namespace.name + ("Mixed_Case_Table",), schema=schema)

    # Load with different case
    table_lower = catalog.load_table(namespace.name + ("mixed_case_table",))
    assert table_lower is not None

    table_upper = catalog.load_table(namespace.name + ("MIXED_CASE_TABLE",))
    assert table_upper is not None

    # Creating with different case should fail
    with pytest.raises(exc.TableAlreadyExistsError):
        catalog.create_table(namespace.name + ("mixed_case_table",), schema=schema)


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


def test_drop_unknown_namespace(warehouse: conftest.Warehouse):
    catalog = warehouse.pyiceberg_catalog
    with pytest.raises(exc.NoSuchNamespaceError):
        catalog.drop_namespace(("unknown_namespace",))


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
    with pytest.raises(exc.NoSuchIdentifierError):
        catalog.create_table(table_name, schema=schema)

    catalog.create_namespace(namespace)
    catalog.create_table((*namespace, table_name), schema=schema)
    loaded_table = catalog.load_table((*namespace, table_name))
    assert len(loaded_table.schema().fields) == 3


def test_create_table_already_exists(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    table_name = "duplicate_table"
    schema = pa.schema(
        [
            pa.field("my_ints", pa.int64()),
            pa.field("my_floats", pa.float64()),
            pa.field("strings", pa.string()),
        ]
    )
    catalog.create_table((*namespace.name, table_name), schema=schema)
    with pytest.raises(exc.TableAlreadyExistsError):
        catalog.create_table((*namespace.name, table_name), schema=schema)


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
    with pytest.raises(exc.NoSuchTableError):
        catalog.load_table((*namespace.name, table_name))


def test_drop_unknown_table(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    with pytest.raises(exc.NoSuchTableError):
        catalog.drop_table((*namespace.name, "missing_table"))


def test_load_unknown_table(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
    with pytest.raises(exc.NoSuchTableError):
        catalog.load_table((*namespace.name, "missing_table"))


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

    location = tab.location().rstrip("/") + "/"
    inp = file_io.new_input(location)
    assert inp.exists(), f"Table location {location} still exists"
    
    catalog.drop_table((*namespace.name, table_name), purge_requested=True)
    with pytest.raises(exc.NoSuchTableError):
        catalog.load_table((*namespace.name, table_name))

    # sleep to give time for the table to be gone
    time.sleep(5)
    inp = file_io.new_input(location)
    assert not inp.exists(), f"Table location {location} still exists"

    with pytest.raises(exc.NoSuchTableError):
        catalog.load_table((*namespace.name, table_name))


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


def _commit_table_url(warehouse: conftest.Warehouse, namespace_name, table_name):
    """Build the REST API URL for committing updates to a table."""
    ns = "%1F".join(namespace_name)
    return (
        warehouse.server.catalog_url.rstrip("/")
        + "/"
        + "/".join(
            [
                "v1",
                str(warehouse.warehouse_id),
                "namespaces",
                ns,
                "tables",
                table_name,
            ]
        )
    )


def _auth_headers(warehouse: conftest.Warehouse):
    return {"Authorization": f"Bearer {warehouse.access_token}"}


def test_encryption_key_add_and_remove(namespace: conftest.Namespace):
    """Test add-encryption-key and remove-encryption-key REST API updates on a v3 table."""
    catalog = namespace.pyiceberg_catalog
    table_name = "encryption_keys_table"
    schema = pa.schema([pa.field("id", pa.int64())])
    catalog.create_table(
        (*namespace.name, table_name),
        schema=schema,
        properties={"format-version": "3", "encryption.key-id": "master-key"},
    )

    table = catalog.load_table((*namespace.name, table_name))
    assert table.properties["encryption.key-id"] == "master-key"

    commit_url = _commit_table_url(namespace.warehouse, namespace.name, table_name)
    headers = _auth_headers(namespace.warehouse)

    # Add an encryption key
    import base64

    key_metadata = base64.b64encode(b"some-encrypted-key-bytes").decode()
    resp = requests.post(
        commit_url,
        headers=headers,
        json={
            "requirements": [],
            "updates": [
                {
                    "action": "add-encryption-key",
                    "encryption-key": {
                        "key-id": "dek-1",
                        "encrypted-key-metadata": key_metadata,
                        "encrypted-by-id": "master-key",
                        "properties": {"created-at": "2026-01-01"},
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200, f"add-encryption-key failed: {resp.text}"

    # Verify the key is in metadata
    metadata = resp.json()["metadata"]
    enc_keys = metadata.get("encryption-keys", [])
    assert any(k["key-id"] == "dek-1" for k in enc_keys), f"Key not found in {enc_keys}"

    # Add a second key
    key_metadata_2 = base64.b64encode(b"another-encrypted-key").decode()
    resp = requests.post(
        commit_url,
        headers=headers,
        json={
            "requirements": [],
            "updates": [
                {
                    "action": "add-encryption-key",
                    "encryption-key": {
                        "key-id": "dek-2",
                        "encrypted-key-metadata": key_metadata_2,
                        "encrypted-by-id": "master-key",
                    },
                }
            ],
        },
    )
    assert resp.status_code == 200, f"add second key failed: {resp.text}"
    enc_keys = resp.json()["metadata"].get("encryption-keys", [])
    key_ids = {k["key-id"] for k in enc_keys}
    assert key_ids == {"dek-1", "dek-2"}, f"Expected both keys, got {key_ids}"

    # Remove the first key
    resp = requests.post(
        commit_url,
        headers=headers,
        json={
            "requirements": [],
            "updates": [{"action": "remove-encryption-key", "key-id": "dek-1"}],
        },
    )
    assert resp.status_code == 200, f"remove-encryption-key failed: {resp.text}"
    enc_keys = resp.json()["metadata"].get("encryption-keys", [])
    key_ids = {k["key-id"] for k in enc_keys}
    assert key_ids == {"dek-2"}, f"Expected only dek-2, got {key_ids}"


def test_encryption_key_id_immutable_via_rest(namespace: conftest.Namespace):
    """Test that encryption.key-id cannot be modified or removed via the REST API."""
    catalog = namespace.pyiceberg_catalog
    table_name = "encryption_immutable_rest"
    schema = pa.schema([pa.field("id", pa.int64())])
    catalog.create_table(
        (*namespace.name, table_name),
        schema=schema,
        properties={"encryption.key-id": "my-key"},
    )

    commit_url = _commit_table_url(namespace.warehouse, namespace.name, table_name)
    headers = _auth_headers(namespace.warehouse)

    # Attempt to modify encryption.key-id
    resp = requests.post(
        commit_url,
        headers=headers,
        json={
            "requirements": [],
            "updates": [
                {
                    "action": "set-properties",
                    "updates": {"encryption.key-id": "different-key"},
                }
            ],
        },
    )
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"
    assert "ImmutablePropertyModification" in resp.text

    # Attempt to remove encryption.key-id
    resp = requests.post(
        commit_url,
        headers=headers,
        json={
            "requirements": [],
            "updates": [
                {"action": "remove-properties", "removals": ["encryption.key-id"]}
            ],
        },
    )
    assert resp.status_code == 400, f"Expected 400, got {resp.status_code}: {resp.text}"
    assert "ImmutablePropertyRemoval" in resp.text

    # Verify property is still intact
    table = catalog.load_table((*namespace.name, table_name))
    assert table.properties["encryption.key-id"] == "my-key"


def test_write_read_multiple_tables(namespace: conftest.Namespace):
    catalog = namespace.pyiceberg_catalog
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

    table_1 = catalog.load_table((*namespace.name, table_name_1))
    table_2 = catalog.load_table((*namespace.name, table_name_2))

    df_1 = pd.DataFrame(
        {
            "my_ints": [1, 2, 3],
            "my_floats": [1.1, 2.2, 3.3],
            "strings": ["a", "b", "c"],
        }
    )
    data_1 = pa.Table.from_pandas(df_1)
    table_1.append(data_1)

    df_2 = pd.DataFrame(
        {
            "my_ints": [4, 5, 6],
            "my_floats": [4.4, 5.5, 6.6],
            "strings": ["d", "e", "f"],
        }
    )
    data_2 = pa.Table.from_pandas(df_2)
    table_2.append(data_2)

    read_table_1 = table_1.scan().to_arrow()
    read_df_1 = read_table_1.to_pandas()

    read_table_2 = table_2.scan().to_arrow()
    read_df_2 = read_table_2.to_pandas()

    assert read_df_1.equals(df_1)
    assert read_df_2.equals(df_2)
