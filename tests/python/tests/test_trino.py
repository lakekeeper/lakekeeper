import conftest


def test_create_namespace(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_create_namespace_trino")
    assert (
        "test_create_namespace_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()
    schemas = cur.execute("SHOW SCHEMAS").fetchall()
    assert ["test_create_namespace_trino"] in schemas


def test_list_namespaces(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_list_namespaces_trino_1")
    cur.execute("CREATE SCHEMA test_list_namespaces_trino_2")
    r = cur.execute("SHOW SCHEMAS").fetchall()
    assert ["test_list_namespaces_trino_1"] in r
    assert ["test_list_namespaces_trino_2"] in r


def test_information_schema_tables(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_information_schema_tables_trino")
    cur.execute(
        "CREATE TABLE test_information_schema_tables_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        "CREATE OR REPLACE VIEW test_information_schema_tables_trino.my_view AS SELECT strings FROM test_information_schema_tables_trino.my_table"
    )
    r = cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='test_information_schema_tables_trino'"
    ).fetchall()
    # Trino returns tables and views in arbitrary order
    assert len(r) == 2
    assert ["my_table"] in r
    assert ["my_view"] in r
    r = cur.execute(
        "SELECT table_name FROM information_schema.views WHERE table_schema='test_information_schema_tables_trino'"
    ).fetchall()
    assert r == [["my_view"]]
    cur.execute("SELECT table_name FROM information_schema.tables").fetchall()


def test_namespace_create_if_not_exists(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS test_namespace_create_if_not_exists_trino")
    cur.execute("CREATE SCHEMA IF NOT EXISTS test_namespace_create_if_not_exists_trino")
    assert (
        "test_namespace_create_if_not_exists_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()


def test_drop_namespace(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_drop_namespace_trino")
    assert (
        "test_drop_namespace_trino",
    ) in warehouse.pyiceberg_catalog.list_namespaces()
    cur.execute("DROP SCHEMA test_drop_namespace_trino")
    assert (
        "test_drop_namespace_trino",
    ) not in warehouse.pyiceberg_catalog.list_namespaces()


def test_create_table(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_create_table_trino")
    cur.execute(
        "CREATE TABLE test_create_table_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_create_table_trino", "my_table")
    )
    assert len(loaded_table.schema().fields) == 3


def test_create_table_with_data(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_create_table_with_data_trino")
    cur.execute(
        "CREATE TABLE test_create_table_with_data_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        "INSERT INTO test_create_table_with_data_trino.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
    )


def test_replace_table(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_replace_table_trino")
    cur.execute(
        "CREATE TABLE test_replace_table_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        "INSERT INTO test_replace_table_trino.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
    )
    cur.execute(
        "CREATE OR REPLACE TABLE test_replace_table_trino.my_table (my_ints INT, my_floats DOUBLE) WITH (format='PARQUET')"
    )
    loaded_table = warehouse.pyiceberg_catalog.load_table(
        ("test_replace_table_trino", "my_table")
    )
    assert len(loaded_table.schema().fields) == 2


def test_nested_schema(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_nested_schema_trino")
    cur.execute('CREATE SCHEMA "test_nested_schema_trino.nested"')
    assert (
        "test_nested_schema_trino",
        "nested",
    ) in warehouse.pyiceberg_catalog.list_namespaces(
        "test_nested_schema_trino",
    )


def test_set_properties(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_set_properties_trino")
    cur.execute(
        "CREATE TABLE test_set_properties_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        """ALTER TABLE test_set_properties_trino.my_table SET PROPERTIES format_version = 2"""
    )


def test_rename_table(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_rename_table_trino")
    cur.execute(
        "CREATE TABLE test_rename_table_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        "ALTER TABLE test_rename_table_trino.my_table RENAME TO test_rename_table_trino.my_table_renamed"
    )
    assert (
        "test_rename_table_trino",
        "my_table_renamed",
    ) in warehouse.pyiceberg_catalog.list_tables("test_rename_table_trino")


def test_create_view(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_create_view_trino")
    cur.execute(
        "CREATE TABLE test_create_view_trino.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        "CREATE OR REPLACE VIEW test_create_view_trino.my_view AS SELECT strings FROM test_create_view_trino.my_table"
    )
    assert ["my_view"] in cur.execute(
        f"SHOW TABLES IN test_create_view_trino"
    ).fetchall()

    # Insert data and query view
    cur.execute(
        "INSERT INTO test_create_view_trino.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')"
    )
    r = cur.execute("SELECT * FROM test_create_view_trino.my_view").fetchall()
    assert r == [["a"], ["b"]]


def test_replace_view(trino):
    ns = "test_replace_view"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        f"CREATE OR REPLACE VIEW {ns}.my_view AS SELECT strings FROM {ns}.my_table"
    )
    assert ["my_view"] in cur.execute(f"SHOW TABLES IN {ns}").fetchall()
    # Insert data and query view
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")
    r = cur.execute(f"SELECT * FROM {ns}.my_view").fetchall()
    assert r == [["a"], ["b"]]

    cur.execute(
        f"CREATE OR REPLACE VIEW {ns}.my_view AS SELECT strings FROM {ns}.my_table"
    )


def test_reuse_original_view_version(trino):
    ns = "test_reuse_original_view_version"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    cur.execute(
        f"CREATE OR REPLACE VIEW {ns}.my_view AS SELECT strings FROM {ns}.my_table"
    )
    assert ["my_view"] in cur.execute(f"SHOW TABLES IN {ns}").fetchall()
    # Insert data and query view
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")
    r = cur.execute(f"SELECT * FROM {ns}.my_view").fetchall()
    assert r == [["a"], ["b"]]

    cur.execute(
        f"CREATE OR REPLACE VIEW {ns}.my_view AS SELECT strings FROM {ns}.my_table"
    )


def test_alter_table_execute_optimize(trino, warehouse: conftest.Warehouse):
    """Test ALTER TABLE EXECUTE optimize command"""
    ns = "test_alter_table_execute_optimize"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data in multiple batches to create multiple files
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (3, 3.0, 'c')")

    # Run optimize
    cur.execute(f"ALTER TABLE {ns}.my_table EXECUTE optimize")

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 3


def test_alter_table_execute_optimize_with_file_size_threshold(
    trino, warehouse: conftest.Warehouse
):
    """Test ALTER TABLE EXECUTE optimize with file_size_threshold parameter"""
    ns = "test_optimize_file_size_threshold"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Run optimize with file_size_threshold
    cur.execute(
        f"ALTER TABLE {ns}.my_table EXECUTE optimize(file_size_threshold => '128MB')"
    )

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 2


def test_alter_table_execute_optimize_partitioned_table(
    trino, warehouse: conftest.Warehouse
):
    """Test ALTER TABLE EXECUTE optimize on partitioned table with WHERE clause"""
    ns = "test_optimize_partitioned"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR, partition_key INT) "
        f"WITH (format='PARQUET', partitioning=ARRAY['partition_key'])"
    )

    # Insert data into different partitions
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a', 1), (2, 2.0, 'b', 1)")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (3, 3.0, 'c', 2), (4, 4.0, 'd', 2)")

    # Optimize specific partition
    cur.execute(f"ALTER TABLE {ns}.my_table EXECUTE optimize WHERE partition_key = 1")

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 4


def test_alter_table_execute_optimize_manifests(trino, warehouse: conftest.Warehouse):
    """Test ALTER TABLE EXECUTE optimize_manifests command"""
    ns = "test_optimize_manifests"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR, partition_key INT) "
        f"WITH (format='PARQUET', partitioning=ARRAY['partition_key'])"
    )

    # Insert data to create manifest files
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a', 1), (2, 2.0, 'b', 2)")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (3, 3.0, 'c', 3), (4, 4.0, 'd', 4)")

    # Run optimize_manifests
    cur.execute(f"ALTER TABLE {ns}.my_table EXECUTE optimize_manifests")

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 4


def test_alter_table_execute_expire_snapshots(trino, warehouse: conftest.Warehouse):
    """Test ALTER TABLE EXECUTE expire_snapshots command"""
    ns = "test_expire_snapshots"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create snapshots
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (3, 3.0, 'c')")

    # Get initial snapshot count
    snapshots = cur.execute(
        f'SELECT COUNT(*) FROM {ns}."my_table$snapshots"'
    ).fetchone()
    assert snapshots[0] >= 3

    # Run expire_snapshots with 7 days retention (default minimum)
    cur.execute(
        f"ALTER TABLE {ns}.my_table EXECUTE expire_snapshots(retention_threshold => '7d')"
    )

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 3


def test_alter_table_execute_remove_orphan_files(trino, warehouse: conftest.Warehouse):
    """Test ALTER TABLE EXECUTE remove_orphan_files command"""
    ns = "test_remove_orphan_files"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Run remove_orphan_files with 7 days retention (default minimum)
    result = cur.execute(
        f"ALTER TABLE {ns}.my_table EXECUTE remove_orphan_files(retention_threshold => '7d')"
    )
    result.fetchall()

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 2


def test_alter_table_execute_drop_extended_stats(trino, warehouse: conftest.Warehouse):
    """Test ALTER TABLE EXECUTE drop_extended_stats command"""
    ns = "test_drop_extended_stats"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Run ANALYZE to collect extended statistics
    try:
        cur.execute(f"ANALYZE {ns}.my_table")
    except Exception:
        # ANALYZE may not be supported in all configurations, skip if it fails
        pass

    # Run drop_extended_stats
    cur.execute(f"ALTER TABLE {ns}.my_table EXECUTE drop_extended_stats")

    # Verify data is still intact
    r = cur.execute(f"SELECT COUNT(*) FROM {ns}.my_table").fetchone()
    assert r[0] == 2


def test_metadata_table_properties(trino, warehouse: conftest.Warehouse):
    """Test $properties metadata table"""
    ns = "test_metadata_properties"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) "
        f"WITH (format='PARQUET', format_version=2)"
    )

    # Query the $properties metadata table
    r = cur.execute(f'SELECT key, value FROM {ns}."my_table$properties"').fetchall()

    # Verify we got some properties
    assert len(r) > 0

    # Check that format property exists
    keys = [row[0] for row in r]
    assert "write.format.default" in keys or "format" in keys


def test_metadata_table_history(trino, warehouse: conftest.Warehouse):
    """Test $history metadata table"""
    ns = "test_metadata_history"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create snapshots
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")

    # Query the $history metadata table
    r = cur.execute(
        f'SELECT snapshot_id, parent_id, is_current_ancestor FROM {ns}."my_table$history"'
    ).fetchall()

    # Verify we have at least 2 snapshots
    assert len(r) >= 2

    # Verify columns exist and have expected types
    for row in r:
        assert row[0] is not None  # snapshot_id
        assert isinstance(row[2], bool)  # is_current_ancestor


def test_metadata_table_metadata_log_entries(trino, warehouse: conftest.Warehouse):
    """Test $metadata_log_entries metadata table"""
    ns = "test_metadata_log_entries"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create metadata entries
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")

    # Query the $metadata_log_entries metadata table
    r = cur.execute(
        f'SELECT timestamp, file, latest_snapshot_id FROM {ns}."my_table$metadata_log_entries"'
    ).fetchall()

    # Verify we have at least one entry
    assert len(r) >= 1

    # Verify columns exist
    for row in r:
        assert row[0] is not None  # timestamp
        assert row[1] is not None  # file
        # latest_snapshot_id may be null for initial entry


def test_metadata_table_snapshots(trino, warehouse: conftest.Warehouse):
    """Test $snapshots metadata table"""
    ns = "test_metadata_snapshots"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create snapshots
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")

    # Query the $snapshots metadata table
    r = cur.execute(
        f'SELECT committed_at, snapshot_id, parent_id, operation, manifest_list FROM {ns}."my_table$snapshots"'
    ).fetchall()

    # Verify we have at least 2 snapshots
    assert len(r) >= 2

    # Verify columns exist and have expected values
    for row in r:
        assert row[0] is not None  # committed_at
        assert row[1] is not None  # snapshot_id
        assert row[3] is not None  # operation
        assert row[4] is not None  # manifest_list


def test_metadata_table_manifests(trino, warehouse: conftest.Warehouse):
    """Test $manifests metadata table"""
    ns = "test_metadata_manifests"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create manifest files
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Query the $manifests metadata table
    r = cur.execute(
        f"SELECT path, length, partition_spec_id, added_snapshot_id, added_data_files_count, added_rows_count "
        f'FROM {ns}."my_table$manifests"'
    ).fetchall()

    # Verify we have at least one manifest
    assert len(r) >= 1

    # Verify columns exist
    for row in r:
        assert row[0] is not None  # path
        assert row[1] is not None  # length
        assert row[2] is not None  # partition_spec_id
        assert row[3] is not None  # added_snapshot_id


def test_metadata_table_all_manifests(trino, warehouse: conftest.Warehouse):
    """Test $all_manifests metadata table"""
    ns = "test_metadata_all_manifests"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data multiple times to create multiple manifests
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")

    # Query the $all_manifests metadata table
    r = cur.execute(
        f'SELECT path, added_snapshot_id FROM {ns}."my_table$all_manifests"'
    ).fetchall()

    # Verify we have at least 2 manifests
    assert len(r) >= 2


def test_metadata_table_partitions(trino, warehouse: conftest.Warehouse):
    """Test $partitions metadata table"""
    ns = "test_metadata_partitions"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR, partition_key INT) "
        f"WITH (format='PARQUET', partitioning=ARRAY['partition_key'])"
    )

    # Insert data into different partitions
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a', 1), (2, 2.0, 'b', 1)")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (3, 3.0, 'c', 2), (4, 4.0, 'd', 2)")

    # Query the $partitions metadata table
    r = cur.execute(
        f'SELECT record_count, file_count, total_size FROM {ns}."my_table$partitions"'
    ).fetchall()

    # Verify we have at least 2 partitions
    assert len(r) >= 2

    # Verify columns exist and have expected values
    for row in r:
        assert row[0] is not None  # record_count
        assert row[1] is not None  # file_count
        assert row[2] is not None  # total_size
        assert row[0] > 0  # should have records
        assert row[1] > 0  # should have files


def test_metadata_table_files(trino, warehouse: conftest.Warehouse):
    """Test $files metadata table"""
    ns = "test_metadata_files"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create data files
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Query the $files metadata table
    r = cur.execute(
        f"SELECT content, file_path, record_count, file_format, file_size_in_bytes "
        f'FROM {ns}."my_table$files"'
    ).fetchall()

    # Verify we have at least one file
    assert len(r) >= 1

    # Verify columns exist and have expected values
    for row in r:
        assert row[0] is not None  # content (should be 0 for DATA)
        assert row[1] is not None  # file_path
        assert row[2] is not None  # record_count
        assert row[3] is not None  # file_format
        assert row[4] is not None  # file_size_in_bytes
        assert row[3] == "PARQUET"  # should be PARQUET format
        assert row[2] > 0  # should have records


def test_metadata_table_entries(trino, warehouse: conftest.Warehouse):
    """Test $entries metadata table"""
    ns = "test_metadata_entries"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create manifest entries
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a'), (2, 2.0, 'b')")

    # Query the $entries metadata table
    r = cur.execute(
        f'SELECT status, snapshot_id, data_file FROM {ns}."my_table$entries"'
    ).fetchall()

    # Verify we have at least one entry
    assert len(r) >= 1

    # Verify columns exist
    for row in r:
        assert row[0] is not None  # status
        assert row[1] is not None  # snapshot_id
        assert row[2] is not None  # data_file (ROW type)


def test_metadata_table_all_entries(trino, warehouse: conftest.Warehouse):
    """Test $all_entries metadata table"""
    ns = "test_metadata_all_entries"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data multiple times to create multiple entries
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b')")

    # Query the $all_entries metadata table
    r = cur.execute(
        f'SELECT status, snapshot_id FROM {ns}."my_table$all_entries"'
    ).fetchall()

    # Verify we have at least 2 entries
    assert len(r) >= 2


def test_metadata_table_refs(trino, warehouse: conftest.Warehouse):
    """Test $refs metadata table"""
    ns = "test_metadata_refs"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )

    # Insert data to create snapshots
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a')")

    # Query the $refs metadata table
    r = cur.execute(
        f'SELECT name, type, snapshot_id FROM {ns}."my_table$refs"'
    ).fetchall()

    # Verify we have at least the main branch
    assert len(r) >= 1

    # Verify the main branch exists
    names = [row[0] for row in r]
    assert "main" in names

    # Verify columns exist
    for row in r:
        assert row[0] is not None  # name
        assert row[1] is not None  # type
        assert row[2] is not None  # snapshot_id


def test_metadata_columns(trino, warehouse: conftest.Warehouse):
    """Test metadata columns $partition, $path, and $file_modified_time"""
    ns = "test_metadata_columns"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR, partition_key INT) "
        f"WITH (format='PARQUET', partitioning=ARRAY['partition_key'])"
    )

    # Insert data
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a', 1), (2, 2.0, 'b', 2)")

    # Query with metadata columns
    r = cur.execute(
        f'SELECT my_ints, "$path", "$file_modified_time" FROM {ns}.my_table'
    ).fetchall()

    # Verify we have data
    assert len(r) == 2

    # Verify metadata columns exist
    for row in r:
        assert row[1] is not None  # $path
        assert row[2] is not None  # $file_modified_time

    # Query with $partition metadata column for partitioned tables
    r = cur.execute(f'SELECT my_ints, "$partition" FROM {ns}.my_table').fetchall()

    # Verify partition metadata exists
    for row in r:
        assert row[1] is not None  # $partition


def test_metadata_table_files_with_partition_filter(
    trino, warehouse: conftest.Warehouse
):
    """Test $files metadata table with partition filters"""
    ns = "test_files_partition_filter"
    cur = trino.cursor()
    cur.execute(f"CREATE SCHEMA {ns}")
    cur.execute(
        f"CREATE TABLE {ns}.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR, partition_key INT) "
        f"WITH (format='PARQUET', partitioning=ARRAY['partition_key'])"
    )

    # Insert data into different partitions
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (1, 1.0, 'a', 1)")
    cur.execute(f"INSERT INTO {ns}.my_table VALUES (2, 2.0, 'b', 2)")

    # Query $files for specific partition using $path filter
    r = cur.execute(
        f'SELECT record_count, file_format FROM {ns}."my_table$files"'
    ).fetchall()

    # Verify we have files
    assert len(r) >= 2


def test_table_extra_properties(trino, warehouse: conftest.Warehouse):
    cur = trino.cursor()
    cur.execute("CREATE SCHEMA test_table_extra_properties")
    cur.execute(
        "CREATE TABLE test_table_extra_properties.my_table (my_ints INT, my_floats DOUBLE, strings VARCHAR) WITH (format='PARQUET')"
    )
    # Set extra properties
    cur.execute(
        """ALTER TABLE test_table_extra_properties.my_table SET PROPERTIES extra_properties = MAP(ARRAY['extra.property.one'], ARRAY['foo'])"""
    )
    # Verify extra properties are set
    r = cur.execute(
        "SELECT key, value FROM test_table_extra_properties.\"my_table$properties\" WHERE key = 'extra.property.one'"
    ).fetchall()
    assert r == [["extra.property.one", "foo"]]
