drop trigger before_insert_check_metadata ON "table";

INSERT INTO public.project (project_id, project_name, created_at, updated_at)
VALUES ('01933eb6-589e-7840-8460-328e1c6d186d', 'project-c2f2a268-5028-43ff-81c1-b4a210b03d85',
        '2024-11-18 09:59:17.918681+00', NULL);

INSERT INTO public.warehouse (warehouse_id, project_id, warehouse_name, storage_profile, storage_secret_id, created_at,
                              updated_at, status, tabular_expiration_seconds, tabular_delete_mode)
VALUES ('c66b1372-a593-11ef-8d1a-eb80a0602bd7', '01933eb6-589e-7840-8460-328e1c6d186d',
        'warehouse-5ad122d9-6f12-4fb4-8809-1365a4f831d1', '{
    "type": "s3",
    "bucket": "tests",
    "flavor": "minio",
    "region": "local",
    "endpoint": "http://minio:9000/",
    "key-prefix": null,
    "sts-enabled": true,
    "sts-role-arn": null,
    "assume-role-arn": null,
    "path-style-access": true
  }', 'c66ac002-a593-11ef-a6bf-1b71ac1a7773', '2024-11-18 09:59:17.942683+00', NULL, 'active', '2', 'soft'),
       ('cb92c5e8-a593-11ef-8d1a-af251844b97c', '01933eb6-589e-7840-8460-328e1c6d186d',
        'warehouse-31923a86-c4ba-4358-a042-429c7acfa95d', '{
         "type": "s3",
         "bucket": "tests",
         "flavor": "minio",
         "region": "local",
         "endpoint": "http://minio:9000/",
         "key-prefix": null,
         "sts-enabled": false,
         "sts-role-arn": null,
         "assume-role-arn": null,
         "path-style-access": true
       }', 'cb927c32-a593-11ef-a6bf-934580a8bca8', '2024-11-18 09:59:26.592326+00', NULL, 'active', '2', 'soft');

INSERT INTO public.namespace (namespace_id, warehouse_id, namespace_name, namespace_properties, created_at, updated_at)
VALUES ('01933eb6-7202-7291-8b54-690b434ecf88', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-0c7554ac-9ea3-4268-bfe3-97c8a8502ddc}', '{
    "location": "s3://tests/01933eb6-7202-7291-8b54-690b434ecf88"
  }', '2024-11-18 09:59:24.41881+00', NULL);

INSERT INTO public.server (single_row, server_id, terms_accepted, open_for_bootstrap, created_at, updated_at)
VALUES ('t', '00000000-0000-0000-0000-000000000000', 't', 'f', '2024-11-18 09:59:17.825735+00', NULL);

INSERT INTO public.tabular (tabular_id, namespace_id, name, typ, metadata_location, location, created_at, updated_at,
                            deleted_at)
VALUES ('01933eb6-720a-7f50-abc8-2bd8119129cf', '01933eb6-7202-7291-8b54-690b434ecf88', 'my_table', 'table',
        's3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf/metadata/01933eb6-7a66-7c70-8abd-d2be670bbaea.gz.metadata.json',
        's3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf',
        '2024-11-18 09:59:24.426262+00', '2024-11-18 09:59:26.566115+00', now());

INSERT INTO public."table" (table_id, metadata, created_at, updated_at)
VALUES ('01933eb6-720a-7f50-abc8-2bd8119129cf', '{
  "refs": {
    "main": {
      "type": "branch",
      "snapshot-id": 5235513341919426583
    }
  },
  "schemas": [
    {
      "type": "struct",
      "fields": [
        {
          "id": 1,
          "name": "my_ints",
          "type": "long",
          "required": false
        },
        {
          "id": 2,
          "name": "my_floats",
          "type": "double",
          "required": false
        },
        {
          "id": 3,
          "name": "strings",
          "type": "string",
          "required": false
        }
      ],
      "schema-id": 0
    }
  ],
  "location": "s3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf",
  "snapshots": [
    {
      "summary": {
        "operation": "append",
        "added-records": "3",
        "total-records": "3",
        "added-data-files": "1",
        "added-files-size": "1329",
        "total-data-files": "1",
        "total-files-size": "1329",
        "total-delete-files": "0",
        "total-equality-deletes": "0",
        "total-position-deletes": "0"
      },
      "schema-id": 0,
      "snapshot-id": 5235513341919426583,
      "timestamp-ms": 1731923966564,
      "manifest-list": "s3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf/metadata/snap-5235513341919426583-0-abf69994-6716-4b6e-8447-488f54106371.avro",
      "sequence-number": 1
    }
  ],
  "properties": {},
  "table-uuid": "01933eb6-720a-7f50-abc8-2bd8119129cf",
  "sort-orders": [
    {
      "fields": [],
      "order-id": 0
    }
  ],
  "metadata-log": [
    {
      "timestamp-ms": 1731923964426,
      "metadata-file": "s3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf/metadata/01933eb6-720a-7f50-abc8-2be13326aa94.gz.metadata.json"
    }
  ],
  "snapshot-log": [
    {
      "snapshot-id": 5235513341919426583,
      "timestamp-ms": 1731923966564
    }
  ],
  "format-version": 2,
  "last-column-id": 3,
  "default-spec-id": 0,
  "last-updated-ms": 1731923966564,
  "partition-specs": [
    {
      "fields": [],
      "spec-id": 0
    }
  ],
  "current-schema-id": 0,
  "last-partition-id": 999,
  "current-snapshot-id": 5235513341919426583,
  "last-sequence-number": 1,
  "default-sort-order-id": 0
}', '2024-11-18 09:59:24.426262+00', '2024-11-18 09:59:26.566115+00');

INSERT INTO task (task_id, warehouse_id, idempotency_key, queue_name, status, suspend_until, attempt)
VALUES ('01933eb6-720a-7f50-abc8-2bd8119129cf', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '01933eb6-720a-7f50-abc8-2bd8119129cf', 'tabular_expiration', 'pending', now(), 1);

INSERT INTO tabular_expirations (tabular_id, warehouse_id, typ, deletion_kind, task_id)
VALUES ('01933eb6-720a-7f50-abc8-2bd8119129cf', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', 'table', 'purge',
        '01933eb6-720a-7f50-abc8-2bd8119129cf');


-- Create the function
CREATE OR REPLACE FUNCTION check_metadata_not_null()
    RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.metadata IS NOT NULL THEN
        RAISE EXCEPTION 'Insert failed: metadata must be null';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create the trigger
CREATE TRIGGER before_insert_check_metadata
    BEFORE INSERT OR UPDATE
    ON "table"
    FOR EACH ROW
EXECUTE FUNCTION check_metadata_not_null();