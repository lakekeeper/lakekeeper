-- created via: pg_dump postgres --data-only -h 127.0.0.1 -U postgres -p 31102 -t "(project)|(warehouse)|(namespace)|(tabular)|(table)|(view)|(server)" > dump.sql
-- converted via:
-- new_lines = []
-- prev = None
-- with open("dump.sql") as inf:
--     lines = [l.strip() for l in inf if not l[:2] == '--' and l != "\\."]
-- for l in lines:
--     if l == "\\." or ((prev == "" or prev is None) and l == ''):
--         #prev = l
--         continue
--     is_value = not l.endswith("FROM stdin;")
--     l = l.replace("COPY", "INSERT INTO").replace("FROM stdin;", "VALUES")
--     if is_value and l != "" and not l.startswith("INSERT"):
--         parts = l.split("\t")
--         wrapped = [("'" + x + "'").replace("'\\N'", "NULL") for x in parts]
--         csv = ",".join(wrapped)
--         bracks = "(" + csv + "),"
--         new_lines.append(bracks)
--         l = bracks
--
--         #l = "(" + ",".join(map(lambda x: f"'{x}'", l.split("\t"))) + "),"
--     else:
--         new_lines.append(l)
--
--     if l == "":
--         print(new_lines[-2])
--         new_lines[-2] = new_lines[-2].rstrip(",") + ";"
--     prev = l
-- if new_lines[-1].endswith(","):
--     new_lines[-1] = new_lines[-1].rstrip(",") + ";"
-- with open("fixture.sql", "w") as outf:
--     outf.write("\n".join(new_lines))
drop trigger before_insert_check_metadata ON "table";

INSERT INTO public.project (project_id, project_name, created_at, updated_at)
VALUES ('00000000-0000-0000-0000-000000000000', 'Default Project', '2024-11-18 09:59:17.913433+00', NULL),
       ('01933eb6-589e-7840-8460-328e1c6d186d', 'project-c2f2a268-5028-43ff-81c1-b4a210b03d85',
        '2024-11-18 09:59:17.918681+00', NULL),
       ('01933eb6-a045-7583-978c-2dc72c24281e', 'project-c47d8b61-c60a-49a1-929e-3b924dc974f9',
        '2024-11-18 09:59:36.261305+00', NULL),
       ('01933eb7-336b-7633-951c-d8348565efa5', 'project-6adf1882-0169-4acf-a6e4-76dc64cabd43',
        '2024-11-18 10:00:13.931398+00', NULL),
       ('01933eb7-ccfb-73e3-9bf5-1977515096da', 'project-2d9f5b67-339e-48ba-8c29-67f1bf19a73c',
        '2024-11-18 10:00:53.243599+00', NULL),
       ('01933ebb-7a3f-7a91-b24c-6f46ead23e09', 'project-d5c8506d-5557-4551-8f59-b16fa10c4dc8',
        '2024-11-18 10:04:54.207068+00', NULL),
       ('01933ebb-8f06-7372-83e6-cb98a96e33c6', 'project-12b56a7c-45d9-4268-9c61-9e6c14c39cb0',
        '2024-11-18 10:04:59.526886+00', NULL);

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
       }', 'cb927c32-a593-11ef-a6bf-934580a8bca8', '2024-11-18 09:59:26.592326+00', NULL, 'active', '2', 'soft'),
       ('d15f681e-a593-11ef-a6bf-a7b8d49dface', '01933eb6-a045-7583-978c-2dc72c24281e',
        'warehouse-b8682b5a-af4f-4341-9b12-e4033515e374', '{
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
       }', 'd15c017e-a593-11ef-8d1a-87ba9f2ebd5f', '2024-11-18 09:59:36.301381+00', NULL, 'active', '2', 'soft'),
       ('e7cd9206-a593-11ef-a6bf-9f312c194b77', '01933eb7-336b-7633-951c-d8348565efa5',
        'warehouse-d21d2336-1348-443a-9a3f-83d0a410485a', '{
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
       }', 'e7cd3d24-a593-11ef-8d1a-f716785ef4c3', '2024-11-18 10:00:13.953602+00', NULL, 'active', '2', 'soft'),
       ('01dc26d0-a594-11ef-a6bf-a716f9026cfa', '01933eb7-ccfb-73e3-9bf5-1977515096da',
        'warehouse-0b1c3178-3c36-4208-927d-0b634ce1f107', '{
         "type": "gcs",
         "bucket": "ht-catalog-dev",
         "key-prefix": null
       }', '01d86dba-a594-11ef-8d1a-8f3f7897e052', '2024-11-18 10:00:57.646455+00', NULL, 'active', '2', 'soft'),
       ('8ee237fe-a594-11ef-8d1a-a7d902ba0b9b', '01933ebb-7a3f-7a91-b24c-6f46ead23e09',
        'warehouse-e7478455-e2be-4bb9-a8dd-e0f637eec534', '{
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
       }', '8ee1c242-a594-11ef-966c-733b7599e296', '2024-11-18 10:04:54.265615+00', NULL, 'active', '2', 'soft'),
       ('920980b8-a594-11ef-966c-035e5e8f564b', '01933ebb-8f06-7372-83e6-cb98a96e33c6',
        'warehouse-93d44b5c-f9b6-4941-8fa4-d9e5f3cf5c22', '{
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
       }', '9209361c-a594-11ef-a6bf-37049daef2fe', '2024-11-18 10:04:59.559331+00', NULL, 'active', '2', 'soft');

INSERT INTO public.namespace (namespace_id, warehouse_id, namespace_name, namespace_properties, created_at, updated_at)
VALUES ('01933eb6-58c6-7091-b930-af2594949c11', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', '{test_create_namespace}', '{
  "location": "s3://tests/01933eb6-58c6-7091-b930-af2594949c11"
}', '2024-11-18 09:59:17.958075+00', NULL),
       ('01933eb6-58d4-7312-ae46-9899a665b2b8', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', '{test_list_namespaces_1}', '{
         "location": "s3://tests/01933eb6-58d4-7312-ae46-9899a665b2b8"
       }', '2024-11-18 09:59:17.972296+00', NULL),
       ('01933eb6-58db-71c0-99aa-97f72c1d6011', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', '{test_list_namespaces_2}', '{
         "location": "s3://tests/01933eb6-58db-71c0-99aa-97f72c1d6011"
       }', '2024-11-18 09:59:17.979457+00', NULL),
       ('01933eb6-58ed-76a0-bb0f-4049ace62cf7', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{test_list_hierarchical_namespaces_1}', '{
         "location": "s3://tests/01933eb6-58ed-76a0-bb0f-4049ace62cf7"
       }', '2024-11-18 09:59:17.997834+00', NULL),
       ('01933eb6-58f1-7322-ab45-d93ab98ee407', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{test_list_hierarchical_namespaces_1,test_list_hierarchical_namespaces_2}', '{
         "location": "s3://tests/01933eb6-58f1-7322-ab45-d93ab98ee407"
       }', '2024-11-18 09:59:18.000594+00', NULL),
       ('01933eb6-5943-7f73-859e-0a1c36e0a541', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{test_default_location_for_namespace}', '{
         "location": "s3://tests/01933eb6-5943-7f73-859e-0a1c36e0a541"
       }', '2024-11-18 09:59:18.083508+00', NULL),
       ('01933eb6-594b-7be0-ab9b-7e743c7f9846', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', '{test_namespace_properties}',
        '{
          "key2": "value2",
          "key-1": "value-1",
          "location": "s3://tests/01933eb6-594b-7be0-ab9b-7e743c7f9846"
        }', '2024-11-18 09:59:18.091219+00', NULL),
       ('01933eb6-597a-7ca0-82a5-8b526d519060', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7', '{test_create_table}', '{
         "location": "s3://tests/01933eb6-597a-7ca0-82a5-8b526d519060"
       }', '2024-11-18 09:59:18.138874+00', NULL),
       ('01933eb6-599f-7611-9df7-989fc2c7bb3e', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-fc9fa272-9c51-493e-83ef-874c58cc4b63}', '{
         "location": "s3://tests/01933eb6-599f-7611-9df7-989fc2c7bb3e"
       }', '2024-11-18 09:59:18.175784+00', NULL),
       ('01933eb6-59d2-7342-8c72-b195243b449d', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-087894a7-7755-4487-a0d1-93ca2fb99173}', '{
         "location": "s3://tests/01933eb6-59d2-7342-8c72-b195243b449d"
       }', '2024-11-18 09:59:18.225993+00', NULL),
       ('01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-4f4a1f15-5ef9-4a9b-8e66-ba208375de78}', '{
         "location": "s3://tests/01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3"
       }', '2024-11-18 09:59:24.33264+00', NULL),
       ('01933eb6-71d7-7ff3-9514-fb96e92b739a', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-e03f202b-028a-4247-bf9a-bc91a1087e3e}', '{
         "location": "s3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a"
       }', '2024-11-18 09:59:24.375334+00', NULL),
       ('01933eb6-7202-7291-8b54-690b434ecf88', 'c66b1372-a593-11ef-8d1a-eb80a0602bd7',
        '{namespace-0c7554ac-9ea3-4268-bfe3-97c8a8502ddc}', '{
         "location": "s3://tests/01933eb6-7202-7291-8b54-690b434ecf88"
       }', '2024-11-18 09:59:24.41881+00', NULL),
       ('01933eb6-7a88-74f1-b285-07e7631a0fc3', 'cb92c5e8-a593-11ef-8d1a-af251844b97c', '{test_create_namespace}', '{
         "location": "s3://tests/01933eb6-7a88-74f1-b285-07e7631a0fc3"
       }', '2024-11-18 09:59:26.600443+00', NULL),
       ('01933eb6-7a8e-79e1-b65c-849a5f3380b3', 'cb92c5e8-a593-11ef-8d1a-af251844b97c', '{test_list_namespaces_1}', '{
         "location": "s3://tests/01933eb6-7a8e-79e1-b65c-849a5f3380b3"
       }', '2024-11-18 09:59:26.606508+00', NULL),
       ('01933eb6-7a90-79f2-aa05-53b4e227e713', 'cb92c5e8-a593-11ef-8d1a-af251844b97c', '{test_list_namespaces_2}', '{
         "location": "s3://tests/01933eb6-7a90-79f2-aa05-53b4e227e713"
       }', '2024-11-18 09:59:26.60888+00', NULL),
       ('01933eb6-7a96-7d90-8562-132d730504a6', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{test_list_hierarchical_namespaces_1}', '{
         "location": "s3://tests/01933eb6-7a96-7d90-8562-132d730504a6"
       }', '2024-11-18 09:59:26.614891+00', NULL),
       ('01933eb6-7a99-7c03-82ea-b50d3eec435f', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{test_list_hierarchical_namespaces_1,test_list_hierarchical_namespaces_2}', '{
         "location": "s3://tests/01933eb6-7a99-7c03-82ea-b50d3eec435f"
       }', '2024-11-18 09:59:26.617332+00', NULL),
       ('01933eb6-7aa2-7c71-8fc3-3cf8d9e63cb3', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{test_default_location_for_namespace}', '{
         "location": "s3://tests/01933eb6-7aa2-7c71-8fc3-3cf8d9e63cb3"
       }', '2024-11-18 09:59:26.626917+00', NULL),
       ('01933eb6-7aab-7561-b6a4-774db170ada5', 'cb92c5e8-a593-11ef-8d1a-af251844b97c', '{test_namespace_properties}',
        '{
          "key2": "value2",
          "key-1": "value-1",
          "location": "s3://tests/01933eb6-7aab-7561-b6a4-774db170ada5"
        }', '2024-11-18 09:59:26.635404+00', NULL),
       ('01933eb6-7abb-7370-8a9f-9c457d004f71', 'cb92c5e8-a593-11ef-8d1a-af251844b97c', '{test_create_table}', '{
         "location": "s3://tests/01933eb6-7abb-7370-8a9f-9c457d004f71"
       }', '2024-11-18 09:59:26.65135+00', NULL),
       ('01933eb6-7af0-7e81-9fd6-7cd781d7290d', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{namespace-45df6e8c-5602-464d-93ad-c389669c4e20}', '{
         "location": "s3://tests/01933eb6-7af0-7e81-9fd6-7cd781d7290d"
       }', '2024-11-18 09:59:26.704354+00', NULL),
       ('01933eb6-7b0a-77a0-af65-81eccd960f0d', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{namespace-f0f91fb0-0a54-4d56-9777-76f99a9e3e03}', '{
         "location": "s3://tests/01933eb6-7b0a-77a0-af65-81eccd960f0d"
       }', '2024-11-18 09:59:26.730706+00', NULL),
       ('01933eb6-96aa-7f80-98dc-2ed7e9cabb08', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{namespace-37b038a9-07cb-4fe9-8838-83af3b519706}', '{
         "location": "s3://tests/01933eb6-96aa-7f80-98dc-2ed7e9cabb08"
       }', '2024-11-18 09:59:33.802176+00', NULL),
       ('01933eb6-96d0-7c23-953f-63f8de46be1b', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{namespace-8ebc0f0c-f53c-4a73-a704-2ee2a91e5bf6}', '{
         "location": "s3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b"
       }', '2024-11-18 09:59:33.840294+00', NULL),
       ('01933eb6-96f1-78b1-a822-f42908748af1', 'cb92c5e8-a593-11ef-8d1a-af251844b97c',
        '{namespace-f9025601-75e5-4a37-bee1-3326184ec376}', '{
         "location": "s3://tests/01933eb6-96f1-78b1-a822-f42908748af1"
       }', '2024-11-18 09:59:33.873534+00', NULL),
       ('01933eb6-bee8-7500-b015-33453b75c01d', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_create_namespace_spark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb6-bee8-7500-b015-33453b75c01d"
        }', '2024-11-18 09:59:44.104612+00', NULL),
       ('01933eb6-befe-7322-914f-6cb7be7d18d0', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{test_list_namespaces_spark_1}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-befe-7322-914f-6cb7be7d18d0"
       }', '2024-11-18 09:59:44.126668+00', NULL),
       ('01933eb6-bf0b-7c53-9b13-94f7024d65f8', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{test_list_namespaces_spark_2}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-bf0b-7c53-9b13-94f7024d65f8"
       }', '2024-11-18 09:59:44.139396+00', NULL),
       ('01933eb6-c065-7d61-a1f6-d48039361011', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{test_namespace_create_if_not_exists}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-c065-7d61-a1f6-d48039361011"
       }', '2024-11-18 09:59:44.485057+00', NULL),
       ('01933eb6-c0b1-7f10-ac51-217bddd1f6cd', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_create_table_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-c0b1-7f10-ac51-217bddd1f6cd"
       }', '2024-11-18 09:59:44.561097+00', NULL),
       ('01933eb6-c122-7352-8489-ca06621a8d60', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_create_table_pyspark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb6-c122-7352-8489-ca06621a8d60"
        }', '2024-11-18 09:59:44.674934+00', NULL),
       ('01933eb6-c6ef-7003-8642-46659c776161', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_replace_table_pyspark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb6-c6ef-7003-8642-46659c776161"
        }', '2024-11-18 09:59:46.15968+00', NULL),
       ('01933eb6-c8f4-7642-b2f8-a0a50c5703bf', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_create_view}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf"
       }', '2024-11-18 09:59:46.676761+00', NULL),
       ('01933eb6-c98e-7043-9c9b-ffd22851bb31', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{test_create_replace_view_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31"
       }', '2024-11-18 09:59:46.830308+00', NULL),
       ('01933eb6-ca47-7382-bae8-3b04405f693f', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_rename_view_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-ca47-7382-bae8-3b04405f693f"
       }', '2024-11-18 09:59:47.015858+00', NULL),
       ('01933eb6-cc5a-7080-952a-7f3885c2208a', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_create_drop_view_spark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb6-cc5a-7080-952a-7f3885c2208a"
        }', '2024-11-18 09:59:47.546239+00', NULL),
       ('01933eb6-ccb8-74d2-bea0-bcad74194bc8', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_view_exists_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8"
       }', '2024-11-18 09:59:47.640317+00', NULL),
       ('01933eb6-cd04-7903-b279-0bf2a202e90b', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_merge_into}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b"
       }', '2024-11-18 09:59:47.716261+00', NULL),
       ('01933eb6-d191-7f41-802d-95e13b8566fb', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_drop_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb6-d191-7f41-802d-95e13b8566fb"
       }', '2024-11-18 09:59:48.881386+00', NULL),
       ('01933eb6-d1c0-7022-a425-99a84e2cdfab', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_drop_table_purge_http}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb6-d1c0-7022-a425-99a84e2cdfab"
        }', '2024-11-18 09:59:48.928497+00', NULL),
       ('01933eb7-147b-7f10-a5d5-a232502b283f', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_write_read_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-147b-7f10-a5d5-a232502b283f"
       }', '2024-11-18 10:00:06.011309+00', NULL),
       ('01933eb7-1686-7e82-aa2d-226f56647f6e', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-74113b24-186b-4103-8a7c-a01738357e17}', '{
         "location": "s3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e"
       }', '2024-11-18 10:00:06.534002+00', NULL),
       ('01933eb7-1724-7e82-a0b8-8b85d8b86596', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-43e76097-e26d-4b6e-8239-cfdd7dcbee32}', '{
         "location": "s3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596"
       }', '2024-11-18 10:00:06.692038+00', NULL),
       ('01933eb7-1935-7a32-98f2-6d17e86281cd', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-df95edcf-01d6-4d3b-a90e-49e2d52f0a90}', '{
         "location": "s3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd"
       }', '2024-11-18 10:00:07.221099+00', NULL),
       ('01933eb7-19e6-7041-8eec-0b065c5cec60', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-b226b380-9258-490d-9cde-86f911761bcd}', '{
         "location": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60"
       }', '2024-11-18 10:00:07.398272+00', NULL),
       ('01933eb7-1eb8-7f41-aad4-980fbde2cc6c', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-365c3d7e-c184-4bad-b251-33c91d5bf675}', '{
         "location": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c"
       }', '2024-11-18 10:00:08.632363+00', NULL),
       ('01933eb7-21a2-7a43-9dd0-889883b73d98', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-5c790c8d-baf2-4e1a-8410-9764746cbd14}', '{
         "location": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98"
       }', '2024-11-18 10:00:09.378429+00', NULL),
       ('01933eb7-25e8-79e1-8052-4bbcb38172a5', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-637a42af-2d65-4512-a5af-7c9962a3f223}', '{
         "location": "s3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5"
       }', '2024-11-18 10:00:10.472182+00', NULL),
       ('01933eb7-26a0-7953-be13-3c1668fa9e73', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-5a96d259-b2b8-46b0-b37b-dbe563ebc2b1}', '{
         "location": "s3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73"
       }', '2024-11-18 10:00:10.656631+00', NULL),
       ('01933eb7-3ddd-7093-9179-0a85c364536f', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_create_namespace_spark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb7-3ddd-7093-9179-0a85c364536f"
        }', '2024-11-18 10:00:16.605384+00', NULL),
       ('01933eb7-3e07-7501-b5bd-495e2b03b6e3', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{test_list_namespaces_spark_2}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-3e07-7501-b5bd-495e2b03b6e3"
       }', '2024-11-18 10:00:16.647417+00', NULL),
       ('01933eb7-3fb2-72f0-80d9-768a705c0dfe', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_create_table_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-3fb2-72f0-80d9-768a705c0dfe"
       }', '2024-11-18 10:00:17.074131+00', NULL),
       ('01933eb7-401d-7f43-bfeb-708eccb528c7', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_create_table_pyspark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb7-401d-7f43-bfeb-708eccb528c7"
        }', '2024-11-18 10:00:17.181308+00', NULL),
       ('01933eb7-4568-7fd3-95d7-bdfa849f10c7', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_replace_table_pyspark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7"
        }', '2024-11-18 10:00:18.536284+00', NULL),
       ('01933eb7-47af-7010-b530-cea9134c41fc', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{test_create_replace_view_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-47af-7010-b530-cea9134c41fc"
       }', '2024-11-18 10:00:19.119611+00', NULL),
       ('01933eb7-486a-7b62-8a34-ce35f730d71f', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_rename_view_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f"
       }', '2024-11-18 10:00:19.306199+00', NULL),
       ('01933eb7-4a41-7681-b1b4-e0049fcf4f9c', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_create_drop_view_spark}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb7-4a41-7681-b1b4-e0049fcf4f9c"
        }', '2024-11-18 10:00:19.777637+00', NULL),
       ('01933eb7-a934-77e1-9d0a-30d98347067d', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_table_properties}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-a934-77e1-9d0a-30d98347067d"
       }', '2024-11-18 10:00:44.084057+00', NULL),
       ('01933eb7-a9ac-74d2-a815-8aa7da64aa17', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_write_read_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17"
       }', '2024-11-18 10:00:44.204432+00', NULL),
       ('01933eb7-13d9-7590-86b5-2bae92be7faf', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_query_empty_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-13d9-7590-86b5-2bae92be7faf"
       }', '2024-11-18 10:00:05.849694+00', NULL),
       ('01933eb7-1425-7680-9530-5587fde26b90', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_table_properties}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-1425-7680-9530-5587fde26b90"
       }', '2024-11-18 10:00:05.925432+00', NULL),
       ('01933eb7-1539-7ac3-bb04-6e720df6cf36', 'd15f681e-a593-11ef-a6bf-a7b8d49dface', '{test_list_tables}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-1539-7ac3-bb04-6e720df6cf36"
       }', '2024-11-18 10:00:06.201541+00', NULL),
       ('01933eb7-155c-78d1-bc55-690f1bd3984e', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-3a64e541-a3cd-430f-9dc5-d2afb2fef148}', '{
         "location": "s3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e"
       }', '2024-11-18 10:00:06.236545+00', NULL),
       ('01933eb7-17a3-7c12-9de5-c58fb4a1cc53', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-7d3a7b65-729c-4adb-93ad-28b96dc78d90}', '{
         "location": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53"
       }', '2024-11-18 10:00:06.819608+00', NULL),
       ('01933eb7-1b1c-7790-b8df-94fd06361623', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-fad55127-2a50-4524-8232-dffb2b222ca3}', '{
         "location": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623"
       }', '2024-11-18 10:00:07.708047+00', NULL),
       ('01933eb7-1d7c-79b0-b397-12a5d1a904fc', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-24bca36b-8ebd-4b6e-83de-ed09b8977081}', '{
         "location": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc"
       }', '2024-11-18 10:00:08.316184+00', NULL),
       ('01933eb7-1fdc-7391-a27c-93c0025afdfe', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-22721f4e-06b2-4c65-8ec2-f6db7c047618}', '{
         "location": "s3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe"
       }', '2024-11-18 10:00:08.924891+00', NULL),
       ('01933eb7-2076-75b3-8100-b0c0b8d3da98', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-099015ca-0782-4f84-9f36-fba2a6d03b37}', '{
         "location": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98"
       }', '2024-11-18 10:00:09.078231+00', NULL),
       ('01933eb7-2536-7c31-b746-c09a2467ec76', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-9f453537-4b6c-4531-b8db-d077126cdbd2}', '{
         "location": "s3://tests/01933eb7-2536-7c31-b746-c09a2467ec76"
       }', '2024-11-18 10:00:10.294632+00', NULL),
       ('01933eb7-274b-7333-a74b-fe26e4690130', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-39497c7e-334b-4f0d-b129-7a1e56c419b4}', '{
         "location": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130"
       }', '2024-11-18 10:00:10.827117+00', NULL),
       ('01933eb7-2923-7fb0-80eb-96762c3d6034', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-6c5135ca-0ac1-4f0e-a7be-113bb5b65b29}', '{
         "location": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034"
       }', '2024-11-18 10:00:11.299274+00', NULL),
       ('01933eb7-2ab4-73d3-bea1-8f8f131a3853', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-244a1b75-bdd6-45de-b3ce-3a5fd2e469b4}', '{
         "location": "s3://tests/01933eb7-2ab4-73d3-bea1-8f8f131a3853"
       }', '2024-11-18 10:00:11.700021+00', NULL),
       ('01933eb7-2ac9-7421-a6b1-0dc58c9fee55', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-244a1b75-bdd6-45de-b3ce-3a5fd2e469b4,nest1}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-2ac9-7421-a6b1-0dc58c9fee55"
       }', '2024-11-18 10:00:11.721449+00', NULL),
       ('01933eb7-2b59-7f40-9bc6-ac710708918e', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-244a1b75-bdd6-45de-b3ce-3a5fd2e469b4,nest1,nest2}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-2b59-7f40-9bc6-ac710708918e"
       }', '2024-11-18 10:00:11.865901+00', NULL),
       ('01933eb7-2be1-76b1-a0e5-be4f22396d58', 'd15f681e-a593-11ef-a6bf-a7b8d49dface',
        '{namespace-244a1b75-bdd6-45de-b3ce-3a5fd2e469b4,nest1,nest2,nest3}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-2be1-76b1-a0e5-be4f22396d58"
       }', '2024-11-18 10:00:12.001412+00', NULL),
       ('01933eb7-3df7-7002-b0be-d2c86cb8cbe2', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{test_list_namespaces_spark_1}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-3df7-7002-b0be-d2c86cb8cbe2"
       }', '2024-11-18 10:00:16.631953+00', NULL),
       ('01933eb7-3f68-7253-afba-37d4a1fbf0c2', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{test_namespace_create_if_not_exists}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-3f68-7253-afba-37d4a1fbf0c2"
       }', '2024-11-18 10:00:17.000213+00', NULL),
       ('01933eb7-4723-7dc3-94a8-61ee8faf0e9b', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_create_view}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b"
       }', '2024-11-18 10:00:18.979191+00', NULL),
       ('01933eb7-4aa7-71d2-a7e6-ff7e7399be73', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_view_exists_spark}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73"
       }', '2024-11-18 10:00:19.879168+00', NULL),
       ('01933eb7-4af5-7902-8c9b-b4edff484de0', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_merge_into}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0"
       }', '2024-11-18 10:00:19.957029+00', NULL),
       ('01933eb7-4f5d-7e33-8245-b485ea9daf37', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_drop_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-4f5d-7e33-8245-b485ea9daf37"
       }', '2024-11-18 10:00:21.085668+00', NULL),
       ('01933eb7-4f8b-7763-b6c6-9657905ffaa6', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_drop_table_purge_http}',
        '{
          "owner": "root",
          "location": "s3://tests/01933eb7-4f8b-7763-b6c6-9657905ffaa6"
        }', '2024-11-18 10:00:21.131544+00', NULL),
       ('01933eb7-a8ce-73a1-9a96-096b72e2a3fa', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_query_empty_table}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-a8ce-73a1-9a96-096b72e2a3fa"
       }', '2024-11-18 10:00:43.982788+00', NULL),
       ('01933eb7-aaac-7321-b222-9efacf41758f', 'e7cd9206-a593-11ef-a6bf-9f312c194b77', '{test_list_tables}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-aaac-7321-b222-9efacf41758f"
       }', '2024-11-18 10:00:44.459991+00', NULL),
       ('01933eb7-aadf-7a40-968b-5bc0a7a1f6c0', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-5e1e9183-0797-4220-81d3-210aae7782c0}', '{
         "location": "s3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0"
       }', '2024-11-18 10:00:44.511082+00', NULL),
       ('01933eb7-ac3e-70e2-851a-fee8607189e6', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-664fdb6a-0c78-4b8a-b11b-3a38b09b49a1}', '{
         "location": "s3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6"
       }', '2024-11-18 10:00:44.862556+00', NULL),
       ('01933eb7-acf4-7a91-86e1-e5bb19aa5c77', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-c8b7a1b9-d649-419f-87e0-f98a6441365c}', '{
         "location": "s3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77"
       }', '2024-11-18 10:00:45.044151+00', NULL),
       ('01933eb7-ad90-7080-bd48-c711a14b54de', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-130f59c5-5eb7-4476-8024-4c74f57bf961}', '{
         "location": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de"
       }', '2024-11-18 10:00:45.200929+00', NULL),
       ('01933eb7-af6d-7021-9d41-90531b3b12f2', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-8153c689-587c-495d-a95c-c7e39cb4f87e}', '{
         "location": "s3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2"
       }', '2024-11-18 10:00:45.677182+00', NULL),
       ('01933eb7-b051-7691-8411-85e61072d468', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-a059dda4-0aab-4fcc-92aa-64e2b66428e6}', '{
         "location": "s3://tests/01933eb7-b051-7691-8411-85e61072d468"
       }', '2024-11-18 10:00:45.904986+00', NULL),
       ('01933eb7-b1b4-7861-9869-6ff53a21abb2', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-a7b9ee27-c7fb-4a0e-8b06-d42ff1ca0c7c}', '{
         "location": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2"
       }', '2024-11-18 10:00:46.260758+00', NULL),
       ('01933eb7-b443-7563-8995-7dccf97b2fe4', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-f8136b7c-64cd-4a7b-8e74-fbf91d39914a}', '{
         "location": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4"
       }', '2024-11-18 10:00:46.914999+00', NULL),
       ('01933eb7-b58e-7570-873b-64c7773c76df', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-9df56e25-3f45-46ff-b573-b31d9feb26aa}', '{
         "location": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df"
       }', '2024-11-18 10:00:47.246809+00', NULL),
       ('01933eb7-b6c7-7190-bac1-3d1ec943c2f5', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-60a5538b-e0e6-4d40-8fcb-d58e3cad699f}', '{
         "location": "s3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5"
       }', '2024-11-18 10:00:47.55954+00', NULL),
       ('01933eb7-b75b-7931-9902-b17767208ad4', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-3628cc51-a161-440b-a8f2-392a778de3f2}', '{
         "location": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4"
       }', '2024-11-18 10:00:47.707232+00', NULL),
       ('01933eb7-b89c-7551-94ab-178a5c9713e7', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-d5c60c63-3e79-4c5e-807f-4d61b3936592}', '{
         "location": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7"
       }', '2024-11-18 10:00:48.028363+00', NULL),
       ('01933eb7-bc55-7252-940f-dd7077b351a1', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-0b47620f-cf9e-4af2-8fe1-7be33e2b4969}', '{
         "location": "s3://tests/01933eb7-bc55-7252-940f-dd7077b351a1"
       }', '2024-11-18 10:00:48.980972+00', NULL),
       ('01933eb7-bd0a-7732-abe5-e000d0902ca4', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-c9bd4252-cd4e-4706-abf8-f7968b88ffbb}', '{
         "location": "s3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4"
       }', '2024-11-18 10:00:49.16237+00', NULL),
       ('01933eb7-bf2d-77c2-8c09-4a1940206f6f', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-dc4d1fa9-bc23-4a41-a9c9-c45fec3d1df3}', '{
         "location": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f"
       }', '2024-11-18 10:00:49.70977+00', NULL),
       ('01933eb7-c13f-71c0-9500-5e0659cf3f73', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-ca16c28e-8baa-4499-9ab0-01192180e931}', '{
         "location": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73"
       }', '2024-11-18 10:00:50.23914+00', NULL),
       ('01933eb7-c2d3-7f62-9e75-432c506f9539', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-fe9e9799-5425-46b4-9bb5-e28084eef925}', '{
         "location": "s3://tests/01933eb7-c2d3-7f62-9e75-432c506f9539"
       }', '2024-11-18 10:00:50.643478+00', NULL),
       ('01933eb7-c2e4-70a2-953b-3af4cee47b39', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-fe9e9799-5425-46b4-9bb5-e28084eef925,nest1}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-c2e4-70a2-953b-3af4cee47b39"
       }', '2024-11-18 10:00:50.66026+00', NULL),
       ('01933eb7-c372-77e2-915c-5461686b0df2', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-fe9e9799-5425-46b4-9bb5-e28084eef925,nest1,nest2}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-c372-77e2-915c-5461686b0df2"
       }', '2024-11-18 10:00:50.802815+00', NULL),
       ('01933eb7-c3ff-7ce0-aab6-ad5338d4955a', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-fe9e9799-5425-46b4-9bb5-e28084eef925,nest1,nest2,nest3}', '{
         "owner": "root",
         "location": "s3://tests/01933eb7-c3ff-7ce0-aab6-ad5338d4955a"
       }', '2024-11-18 10:00:50.943884+00', NULL),
       ('01933eb7-e9e9-7bb0-b15f-8d95074afc9f', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_create_namespace_spark}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb7-e9e9-7bb0-b15f-8d95074afc9f"
        }', '2024-11-18 10:01:00.649911+00', NULL),
       ('01933eb7-ebb5-7672-8832-c23788468a96', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{test_namespace_create_if_not_exists}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb7-ebb5-7672-8832-c23788468a96"
       }', '2024-11-18 10:01:01.109679+00', NULL),
       ('01933eb7-f3b4-7743-861e-aacc8dfd1750', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_create_table_pyspark}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb7-f3b4-7743-861e-aacc8dfd1750"
        }', '2024-11-18 10:01:03.156075+00', NULL),
       ('01933eb8-183f-7161-b5d1-2ce79b3a5e51', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_create_view}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51"
       }', '2024-11-18 10:01:12.511925+00', NULL),
       ('01933eb8-3106-7920-aa79-96cbd5b79a1f', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_rename_view_spark}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f"
       }', '2024-11-18 10:01:18.854129+00', NULL),
       ('01933eb8-3d70-7781-bcbe-cecc1747417a', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_create_drop_view_spark}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb8-3d70-7781-bcbe-cecc1747417a"
        }', '2024-11-18 10:01:22.032124+00', NULL),
       ('01933eb8-e4e4-7f80-9471-d543ea2f8e1b', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_write_read_table}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b"
       }', '2024-11-18 10:02:04.900744+00', NULL),
       ('01933eb8-fbb8-77a3-a202-7506067ac3f4', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-b9a5093b-d8f7-4305-abbd-db2de78ca14a}', '{
         "location": "gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4"
       }', '2024-11-18 10:02:10.744893+00', NULL),
       ('01933eb9-51d5-7b71-a198-c0fa5d5550be', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-5fb73fb6-ad4e-45fe-9de4-598f3546c152}', '{
         "location": "gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be"
       }', '2024-11-18 10:02:32.789515+00', NULL),
       ('01933eb9-895b-7d51-b004-b5d32d8a8d96', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-c48d7cb2-ce28-4b5c-bdd7-d08a2f2028e2}', '{
         "location": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96"
       }', '2024-11-18 10:02:47.003895+00', NULL),
       ('01933eb9-e729-7e81-945e-f27ad0fdb595', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-3a2a2135-a05a-4327-9d26-d7eb92d8d06f}', '{
         "location": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595"
       }', '2024-11-18 10:03:11.017931+00', NULL),
       ('01933eba-b7a6-7f12-aec3-7f57fa74a505', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-4a2a243a-99fd-4b24-ba58-d5e5290e5b6e}', '{
         "location": "gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505"
       }', '2024-11-18 10:04:04.390108+00', NULL),
       ('01933ebb-486e-7982-a1f1-acf24f18a74f', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-27194af2-65eb-4697-9eeb-55beedc540ea}', '{
         "location": "gs://ht-catalog-dev/01933ebb-486e-7982-a1f1-acf24f18a74f"
       }', '2024-11-18 10:04:41.45493+00', NULL),
       ('01933eb7-bdc1-7d13-b222-4b225553abe5', 'e7cd9206-a593-11ef-a6bf-9f312c194b77',
        '{namespace-fa3a26bb-8b26-4dc1-85ce-ced47134bc37}', '{
         "location": "s3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5"
       }', '2024-11-18 10:00:49.345734+00', NULL),
       ('01933eb7-ea58-71d1-b2d9-accac27d4174', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{test_list_namespaces_spark_1}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb7-ea58-71d1-b2d9-accac27d4174"
       }', '2024-11-18 10:01:00.760412+00', NULL),
       ('01933eb7-ea66-7682-ad6b-6a2cf443ceb4', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{test_list_namespaces_spark_2}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb7-ea66-7682-ad6b-6a2cf443ceb4"
       }', '2024-11-18 10:01:00.774015+00', NULL),
       ('01933eb7-ec08-7cc0-a665-79d9fed8c5d3', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_create_table_spark}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb7-ec08-7cc0-a665-79d9fed8c5d3"
       }', '2024-11-18 10:01:01.192188+00', NULL),
       ('01933eb8-0283-79c1-8852-3fc21025b598', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_replace_table_pyspark}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598"
        }', '2024-11-18 10:01:06.947014+00', NULL),
       ('01933eb8-21f0-70e3-ae90-83a460633f13', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{test_create_replace_view_spark}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13"
       }', '2024-11-18 10:01:14.992765+00', NULL),
       ('01933eb8-472c-7a53-83e1-1aee37f700fb', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_view_exists_spark}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb"
       }', '2024-11-18 10:01:24.524065+00', NULL),
       ('01933eb8-4f4a-70a2-b2ec-d30b6237ff9b', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_merge_into}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b"
       }', '2024-11-18 10:01:26.602711+00', NULL),
       ('01933eb8-76d0-7620-925a-083d7d6fff4f', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_drop_table}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-76d0-7620-925a-083d7d6fff4f"
       }', '2024-11-18 10:01:36.720502+00', NULL),
       ('01933eb8-7cc5-76b2-aac3-5062a389822a', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_drop_table_purge_spark}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb8-7cc5-76b2-aac3-5062a389822a"
        }', '2024-11-18 10:01:38.245686+00', NULL),
       ('01933eb8-8540-7b00-8f31-d4d919b07d2b', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_drop_table_purge_http}',
        '{
          "owner": "root",
          "location": "gs://ht-catalog-dev/01933eb8-8540-7b00-8f31-d4d919b07d2b"
        }', '2024-11-18 10:01:40.416297+00', NULL),
       ('01933eb8-d541-70d0-a5c8-26f258d3b57d', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_query_empty_table}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-d541-70d0-a5c8-26f258d3b57d"
       }', '2024-11-18 10:02:00.897719+00', NULL),
       ('01933eb8-db32-7ad3-81dc-b9c75dd674e0', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_table_properties}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-db32-7ad3-81dc-b9c75dd674e0"
       }', '2024-11-18 10:02:02.418422+00', NULL),
       ('01933eb8-f6fe-7dc0-9484-865f90b9de2b', '01dc26d0-a594-11ef-a6bf-a716f9026cfa', '{test_list_tables}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933eb8-f6fe-7dc0-9484-865f90b9de2b"
       }', '2024-11-18 10:02:09.534038+00', NULL),
       ('01933eb9-0fc8-7e43-b604-cd16dd6d52e6', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-5a445a51-6531-42d8-b7b0-1ec912d2d5ef}', '{
         "location": "gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6"
       }', '2024-11-18 10:02:15.88047+00', NULL),
       ('01933eb9-1f5b-7852-88ec-8b3c0464d497', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-39d09639-a9b7-4a97-9b1f-0a4657636f21}', '{
         "location": "gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497"
       }', '2024-11-18 10:02:19.867372+00', NULL),
       ('01933eb9-2eb8-71f1-ad43-52c963f18210', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-229fed1c-38f4-465b-9a1b-dd141dda2a95}', '{
         "location": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210"
       }', '2024-11-18 10:02:23.800557+00', NULL),
       ('01933eb9-6343-7511-9ef4-90d0fdfe16b9', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-7db200e3-f4e9-4f32-b774-1474e40350a9}', '{
         "location": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9"
       }', '2024-11-18 10:02:37.251527+00', NULL),
       ('01933eb9-c30d-7f02-ab54-da33a381ef5e', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-f829f8bf-70f9-4072-867a-d714de8a07b6}', '{
         "location": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e"
       }', '2024-11-18 10:03:01.773257+00', NULL),
       ('01933eba-0ba5-7703-8d44-20a7a3f23cfa', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-c9d7a4dc-8692-4286-834d-419bd03bd18b}', '{
         "location": "gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa"
       }', '2024-11-18 10:03:20.357573+00', NULL),
       ('01933eba-1ea1-7be1-8bc5-7454857f8431', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-054f540f-3e31-475a-8849-e9a9dc540634}', '{
         "location": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431"
       }', '2024-11-18 10:03:25.217547+00', NULL),
       ('01933eba-41f8-7790-903e-06e6422ecca2', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-cd496cd6-4a43-4c71-8035-a69d3562b149}', '{
         "location": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2"
       }', '2024-11-18 10:03:34.264639+00', NULL),
       ('01933eba-9dd7-7e30-ab5b-8420b5d3d8b6', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-83bf52b3-a205-4b32-a36f-36d142614c76}', '{
         "location": "gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6"
       }', '2024-11-18 10:03:57.783117+00', NULL),
       ('01933eba-d130-79c1-8c44-a166ebc0463b', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-0d4765d2-3103-4969-ad69-e3120bf6acb4}', '{
         "location": "gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b"
       }', '2024-11-18 10:04:10.928764+00', NULL),
       ('01933eba-eacf-79e3-900a-32bef3afe08f', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-3e79222b-5d38-4e49-b72d-9f2554dee913}', '{
         "location": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f"
       }', '2024-11-18 10:04:17.487672+00', NULL),
       ('01933ebb-174a-7861-8d9f-b5cf0f8a0d17', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-c0413077-2887-4a00-9edd-62a3eb96f92d}', '{
         "location": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17"
       }', '2024-11-18 10:04:28.874663+00', NULL),
       ('01933ebb-4889-7693-b552-33e70732e655', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-27194af2-65eb-4697-9eeb-55beedc540ea,nest1}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933ebb-4889-7693-b552-33e70732e655"
       }', '2024-11-18 10:04:41.480941+00', NULL),
       ('01933ebb-5550-73e1-9a52-1df0cfc2dab7', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-27194af2-65eb-4697-9eeb-55beedc540ea,nest1,nest2}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933ebb-5550-73e1-9a52-1df0cfc2dab7"
       }', '2024-11-18 10:04:44.751918+00', NULL),
       ('01933ebb-6243-72e3-94f4-0bc65081f5fd', '01dc26d0-a594-11ef-a6bf-a716f9026cfa',
        '{namespace-27194af2-65eb-4697-9eeb-55beedc540ea,nest1,nest2,nest3}', '{
         "owner": "root",
         "location": "gs://ht-catalog-dev/01933ebb-6243-72e3-94f4-0bc65081f5fd"
       }', '2024-11-18 10:04:48.067511+00', NULL),
       ('01933ebb-7ebf-7e43-af75-bd951e1c517f', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b', '{test_create_namespace_trino}',
        '{
          "location": "s3://tests/01933ebb-7ebf-7e43-af75-bd951e1c517f"
        }', '2024-11-18 10:04:55.358069+00', NULL),
       ('01933ebb-7ee4-74f2-9a4d-b0b8925a46e1', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b',
        '{test_list_namespaces_trino_1}', '{
         "location": "s3://tests/01933ebb-7ee4-74f2-9a4d-b0b8925a46e1"
       }', '2024-11-18 10:04:55.396193+00', NULL),
       ('01933ebb-7efa-7303-ac25-2c94addb1c8b', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b',
        '{test_list_namespaces_trino_2}', '{
         "location": "s3://tests/01933ebb-7efa-7303-ac25-2c94addb1c8b"
       }', '2024-11-18 10:04:55.418074+00', NULL),
       ('01933ebb-803e-75b0-8274-a320f3a67b3d', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b',
        '{test_namespace_create_if_not_exists_trino}', '{
         "location": "s3://tests/01933ebb-803e-75b0-8274-a320f3a67b3d"
       }', '2024-11-18 10:04:55.742447+00', NULL),
       ('01933ebb-807e-7750-8650-e43ef1b7d8d7', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b', '{test_create_table_trino}', '{
         "location": "s3://tests/01933ebb-807e-7750-8650-e43ef1b7d8d7"
       }', '2024-11-18 10:04:55.80656+00', NULL),
       ('01933ebb-81e1-7da2-8183-34caa584380b', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b',
        '{test_create_table_with_data_trino}', '{
         "location": "s3://tests/01933ebb-81e1-7da2-8183-34caa584380b"
       }', '2024-11-18 10:04:56.1614+00', NULL),
       ('01933ebb-8459-78a2-ae42-b41dca360cd5', '8ee237fe-a594-11ef-8d1a-a7d902ba0b9b', '{test_replace_table_trino}', '{
         "location": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5"
       }', '2024-11-18 10:04:56.792964+00', NULL),
       ('01933ebb-91e3-78e0-bcf2-97c9489dc642', '920980b8-a594-11ef-966c-035e5e8f564b', '{test_create_namespace_sr}', '{
         "location": "s3://tests/01933ebb-91e3-78e0-bcf2-97c9489dc642"
       }', '2024-11-18 10:05:00.259613+00', NULL),
       ('01933ebb-91f3-71b2-a93f-cfbc9e429d6f', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_list_namespaces_starrocks_1}', '{
         "location": "s3://tests/01933ebb-91f3-71b2-a93f-cfbc9e429d6f"
       }', '2024-11-18 10:05:00.275042+00', NULL),
       ('01933ebb-9359-7e20-9a57-5e508051df61', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_create_table_with_data_starrocks}', '{
         "location": "s3://tests/01933ebb-9359-7e20-9a57-5e508051df61"
       }', '2024-11-18 10:05:00.633911+00', NULL),
       ('01933ebb-91f8-7fc0-b3f4-89f201ac37c1', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_list_namespaces_starrocks_2}', '{
         "location": "s3://tests/01933ebb-91f8-7fc0-b3f4-89f201ac37c1"
       }', '2024-11-18 10:05:00.280301+00', NULL),
       ('01933ebb-9285-7ba2-b14b-418193c29a88', '920980b8-a594-11ef-966c-035e5e8f564b', '{test_drop_table_starrocks}',
        '{
          "location": "s3://tests/01933ebb-9285-7ba2-b14b-418193c29a88"
        }', '2024-11-18 10:05:00.421248+00', NULL),
       ('01933ebb-edab-7ea1-ad96-efada9261ae2', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_write_read_data_starrocks}', '{
         "location": "s3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2"
       }', '2024-11-18 10:05:23.75376+00', NULL),
       ('01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b', '920980b8-a594-11ef-966c-035e5e8f564b', '{test_partition_starrocks}', '{
         "location": "s3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b"
       }', '2024-11-18 10:05:23.899545+00', NULL),
       ('01933ebb-eed3-7310-b70f-18ab74f8566a', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_alter_partition_starrocks}', '{
         "location": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a"
       }', '2024-11-18 10:05:24.05126+00', NULL),
       ('01933ebb-efd1-7221-9bee-dce604007b98', '920980b8-a594-11ef-966c-035e5e8f564b', '{test_empty_table_starrocks}',
        '{
          "location": "s3://tests/01933ebb-efd1-7221-9bee-dce604007b98"
        }', '2024-11-18 10:05:24.305516+00', NULL),
       ('01933ebb-9201-7492-9154-74ba345f14c2', '920980b8-a594-11ef-966c-035e5e8f564b',
        '{test_namespace_create_if_not_exists_starrocks}', '{
         "location": "s3://tests/01933ebb-9201-7492-9154-74ba345f14c2"
       }', '2024-11-18 10:05:00.289506+00', NULL),
       ('01933ebb-9227-7f32-a10f-f34cb33d7d17', '920980b8-a594-11ef-966c-035e5e8f564b', '{test_create_table_starrocks}',
        '{
          "location": "s3://tests/01933ebb-9227-7f32-a10f-f34cb33d7d17"
        }', '2024-11-18 10:05:00.327152+00', NULL);

INSERT INTO public.server (single_row, server_id, terms_accepted, open_for_bootstrap, created_at, updated_at)
VALUES ('t', '00000000-0000-0000-0000-000000000000', 't', 'f', '2024-11-18 09:59:17.825735+00', NULL);

INSERT INTO public.tabular (tabular_id, namespace_id, name, typ, metadata_location, location, created_at, updated_at,
                            deleted_at)
VALUES ('01933eb6-597e-7d50-8402-b7fd4b68f27c', '01933eb6-597a-7ca0-82a5-8b526d519060', 'my_table', 'table',
        's3://tests/01933eb6-597a-7ca0-82a5-8b526d519060/01933eb6-597e-7d50-8402-b7fd4b68f27c/metadata/01933eb6-597f-7241-9801-bc6184032d93.gz.metadata.json',
        's3://tests/01933eb6-597a-7ca0-82a5-8b526d519060/01933eb6-597e-7d50-8402-b7fd4b68f27c',
        '2024-11-18 09:59:18.142681+00', NULL, NULL),
       ('01933ebb-6252-7022-89c9-82ac478186c3', '01933ebb-6243-72e3-94f4-0bc65081f5fd', 'my_table', 'table',
        'gs://ht-catalog-dev/01933ebb-6243-72e3-94f4-0bc65081f5fd/01933ebb-6252-7022-89c9-82ac478186c3/metadata/01933ebb-6937-7a22-bd41-ec0f0efaf4ff.gz.metadata.json',
        'gs://ht-catalog-dev/01933ebb-6243-72e3-94f4-0bc65081f5fd/01933ebb-6252-7022-89c9-82ac478186c3',
        '2024-11-18 10:04:48.082631+00', '2024-11-18 10:04:49.846526+00', NULL),
       ('01933eb6-71b6-79c1-ab39-17acdf085f95', '01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3', 'my_table', 'table',
        's3://tests/01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3/01933eb6-71b6-79c1-ab39-17acdf085f95/metadata/01933eb6-71b6-79c1-ab39-17b463fa25c8.gz.metadata.json',
        's3://tests/01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3/01933eb6-71b6-79c1-ab39-17acdf085f95',
        '2024-11-18 09:59:24.342542+00', NULL, NULL),
       ('01933eb6-71e0-7d31-80e8-e03f1272c54c', '01933eb6-71d7-7ff3-9514-fb96e92b739a', 'my_table_1', 'table',
        's3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71e0-7d31-80e8-e03f1272c54c/metadata/01933eb6-71e0-7d31-80e8-e04677894bb3.gz.metadata.json',
        's3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71e0-7d31-80e8-e03f1272c54c',
        '2024-11-18 09:59:24.384348+00', NULL, NULL),
       ('01933eb6-71ee-7c13-a55d-bc4a302e1176', '01933eb6-71d7-7ff3-9514-fb96e92b739a', 'my_table_2', 'table',
        's3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71ee-7c13-a55d-bc4a302e1176/metadata/01933eb6-71ee-7c13-a55d-bc5898fb63a9.gz.metadata.json',
        's3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71ee-7c13-a55d-bc4a302e1176',
        '2024-11-18 09:59:24.398035+00', NULL, NULL),
       ('01933eb6-720a-7f50-abc8-2bd8119129cf', '01933eb6-7202-7291-8b54-690b434ecf88', 'my_table', 'table',
        's3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf/metadata/01933eb6-7a66-7c70-8abd-d2be670bbaea.gz.metadata.json',
        's3://tests/01933eb6-7202-7291-8b54-690b434ecf88/01933eb6-720a-7f50-abc8-2bd8119129cf',
        '2024-11-18 09:59:24.426262+00', '2024-11-18 09:59:26.566115+00', NULL),
       ('01933eb6-7abe-7fd3-9edf-b60b0739e5f2', '01933eb6-7abb-7370-8a9f-9c457d004f71', 'my_table', 'table',
        's3://tests/01933eb6-7abb-7370-8a9f-9c457d004f71/01933eb6-7abe-7fd3-9edf-b60b0739e5f2/metadata/01933eb6-7abe-7fd3-9edf-b619aea007c3.gz.metadata.json',
        's3://tests/01933eb6-7abb-7370-8a9f-9c457d004f71/01933eb6-7abe-7fd3-9edf-b60b0739e5f2',
        '2024-11-18 09:59:26.654187+00', NULL, NULL),
       ('01933ebb-efd9-7fe3-9ee8-2b5480a474cb', '01933ebb-efd1-7221-9bee-dce604007b98', 'my_table', 'table',
        's3://tests/01933ebb-efd1-7221-9bee-dce604007b98/01933ebb-efd9-7fe3-9ee8-2b5480a474cb/metadata/01933ebb-efd9-7fe3-9ee8-2b6d20cd145a.gz.metadata.json',
        's3://tests/01933ebb-efd1-7221-9bee-dce604007b98/01933ebb-efd9-7fe3-9ee8-2b5480a474cb',
        '2024-11-18 10:05:24.313052+00', NULL, NULL),
       ('01933eb6-96b9-7be1-952e-8ad98f92ca25', '01933eb6-96aa-7f80-98dc-2ed7e9cabb08', 'my_table', 'table',
        's3://tests/01933eb6-96aa-7f80-98dc-2ed7e9cabb08/01933eb6-96b9-7be1-952e-8ad98f92ca25/metadata/01933eb6-96b9-7be1-952e-8aefad2681c4.gz.metadata.json',
        's3://tests/01933eb6-96aa-7f80-98dc-2ed7e9cabb08/01933eb6-96b9-7be1-952e-8ad98f92ca25',
        '2024-11-18 09:59:33.81707+00', NULL, NULL),
       ('01933eb6-96da-71e3-a1d1-42ae6a8f89f7', '01933eb6-96d0-7c23-953f-63f8de46be1b', 'my_table_1', 'table',
        's3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96da-71e3-a1d1-42ae6a8f89f7/metadata/01933eb6-96da-71e3-a1d1-42b4f7f6e9e7.gz.metadata.json',
        's3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96da-71e3-a1d1-42ae6a8f89f7',
        '2024-11-18 09:59:33.850382+00', NULL, NULL),
       ('01933eb6-96e4-70f2-81fe-b1d898491bf8', '01933eb6-96d0-7c23-953f-63f8de46be1b', 'my_table_2', 'table',
        's3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96e4-70f2-81fe-b1d898491bf8/metadata/01933eb6-96e4-70f2-81fe-b1eaa44b850c.gz.metadata.json',
        's3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96e4-70f2-81fe-b1d898491bf8',
        '2024-11-18 09:59:33.859946+00', NULL, NULL),
       ('01933eb6-96f8-75a0-95e0-c777cb0da57d', '01933eb6-96f1-78b1-a822-f42908748af1', 'my_table', 'table',
        's3://tests/01933eb6-96f1-78b1-a822-f42908748af1/01933eb6-96f8-75a0-95e0-c777cb0da57d/metadata/01933eb6-9816-7011-82af-cedeca3111a5.gz.metadata.json',
        's3://tests/01933eb6-96f1-78b1-a822-f42908748af1/01933eb6-96f8-75a0-95e0-c777cb0da57d',
        '2024-11-18 09:59:33.880746+00', '2024-11-18 09:59:34.166133+00', NULL),
       ('01933eb6-c0e1-7543-b389-92bff466c70a', '01933eb6-c0b1-7f10-ac51-217bddd1f6cd', 'my_table', 'table',
        's3://tests/01933eb6-c0b1-7f10-ac51-217bddd1f6cd/01933eb6-c0e1-7543-b389-92bff466c70a/metadata/01933eb6-c0e2-7e31-a08c-b721dd82b41f.gz.metadata.json',
        's3://tests/01933eb6-c0b1-7f10-ac51-217bddd1f6cd/01933eb6-c0e1-7543-b389-92bff466c70a',
        '2024-11-18 09:59:44.609684+00', NULL, NULL),
       ('01933eb6-c17e-7191-ab19-f59fd011bfa4', '01933eb6-c122-7352-8489-ca06621a8d60', 'my_table', 'table',
        's3://tests/01933eb6-c122-7352-8489-ca06621a8d60/01933eb6-c17e-7191-ab19-f59fd011bfa4/metadata/01933eb6-c6db-7731-8da5-0eeab4d6b40a.gz.metadata.json',
        's3://tests/01933eb6-c122-7352-8489-ca06621a8d60/01933eb6-c17e-7191-ab19-f59fd011bfa4',
        '2024-11-18 09:59:44.765956+00', '2024-11-18 09:59:46.139218+00', NULL),
       ('01933eb6-cc63-78c1-b578-1a9d560ac89a', '01933eb6-cc5a-7080-952a-7f3885c2208a', 'my_table', 'table',
        's3://tests/01933eb6-cc5a-7080-952a-7f3885c2208a/01933eb6-cc63-78c1-b578-1a9d560ac89a/metadata/01933eb6-cc63-78c1-b578-1aa2f2e88804.gz.metadata.json',
        's3://tests/01933eb6-cc5a-7080-952a-7f3885c2208a/01933eb6-cc63-78c1-b578-1a9d560ac89a',
        '2024-11-18 09:59:47.55491+00', NULL, NULL),
       ('01933eb6-c70e-70f1-b6ff-98b5886ac153', '01933eb6-c6ef-7003-8642-46659c776161', 'my_table', 'table',
        's3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153/metadata/01933eb6-c8e6-73c1-b692-f53559da4ac0.gz.metadata.json',
        's3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153',
        '2024-11-18 09:59:46.189903+00', '2024-11-18 09:59:46.661548+00', NULL),
       ('01933eb6-c8fe-7e33-9323-c0ef79c6e0c6', '01933eb6-c8f4-7642-b2f8-a0a50c5703bf', 'my_table', 'table',
        's3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf/01933eb6-c8fe-7e33-9323-c0ef79c6e0c6/metadata/01933eb6-c8fe-7e33-9323-c0f81f92c8e9.gz.metadata.json',
        's3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf/01933eb6-c8fe-7e33-9323-c0ef79c6e0c6',
        '2024-11-18 09:59:46.686377+00', NULL, NULL),
       ('01933eb6-c946-7122-bb89-62cb91175c86', '01933eb6-c8f4-7642-b2f8-a0a50c5703bf', 'my_view', 'view',
        's3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf/01933eb6-c946-7122-bb89-62cb91175c86/metadata/01933eb6-c946-7122-bb89-62cb91175c86.gz.metadata.json',
        's3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf/01933eb6-c946-7122-bb89-62cb91175c86',
        '2024-11-18 09:59:46.75799+00', NULL, NULL),
       ('01933eb6-c997-7d03-a0bd-77a7b7fb8a64', '01933eb6-c98e-7043-9c9b-ffd22851bb31', 'my_table', 'table',
        's3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31/01933eb6-c997-7d03-a0bd-77a7b7fb8a64/metadata/01933eb6-c997-7d03-a0bd-77bf0e868c61.gz.metadata.json',
        's3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31/01933eb6-c997-7d03-a0bd-77a7b7fb8a64',
        '2024-11-18 09:59:46.839352+00', NULL, NULL),
       ('01933eb6-c9a8-7250-af28-c011bdf03031', '01933eb6-c98e-7043-9c9b-ffd22851bb31', 'my_view', 'view',
        's3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31/01933eb6-c9a8-7250-af28-c011bdf03031/metadata/01933eb6-c9ff-7310-8954-023cee34de30.gz.metadata.json',
        's3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31/01933eb6-c9a8-7250-af28-c011bdf03031',
        '2024-11-18 09:59:46.941387+00', NULL, NULL),
       ('01933eb6-ca55-7bf3-b4c8-5eb663b75cf3', '01933eb6-ca47-7382-bae8-3b04405f693f', 'my_table', 'table',
        's3://tests/01933eb6-ca47-7382-bae8-3b04405f693f/01933eb6-ca55-7bf3-b4c8-5eb663b75cf3/metadata/01933eb6-ca55-7bf3-b4c8-5ec393ef40b5.gz.metadata.json',
        's3://tests/01933eb6-ca47-7382-bae8-3b04405f693f/01933eb6-ca55-7bf3-b4c8-5eb663b75cf3',
        '2024-11-18 09:59:47.028936+00', NULL, NULL),
       ('01933eb6-ca66-7a91-922a-8625500666e1', '01933eb6-ca47-7382-bae8-3b04405f693f', 'my_view_renamed', 'view',
        's3://tests/01933eb6-ca47-7382-bae8-3b04405f693f/01933eb6-ca66-7a91-922a-8625500666e1/metadata/01933eb6-ca66-7a91-922a-8625500666e1.gz.metadata.json',
        's3://tests/01933eb6-ca47-7382-bae8-3b04405f693f/01933eb6-ca66-7a91-922a-8625500666e1',
        '2024-11-18 09:59:47.046125+00', '2024-11-18 09:59:47.519428+00', NULL),
       ('01933eb6-ccc0-7e23-a23a-d404cc9c5815', '01933eb6-ccb8-74d2-bea0-bcad74194bc8', 'my_table', 'table',
        's3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8/01933eb6-ccc0-7e23-a23a-d404cc9c5815/metadata/01933eb6-ccc0-7e23-a23a-d41a42efd018.gz.metadata.json',
        's3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8/01933eb6-ccc0-7e23-a23a-d404cc9c5815',
        '2024-11-18 09:59:47.647979+00', NULL, NULL),
       ('01933eb6-ccd0-7603-8c0d-d22e44da9c9f', '01933eb6-ccb8-74d2-bea0-bcad74194bc8', 'my_view', 'view',
        's3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8/01933eb6-ccd0-7603-8c0d-d22e44da9c9f/metadata/01933eb6-ccd0-7603-8c0d-d22e44da9c9f.gz.metadata.json',
        's3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8/01933eb6-ccd0-7603-8c0d-d22e44da9c9f',
        '2024-11-18 09:59:47.664422+00', NULL, NULL),
       ('01933eb6-cd0c-7ec2-8c43-f63e1c4be59d', '01933eb6-cd04-7903-b279-0bf2a202e90b', 'my_table', 'table',
        's3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d/metadata/01933eb6-d11b-7fd3-8ae3-367cf036ca53.gz.metadata.json',
        's3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d',
        '2024-11-18 09:59:47.724753+00', '2024-11-18 09:59:48.762644+00', NULL),
       ('01933ebb-809a-7b62-a38f-e177eca4ff16', '01933ebb-807e-7750-8650-e43ef1b7d8d7', 'my_table', 'table',
        's3://tests/01933ebb-807e-7750-8650-e43ef1b7d8d7/my_table-5810f0d2bb3940538c4721f6d883a0e1/metadata/01933ebb-81ad-7982-989a-6e5a53b87f35.gz.metadata.json',
        's3://tests/01933ebb-807e-7750-8650-e43ef1b7d8d7/my_table-5810f0d2bb3940538c4721f6d883a0e1',
        '2024-11-18 10:04:55.833995+00', '2024-11-18 10:04:56.108787+00', NULL),
       ('01933eb7-13ed-7932-9bff-0e06be8fda05', '01933eb7-13d9-7590-86b5-2bae92be7faf', 'my_table', 'table',
        's3://tests/01933eb7-13d9-7590-86b5-2bae92be7faf/01933eb7-13ed-7932-9bff-0e06be8fda05/metadata/01933eb7-13ed-7932-9bff-0e1e9836da05.gz.metadata.json',
        's3://tests/01933eb7-13d9-7590-86b5-2bae92be7faf/01933eb7-13ed-7932-9bff-0e06be8fda05',
        '2024-11-18 10:00:05.869699+00', NULL, NULL),
       ('01933eb7-1432-7831-a993-835956cf8d56', '01933eb7-1425-7680-9530-5587fde26b90', 'my_table', 'table',
        's3://tests/01933eb7-1425-7680-9530-5587fde26b90/01933eb7-1432-7831-a993-835956cf8d56/metadata/01933eb7-144f-7000-8f1e-eca23ba4ffe1.gz.metadata.json',
        's3://tests/01933eb7-1425-7680-9530-5587fde26b90/01933eb7-1432-7831-a993-835956cf8d56',
        '2024-11-18 10:00:05.938691+00', '2024-11-18 10:00:05.967219+00', NULL),
       ('01933eb7-1540-7f53-942b-69fe1b728850', '01933eb7-1539-7ac3-bb04-6e720df6cf36', 'my_table', 'table',
        's3://tests/01933eb7-1539-7ac3-bb04-6e720df6cf36/01933eb7-1540-7f53-942b-69fe1b728850/metadata/01933eb7-1540-7f53-942b-6a0f835f3505.gz.metadata.json',
        's3://tests/01933eb7-1539-7ac3-bb04-6e720df6cf36/01933eb7-1540-7f53-942b-69fe1b728850',
        '2024-11-18 10:00:06.208551+00', NULL, NULL),
       ('01933eb7-1567-74c2-86c8-65faba608de5', '01933eb7-155c-78d1-bc55-690f1bd3984e', 'my_table', 'table',
        's3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e/01933eb7-1567-74c2-86c8-65faba608de5/metadata/01933eb7-15d7-7c62-af6a-d3ac7777c1f0.gz.metadata.json',
        's3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e/01933eb7-1567-74c2-86c8-65faba608de5',
        '2024-11-18 10:00:06.247796+00', '2024-11-18 10:00:06.359322+00', NULL),
       ('01933eb7-1692-7ee3-ae24-327476788114', '01933eb7-1686-7e82-aa2d-226f56647f6e', 'my_table', 'table',
        's3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e/01933eb7-1692-7ee3-ae24-327476788114/metadata/01933eb7-170d-7131-ac0a-b9bb945c9d3d.gz.metadata.json',
        's3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e/01933eb7-1692-7ee3-ae24-327476788114',
        '2024-11-18 10:00:06.546648+00', '2024-11-18 10:00:06.66919+00', NULL),
       ('01933eb7-1b23-7342-8164-4280dd16708a', '01933eb7-1b1c-7790-b8df-94fd06361623', 'my_table', 'table',
        's3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1d15-73b2-808f-a0badda7054b.gz.metadata.json',
        's3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a',
        '2024-11-18 10:00:07.715848+00', '2024-11-18 10:00:08.213446+00', NULL),
       ('01933eb7-17ab-7052-b402-df9e3318bf06', '01933eb7-17a3-7c12-9de5-c58fb4a1cc53', 'my_table', 'table',
        's3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/01933eb7-18a5-7873-87fa-7d86cd926d5e.gz.metadata.json',
        's3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06',
        '2024-11-18 10:00:06.827277+00', '2024-11-18 10:00:07.077447+00', NULL),
       ('01933eb7-2604-7411-a7b6-8e993ee76115', '01933eb7-25e8-79e1-8052-4bbcb38172a5', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/custom_location/metadata/01933eb7-2660-7442-b101-be921fe9375a.gz.metadata.json',
        's3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/custom_location', '2024-11-18 10:00:10.500304+00',
        '2024-11-18 10:00:10.592315+00', NULL),
       ('01933eb7-1d84-7012-8922-4794c6bd696c', '01933eb7-1d7c-79b0-b397-12a5d1a904fc', 'my_table', 'table',
        's3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/01933eb7-1e3e-7d10-89ba-0ef843733068.gz.metadata.json',
        's3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c',
        '2024-11-18 10:00:08.324769+00', '2024-11-18 10:00:08.51016+00', NULL),
       ('01933eb7-26b9-78b2-98ee-63698ac7264b', '01933eb7-26a0-7953-be13-3c1668fa9e73', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/custom_location/metadata/01933eb7-2709-70e3-9d5b-18136f0cfbe3.gz.metadata.json',
        's3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/custom_location', '2024-11-18 10:00:10.681514+00',
        '2024-11-18 10:00:10.761106+00', NULL),
       ('01933eb7-1fe8-7b43-b1bf-7215d3c8563f', '01933eb7-1fdc-7391-a27c-93c0025afdfe', 'my_table', 'table',
        's3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f/metadata/01933eb7-2049-7892-ad4d-6e26a014f4f3.gz.metadata.json',
        's3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f',
        '2024-11-18 10:00:08.936805+00', '2024-11-18 10:00:09.032839+00', NULL),
       ('01933eb7-2b61-7d21-a562-855cfdb2132d', '01933eb7-2b59-7f40-9bc6-ac710708918e', 'my_table', 'table',
        's3://tests/01933eb7-2b59-7f40-9bc6-ac710708918e/01933eb7-2b61-7d21-a562-855cfdb2132d/metadata/01933eb7-2ba6-7013-a2fb-a2eb2b99f2a0.gz.metadata.json',
        's3://tests/01933eb7-2b59-7f40-9bc6-ac710708918e/01933eb7-2b61-7d21-a562-855cfdb2132d',
        '2024-11-18 10:00:11.873306+00', '2024-11-18 10:00:11.941983+00', NULL),
       ('01933eb7-207d-7b50-90df-80584beb30b8', '01933eb7-2076-75b3-8100-b0c0b8d3da98', 'my_table', 'table',
        's3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/01933eb7-2135-7490-8614-7fa9372b68a1.gz.metadata.json',
        's3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8',
        '2024-11-18 10:00:09.085687+00', '2024-11-18 10:00:09.268965+00', NULL),
       ('01933eb7-253e-74e2-8c8b-5e7a84d51884', '01933eb7-2536-7c31-b746-c09a2467ec76', 'my_table', 'table',
        's3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/01933eb7-253e-74e2-8c8b-5e7a84d51884/metadata/01933eb7-253e-74e2-8c8b-5e897b40199b.gz.metadata.json',
        's3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/01933eb7-253e-74e2-8c8b-5e7a84d51884',
        '2024-11-18 10:00:10.302133+00', NULL, NULL),
       ('01933eb7-2757-7d02-9f04-455cb0bb4d3f', '01933eb7-274b-7333-a74b-fe26e4690130',
        'old_metadata_files_are_deleted_no_cleanup', 'table',
        's3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/01933eb7-28b7-7fc3-b8ab-16587853a26e.gz.metadata.json',
        's3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f',
        '2024-11-18 10:00:10.839302+00', '2024-11-18 10:00:11.190853+00', NULL),
       ('01933eb7-292a-76a1-980f-4ca8dff882db', '01933eb7-2923-7fb0-80eb-96762c3d6034',
        'old_metadata_files_are_deleted_cleanup', 'table',
        's3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/01933eb7-2a88-78f3-bef4-936a4af19619.gz.metadata.json',
        's3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db',
        '2024-11-18 10:00:11.306567+00', '2024-11-18 10:00:11.656564+00', NULL),
       ('01933eb7-2ad0-7db3-8da8-0eef05de57a3', '01933eb7-2ac9-7421-a6b1-0dc58c9fee55', 'my_table', 'table',
        's3://tests/01933eb7-2ac9-7421-a6b1-0dc58c9fee55/01933eb7-2ad0-7db3-8da8-0eef05de57a3/metadata/01933eb7-2b19-7410-b97d-52c29a9fbb67.gz.metadata.json',
        's3://tests/01933eb7-2ac9-7421-a6b1-0dc58c9fee55/01933eb7-2ad0-7db3-8da8-0eef05de57a3',
        '2024-11-18 10:00:11.728371+00', '2024-11-18 10:00:11.80005+00', NULL),
       ('01933eb7-2be8-7783-9cfe-db737250bc4b', '01933eb7-2be1-76b1-a0e5-be4f22396d58', 'my_table', 'table',
        's3://tests/01933eb7-2be1-76b1-a0e5-be4f22396d58/01933eb7-2be8-7783-9cfe-db737250bc4b/metadata/01933eb7-2c26-7571-b1ff-570fdc284ebe.gz.metadata.json',
        's3://tests/01933eb7-2be1-76b1-a0e5-be4f22396d58/01933eb7-2be8-7783-9cfe-db737250bc4b',
        '2024-11-18 10:00:12.008531+00', '2024-11-18 10:00:12.07004+00', NULL),
       ('01933eb7-472d-7ec0-b1d7-b0e694ff57e6', '01933eb7-4723-7dc3-94a8-61ee8faf0e9b', 'my_table', 'table',
        's3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b/01933eb7-472d-7ec0-b1d7-b0e694ff57e6/metadata/01933eb7-472d-7ec0-b1d7-b0f3f2bc7044.gz.metadata.json',
        's3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b/01933eb7-472d-7ec0-b1d7-b0e694ff57e6',
        '2024-11-18 10:00:18.989678+00', NULL, NULL),
       ('01933eb7-4884-7c91-9006-f616216f92ba', '01933eb7-486a-7b62-8a34-ce35f730d71f', 'my_view_renamed', 'view',
        's3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f/01933eb7-4884-7c91-9006-f616216f92ba/metadata/01933eb7-4884-7c91-9006-f616216f92ba.gz.metadata.json',
        's3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f/01933eb7-4884-7c91-9006-f616216f92ba',
        '2024-11-18 10:00:19.332056+00', '2024-11-18 10:00:19.747534+00', NULL),
       ('01933eb7-4ab0-78e1-90b4-abfecc8d2290', '01933eb7-4aa7-71d2-a7e6-ff7e7399be73', 'my_table', 'table',
        's3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73/01933eb7-4ab0-78e1-90b4-abfecc8d2290/metadata/01933eb7-4ab0-78e1-90b4-ac02ae0311b2.gz.metadata.json',
        's3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73/01933eb7-4ab0-78e1-90b4-abfecc8d2290',
        '2024-11-18 10:00:19.888004+00', NULL, NULL),
       ('01933ebb-81ee-7e82-b92f-1661a0a0b83c', '01933ebb-81e1-7da2-8183-34caa584380b', 'my_table', 'table',
        's3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce/metadata/01933ebb-841c-7982-b11d-4e3f1ac5173a.gz.metadata.json',
        's3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce',
        '2024-11-18 10:04:56.174793+00', '2024-11-18 10:04:56.731906+00', NULL),
       ('01933eb7-1485-72f3-818c-1a447c90b05c', '01933eb7-147b-7f10-a5d5-a232502b283f', 'my_table', 'table',
        's3://tests/01933eb7-147b-7f10-a5d5-a232502b283f/01933eb7-1485-72f3-818c-1a447c90b05c/metadata/01933eb7-14e3-7fb2-8c52-595fc9c08773.gz.metadata.json',
        's3://tests/01933eb7-147b-7f10-a5d5-a232502b283f/01933eb7-1485-72f3-818c-1a447c90b05c',
        '2024-11-18 10:00:06.021814+00', '2024-11-18 10:00:06.115094+00', NULL),
       ('01933eb7-172c-7ea3-8fd5-2b7782c9773a', '01933eb7-1724-7e82-a0b8-8b85d8b86596', 'my_table', 'table',
        's3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596/01933eb7-172c-7ea3-8fd5-2b7782c9773a/metadata/01933eb7-178e-7c11-8ed6-74ab13b95c17.gz.metadata.json',
        's3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596/01933eb7-172c-7ea3-8fd5-2b7782c9773a',
        '2024-11-18 10:00:06.700736+00', '2024-11-18 10:00:06.797719+00', NULL),
       ('01933eb7-1941-7262-bcfd-0858cd33572e', '01933eb7-1935-7a32-98f2-6d17e86281cd', 'my_table', 'table',
        's3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd/01933eb7-1941-7262-bcfd-0858cd33572e/metadata/01933eb7-19a9-7d21-9146-1b04e70ef1ac.gz.metadata.json',
        's3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd/01933eb7-1941-7262-bcfd-0858cd33572e',
        '2024-11-18 10:00:07.233681+00', '2024-11-18 10:00:07.336642+00', NULL),
       ('01933eb7-47ce-7880-a2b8-613531e5315a', '01933eb7-47af-7010-b530-cea9134c41fc', 'my_view', 'view',
        's3://tests/01933eb7-47af-7010-b530-cea9134c41fc/01933eb7-47ce-7880-a2b8-613531e5315a/metadata/01933eb7-4821-7200-b6c4-9c6463bfab0d.gz.metadata.json',
        's3://tests/01933eb7-47af-7010-b530-cea9134c41fc/01933eb7-47ce-7880-a2b8-613531e5315a',
        '2024-11-18 10:00:19.232096+00', NULL, NULL),
       ('01933eb7-4873-7323-9798-17c10651957f', '01933eb7-486a-7b62-8a34-ce35f730d71f', 'my_table', 'table',
        's3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f/01933eb7-4873-7323-9798-17c10651957f/metadata/01933eb7-4873-7323-9798-17da1dabad7a.gz.metadata.json',
        's3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f/01933eb7-4873-7323-9798-17c10651957f',
        '2024-11-18 10:00:19.31536+00', NULL, NULL),
       ('01933eb7-21a9-7b83-b0ef-df2253164373', '01933eb7-21a2-7a43-9dd0-889883b73d98', 'my_table', 'table',
        's3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-24dc-7892-8724-1e702c77a15d.gz.metadata.json',
        's3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373',
        '2024-11-18 10:00:09.385816+00', '2024-11-18 10:00:10.204086+00', NULL),
       ('01933eb7-19ef-7b42-9425-3f4784e2d2d4', '01933eb7-19e6-7041-8eec-0b065c5cec60', 'my_table', 'table',
        's3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/01933eb7-1acb-7db2-bbdc-6a5082f4130d.gz.metadata.json',
        's3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4',
        '2024-11-18 10:00:07.40713+00', '2024-11-18 10:00:07.627021+00', NULL),
       ('01933eb7-2554-7740-9968-1d9f4aa00245', '01933eb7-2536-7c31-b746-c09a2467ec76', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/custom_location/metadata/01933eb7-2599-72c2-83d7-4cbc81ba526c.gz.metadata.json',
        's3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/custom_location', '2024-11-18 10:00:10.324743+00',
        '2024-11-18 10:00:10.393206+00', NULL),
       ('01933eb7-1ebf-7d72-a996-a71ff2b1a771', '01933eb7-1eb8-7f41-aad4-980fbde2cc6c', 'my_table', 'table',
        's3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/01933eb7-1f6d-7e40-b523-f63a323cf88f.gz.metadata.json',
        's3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771',
        '2024-11-18 10:00:08.639765+00', '2024-11-18 10:00:08.813198+00', NULL),
       ('01933eb7-25f0-7702-9f09-88b5092b93e0', '01933eb7-25e8-79e1-8052-4bbcb38172a5', 'my_table', 'table',
        's3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/01933eb7-25f0-7702-9f09-88b5092b93e0/metadata/01933eb7-25f0-7702-9f09-88c62cb057ad.gz.metadata.json',
        's3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/01933eb7-25f0-7702-9f09-88b5092b93e0',
        '2024-11-18 10:00:10.480247+00', NULL, NULL),
       ('01933ebb-8467-7260-80e3-7a203167b21e', '01933ebb-8459-78a2-ae42-b41dca360cd5', 'my_table', 'table',
        's3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/01933ebb-8553-7a42-ba37-e51dec891d60.gz.metadata.json',
        's3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5',
        '2024-11-18 10:04:56.807047+00', '2024-11-18 10:04:57.042743+00', NULL),
       ('01933eb7-26a8-7070-93b6-2496798a7e07', '01933eb7-26a0-7953-be13-3c1668fa9e73', 'my_table', 'table',
        's3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/01933eb7-26a8-7070-93b6-2496798a7e07/metadata/01933eb7-26a8-7070-93b6-24a9e87ca1fd.gz.metadata.json',
        's3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/01933eb7-26a8-7070-93b6-2496798a7e07',
        '2024-11-18 10:00:10.664417+00', NULL, NULL),
       ('01933eb7-4a4b-7992-ac1b-9463cba114e9', '01933eb7-4a41-7681-b1b4-e0049fcf4f9c', 'my_table', 'table',
        's3://tests/01933eb7-4a41-7681-b1b4-e0049fcf4f9c/01933eb7-4a4b-7992-ac1b-9463cba114e9/metadata/01933eb7-4a4b-7992-ac1b-9474371b2fda.gz.metadata.json',
        's3://tests/01933eb7-4a41-7681-b1b4-e0049fcf4f9c/01933eb7-4a4b-7992-ac1b-9463cba114e9',
        '2024-11-18 10:00:19.787179+00', NULL, NULL),
       ('01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2', '01933eb7-3fb2-72f0-80d9-768a705c0dfe', 'my_table', 'table',
        's3://tests/01933eb7-3fb2-72f0-80d9-768a705c0dfe/01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2/metadata/01933eb7-3fdc-7303-aa78-5da636d00c3b.gz.metadata.json',
        's3://tests/01933eb7-3fb2-72f0-80d9-768a705c0dfe/01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2',
        '2024-11-18 10:00:17.116291+00', NULL, NULL),
       ('01933eb7-407d-7271-88ce-726c1b60537f', '01933eb7-401d-7f43-bfeb-708eccb528c7', 'my_table', 'table',
        's3://tests/01933eb7-401d-7f43-bfeb-708eccb528c7/01933eb7-407d-7271-88ce-726c1b60537f/metadata/01933eb7-4553-7351-a853-9089a5d5de64.gz.metadata.json',
        's3://tests/01933eb7-401d-7f43-bfeb-708eccb528c7/01933eb7-407d-7271-88ce-726c1b60537f',
        '2024-11-18 10:00:17.27744+00', '2024-11-18 10:00:18.515069+00', NULL),
       ('01933eb7-4ac0-7d13-aa02-77472e809d63', '01933eb7-4aa7-71d2-a7e6-ff7e7399be73', 'my_view', 'view',
        's3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73/01933eb7-4ac0-7d13-aa02-77472e809d63/metadata/01933eb7-4ac0-7d13-aa02-77472e809d63.gz.metadata.json',
        's3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73/01933eb7-4ac0-7d13-aa02-77472e809d63',
        '2024-11-18 10:00:19.904347+00', NULL, NULL),
       ('01933eb7-4580-7f73-b702-dca108027395', '01933eb7-4568-7fd3-95d7-bdfa849f10c7', 'my_table', 'table',
        's3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395/metadata/01933eb7-4714-7970-af14-f529dc8c86c7.gz.metadata.json',
        's3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395',
        '2024-11-18 10:00:18.560161+00', '2024-11-18 10:00:18.963845+00', NULL),
       ('01933eb7-4770-72b1-b4a6-369a0eae67f2', '01933eb7-4723-7dc3-94a8-61ee8faf0e9b', 'my_view', 'view',
        's3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b/01933eb7-4770-72b1-b4a6-369a0eae67f2/metadata/01933eb7-4770-72b1-b4a6-369a0eae67f2.gz.metadata.json',
        's3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b/01933eb7-4770-72b1-b4a6-369a0eae67f2',
        '2024-11-18 10:00:19.055972+00', NULL, NULL),
       ('01933eb7-47bb-7352-b265-365d38311926', '01933eb7-47af-7010-b530-cea9134c41fc', 'my_table', 'table',
        's3://tests/01933eb7-47af-7010-b530-cea9134c41fc/01933eb7-47bb-7352-b265-365d38311926/metadata/01933eb7-47bb-7352-b265-366eee41c399.gz.metadata.json',
        's3://tests/01933eb7-47af-7010-b530-cea9134c41fc/01933eb7-47bb-7352-b265-365d38311926',
        '2024-11-18 10:00:19.13163+00', NULL, NULL),
       ('01933eb7-a8e3-72d2-8058-b0675adb7a2b', '01933eb7-a8ce-73a1-9a96-096b72e2a3fa', 'my_table', 'table',
        's3://tests/01933eb7-a8ce-73a1-9a96-096b72e2a3fa/01933eb7-a8e3-72d2-8058-b0675adb7a2b/metadata/01933eb7-a8e3-72d2-8058-b07978a821d7.gz.metadata.json',
        's3://tests/01933eb7-a8ce-73a1-9a96-096b72e2a3fa/01933eb7-a8e3-72d2-8058-b0675adb7a2b',
        '2024-11-18 10:00:44.002919+00', NULL, NULL),
       ('01933eb7-a945-7b51-af42-eb711dcd54f9', '01933eb7-a934-77e1-9d0a-30d98347067d', 'my_table', 'table',
        's3://tests/01933eb7-a934-77e1-9d0a-30d98347067d/01933eb7-a945-7b51-af42-eb711dcd54f9/metadata/01933eb7-a96d-7703-831c-d09331421291.gz.metadata.json',
        's3://tests/01933eb7-a934-77e1-9d0a-30d98347067d/01933eb7-a945-7b51-af42-eb711dcd54f9',
        '2024-11-18 10:00:44.101388+00', '2024-11-18 10:00:44.140725+00', NULL),
       ('01933eb7-a9b8-71d1-8291-ee384f774d99', '01933eb7-a9ac-74d2-a815-8aa7da64aa17', 'my_table', 'table',
        's3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17/01933eb7-a9b8-71d1-8291-ee384f774d99/metadata/01933eb7-aa2b-7703-9b81-48bff9755965.gz.metadata.json',
        's3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17/01933eb7-a9b8-71d1-8291-ee384f774d99',
        '2024-11-18 10:00:44.2162+00', '2024-11-18 10:00:44.331182+00', NULL),
       ('01933eb7-af7b-7d73-a3bf-1e0abc96a322', '01933eb7-af6d-7021-9d41-90531b3b12f2', 'my_table', 'table',
        's3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2/01933eb7-af7b-7d73-a3bf-1e0abc96a322/metadata/01933eb7-b00b-7b61-82ae-1543fc9f7cd0.gz.metadata.json',
        's3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2/01933eb7-af7b-7d73-a3bf-1e0abc96a322',
        '2024-11-18 10:00:45.691205+00', '2024-11-18 10:00:45.834735+00', NULL),
       ('01933eb7-4afd-7dc1-955d-4c4634a6e084', '01933eb7-4af5-7902-8c9b-b4edff484de0', 'my_table', 'table',
        's3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084/metadata/01933eb7-4ef1-75f2-857d-e2022e26cc92.gz.metadata.json',
        's3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084',
        '2024-11-18 10:00:19.965173+00', '2024-11-18 10:00:20.977436+00', NULL),
       ('01933eb7-b763-7ff2-8792-a713e13d606e', '01933eb7-b75b-7931-9902-b17767208ad4', 'my_table', 'table',
        's3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/01933eb7-b828-7da2-b6b5-b5957e8b84ee.gz.metadata.json',
        's3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e',
        '2024-11-18 10:00:47.715841+00', '2024-11-18 10:00:47.91191+00', NULL),
       ('01933eb7-aab9-77c2-938f-166451f39f81', '01933eb7-aaac-7321-b222-9efacf41758f', 'my_table', 'table',
        's3://tests/01933eb7-aaac-7321-b222-9efacf41758f/01933eb7-aab9-77c2-938f-166451f39f81/metadata/01933eb7-aab9-77c2-938f-16796eb4751b.gz.metadata.json',
        's3://tests/01933eb7-aaac-7321-b222-9efacf41758f/01933eb7-aab9-77c2-938f-166451f39f81',
        '2024-11-18 10:00:44.473552+00', NULL, NULL),
       ('01933eb7-aaef-7b81-8abd-b3e5ceba5ea5', '01933eb7-aadf-7a40-968b-5bc0a7a1f6c0', 'my_table', 'table',
        's3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0/01933eb7-aaef-7b81-8abd-b3e5ceba5ea5/metadata/01933eb7-ab7e-7523-b778-5c8ee11a735f.gz.metadata.json',
        's3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0/01933eb7-aaef-7b81-8abd-b3e5ceba5ea5',
        '2024-11-18 10:00:44.527207+00', '2024-11-18 10:00:44.669994+00', NULL),
       ('01933eb7-ac49-7cb0-a3ab-616721216fc2', '01933eb7-ac3e-70e2-851a-fee8607189e6', 'my_table', 'table',
        's3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6/01933eb7-ac49-7cb0-a3ab-616721216fc2/metadata/01933eb7-acd5-7a41-aa16-329e26b26f79.gz.metadata.json',
        's3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6/01933eb7-ac49-7cb0-a3ab-616721216fc2',
        '2024-11-18 10:00:44.873592+00', '2024-11-18 10:00:45.012336+00', NULL),
       ('01933eb7-acff-7813-8ce8-ac6a2ac4a81f', '01933eb7-acf4-7a91-86e1-e5bb19aa5c77', 'my_table', 'table',
        's3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77/01933eb7-acff-7813-8ce8-ac6a2ac4a81f/metadata/01933eb7-ad77-7af3-b8a2-b10a89e83291.gz.metadata.json',
        's3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77/01933eb7-acff-7813-8ce8-ac6a2ac4a81f',
        '2024-11-18 10:00:45.055638+00', '2024-11-18 10:00:45.174458+00', NULL),
       ('01933eb7-b44b-74f3-a87a-e1542e7b682a', '01933eb7-b443-7563-8995-7dccf97b2fe4', 'my_table', 'table',
        's3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/01933eb7-b50d-7f40-b674-1f4d608b33c1.gz.metadata.json',
        's3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a',
        '2024-11-18 10:00:46.923153+00', '2024-11-18 10:00:47.116581+00', NULL),
       ('01933eb7-ad9b-7652-a4fe-176b233b09fe', '01933eb7-ad90-7080-bd48-c711a14b54de', 'my_table', 'table',
        's3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/01933eb7-aebe-79f3-8092-ed6f7bdb8b10.gz.metadata.json',
        's3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe',
        '2024-11-18 10:00:45.211152+00', '2024-11-18 10:00:45.501948+00', NULL),
       ('01933eb7-b05a-7361-8c6f-21c805474feb', '01933eb7-b051-7691-8411-85e61072d468', 'my_table', 'table',
        's3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/01933eb7-b152-73f2-98b8-9bb1dda27c95.gz.metadata.json',
        's3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb',
        '2024-11-18 10:00:45.913912+00', '2024-11-18 10:00:46.161582+00', NULL),
       ('01933eb7-bc5d-7de2-9ac1-a57b6b8db827', '01933eb7-bc55-7252-940f-dd7077b351a1', 'my_table', 'table',
        's3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/01933eb7-bc5d-7de2-9ac1-a57b6b8db827/metadata/01933eb7-bc5d-7de2-9ac1-a583e4e4ae76.gz.metadata.json',
        's3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/01933eb7-bc5d-7de2-9ac1-a57b6b8db827',
        '2024-11-18 10:00:48.989014+00', NULL, NULL),
       ('01933eb7-b597-7423-aadd-adfc5d176d1c', '01933eb7-b58e-7570-873b-64c7773c76df', 'my_table', 'table',
        's3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/01933eb7-b652-72e1-a480-56515a81ba2d.gz.metadata.json',
        's3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c',
        '2024-11-18 10:00:47.255815+00', '2024-11-18 10:00:47.442216+00', NULL),
       ('01933eb7-b1be-7cf0-8d0a-1ea091305f0b', '01933eb7-b1b4-7861-9869-6ff53a21abb2', 'my_table', 'table',
        's3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b3d9-76e3-96ac-6bb812af9b02.gz.metadata.json',
        's3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b',
        '2024-11-18 10:00:46.270047+00', '2024-11-18 10:00:46.80944+00', NULL),
       ('01933eb7-bc75-72e1-88f3-3dcc0a877152', '01933eb7-bc55-7252-940f-dd7077b351a1', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/custom_location/metadata/01933eb7-bcbd-7fa3-9048-112b85b614a3.gz.metadata.json',
        's3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/custom_location', '2024-11-18 10:00:49.012937+00',
        '2024-11-18 10:00:49.085504+00', NULL),
       ('01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c', '01933eb7-b6c7-7190-bac1-3d1ec943c2f5', 'my_table', 'table',
        's3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c/metadata/01933eb7-b72e-7772-ab5c-ee08ec6998b6.gz.metadata.json',
        's3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c',
        '2024-11-18 10:00:47.570485+00', '2024-11-18 10:00:47.662245+00', NULL),
       ('01933eb7-bd11-7ce3-ab67-4fabf59a4cc9', '01933eb7-bd0a-7732-abe5-e000d0902ca4', 'my_table', 'table',
        's3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/01933eb7-bd11-7ce3-ab67-4fabf59a4cc9/metadata/01933eb7-bd11-7ce3-ab67-4fb99bab2dff.gz.metadata.json',
        's3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/01933eb7-bd11-7ce3-ab67-4fabf59a4cc9',
        '2024-11-18 10:00:49.169513+00', NULL, NULL),
       ('01933eb7-bd2b-7ba1-b83b-568330dc5760', '01933eb7-bd0a-7732-abe5-e000d0902ca4', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/custom_location/metadata/01933eb7-bd7f-7e91-8c03-0cd9c455dedd.gz.metadata.json',
        's3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/custom_location', '2024-11-18 10:00:49.194969+00',
        '2024-11-18 10:00:49.279404+00', NULL),
       ('01933eb7-b8a3-7482-be7d-d31194aaffb8', '01933eb7-b89c-7551-94ab-178a5c9713e7', 'my_table', 'table',
        's3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-bc00-7473-9711-b8e74416f6e8.gz.metadata.json',
        's3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8',
        '2024-11-18 10:00:48.035629+00', '2024-11-18 10:00:48.895629+00', NULL),
       ('01933eb7-bdc9-76f3-bf64-fdee334c07e1', '01933eb7-bdc1-7d13-b222-4b225553abe5', 'my_table', 'table',
        's3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/01933eb7-bdc9-76f3-bf64-fdee334c07e1/metadata/01933eb7-bdc9-76f3-bf64-fdf10b89d46d.gz.metadata.json',
        's3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/01933eb7-bdc9-76f3-bf64-fdee334c07e1',
        '2024-11-18 10:00:49.353533+00', NULL, NULL),
       ('01933eb7-bddd-7132-af84-267337163841', '01933eb7-bdc1-7d13-b222-4b225553abe5', 'my_table_custom_location',
        'table',
        's3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/custom_location/metadata/01933eb7-be2a-77b0-9013-58e566ffd7fa.gz.metadata.json',
        's3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/custom_location', '2024-11-18 10:00:49.373671+00',
        '2024-11-18 10:00:49.450232+00', NULL),
       ('01933eb7-bf3b-75e1-819f-94b12349de42', '01933eb7-bf2d-77c2-8c09-4a1940206f6f',
        'old_metadata_files_are_deleted_no_cleanup', 'table',
        's3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/01933eb7-c0b0-7e91-8ff7-b71658df54c3.gz.metadata.json',
        's3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42',
        '2024-11-18 10:00:49.723495+00', '2024-11-18 10:00:50.096409+00', NULL),
       ('01933eb7-c148-7c41-81fc-5cf20e0175e7', '01933eb7-c13f-71c0-9500-5e0659cf3f73',
        'old_metadata_files_are_deleted_cleanup', 'table',
        's3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/01933eb7-c2a4-7e53-9d67-c8272477e2d2.gz.metadata.json',
        's3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7',
        '2024-11-18 10:00:50.248897+00', '2024-11-18 10:00:50.596591+00', NULL),
       ('01933eb7-c2eb-7e33-a112-d41577fef611', '01933eb7-c2e4-70a2-953b-3af4cee47b39', 'my_table', 'table',
        's3://tests/01933eb7-c2e4-70a2-953b-3af4cee47b39/01933eb7-c2eb-7e33-a112-d41577fef611/metadata/01933eb7-c32e-7fd0-8a4c-7a93bc2a6888.gz.metadata.json',
        's3://tests/01933eb7-c2e4-70a2-953b-3af4cee47b39/01933eb7-c2eb-7e33-a112-d41577fef611',
        '2024-11-18 10:00:50.667532+00', '2024-11-18 10:00:50.734244+00', NULL),
       ('01933eb7-c37b-7572-b694-c697e0f3b17e', '01933eb7-c372-77e2-915c-5461686b0df2', 'my_table', 'table',
        's3://tests/01933eb7-c372-77e2-915c-5461686b0df2/01933eb7-c37b-7572-b694-c697e0f3b17e/metadata/01933eb7-c3c0-7d12-a0ad-904eb3fb8022.gz.metadata.json',
        's3://tests/01933eb7-c372-77e2-915c-5461686b0df2/01933eb7-c37b-7572-b694-c697e0f3b17e',
        '2024-11-18 10:00:50.810871+00', '2024-11-18 10:00:50.880464+00', NULL),
       ('01933eb7-c407-7520-9831-fc501240440e', '01933eb7-c3ff-7ce0-aab6-ad5338d4955a', 'my_table', 'table',
        's3://tests/01933eb7-c3ff-7ce0-aab6-ad5338d4955a/01933eb7-c407-7520-9831-fc501240440e/metadata/01933eb7-c448-78c1-8bee-2b3baf357ccd.gz.metadata.json',
        's3://tests/01933eb7-c3ff-7ce0-aab6-ad5338d4955a/01933eb7-c407-7520-9831-fc501240440e',
        '2024-11-18 10:00:50.951289+00', '2024-11-18 10:00:51.01645+00', NULL),
       ('01933eb7-ec34-7bc2-aa0b-ffc3f2783457', '01933eb7-ec08-7cc0-a665-79d9fed8c5d3', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb7-ec08-7cc0-a665-79d9fed8c5d3/01933eb7-ec34-7bc2-aa0b-ffc3f2783457/metadata/01933eb7-ec34-7bc2-aa0b-ffd04b398f3a.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb7-ec08-7cc0-a665-79d9fed8c5d3/01933eb7-ec34-7bc2-aa0b-ffc3f2783457',
        '2024-11-18 10:01:01.236115+00', NULL, NULL),
       ('01933eb7-f416-7822-ad99-9d65c13e1168', '01933eb7-f3b4-7743-861e-aacc8dfd1750', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb7-f3b4-7743-861e-aacc8dfd1750/01933eb7-f416-7822-ad99-9d65c13e1168/metadata/01933eb8-00a1-7570-9679-6b35b4f4ea93.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb7-f3b4-7743-861e-aacc8dfd1750/01933eb7-f416-7822-ad99-9d65c13e1168',
        '2024-11-18 10:01:03.254713+00', '2024-11-18 10:01:06.464574+00', NULL),
       ('01933ebb-9249-75b2-9dc1-6370dbb6cda7', '01933ebb-9227-7f32-a10f-f34cb33d7d17', 'my_table', 'table',
        's3://tests/01933ebb-9227-7f32-a10f-f34cb33d7d17/01933ebb-9249-75b2-9dc1-6370dbb6cda7/metadata/01933ebb-924a-7b72-aa66-b69727fb9e48.gz.metadata.json',
        's3://tests/01933ebb-9227-7f32-a10f-f34cb33d7d17/01933ebb-9249-75b2-9dc1-6370dbb6cda7',
        '2024-11-18 10:05:00.361788+00', NULL, NULL),
       ('01933eb8-02a5-7610-a61a-43ff8c081518', '01933eb8-0283-79c1-8852-3fc21025b598', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518/metadata/01933eb8-1666-7062-bb37-33c4b3f495b3.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518',
        '2024-11-18 10:01:06.980987+00', '2024-11-18 10:01:12.035816+00', NULL),
       ('01933eb8-2201-7663-bb7d-956d2048159d', '01933eb8-21f0-70e3-ae90-83a460633f13', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13/01933eb8-2201-7663-bb7d-956d2048159d/metadata/01933eb8-2201-7663-bb7d-957b4e03eebf.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13/01933eb8-2201-7663-bb7d-956d2048159d',
        '2024-11-18 10:01:15.009629+00', NULL, NULL),
       ('01933eb8-2725-72f0-a1f7-52bc3286dbaa', '01933eb8-21f0-70e3-ae90-83a460633f13', 'my_view', 'view',
        'gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13/01933eb8-2725-72f0-a1f7-52bc3286dbaa/metadata/01933eb8-2c99-7e91-b840-90397eb8d98b.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13/01933eb8-2725-72f0-a1f7-52bc3286dbaa',
        '2024-11-18 10:01:17.719835+00', NULL, NULL),
       ('01933eb8-4738-7eb1-a914-60ae6101b0a2', '01933eb8-472c-7a53-83e1-1aee37f700fb', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb/01933eb8-4738-7eb1-a914-60ae6101b0a2/metadata/01933eb8-4739-7c11-9c2d-ffcb03862e83.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb/01933eb8-4738-7eb1-a914-60ae6101b0a2',
        '2024-11-18 10:01:24.536251+00', NULL, NULL),
       ('01933ebb-9361-7e33-9b6a-1212ff7f9739', '01933ebb-9359-7e20-9a57-5e508051df61', 'my_table', 'table',
        's3://tests/01933ebb-9359-7e20-9a57-5e508051df61/01933ebb-9361-7e33-9b6a-1212ff7f9739/metadata/01933ebb-ed0c-73b0-9ca3-b954dc30f54c.gz.metadata.json',
        's3://tests/01933ebb-9359-7e20-9a57-5e508051df61/01933ebb-9361-7e33-9b6a-1212ff7f9739',
        '2024-11-18 10:05:00.641418+00', '2024-11-18 10:05:23.596032+00', NULL),
       ('01933ebb-edbb-7b40-b51c-700aec28733f', '01933ebb-edab-7ea1-ad96-efada9261ae2', 'my_table', 'table',
        's3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2/01933ebb-edbb-7b40-b51c-700aec28733f/metadata/01933ebb-ee16-7b92-a5d2-9465d50fde28.gz.metadata.json',
        's3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2/01933ebb-edbb-7b40-b51c-700aec28733f',
        '2024-11-18 10:05:23.770942+00', '2024-11-18 10:05:23.861698+00', NULL),
       ('01933ebb-ee44-74a1-967b-f81fff985352', '01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b', 'my_table', 'table',
        's3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b/01933ebb-ee44-74a1-967b-f81fff985352/metadata/01933ebb-ee9e-7c52-9cab-81a3d8f05818.gz.metadata.json',
        's3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b/01933ebb-ee44-74a1-967b-f81fff985352',
        '2024-11-18 10:05:23.908613+00', '2024-11-18 10:05:23.997999+00', NULL),
       ('01933ebb-eeda-7410-95c3-76d15984f743', '01933ebb-eed3-7310-b70f-18ab74f8566a', 'my_table', 'table',
        's3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/01933ebb-ef9e-7770-9236-2c835154cf62.gz.metadata.json',
        's3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743',
        '2024-11-18 10:05:24.058666+00', '2024-11-18 10:05:24.253683+00', NULL),
       ('01933eb8-185e-7ed1-af31-3075474262d0', '01933eb8-183f-7161-b5d1-2ce79b3a5e51', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51/01933eb8-185e-7ed1-af31-3075474262d0/metadata/01933eb8-185e-7ed1-af31-308b59639813.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51/01933eb8-185e-7ed1-af31-3075474262d0',
        '2024-11-18 10:01:12.542025+00', NULL, NULL),
       ('01933eb8-1d7e-7a52-9a4f-7267d1f16f7a', '01933eb8-183f-7161-b5d1-2ce79b3a5e51', 'my_view', 'view',
        'gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51/01933eb8-1d7e-7a52-9a4f-7267d1f16f7a/metadata/01933eb8-1d7e-7a52-9a4f-7267d1f16f7a.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51/01933eb8-1d7e-7a52-9a4f-7267d1f16f7a',
        '2024-11-18 10:01:13.854162+00', NULL, NULL),
       ('01933eb8-3111-7f43-9a07-17f3227c36d2', '01933eb8-3106-7920-aa79-96cbd5b79a1f', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f/01933eb8-3111-7f43-9a07-17f3227c36d2/metadata/01933eb8-3111-7f43-9a07-18076ce7a912.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f/01933eb8-3111-7f43-9a07-17f3227c36d2',
        '2024-11-18 10:01:18.865491+00', NULL, NULL),
       ('01933eb8-35dc-7371-a7f8-bc6612db859e', '01933eb8-3106-7920-aa79-96cbd5b79a1f', 'my_view_renamed', 'view',
        'gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f/01933eb8-35dc-7371-a7f8-bc6612db859e/metadata/01933eb8-35dc-7371-a7f8-bc6612db859e.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f/01933eb8-35dc-7371-a7f8-bc6612db859e',
        '2024-11-18 10:01:20.09161+00', '2024-11-18 10:01:21.997399+00', NULL),
       ('01933eb8-3d78-77f0-81df-55bab3ad74e4', '01933eb8-3d70-7781-bcbe-cecc1747417a', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-3d70-7781-bcbe-cecc1747417a/01933eb8-3d78-77f0-81df-55bab3ad74e4/metadata/01933eb8-3d78-77f0-81df-55c3e412e6ec.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-3d70-7781-bcbe-cecc1747417a/01933eb8-3d78-77f0-81df-55bab3ad74e4',
        '2024-11-18 10:01:22.040608+00', NULL, NULL),
       ('01933eb8-4c09-7ae0-ba19-5a54ce4307a5', '01933eb8-472c-7a53-83e1-1aee37f700fb', 'my_view', 'view',
        'gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb/01933eb8-4c09-7ae0-ba19-5a54ce4307a5/metadata/01933eb8-4c09-7ae0-ba19-5a54ce4307a5.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb/01933eb8-4c09-7ae0-ba19-5a54ce4307a5',
        '2024-11-18 10:01:25.769196+00', NULL, NULL),
       ('01933eb8-4f55-7833-b225-8ef9257fc646', '01933eb8-4f4a-70a2-b2ec-d30b6237ff9b', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646/metadata/01933eb8-6db9-7142-9a5a-3121ff2709cd.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646',
        '2024-11-18 10:01:26.613252+00', '2024-11-18 10:01:34.39052+00', NULL),
       ('01933eb8-db3e-7a32-8f02-c6391ce0ef83', '01933eb8-db32-7ad3-81dc-b9c75dd674e0', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-db32-7ad3-81dc-b9c75dd674e0/01933eb8-db3e-7a32-8f02-c6391ce0ef83/metadata/01933eb8-e2ea-76f0-8737-aeec208226ea.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-db32-7ad3-81dc-b9c75dd674e0/01933eb8-db3e-7a32-8f02-c6391ce0ef83',
        '2024-11-18 10:02:02.430192+00', '2024-11-18 10:02:04.393802+00', NULL),
       ('01933eb8-d555-7500-a136-0eb2a3d2ffc5', '01933eb8-d541-70d0-a5c8-26f258d3b57d', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-d541-70d0-a5c8-26f258d3b57d/01933eb8-d555-7500-a136-0eb2a3d2ffc5/metadata/01933eb8-d555-7500-a136-0ec948b8c024.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-d541-70d0-a5c8-26f258d3b57d/01933eb8-d555-7500-a136-0eb2a3d2ffc5',
        '2024-11-18 10:02:00.917431+00', NULL, NULL),
       ('01933eb8-e4ed-7ee0-ae88-81b77d872451', '01933eb8-e4e4-7f80-9471-d543ea2f8e1b', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b/01933eb8-e4ed-7ee0-ae88-81b77d872451/metadata/01933eb8-eea4-7571-bbe9-32e309afab3b.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b/01933eb8-e4ed-7ee0-ae88-81b77d872451',
        '2024-11-18 10:02:04.909726+00', '2024-11-18 10:02:07.39271+00', NULL),
       ('01933eb8-f70b-7d00-b2f3-68a92a576801', '01933eb8-f6fe-7dc0-9484-865f90b9de2b', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-f6fe-7dc0-9484-865f90b9de2b/01933eb8-f70b-7d00-b2f3-68a92a576801/metadata/01933eb8-f70b-7d00-b2f3-68b6807a11e0.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-f6fe-7dc0-9484-865f90b9de2b/01933eb8-f70b-7d00-b2f3-68a92a576801',
        '2024-11-18 10:02:09.547162+00', NULL, NULL),
       ('01933eb8-fbc6-7910-8214-20ada70ab1f6', '01933eb8-fbb8-77a3-a202-7506067ac3f4', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4/01933eb8-fbc6-7910-8214-20ada70ab1f6/metadata/01933eb9-05f6-7fa2-9b12-ba13c9c53b1b.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4/01933eb8-fbc6-7910-8214-20ada70ab1f6',
        '2024-11-18 10:02:10.757948+00', '2024-11-18 10:02:13.364974+00', NULL),
       ('01933eb9-0fd5-7961-9d9f-72e131f79898', '01933eb9-0fc8-7e43-b604-cd16dd6d52e6', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6/01933eb9-0fd5-7961-9d9f-72e131f79898/metadata/01933eb9-1a77-7093-bbd8-fe8fcb86ba9e.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6/01933eb9-0fd5-7961-9d9f-72e131f79898',
        '2024-11-18 10:02:15.893666+00', '2024-11-18 10:02:18.614773+00', NULL),
       ('01933eb9-1f71-7c60-91eb-d7748b04bfec', '01933eb9-1f5b-7852-88ec-8b3c0464d497', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497/01933eb9-1f71-7c60-91eb-d7748b04bfec/metadata/01933eb9-29d0-77e0-ba21-7eef55bafa2d.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497/01933eb9-1f71-7c60-91eb-d7748b04bfec',
        '2024-11-18 10:02:19.889688+00', '2024-11-18 10:02:22.543562+00', NULL),
       ('01933eb9-c324-7912-96ab-b4dc6421b0c2', '01933eb9-c30d-7f02-ab54-da33a381ef5e', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/01933eb9-db62-7983-9914-4c311e449b05.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2',
        '2024-11-18 10:03:01.796153+00', '2024-11-18 10:03:08.001299+00', NULL),
       ('01933eb9-2ed2-7882-9461-3f9668f9611c', '01933eb9-2eb8-71f1-ad43-52c963f18210', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/01933eb9-4659-7093-9e7e-46355d249267.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c',
        '2024-11-18 10:02:23.826129+00', '2024-11-18 10:02:29.848439+00', NULL),
       ('01933eb9-51f1-7450-b4bc-64acbf3333d1', '01933eb9-51d5-7b71-a198-c0fa5d5550be', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be/01933eb9-51f1-7450-b4bc-64acbf3333d1/metadata/01933eb9-5bf2-7fa1-a299-42dff6f65519.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be/01933eb9-51f1-7450-b4bc-64acbf3333d1',
        '2024-11-18 10:02:32.816879+00', '2024-11-18 10:02:35.374322+00', NULL),
       ('01933eb9-e738-7dd2-aa40-b4c36c0e198d', '01933eb9-e729-7e81-945e-f27ad0fdb595', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/01933eb9-fee0-71b3-99b7-4703d0e8b7b4.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d',
        '2024-11-18 10:03:11.032693+00', '2024-11-18 10:03:17.087785+00', NULL),
       ('01933eb9-635b-71e0-8791-1b500fd4c82c', '01933eb9-6343-7511-9ef4-90d0fdfe16b9', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/01933eb9-7ff3-77a2-bb54-8526c9aabe98.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c',
        '2024-11-18 10:02:37.275748+00', '2024-11-18 10:02:44.592888+00', NULL),
       ('01933eb9-8975-7c33-903f-5e93cc511d1e', '01933eb9-895b-7d51-b004-b5d32d8a8d96', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-b731-7222-b843-725caaa9a0a0.gz.metadata.json',
        'gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e',
        '2024-11-18 10:02:47.029454+00', '2024-11-18 10:02:58.736626+00', NULL),
       ('01933eba-0bb4-78a1-9f5b-5f236c87202b', '01933eba-0ba5-7703-8d44-20a7a3f23cfa', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b/metadata/01933eba-1c65-7560-bb1b-c250a0b4b049.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b',
        '2024-11-18 10:03:20.372245+00', '2024-11-18 10:03:24.644518+00', NULL),
       ('01933eba-1eac-7463-88d4-20759a827b49', '01933eba-1ea1-7be1-8bc5-7454857f8431', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/01933eba-36be-70f1-a85d-1acd842e1bfa.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49',
        '2024-11-18 10:03:25.227971+00', '2024-11-18 10:03:31.388349+00', NULL),
       ('01933eba-4208-7250-9c68-107b11627db7', '01933eba-41f8-7790-903e-06e6422ecca2', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-9661-72d3-b097-dd9676999661.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7',
        '2024-11-18 10:03:34.280038+00', '2024-11-18 10:03:55.872006+00', NULL),
       ('01933eba-9ded-7fa0-b48a-6266c7bc50c3', '01933eba-9dd7-7e30-ab5b-8420b5d3d8b6', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/01933eba-9ded-7fa0-b48a-6266c7bc50c3/metadata/01933eba-9ded-7fa0-b48a-627643edf898.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/01933eba-9ded-7fa0-b48a-6266c7bc50c3',
        '2024-11-18 10:03:57.805589+00', NULL, NULL),
       ('01933eba-bdbf-75b3-ae3e-f4f62d4cc418', '01933eba-b7a6-7f12-aec3-7f57fa74a505', 'my_table_custom_location',
        'table',
        'gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/custom_location/metadata/01933eba-c7a5-7dd1-8f37-6eecdd625f86.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/custom_location', '2024-11-18 10:04:05.951771+00',
        '2024-11-18 10:04:08.4838+00', NULL),
       ('01933eba-d146-7032-9914-cdce96d2b36c', '01933eba-d130-79c1-8c44-a166ebc0463b', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/01933eba-d146-7032-9914-cdce96d2b36c/metadata/01933eba-d146-7032-9914-cdd67b8f9014.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/01933eba-d146-7032-9914-cdce96d2b36c',
        '2024-11-18 10:04:10.950445+00', NULL, NULL),
       ('01933ebb-4891-75d3-a778-75f6bb307576', '01933ebb-4889-7693-b552-33e70732e655', 'my_table', 'table',
        'gs://ht-catalog-dev/01933ebb-4889-7693-b552-33e70732e655/01933ebb-4891-75d3-a778-75f6bb307576/metadata/01933ebb-4ffb-7550-b6f7-a1aadb0471fb.gz.metadata.json',
        'gs://ht-catalog-dev/01933ebb-4889-7693-b552-33e70732e655/01933ebb-4891-75d3-a778-75f6bb307576',
        '2024-11-18 10:04:41.489568+00', '2024-11-18 10:04:43.386331+00', NULL),
       ('01933eba-a3c9-70b3-aa9d-e71d4c8fd589', '01933eba-9dd7-7e30-ab5b-8420b5d3d8b6', 'my_table_custom_location',
        'table',
        'gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/custom_location/metadata/01933eba-ad44-7181-830e-117d251e54a6.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/custom_location', '2024-11-18 10:03:59.305164+00',
        '2024-11-18 10:04:01.731995+00', NULL),
       ('01933eba-b7bf-7d91-8e79-2d9f4052b63d', '01933eba-b7a6-7f12-aec3-7f57fa74a505', 'my_table', 'table',
        'gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/01933eba-b7bf-7d91-8e79-2d9f4052b63d/metadata/01933eba-b7c0-7f02-a920-aa2589b44bca.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/01933eba-b7bf-7d91-8e79-2d9f4052b63d',
        '2024-11-18 10:04:04.415667+00', NULL, NULL),
       ('01933eba-eaeb-7262-a801-d6ac38bd14bc', '01933eba-eacf-79e3-900a-32bef3afe08f',
        'old_metadata_files_are_deleted_no_cleanup', 'table',
        'gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/01933ebb-11ef-7193-a791-8b4f5262f7e5.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc',
        '2024-11-18 10:04:17.515544+00', '2024-11-18 10:04:27.503147+00', NULL),
       ('01933ebb-1757-7550-9bad-ff4fa2769b83', '01933ebb-174a-7861-8d9f-b5cf0f8a0d17',
        'old_metadata_files_are_deleted_cleanup', 'table',
        'gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/01933ebb-4117-7300-9631-d3e9ba060f9f.gz.metadata.json',
        'gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83',
        '2024-11-18 10:04:28.887443+00', '2024-11-18 10:04:39.573639+00', NULL),
       ('01933eba-d748-7c80-b7ec-ebf3221e86ba', '01933eba-d130-79c1-8c44-a166ebc0463b', 'my_table_custom_location',
        'table',
        'gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/custom_location/metadata/01933eba-e0b3-7e73-a5b5-09c06fe396c0.gz.metadata.json',
        'gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/custom_location', '2024-11-18 10:04:12.488144+00',
        '2024-11-18 10:04:14.899163+00', NULL),
       ('01933ebb-555e-7010-acf7-07ef7dbd6dd6', '01933ebb-5550-73e1-9a52-1df0cfc2dab7', 'my_table', 'table',
        'gs://ht-catalog-dev/01933ebb-5550-73e1-9a52-1df0cfc2dab7/01933ebb-555e-7010-acf7-07ef7dbd6dd6/metadata/01933ebb-5c8b-7821-bf59-4bb5a2187094.gz.metadata.json',
        'gs://ht-catalog-dev/01933ebb-5550-73e1-9a52-1df0cfc2dab7/01933ebb-555e-7010-acf7-07ef7dbd6dd6',
        '2024-11-18 10:04:44.766172+00', '2024-11-18 10:04:46.600541+00', NULL);

INSERT INTO public."table" (table_id, metadata, created_at, updated_at)
VALUES ('01933eb6-597e-7d50-8402-b7fd4b68f27c', '{
  "refs": {},
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
  "location": "s3://tests/01933eb6-597a-7ca0-82a5-8b526d519060/01933eb6-597e-7d50-8402-b7fd4b68f27c",
  "snapshots": [],
  "properties": {},
  "table-uuid": "01933eb6-597e-7d50-8402-b7fd4b68f27c",
  "sort-orders": [
    {
      "fields": [],
      "order-id": 0
    }
  ],
  "format-version": 2,
  "last-column-id": 3,
  "default-spec-id": 0,
  "last-updated-ms": 1731923958143,
  "partition-specs": [
    {
      "fields": [],
      "spec-id": 0
    }
  ],
  "current-schema-id": 0,
  "last-partition-id": 999,
  "current-snapshot-id": -1,
  "last-sequence-number": 0,
  "default-sort-order-id": 0
}', '2024-11-18 09:59:18.142681+00', NULL),
       ('01933eb6-71b6-79c1-ab39-17acdf085f95', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-71ac-7a60-bc7a-ce6d5ef7ebd3/01933eb6-71b6-79c1-ab39-17acdf085f95",
         "snapshots": [],
         "properties": {
           "key2": "value2",
           "key-1": "value-1"
         },
         "table-uuid": "01933eb6-71b6-79c1-ab39-17acdf085f95",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923964343,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:24.342542+00', NULL),
       ('01933eb6-71e0-7d31-80e8-e03f1272c54c', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71e0-7d31-80e8-e03f1272c54c",
         "snapshots": [],
         "properties": {},
         "table-uuid": "01933eb6-71e0-7d31-80e8-e03f1272c54c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923964384,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:24.384348+00', NULL),
       ('01933eb6-71ee-7c13-a55d-bc4a302e1176', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-71d7-7ff3-9514-fb96e92b739a/01933eb6-71ee-7c13-a55d-bc4a302e1176",
         "snapshots": [],
         "properties": {},
         "table-uuid": "01933eb6-71ee-7c13-a55d-bc4a302e1176",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923964398,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:24.398035+00', NULL),
       ('01933eb6-720a-7f50-abc8-2bd8119129cf', '{
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
       }', '2024-11-18 09:59:24.426262+00', '2024-11-18 09:59:26.566115+00'),
       ('01933eb6-7abe-7fd3-9edf-b60b0739e5f2', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-7abb-7370-8a9f-9c457d004f71/01933eb6-7abe-7fd3-9edf-b60b0739e5f2",
         "snapshots": [],
         "properties": {},
         "table-uuid": "01933eb6-7abe-7fd3-9edf-b60b0739e5f2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923966654,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:26.654187+00', NULL),
       ('01933eb6-96b9-7be1-952e-8ad98f92ca25', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-96aa-7f80-98dc-2ed7e9cabb08/01933eb6-96b9-7be1-952e-8ad98f92ca25",
         "snapshots": [],
         "properties": {
           "key2": "value2",
           "key-1": "value-1"
         },
         "table-uuid": "01933eb6-96b9-7be1-952e-8ad98f92ca25",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923973817,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:33.81707+00', NULL),
       ('01933eb6-96da-71e3-a1d1-42ae6a8f89f7', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96da-71e3-a1d1-42ae6a8f89f7",
         "snapshots": [],
         "properties": {},
         "table-uuid": "01933eb6-96da-71e3-a1d1-42ae6a8f89f7",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923973850,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:33.850382+00', NULL),
       ('01933eb6-c8fe-7e33-9323-c0ef79c6e0c6', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-c8f4-7642-b2f8-a0a50c5703bf/01933eb6-c8fe-7e33-9323-c0ef79c6e0c6",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-c8fe-7e33-9323-c0ef79c6e0c6",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923986686,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:46.686377+00', NULL),
       ('01933eb6-ca55-7bf3-b4c8-5eb663b75cf3', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-ca47-7382-bae8-3b04405f693f/01933eb6-ca55-7bf3-b4c8-5eb663b75cf3",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-ca55-7bf3-b4c8-5eb663b75cf3",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923987029,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:47.028936+00', NULL),
       ('01933eb6-ccc0-7e23-a23a-d404cc9c5815', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-ccb8-74d2-bea0-bcad74194bc8/01933eb6-ccc0-7e23-a23a-d404cc9c5815",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-ccc0-7e23-a23a-d404cc9c5815",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923987648,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:47.647979+00', NULL),
       ('01933eb7-1432-7831-a993-835956cf8d56', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1425-7680-9530-5587fde26b90/01933eb7-1432-7831-a993-835956cf8d56",
         "snapshots": [],
         "properties": {
           "key1": "value1",
           "key2": "value2",
           "owner": "root"
         },
         "table-uuid": "01933eb7-1432-7831-a993-835956cf8d56",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924005939,
             "metadata-file": "s3://tests/01933eb7-1425-7680-9530-5587fde26b90/01933eb7-1432-7831-a993-835956cf8d56/metadata/01933eb7-1433-7400-ae77-0195e7083fd8.gz.metadata.json"
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924005939,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:05.938691+00', '2024-11-18 10:00:05.967219+00'),
       ('01933eb6-96e4-70f2-81fe-b1d898491bf8', '{
         "refs": {},
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
         "location": "s3://tests/01933eb6-96d0-7c23-953f-63f8de46be1b/01933eb6-96e4-70f2-81fe-b1d898491bf8",
         "snapshots": [],
         "properties": {},
         "table-uuid": "01933eb6-96e4-70f2-81fe-b1d898491bf8",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923973860,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:33.859946+00', NULL),
       ('01933eb6-96f8-75a0-95e0-c777cb0da57d', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6505525127455516155
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
         "location": "s3://tests/01933eb6-96f1-78b1-a822-f42908748af1/01933eb6-96f8-75a0-95e0-c777cb0da57d",
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
             "snapshot-id": 6505525127455516155,
             "timestamp-ms": 1731923974165,
             "manifest-list": "s3://tests/01933eb6-96f1-78b1-a822-f42908748af1/01933eb6-96f8-75a0-95e0-c777cb0da57d/metadata/snap-6505525127455516155-0-4b6cdaaa-6924-491a-adb6-5ccdbacd37b0.avro",
             "sequence-number": 1
           }
         ],
         "properties": {},
         "table-uuid": "01933eb6-96f8-75a0-95e0-c777cb0da57d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731923973881,
             "metadata-file": "s3://tests/01933eb6-96f1-78b1-a822-f42908748af1/01933eb6-96f8-75a0-95e0-c777cb0da57d/metadata/01933eb6-96f8-75a0-95e0-c78ea0bd6f14.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6505525127455516155,
             "timestamp-ms": 1731923974165
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923974165,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6505525127455516155,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:33.880746+00', '2024-11-18 09:59:34.166133+00'),
       ('01933eb6-c0e1-7543-b389-92bff466c70a', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-c0b1-7f10-ac51-217bddd1f6cd/01933eb6-c0e1-7543-b389-92bff466c70a",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-c0e1-7543-b389-92bff466c70a",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923984610,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:44.609684+00', NULL),
       ('01933eb6-c17e-7191-ab19-f59fd011bfa4', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7504863956273554958
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb6-c122-7352-8489-ca06621a8d60/01933eb6-c17e-7191-ab19-f59fd011bfa4",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7504863956273554958,
             "timestamp-ms": 1731923986085,
             "manifest-list": "s3://tests/01933eb6-c122-7352-8489-ca06621a8d60/01933eb6-c17e-7191-ab19-f59fd011bfa4/metadata/snap-7504863956273554958-1-ebf91526-dbbf-4e19-ad9c-2fe335a2e800.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-c17e-7191-ab19-f59fd011bfa4",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7504863956273554958,
             "timestamp-ms": 1731923986085
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923986085,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 7504863956273554958,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:44.765956+00', '2024-11-18 09:59:46.139218+00'),
       ('01933eb6-c70e-70f1-b6ff-98b5886ac153', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5829411999484919384
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2016140404327048173,
             "timestamp-ms": 1731923986419,
             "manifest-list": "s3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153/metadata/snap-2016140404327048173-1-6aeaf01a-30d2-41c3-b6f5-8e7c10dd7fb3.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "961",
               "total-data-files": "1",
               "total-files-size": "961",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5829411999484919384,
             "timestamp-ms": 1731923986648,
             "manifest-list": "s3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153/metadata/snap-5829411999484919384-1-81a1e494-fee8-4174-bf74-cb23b98f62ab.avro",
             "sequence-number": 2
           }
         ],
         "properties": {
           "owner": "root",
           "write.parquet.compression-codec": "zstd"
         },
         "table-uuid": "01933eb6-c70e-70f1-b6ff-98b5886ac153",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731923986419,
             "metadata-file": "s3://tests/01933eb6-c6ef-7003-8642-46659c776161/01933eb6-c70e-70f1-b6ff-98b5886ac153/metadata/01933eb6-c801-7a12-aafe-b7f7e46abe73.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5829411999484919384,
             "timestamp-ms": 1731923986648
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923986648,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5829411999484919384,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:46.189903+00', '2024-11-18 09:59:46.661548+00'),
       ('01933eb6-c997-7d03-a0bd-77a7b7fb8a64', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-c98e-7043-9c9b-ffd22851bb31/01933eb6-c997-7d03-a0bd-77a7b7fb8a64",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-c997-7d03-a0bd-77a7b7fb8a64",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923986839,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:46.839352+00', NULL),
       ('01933eb6-cc63-78c1-b578-1a9d560ac89a', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb6-cc5a-7080-952a-7f3885c2208a/01933eb6-cc63-78c1-b578-1a9d560ac89a",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-cc63-78c1-b578-1a9d560ac89a",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923987555,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:47.55491+00', NULL),
       ('01933eb7-13ed-7932-9bff-0e06be8fda05', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-13d9-7590-86b5-2bae92be7faf/01933eb7-13ed-7932-9bff-0e06be8fda05",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-13ed-7932-9bff-0e06be8fda05",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924005870,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:05.869699+00', NULL),
       ('01933eb6-cd0c-7ec2-8c43-f63e1c4be59d', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8866098485899039039
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d",
         "snapshots": [
           {
             "summary": {
               "operation": "overwrite",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "deleted-records": "1",
               "added-data-files": "1",
               "added-files-size": "958",
               "total-data-files": "2",
               "total-files-size": "1896",
               "deleted-data-files": "1",
               "removed-files-size": "938",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8866098485899039039,
             "timestamp-ms": 1731923988762,
             "manifest-list": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d/metadata/snap-8866098485899039039-1-9125b6d2-4d19-4ce3-b95e-79e3d8f42877.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 9146290187773296457
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1876",
               "total-data-files": "2",
               "total-files-size": "1876",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 9146290187773296457,
             "timestamp-ms": 1731923987848,
             "manifest-list": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d/metadata/snap-9146290187773296457-1-d189cf6b-4ba7-4619-89a5-0b02fe4ed91c.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb6-cd0c-7ec2-8c43-f63e1c4be59d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731923987724,
             "metadata-file": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d/metadata/01933eb6-cd0c-7ec2-8c43-f64eec5c5729.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731923987848,
             "metadata-file": "s3://tests/01933eb6-cd04-7903-b279-0bf2a202e90b/01933eb6-cd0c-7ec2-8c43-f63e1c4be59d/metadata/01933eb6-cd8a-79d0-a625-d1f65afe0f50.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 9146290187773296457,
             "timestamp-ms": 1731923987848
           },
           {
             "snapshot-id": 8866098485899039039,
             "timestamp-ms": 1731923988762
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731923988762,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8866098485899039039,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 09:59:47.724753+00', '2024-11-18 09:59:48.762644+00'),
       ('01933eb7-1485-72f3-818c-1a447c90b05c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8202157944003576711
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-147b-7f10-a5d5-a232502b283f/01933eb7-1485-72f3-818c-1a447c90b05c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8202157944003576711,
             "timestamp-ms": 1731924006114,
             "manifest-list": "s3://tests/01933eb7-147b-7f10-a5d5-a232502b283f/01933eb7-1485-72f3-818c-1a447c90b05c/metadata/snap-8202157944003576711-1-0304e051-cb73-43d0-90c2-517f91e36733.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1485-72f3-818c-1a447c90b05c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924006022,
             "metadata-file": "s3://tests/01933eb7-147b-7f10-a5d5-a232502b283f/01933eb7-1485-72f3-818c-1a447c90b05c/metadata/01933eb7-1486-7800-9cf5-f90147a82e46.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8202157944003576711,
             "timestamp-ms": 1731924006114
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924006114,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8202157944003576711,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.021814+00', '2024-11-18 10:00:06.115094+00'),
       ('01933eb7-172c-7ea3-8fd5-2b7782c9773a', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3377324850720903870
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "m/y fl !? -_ä oats",
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
         "location": "s3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596/01933eb7-172c-7ea3-8fd5-2b7782c9773a",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "2009",
               "total-data-files": "2",
               "total-files-size": "2009",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 3377324850720903870,
             "timestamp-ms": 1731924006797,
             "manifest-list": "s3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596/01933eb7-172c-7ea3-8fd5-2b7782c9773a/metadata/snap-3377324850720903870-1-5e1560c8-5c1d-4222-be3c-fc1922273692.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-172c-7ea3-8fd5-2b7782c9773a",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924006701,
             "metadata-file": "s3://tests/01933eb7-1724-7e82-a0b8-8b85d8b86596/01933eb7-172c-7ea3-8fd5-2b7782c9773a/metadata/01933eb7-172c-7ea3-8fd5-2b861763ec4c.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3377324850720903870,
             "timestamp-ms": 1731924006797
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924006797,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "m/y fl !? -_ä oats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 3377324850720903870,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.700736+00', '2024-11-18 10:00:06.797719+00'),
       ('01933eb7-1941-7262-bcfd-0858cd33572e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8170747408777817615
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd/01933eb7-1941-7262-bcfd-0858cd33572e",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "929",
               "total-data-files": "1",
               "total-files-size": "929",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8170747408777817615,
             "timestamp-ms": 1731924007335,
             "manifest-list": "s3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd/01933eb7-1941-7262-bcfd-0858cd33572e/metadata/snap-8170747408777817615-1-b7ccd2ea-4ed5-4cfa-993f-12ffd51c8d9b.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1941-7262-bcfd-0858cd33572e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924007233,
             "metadata-file": "s3://tests/01933eb7-1935-7a32-98f2-6d17e86281cd/01933eb7-1941-7262-bcfd-0858cd33572e/metadata/01933eb7-1941-7262-bcfd-086adeefecc8.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8170747408777817615,
             "timestamp-ms": 1731924007335
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924007335,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 8170747408777817615,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:07.233681+00', '2024-11-18 10:00:07.336642+00'),
       ('01933eb7-4a4b-7992-ac1b-9463cba114e9', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-4a41-7681-b1b4-e0049fcf4f9c/01933eb7-4a4b-7992-ac1b-9463cba114e9",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-4a4b-7992-ac1b-9463cba114e9",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924019787,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:19.787179+00', NULL),
       ('01933eb7-af7b-7d73-a3bf-1e0abc96a322', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2549852816962140328
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2/01933eb7-af7b-7d73-a3bf-1e0abc96a322",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "929",
               "total-data-files": "1",
               "total-files-size": "929",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2549852816962140328,
             "timestamp-ms": 1731924045834,
             "manifest-list": "s3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2/01933eb7-af7b-7d73-a3bf-1e0abc96a322/metadata/snap-2549852816962140328-1-f8e61b5b-1621-48f8-b931-a5547bd7a38d.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-af7b-7d73-a3bf-1e0abc96a322",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924045691,
             "metadata-file": "s3://tests/01933eb7-af6d-7021-9d41-90531b3b12f2/01933eb7-af7b-7d73-a3bf-1e0abc96a322/metadata/01933eb7-af7b-7d73-a3bf-1e1e0da58171.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2549852816962140328,
             "timestamp-ms": 1731924045834
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924045834,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 2549852816962140328,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:45.691205+00', '2024-11-18 10:00:45.834735+00'),
       ('01933eb7-1540-7f53-942b-69fe1b728850', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1539-7ac3-bb04-6e720df6cf36/01933eb7-1540-7f53-942b-69fe1b728850",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1540-7f53-942b-69fe1b728850",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924006208,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.208551+00', NULL),
       ('01933eb7-1567-74c2-86c8-65faba608de5', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7762872730064192992
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e/01933eb7-1567-74c2-86c8-65faba608de5",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 7762872730064192992,
             "timestamp-ms": 1731924006358,
             "manifest-list": "s3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e/01933eb7-1567-74c2-86c8-65faba608de5/metadata/snap-7762872730064192992-1-d18f2e81-90cb-4782-91b0-428decd35e54.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1567-74c2-86c8-65faba608de5",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924006248,
             "metadata-file": "s3://tests/01933eb7-155c-78d1-bc55-690f1bd3984e/01933eb7-1567-74c2-86c8-65faba608de5/metadata/01933eb7-1568-7fd3-bafd-2565d8fb134f.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7762872730064192992,
             "timestamp-ms": 1731924006358
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924006358,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 7762872730064192992,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.247796+00', '2024-11-18 10:00:06.359322+00'),
       ('01933eb7-1692-7ee3-ae24-327476788114', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6484483824691013789
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "my floats",
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
         "location": "s3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e/01933eb7-1692-7ee3-ae24-327476788114",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1865",
               "total-data-files": "2",
               "total-files-size": "1865",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 6484483824691013789,
             "timestamp-ms": 1731924006668,
             "manifest-list": "s3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e/01933eb7-1692-7ee3-ae24-327476788114/metadata/snap-6484483824691013789-1-c6c38ff0-0b6d-4bcd-b11f-bffd2af535d2.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1692-7ee3-ae24-327476788114",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924006546,
             "metadata-file": "s3://tests/01933eb7-1686-7e82-aa2d-226f56647f6e/01933eb7-1692-7ee3-ae24-327476788114/metadata/01933eb7-1692-7ee3-ae24-328a74d8e19f.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6484483824691013789,
             "timestamp-ms": 1731924006668
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924006668,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my floats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 6484483824691013789,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.546648+00', '2024-11-18 10:00:06.66919+00'),
       ('01933eb7-17ab-7052-b402-df9e3318bf06', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 1805415902532284332
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 207013855193795578,
             "timestamp-ms": 1731924006927,
             "manifest-list": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/snap-207013855193795578-1-995fc2ca-e300-4bb6-a714-5b75d4b80baf.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "3",
               "total-files-size": "2779",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1805415902532284332,
             "timestamp-ms": 1731924007076,
             "manifest-list": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/snap-1805415902532284332-1-55f07f04-807b-40e8-b241-43d42051b209.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 207013855193795578
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-17ab-7052-b402-df9e3318bf06",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924006827,
             "metadata-file": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/01933eb7-17ab-7052-b402-dfab6da52130.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924006927,
             "metadata-file": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/01933eb7-1810-7e83-b490-17586a027983.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924006927,
             "metadata-file": "s3://tests/01933eb7-17a3-7c12-9de5-c58fb4a1cc53/01933eb7-17ab-7052-b402-df9e3318bf06/metadata/01933eb7-1843-7e33-873c-b287aa88e558.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 207013855193795578,
             "timestamp-ms": 1731924006927
           },
           {
             "snapshot-id": 1805415902532284332,
             "timestamp-ms": 1731924007076
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 1,
         "last-updated-ms": 1731924007076,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 1
           },
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 1805415902532284332,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:06.827277+00', '2024-11-18 10:00:07.077447+00'),
       ('01933eb7-aab9-77c2-938f-166451f39f81', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-aaac-7321-b222-9efacf41758f/01933eb7-aab9-77c2-938f-166451f39f81",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-aab9-77c2-938f-166451f39f81",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924044473,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.473552+00', NULL),
       ('01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4123895522058221518
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 4123895522058221518,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4123895522058221518,
             "timestamp-ms": 1731924047637,
             "manifest-list": "s3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c/metadata/snap-4123895522058221518-1-ae9808e9-9fec-4c99-834e-6abc4aa6f8f6.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924047570,
             "metadata-file": "s3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c/metadata/01933eb7-b6d2-76a0-9d05-1aa99f0068b5.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924047637,
             "metadata-file": "s3://tests/01933eb7-b6c7-7190-bac1-3d1ec943c2f5/01933eb7-b6d2-76a0-9d05-1a9c62ffcc5c/metadata/01933eb7-b716-7c31-a399-643a7689dc8a.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4123895522058221518,
             "timestamp-ms": 1731924047637
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924047637,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 4123895522058221518,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:47.570485+00', '2024-11-18 10:00:47.662245+00'),
       ('01933eb7-19ef-7b42-9425-3f4784e2d2d4', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8384313992147367434
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 2
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 1
           }
         ],
         "location": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4523274533818411193,
             "timestamp-ms": 1731924007478,
             "manifest-list": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/snap-4523274533818411193-1-2127e798-d9f6-44d9-8756-8854b0f10a0d.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "909",
               "total-data-files": "2",
               "total-files-size": "1836",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 2,
             "snapshot-id": 8384313992147367434,
             "timestamp-ms": 1731924007626,
             "manifest-list": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/snap-8384313992147367434-1-5b571cf2-41b7-4712-b3c9-6d6d38b50fba.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 4523274533818411193
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-19ef-7b42-9425-3f4784e2d2d4",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924007407,
             "metadata-file": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/01933eb7-19ef-7b42-9425-3f542026b287.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007478,
             "metadata-file": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/01933eb7-1a37-74e0-ba94-fff29064da1d.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007478,
             "metadata-file": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/01933eb7-1a56-7431-babd-24e146f2f390.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007478,
             "metadata-file": "s3://tests/01933eb7-19e6-7041-8eec-0b065c5cec60/01933eb7-19ef-7b42-9425-3f4784e2d2d4/metadata/01933eb7-1a69-7342-bcc6-3bd340b0baef.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4523274533818411193,
             "timestamp-ms": 1731924007478
           },
           {
             "snapshot-id": 8384313992147367434,
             "timestamp-ms": 1731924007626
           }
         ],
         "format-version": 2,
         "last-column-id": 4,
         "default-spec-id": 0,
         "last-updated-ms": 1731924007626,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 2,
         "last-partition-id": 999,
         "current-snapshot-id": 8384313992147367434,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:07.40713+00', '2024-11-18 10:00:07.627021+00'),
       ('01933eb7-2604-7411-a7b6-8e993ee76115', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3627936495789883616
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3627936495789883616,
             "timestamp-ms": 1731924010591,
             "manifest-list": "s3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/custom_location/metadata/snap-3627936495789883616-1-cd80699f-303d-4d50-a5f9-f6b0082095eb.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-2604-7411-a7b6-8e993ee76115",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924010500,
             "metadata-file": "s3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/custom_location/metadata/01933eb7-2604-7411-a7b6-8ea0b91b1abe.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3627936495789883616,
             "timestamp-ms": 1731924010591
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010591,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3627936495789883616,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.500304+00', '2024-11-18 10:00:10.592315+00'),
       ('01933eb7-26b9-78b2-98ee-63698ac7264b', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3185287127683010581
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3185287127683010581,
             "timestamp-ms": 1731924010760,
             "manifest-list": "s3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/custom_location/metadata/snap-3185287127683010581-1-3090f21a-c2af-40ea-a721-21f9839c3d74.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-26b9-78b2-98ee-63698ac7264b",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924010681,
             "metadata-file": "s3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/custom_location/metadata/01933eb7-26b9-78b2-98ee-6370e8955e2c.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3185287127683010581,
             "timestamp-ms": 1731924010760
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010760,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3185287127683010581,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.681514+00', '2024-11-18 10:00:10.761106+00'),
       ('01933eb7-a9b8-71d1-8291-ee384f774d99', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3434987258172356820
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17/01933eb7-a9b8-71d1-8291-ee384f774d99",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3434987258172356820,
             "timestamp-ms": 1731924044330,
             "manifest-list": "s3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17/01933eb7-a9b8-71d1-8291-ee384f774d99/metadata/snap-3434987258172356820-1-82a0a38e-0571-48e8-8e59-9d960660d0fe.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-a9b8-71d1-8291-ee384f774d99",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924044216,
             "metadata-file": "s3://tests/01933eb7-a9ac-74d2-a815-8aa7da64aa17/01933eb7-a9b8-71d1-8291-ee384f774d99/metadata/01933eb7-a9b8-71d1-8291-ee422fec08a2.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3434987258172356820,
             "timestamp-ms": 1731924044330
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924044330,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3434987258172356820,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.2162+00', '2024-11-18 10:00:44.331182+00'),
       ('01933eb7-aaef-7b81-8abd-b3e5ceba5ea5', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7172155668743107950
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0/01933eb7-aaef-7b81-8abd-b3e5ceba5ea5",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 7172155668743107950,
             "timestamp-ms": 1731924044668,
             "manifest-list": "s3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0/01933eb7-aaef-7b81-8abd-b3e5ceba5ea5/metadata/snap-7172155668743107950-1-d7a776c7-1f07-402d-8fd3-7ed542bf63c3.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-aaef-7b81-8abd-b3e5ceba5ea5",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924044527,
             "metadata-file": "s3://tests/01933eb7-aadf-7a40-968b-5bc0a7a1f6c0/01933eb7-aaef-7b81-8abd-b3e5ceba5ea5/metadata/01933eb7-aaef-7b81-8abd-b3f7227f8b53.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7172155668743107950,
             "timestamp-ms": 1731924044668
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924044668,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 7172155668743107950,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.527207+00', '2024-11-18 10:00:44.669994+00'),
       ('01933eb7-1b23-7342-8164-4280dd16708a', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6812533499658073970
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "4",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "4",
               "total-files-size": "3706",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 1349652021685831800,
             "timestamp-ms": 1731924007970,
             "manifest-list": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/snap-1349652021685831800-1-e1e0a171-3574-40eb-bcc8-bac3c8907c73.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 4090940289287623418
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4090940289287623418,
             "timestamp-ms": 1731924007809,
             "manifest-list": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/snap-4090940289287623418-1-f35f5c4a-d79e-4835-ade6-3e5b9f2306a3.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "6",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "6",
               "total-files-size": "5559",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 6812533499658073970,
             "timestamp-ms": 1731924008212,
             "manifest-list": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/snap-6812533499658073970-1-65189f1c-7764-499b-853c-4d16fea6d68d.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 1349652021685831800
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1b23-7342-8164-4280dd16708a",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924007716,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1b24-7551-8c8a-2e446f364f22.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007809,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1b82-74f2-b14b-5be4dc5da458.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007809,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1ba1-72f3-8e21-1e45f08bdb79.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007970,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1c23-7c23-83f0-fec548f62929.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007970,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1c7d-7ac1-929f-ae5eb62741a1.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924007970,
             "metadata-file": "s3://tests/01933eb7-1b1c-7790-b8df-94fd06361623/01933eb7-1b23-7342-8164-4280dd16708a/metadata/01933eb7-1c8a-75f2-9470-580ba27a8129.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4090940289287623418,
             "timestamp-ms": 1731924007809
           },
           {
             "snapshot-id": 1349652021685831800,
             "timestamp-ms": 1731924007970
           },
           {
             "snapshot-id": 6812533499658073970,
             "timestamp-ms": 1731924008212
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 2,
         "last-updated-ms": 1731924008212,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "int_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 1
           },
           {
             "fields": [
               {
                 "name": "string_bucket",
                 "field-id": 1001,
                 "source-id": 3,
                 "transform": "truncate[4]"
               }
             ],
             "spec-id": 2
           },
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1001,
         "current-snapshot-id": 6812533499658073970,
         "last-sequence-number": 3,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:07.715848+00', '2024-11-18 10:00:08.213446+00'),
       ('01933eb7-1d84-7012-8922-4794c6bd696c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 565420456183231526
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 7487857357147429185
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7487857357147429185,
             "timestamp-ms": 1731924008396,
             "manifest-list": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/snap-7487857357147429185-1-5e20b669-5eb9-4165-b0e0-8f1102ca22ae.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 565420456183231526,
             "timestamp-ms": 1731924008509,
             "manifest-list": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/snap-565420456183231526-1-4f0e2473-b01c-44d5-bfea-e7db0449435a.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 7487857357147429185
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1d84-7012-8922-4794c6bd696c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924008325,
             "metadata-file": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/01933eb7-1d85-7651-a8a5-97c9af5f0d5a.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924008396,
             "metadata-file": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/01933eb7-1dce-7f41-a890-fcd96bc34b6d.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924008396,
             "metadata-file": "s3://tests/01933eb7-1d7c-79b0-b397-12a5d1a904fc/01933eb7-1d84-7012-8922-4794c6bd696c/metadata/01933eb7-1deb-74e1-9aa8-d6377e01e947.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7487857357147429185,
             "timestamp-ms": 1731924008396
           },
           {
             "snapshot-id": 565420456183231526,
             "timestamp-ms": 1731924008509
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924008509,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 565420456183231526,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:08.324769+00', '2024-11-18 10:00:08.51016+00'),
       ('01933eb7-1ebf-7d72-a996-a71ff2b1a771', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 731105034662861928
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 7988341524065468329,
             "max-ref-age-ms": 31536000000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7988341524065468329,
             "timestamp-ms": 1731924008707,
             "manifest-list": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/snap-7988341524065468329-1-3a1706b1-c4b8-4af2-9265-40425ffa7318.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 731105034662861928,
             "timestamp-ms": 1731924008812,
             "manifest-list": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/snap-731105034662861928-1-7563d340-c158-4d19-b9f7-449f7b15f3c7.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 7988341524065468329
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1ebf-7d72-a996-a71ff2b1a771",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924008640,
             "metadata-file": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/01933eb7-1ec0-77d1-8d4e-594251dcd344.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924008707,
             "metadata-file": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/01933eb7-1f03-7ce3-9613-41c4e9a36589.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924008707,
             "metadata-file": "s3://tests/01933eb7-1eb8-7f41-aad4-980fbde2cc6c/01933eb7-1ebf-7d72-a996-a71ff2b1a771/metadata/01933eb7-1f19-7f80-b69a-95413606fb4b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7988341524065468329,
             "timestamp-ms": 1731924008707
           },
           {
             "snapshot-id": 731105034662861928,
             "timestamp-ms": 1731924008812
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924008812,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 731105034662861928,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:08.639765+00', '2024-11-18 10:00:08.813198+00'),
       ('01933eb7-1fe8-7b43-b1bf-7215d3c8563f', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2442765089266820045
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 2442765089266820045,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2442765089266820045,
             "timestamp-ms": 1731924009007,
             "manifest-list": "s3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f/metadata/snap-2442765089266820045-1-a84f3810-b6bb-4ba1-95d2-c49126b5a785.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-1fe8-7b43-b1bf-7215d3c8563f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924008937,
             "metadata-file": "s3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f/metadata/01933eb7-1fe9-75b1-9e47-5883099aec9b.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009007,
             "metadata-file": "s3://tests/01933eb7-1fdc-7391-a27c-93c0025afdfe/01933eb7-1fe8-7b43-b1bf-7215d3c8563f/metadata/01933eb7-2030-7740-a91d-26db353b7bd6.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2442765089266820045,
             "timestamp-ms": 1731924009007
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924009007,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 2442765089266820045,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:08.936805+00', '2024-11-18 10:00:09.032839+00'),
       ('01933eb7-253e-74e2-8c8b-5e7a84d51884', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/01933eb7-253e-74e2-8c8b-5e7a84d51884",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-253e-74e2-8c8b-5e7a84d51884",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010302,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.302133+00', NULL),
       ('01933eb7-207d-7b50-90df-80584beb30b8', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 768727632622767844
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 4901357282751647705,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 768727632622767844,
             "timestamp-ms": 1731924009153,
             "manifest-list": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/snap-768727632622767844-1-a5394b56-e652-4128-9aeb-b39c2889e0ad.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4901357282751647705,
             "timestamp-ms": 1731924009268,
             "manifest-list": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/snap-4901357282751647705-1-3880a82d-2d2e-44f4-9696-6ef39c8b997e.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 768727632622767844
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-207d-7b50-90df-80584beb30b8",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924009086,
             "metadata-file": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/01933eb7-207d-7b50-90df-806f331aa0fb.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009153,
             "metadata-file": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/01933eb7-20c2-7491-90c1-77c421ca008e.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009153,
             "metadata-file": "s3://tests/01933eb7-2076-75b3-8100-b0c0b8d3da98/01933eb7-207d-7b50-90df-80584beb30b8/metadata/01933eb7-20dd-7720-8309-c79341bcf637.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 768727632622767844,
             "timestamp-ms": 1731924009153
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924009268,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 768727632622767844,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:09.085687+00', '2024-11-18 10:00:09.268965+00'),
       ('01933eb7-bd2b-7ba1-b83b-568330dc5760', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3149793295807083128
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3149793295807083128,
             "timestamp-ms": 1731924049278,
             "manifest-list": "s3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/custom_location/metadata/snap-3149793295807083128-1-0fb21101-2b5b-4f22-bf7b-7478bddfddfc.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bd2b-7ba1-b83b-568330dc5760",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924049195,
             "metadata-file": "s3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/custom_location/metadata/01933eb7-bd2b-7ba1-b83b-569c19a5f786.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3149793295807083128,
             "timestamp-ms": 1731924049278
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924049278,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3149793295807083128,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.194969+00', '2024-11-18 10:00:49.279404+00'),
       ('01933eb7-4580-7f73-b702-dca108027395', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6063639242944738447
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 9062197164530983204,
             "timestamp-ms": 1731924018749,
             "manifest-list": "s3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395/metadata/snap-9062197164530983204-1-eee5d75c-8073-4a41-8b26-7b8a22ab3b32.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "961",
               "total-data-files": "1",
               "total-files-size": "961",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6063639242944738447,
             "timestamp-ms": 1731924018950,
             "manifest-list": "s3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395/metadata/snap-6063639242944738447-1-0038cc23-db3a-4bb7-b28c-734d2fc5ac92.avro",
             "sequence-number": 2
           }
         ],
         "properties": {
           "owner": "root",
           "write.parquet.compression-codec": "zstd"
         },
         "table-uuid": "01933eb7-4580-7f73-b702-dca108027395",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924018749,
             "metadata-file": "s3://tests/01933eb7-4568-7fd3-95d7-bdfa849f10c7/01933eb7-4580-7f73-b702-dca108027395/metadata/01933eb7-464a-7da0-9e7f-c07bff0992cc.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6063639242944738447,
             "timestamp-ms": 1731924018950
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924018950,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6063639242944738447,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:18.560161+00', '2024-11-18 10:00:18.963845+00'),
       ('01933eb7-47bb-7352-b265-365d38311926', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-47af-7010-b530-cea9134c41fc/01933eb7-47bb-7352-b265-365d38311926",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-47bb-7352-b265-365d38311926",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924019131,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:19.13163+00', NULL),
       ('01933eb7-bdc9-76f3-bf64-fdee334c07e1', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/01933eb7-bdc9-76f3-bf64-fdee334c07e1",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bdc9-76f3-bf64-fdee334c07e1",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924049353,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.353533+00', NULL),
       ('01933eb8-185e-7ed1-af31-3075474262d0', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-183f-7161-b5d1-2ce79b3a5e51/01933eb8-185e-7ed1-af31-3075474262d0",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-185e-7ed1-af31-3075474262d0",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924072542,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:12.542025+00', NULL),
       ('01933eb7-26a8-7070-93b6-2496798a7e07', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-26a0-7953-be13-3c1668fa9e73/01933eb7-26a8-7070-93b6-2496798a7e07",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-26a8-7070-93b6-2496798a7e07",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010664,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.664417+00', NULL),
       ('01933eb7-21a9-7b83-b0ef-df2253164373', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8810033673381752949
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373",
         "snapshots": [
           {
             "summary": {
               "operation": "replace",
               "added-records": "7",
               "total-records": "7",
               "deleted-records": "7",
               "added-data-files": "1",
               "added-files-size": "1132",
               "total-data-files": "1",
               "total-files-size": "1132",
               "deleted-data-files": "7",
               "removed-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8810033673381752949,
             "timestamp-ms": 1731924010203,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-8810033673381752949-1-c4e7e134-f298-4875-9a94-a23e1f1a9349.avro",
             "sequence-number": 7,
             "parent-snapshot-id": 1321140442247297192
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1678037102250704186,
             "timestamp-ms": 1731924009457,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-1678037102250704186-1-838daf35-027e-4745-bc6c-0c3df75f18a6.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "4",
               "total-files-size": "3707",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8067953742642373771,
             "timestamp-ms": 1731924009633,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-8067953742642373771-1-cf9e75b4-db9a-4932-80ed-7137228c083f.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 4137545590675797583
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "6",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "6",
               "total-files-size": "5561",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7881033999120529383,
             "timestamp-ms": 1731924009808,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-7881033999120529383-1-56af0252-61eb-474a-8954-f553c423aefd.avro",
             "sequence-number": 5,
             "parent-snapshot-id": 3731396514671587542
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "3",
               "total-files-size": "2780",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4137545590675797583,
             "timestamp-ms": 1731924009540,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-4137545590675797583-1-b8e8e61a-ddf7-43c9-a57f-7a3c5a1dcc07.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 1678037102250704186
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "5",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "5",
               "total-files-size": "4634",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3731396514671587542,
             "timestamp-ms": 1731924009721,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-3731396514671587542-1-8adb2d98-320b-4a52-89e6-60add6869b8f.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 8067953742642373771
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "7",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "7",
               "total-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1321140442247297192,
             "timestamp-ms": 1731924009898,
             "manifest-list": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/snap-1321140442247297192-1-5d0ad021-ea2c-4577-96e6-cdb67dc76878.avro",
             "sequence-number": 6,
             "parent-snapshot-id": 7881033999120529383
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-21a9-7b83-b0ef-df2253164373",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924009386,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-21aa-72c0-8e5b-06f7d2303066.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009457,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-21f2-7651-a5f4-09d4f6297083.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009540,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-2245-7f22-a66d-cb0d92edbef7.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009633,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-22a1-79c0-92d5-64e8d5f0f5e2.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009721,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-22fa-72e2-a361-d22c325780ff.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009808,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-2351-7f91-9d92-a0141c80d767.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924009898,
             "metadata-file": "s3://tests/01933eb7-21a2-7a43-9dd0-889883b73d98/01933eb7-21a9-7b83-b0ef-df2253164373/metadata/01933eb7-23ab-7f62-9076-53dbc6a06a5b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 1678037102250704186,
             "timestamp-ms": 1731924009457
           },
           {
             "snapshot-id": 4137545590675797583,
             "timestamp-ms": 1731924009540
           },
           {
             "snapshot-id": 8067953742642373771,
             "timestamp-ms": 1731924009633
           },
           {
             "snapshot-id": 3731396514671587542,
             "timestamp-ms": 1731924009721
           },
           {
             "snapshot-id": 7881033999120529383,
             "timestamp-ms": 1731924009808
           },
           {
             "snapshot-id": 1321140442247297192,
             "timestamp-ms": 1731924009898
           },
           {
             "snapshot-id": 8810033673381752949,
             "timestamp-ms": 1731924010203
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010203,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8810033673381752949,
         "last-sequence-number": 7,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:09.385816+00', '2024-11-18 10:00:10.204086+00'),
       ('01933eb7-2554-7740-9968-1d9f4aa00245', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7828419607898282587
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7828419607898282587,
             "timestamp-ms": 1731924010392,
             "manifest-list": "s3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/custom_location/metadata/snap-7828419607898282587-1-3510e713-8f89-4563-9a4d-5a02b6ecec62.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-2554-7740-9968-1d9f4aa00245",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924010325,
             "metadata-file": "s3://tests/01933eb7-2536-7c31-b746-c09a2467ec76/custom_location/metadata/01933eb7-2554-7740-9968-1da30e783970.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7828419607898282587,
             "timestamp-ms": 1731924010392
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010392,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 7828419607898282587,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.324743+00', '2024-11-18 10:00:10.393206+00'),
       ('01933eb7-25f0-7702-9f09-88b5092b93e0', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-25e8-79e1-8052-4bbcb38172a5/01933eb7-25f0-7702-9f09-88b5092b93e0",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-25f0-7702-9f09-88b5092b93e0",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924010480,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.480247+00', NULL),
       ('01933eb7-2b61-7d21-a562-855cfdb2132d', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7109999772683980510
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2b59-7f40-9bc6-ac710708918e/01933eb7-2b61-7d21-a562-855cfdb2132d",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7109999772683980510,
             "timestamp-ms": 1731924011935,
             "manifest-list": "s3://tests/01933eb7-2b59-7f40-9bc6-ac710708918e/01933eb7-2b61-7d21-a562-855cfdb2132d/metadata/snap-7109999772683980510-1-99f4ba7c-9aa3-431a-87b3-32e31fe257c4.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-2b61-7d21-a562-855cfdb2132d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7109999772683980510,
             "timestamp-ms": 1731924011935
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924011935,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 7109999772683980510,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:11.873306+00', '2024-11-18 10:00:11.941983+00'),
       ('01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-3fb2-72f0-80d9-768a705c0dfe/01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-3fdc-7303-aa78-5d9bbfc1c6b2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924017116,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:17.116291+00', NULL),
       ('01933eb7-292a-76a1-980f-4ca8dff882db', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 135976350013545323
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8482839147134852152,
             "timestamp-ms": 1731924011570,
             "manifest-list": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/snap-8482839147134852152-1-a585c106-082c-449a-bae0-a20cf00de5cb.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 5865752358281152209
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 135976350013545323,
             "timestamp-ms": 1731924011656,
             "manifest-list": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/snap-135976350013545323-1-bc887b45-229a-42e6-b8c4-86fba4b7a313.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 8482839147134852152
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5865752358281152209,
             "timestamp-ms": 1731924011491,
             "manifest-list": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/snap-5865752358281152209-1-a66c91b0-54bd-48ad-84dc-6769e87c0ab6.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 1110470668037741244
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1110470668037741244,
             "timestamp-ms": 1731924011370,
             "manifest-list": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/snap-1110470668037741244-1-aedad669-5a41-4431-a243-95a248539f02.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2",
           "write.metadata.delete-after-commit.enabled": "true"
         },
         "table-uuid": "01933eb7-292a-76a1-980f-4ca8dff882db",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924011491,
             "metadata-file": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/01933eb7-29e4-7151-b86d-21688fd0f4d0.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924011570,
             "metadata-file": "s3://tests/01933eb7-2923-7fb0-80eb-96762c3d6034/01933eb7-292a-76a1-980f-4ca8dff882db/metadata/01933eb7-2a33-75f3-8154-c251229f9fc3.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 1110470668037741244,
             "timestamp-ms": 1731924011370
           },
           {
             "snapshot-id": 5865752358281152209,
             "timestamp-ms": 1731924011491
           },
           {
             "snapshot-id": 8482839147134852152,
             "timestamp-ms": 1731924011570
           },
           {
             "snapshot-id": 135976350013545323,
             "timestamp-ms": 1731924011656
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924011656,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 135976350013545323,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:11.306567+00', '2024-11-18 10:00:11.656564+00'),
       ('01933eb7-2757-7d02-9f04-455cb0bb4d3f', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8377658217198871401
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8377658217198871401,
             "timestamp-ms": 1731924011190,
             "manifest-list": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/snap-8377658217198871401-1-699d1061-ca3e-4997-83c2-f09fce42e137.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 7064078557528983803
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7064078557528983803,
             "timestamp-ms": 1731924011106,
             "manifest-list": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/snap-7064078557528983803-1-229dd9ee-9bbf-4731-bb1e-d879427ac3ac.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 188459324081554828
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 93650612576629455,
             "timestamp-ms": 1731924010912,
             "manifest-list": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/snap-93650612576629455-1-d1f959ef-9aa5-450b-994f-085e70490a4f.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 188459324081554828,
             "timestamp-ms": 1731924011025,
             "manifest-list": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/snap-188459324081554828-1-32a56be3-608f-459b-93eb-2aa865e311e5.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 93650612576629455
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2"
         },
         "table-uuid": "01933eb7-2757-7d02-9f04-455cb0bb4d3f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924011025,
             "metadata-file": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/01933eb7-2812-7061-bbd4-347ed0809172.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924011106,
             "metadata-file": "s3://tests/01933eb7-274b-7333-a74b-fe26e4690130/01933eb7-2757-7d02-9f04-455cb0bb4d3f/metadata/01933eb7-2863-70f0-8e77-4d7510a425c6.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 93650612576629455,
             "timestamp-ms": 1731924010912
           },
           {
             "snapshot-id": 188459324081554828,
             "timestamp-ms": 1731924011025
           },
           {
             "snapshot-id": 7064078557528983803,
             "timestamp-ms": 1731924011106
           },
           {
             "snapshot-id": 8377658217198871401,
             "timestamp-ms": 1731924011190
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924011190,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8377658217198871401,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:10.839302+00', '2024-11-18 10:00:11.190853+00'),
       ('01933eb7-2ad0-7db3-8da8-0eef05de57a3', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5502816705141049370
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2ac9-7421-a6b1-0dc58c9fee55/01933eb7-2ad0-7db3-8da8-0eef05de57a3",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5502816705141049370,
             "timestamp-ms": 1731924011791,
             "manifest-list": "s3://tests/01933eb7-2ac9-7421-a6b1-0dc58c9fee55/01933eb7-2ad0-7db3-8da8-0eef05de57a3/metadata/snap-5502816705141049370-1-64a95aa5-d87f-4000-8c92-bc51985374b4.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-2ad0-7db3-8da8-0eef05de57a3",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5502816705141049370,
             "timestamp-ms": 1731924011791
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924011791,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5502816705141049370,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:11.728371+00', '2024-11-18 10:00:11.80005+00'),
       ('01933eb7-2be8-7783-9cfe-db737250bc4b', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5858371985297381161
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-2be1-76b1-a0e5-be4f22396d58/01933eb7-2be8-7783-9cfe-db737250bc4b",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731923982727",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5858371985297381161,
             "timestamp-ms": 1731924012064,
             "manifest-list": "s3://tests/01933eb7-2be1-76b1-a0e5-be4f22396d58/01933eb7-2be8-7783-9cfe-db737250bc4b/metadata/snap-5858371985297381161-1-f40a6ad4-9c0a-4c10-ab79-f4f0e9547958.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-2be8-7783-9cfe-db737250bc4b",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5858371985297381161,
             "timestamp-ms": 1731924012064
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924012064,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5858371985297381161,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:12.008531+00', '2024-11-18 10:00:12.07004+00'),
       ('01933eb7-4873-7323-9798-17c10651957f', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-486a-7b62-8a34-ce35f730d71f/01933eb7-4873-7323-9798-17c10651957f",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-4873-7323-9798-17c10651957f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924019315,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:19.31536+00', NULL),
       ('01933eb7-407d-7271-88ce-726c1b60537f', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4065326354824382353
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-401d-7f43-bfeb-708eccb528c7/01933eb7-407d-7271-88ce-726c1b60537f",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4065326354824382353,
             "timestamp-ms": 1731924018460,
             "manifest-list": "s3://tests/01933eb7-401d-7f43-bfeb-708eccb528c7/01933eb7-407d-7271-88ce-726c1b60537f/metadata/snap-4065326354824382353-1-08bde16f-4976-42c7-8bc3-1c855698c5bf.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-407d-7271-88ce-726c1b60537f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4065326354824382353,
             "timestamp-ms": 1731924018460
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924018460,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 4065326354824382353,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:17.27744+00', '2024-11-18 10:00:18.515069+00'),
       ('01933eb7-472d-7ec0-b1d7-b0e694ff57e6', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-4723-7dc3-94a8-61ee8faf0e9b/01933eb7-472d-7ec0-b1d7-b0e694ff57e6",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-472d-7ec0-b1d7-b0e694ff57e6",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924018990,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:18.989678+00', NULL),
       ('01933eb7-4ab0-78e1-90b4-abfecc8d2290', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-4aa7-71d2-a7e6-ff7e7399be73/01933eb7-4ab0-78e1-90b4-abfecc8d2290",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-4ab0-78e1-90b4-abfecc8d2290",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924019888,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:19.888004+00', NULL),
       ('01933eb7-4afd-7dc1-955d-4c4634a6e084', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7904075293341452836
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1876",
               "total-data-files": "2",
               "total-files-size": "1876",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4579039842080168609,
             "timestamp-ms": 1731924020081,
             "manifest-list": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084/metadata/snap-4579039842080168609-1-2820a44a-585c-4b49-b26f-e9ee71222eb9.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "overwrite",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "deleted-records": "1",
               "added-data-files": "1",
               "added-files-size": "958",
               "total-data-files": "2",
               "total-files-size": "1896",
               "deleted-data-files": "1",
               "removed-files-size": "938",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7904075293341452836,
             "timestamp-ms": 1731924020976,
             "manifest-list": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084/metadata/snap-7904075293341452836-1-b5d41109-b118-465e-9cdc-57e508fa688c.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 4579039842080168609
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-4afd-7dc1-955d-4c4634a6e084",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924019965,
             "metadata-file": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084/metadata/01933eb7-4afd-7dc1-955d-4c56228a4c1f.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924020081,
             "metadata-file": "s3://tests/01933eb7-4af5-7902-8c9b-b4edff484de0/01933eb7-4afd-7dc1-955d-4c4634a6e084/metadata/01933eb7-4b72-74e2-b591-7df8b24e73de.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4579039842080168609,
             "timestamp-ms": 1731924020081
           },
           {
             "snapshot-id": 7904075293341452836,
             "timestamp-ms": 1731924020976
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924020976,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 7904075293341452836,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:19.965173+00', '2024-11-18 10:00:20.977436+00'),
       ('01933eb7-a8e3-72d2-8058-b0675adb7a2b', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-a8ce-73a1-9a96-096b72e2a3fa/01933eb7-a8e3-72d2-8058-b0675adb7a2b",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-a8e3-72d2-8058-b0675adb7a2b",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924044003,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.002919+00', NULL),
       ('01933eb7-a945-7b51-af42-eb711dcd54f9', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-a934-77e1-9d0a-30d98347067d/01933eb7-a945-7b51-af42-eb711dcd54f9",
         "snapshots": [],
         "properties": {
           "key1": "value1",
           "key2": "value2",
           "owner": "root"
         },
         "table-uuid": "01933eb7-a945-7b51-af42-eb711dcd54f9",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924044101,
             "metadata-file": "s3://tests/01933eb7-a934-77e1-9d0a-30d98347067d/01933eb7-a945-7b51-af42-eb711dcd54f9/metadata/01933eb7-a945-7b51-af42-eb89c8ecb59f.gz.metadata.json"
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924044101,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.101388+00', '2024-11-18 10:00:44.140725+00'),
       ('01933eb7-ac49-7cb0-a3ab-616721216fc2', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4752579997069407362
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "my floats",
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
         "location": "s3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6/01933eb7-ac49-7cb0-a3ab-616721216fc2",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1865",
               "total-data-files": "2",
               "total-files-size": "1865",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4752579997069407362,
             "timestamp-ms": 1731924045011,
             "manifest-list": "s3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6/01933eb7-ac49-7cb0-a3ab-616721216fc2/metadata/snap-4752579997069407362-1-2d936c5a-167a-41c1-80d4-3cfedfbfa1c3.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-ac49-7cb0-a3ab-616721216fc2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924044873,
             "metadata-file": "s3://tests/01933eb7-ac3e-70e2-851a-fee8607189e6/01933eb7-ac49-7cb0-a3ab-616721216fc2/metadata/01933eb7-ac49-7cb0-a3ab-61713775a07c.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4752579997069407362,
             "timestamp-ms": 1731924045011
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924045011,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my floats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 4752579997069407362,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:44.873592+00', '2024-11-18 10:00:45.012336+00'),
       ('01933eb7-acff-7813-8ce8-ac6a2ac4a81f', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4491062769223796744
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "m/y fl !? -_ä oats",
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
         "location": "s3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77/01933eb7-acff-7813-8ce8-ac6a2ac4a81f",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "2009",
               "total-data-files": "2",
               "total-files-size": "2009",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4491062769223796744,
             "timestamp-ms": 1731924045173,
             "manifest-list": "s3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77/01933eb7-acff-7813-8ce8-ac6a2ac4a81f/metadata/snap-4491062769223796744-1-e0f44f08-50fb-4783-89ee-16ee23c70b67.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-acff-7813-8ce8-ac6a2ac4a81f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924045056,
             "metadata-file": "s3://tests/01933eb7-acf4-7a91-86e1-e5bb19aa5c77/01933eb7-acff-7813-8ce8-ac6a2ac4a81f/metadata/01933eb7-ad00-7c12-b4b2-c7fa58798513.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4491062769223796744,
             "timestamp-ms": 1731924045173
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924045173,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "m/y fl !? -_ä oats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 4491062769223796744,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:45.055638+00', '2024-11-18 10:00:45.174458+00'),
       ('01933eb7-ad9b-7652-a4fe-176b233b09fe', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2618341287427698657
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "3",
               "total-files-size": "2779",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2618341287427698657,
             "timestamp-ms": 1731924045501,
             "manifest-list": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/snap-2618341287427698657-1-ef967916-7706-43a3-ba8c-eddaba4353bf.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 133402240932794081
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 133402240932794081,
             "timestamp-ms": 1731924045328,
             "manifest-list": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/snap-133402240932794081-1-a398efd1-c725-46bc-b5ba-b774f775a6aa.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-ad9b-7652-a4fe-176b233b09fe",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924045211,
             "metadata-file": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/01933eb7-ad9b-7652-a4fe-1770f7943ad8.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924045328,
             "metadata-file": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/01933eb7-ae11-73e0-a3f1-d0ecc50f7d36.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924045328,
             "metadata-file": "s3://tests/01933eb7-ad90-7080-bd48-c711a14b54de/01933eb7-ad9b-7652-a4fe-176b233b09fe/metadata/01933eb7-ae47-73c0-99d7-fdd78be9d888.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 133402240932794081,
             "timestamp-ms": 1731924045328
           },
           {
             "snapshot-id": 2618341287427698657,
             "timestamp-ms": 1731924045501
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 1,
         "last-updated-ms": 1731924045501,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 1
           },
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 2618341287427698657,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:45.211152+00', '2024-11-18 10:00:45.501948+00'),
       ('01933eb7-b05a-7361-8c6f-21c805474feb', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 832394056303143978
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 1
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
           },
           {
             "type": "struct",
             "fields": [
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 2
           }
         ],
         "location": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 356785706865236537,
             "timestamp-ms": 1731924045998,
             "manifest-list": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/snap-356785706865236537-1-d4bc8e15-9ae0-4339-a377-9e91d4eea7f4.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "909",
               "total-data-files": "2",
               "total-files-size": "1836",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 2,
             "snapshot-id": 832394056303143978,
             "timestamp-ms": 1731924046160,
             "manifest-list": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/snap-832394056303143978-1-88d0df74-d8d6-486b-8b6b-958ec1aa6a41.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 356785706865236537
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b05a-7361-8c6f-21c805474feb",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924045914,
             "metadata-file": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/01933eb7-b05a-7361-8c6f-21d400d9ed4a.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924045998,
             "metadata-file": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/01933eb7-b0af-7001-b757-15fef9148981.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924045998,
             "metadata-file": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/01933eb7-b0d2-7852-bc08-94333e56172a.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924045998,
             "metadata-file": "s3://tests/01933eb7-b051-7691-8411-85e61072d468/01933eb7-b05a-7361-8c6f-21c805474feb/metadata/01933eb7-b0e5-70f2-ae00-48fd25db7c78.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 356785706865236537,
             "timestamp-ms": 1731924045998
           },
           {
             "snapshot-id": 832394056303143978,
             "timestamp-ms": 1731924046160
           }
         ],
         "format-version": 2,
         "last-column-id": 4,
         "default-spec-id": 0,
         "last-updated-ms": 1731924046160,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 2,
         "last-partition-id": 999,
         "current-snapshot-id": 832394056303143978,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:45.913912+00', '2024-11-18 10:00:46.161582+00'),
       ('01933eb7-b1be-7cf0-8d0a-1ea091305f0b', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4843707105371951436
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "6",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "6",
               "total-files-size": "5559",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4843707105371951436,
             "timestamp-ms": 1731924046808,
             "manifest-list": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/snap-4843707105371951436-1-b821be32-671d-4756-9729-bca3ec67a6bb.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 469732036991102475
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7995469252473827205,
             "timestamp-ms": 1731924046354,
             "manifest-list": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/snap-7995469252473827205-1-138a458f-c2f7-4229-805d-29fb65330c7e.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "4",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "4",
               "total-files-size": "3706",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 469732036991102475,
             "timestamp-ms": 1731924046543,
             "manifest-list": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/snap-469732036991102475-1-fd82e13f-26be-45b9-8b20-25803eb444ca.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 7995469252473827205
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b1be-7cf0-8d0a-1ea091305f0b",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924046270,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b1be-7cf0-8d0a-1eb85cecb42e.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046354,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b214-7e81-8540-b7fdaf5f87b1.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046354,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b232-75f0-997a-5f84208cb013.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046543,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b2d0-7ac2-aa45-55ebe30ec976.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046543,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b33b-7b13-8fc8-9828dcca11cb.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046543,
             "metadata-file": "s3://tests/01933eb7-b1b4-7861-9869-6ff53a21abb2/01933eb7-b1be-7cf0-8d0a-1ea091305f0b/metadata/01933eb7-b347-7f71-ba20-d5863a509fdd.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7995469252473827205,
             "timestamp-ms": 1731924046354
           },
           {
             "snapshot-id": 469732036991102475,
             "timestamp-ms": 1731924046543
           },
           {
             "snapshot-id": 4843707105371951436,
             "timestamp-ms": 1731924046808
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 2,
         "last-updated-ms": 1731924046808,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "string_bucket",
                 "field-id": 1001,
                 "source-id": 3,
                 "transform": "truncate[4]"
               }
             ],
             "spec-id": 2
           },
           {
             "fields": [
               {
                 "name": "int_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 1
           },
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1001,
         "current-snapshot-id": 4843707105371951436,
         "last-sequence-number": 3,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:46.270047+00', '2024-11-18 10:00:46.80944+00'),
       ('01933eb7-b597-7423-aadd-adfc5d176d1c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3383450313947672538
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 333369602374740663,
             "max-ref-age-ms": 31536000000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 333369602374740663,
             "timestamp-ms": 1731924047329,
             "manifest-list": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/snap-333369602374740663-1-215e15bb-305e-47a3-856e-153279276ca9.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3383450313947672538,
             "timestamp-ms": 1731924047441,
             "manifest-list": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/snap-3383450313947672538-1-178c7ab9-473d-4a0a-9b2f-306980e13f62.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 333369602374740663
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b597-7423-aadd-adfc5d176d1c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924047256,
             "metadata-file": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/01933eb7-b597-7423-aadd-ae03024018a0.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924047329,
             "metadata-file": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/01933eb7-b5e1-7471-85c4-e9ea6cc64161.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924047329,
             "metadata-file": "s3://tests/01933eb7-b58e-7570-873b-64c7773c76df/01933eb7-b597-7423-aadd-adfc5d176d1c/metadata/01933eb7-b5f9-70e3-869b-dc13a02fb744.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 333369602374740663,
             "timestamp-ms": 1731924047329
           },
           {
             "snapshot-id": 3383450313947672538,
             "timestamp-ms": 1731924047441
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924047441,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3383450313947672538,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:47.255815+00', '2024-11-18 10:00:47.442216+00'),
       ('01933eb7-b44b-74f3-a87a-e1542e7b682a', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 1721775626623909483
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 4853968539489212773
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1721775626623909483,
             "timestamp-ms": 1731924047116,
             "manifest-list": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/snap-1721775626623909483-1-7ba55044-9a78-4886-bed1-5e4654deea39.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 4853968539489212773
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4853968539489212773,
             "timestamp-ms": 1731924046998,
             "manifest-list": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/snap-4853968539489212773-1-2e9b10fc-d56a-4fdf-a10f-f717a9d896ad.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b44b-74f3-a87a-e1542e7b682a",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924046923,
             "metadata-file": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/01933eb7-b44b-74f3-a87a-e163809b7e8d.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046998,
             "metadata-file": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/01933eb7-b497-70d0-9602-c1681526b957.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924046998,
             "metadata-file": "s3://tests/01933eb7-b443-7563-8995-7dccf97b2fe4/01933eb7-b44b-74f3-a87a-e1542e7b682a/metadata/01933eb7-b4b2-7d70-b8ee-17d7401eafdf.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4853968539489212773,
             "timestamp-ms": 1731924046998
           },
           {
             "snapshot-id": 1721775626623909483,
             "timestamp-ms": 1731924047116
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924047116,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 1721775626623909483,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:46.923153+00', '2024-11-18 10:00:47.116581+00'),
       ('01933eb7-bc5d-7de2-9ac1-a57b6b8db827', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/01933eb7-bc5d-7de2-9ac1-a57b6b8db827",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bc5d-7de2-9ac1-a57b6b8db827",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924048989,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:48.989014+00', NULL),
       ('01933eb7-c2eb-7e33-a112-d41577fef611', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 655266999750843557
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-c2e4-70a2-953b-3af4cee47b39/01933eb7-c2eb-7e33-a112-d41577fef611",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 655266999750843557,
             "timestamp-ms": 1731924050728,
             "manifest-list": "s3://tests/01933eb7-c2e4-70a2-953b-3af4cee47b39/01933eb7-c2eb-7e33-a112-d41577fef611/metadata/snap-655266999750843557-1-175e4eb3-e6f3-4475-b1ce-0c3feca6237f.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-c2eb-7e33-a112-d41577fef611",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 655266999750843557,
             "timestamp-ms": 1731924050728
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924050728,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 655266999750843557,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:50.667532+00', '2024-11-18 10:00:50.734244+00'),
       ('01933eb7-b763-7ff2-8792-a713e13d606e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6158867485873285169
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 3109546946843117094,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6158867485873285169,
             "timestamp-ms": 1731924047790,
             "manifest-list": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/snap-6158867485873285169-1-eb888da3-744f-41fe-b545-f1229424b5b3.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3109546946843117094,
             "timestamp-ms": 1731924047911,
             "manifest-list": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/snap-3109546946843117094-1-537609e3-351a-4037-88ed-7801f1de361d.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 6158867485873285169
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b763-7ff2-8792-a713e13d606e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924047716,
             "metadata-file": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/01933eb7-b764-7cd0-b448-9440cc08dc56.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924047790,
             "metadata-file": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/01933eb7-b7af-7d33-95d4-c53c37e014c6.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924047790,
             "metadata-file": "s3://tests/01933eb7-b75b-7931-9902-b17767208ad4/01933eb7-b763-7ff2-8792-a713e13d606e/metadata/01933eb7-b7cf-7b21-abf1-bd7719ac3ba1.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6158867485873285169,
             "timestamp-ms": 1731924047790
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924047911,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6158867485873285169,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:47.715841+00', '2024-11-18 10:00:47.91191+00'),
       ('01933eb7-b8a3-7482-be7d-d31194aaffb8', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 20551329865583446
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2625683097944305430,
             "timestamp-ms": 1731924048105,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-2625683097944305430-1-9c8e7446-1970-479b-835e-0e093c9d85d6.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "3",
               "total-files-size": "2780",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8359719989215943497,
             "timestamp-ms": 1731924048191,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-8359719989215943497-1-1ed18746-1252-4f18-ade0-d860665a8955.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 2625683097944305430
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "5",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "5",
               "total-files-size": "4634",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1160721244667940476,
             "timestamp-ms": 1731924048365,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-1160721244667940476-1-3f951da5-e9d7-4d10-a0e5-006ad50cdc3e.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 2877653363747437295
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "6",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "6",
               "total-files-size": "5561",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3680733727833058977,
             "timestamp-ms": 1731924048458,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-3680733727833058977-1-b800714e-4d87-422a-8250-4a8670e3fb2b.avro",
             "sequence-number": 5,
             "parent-snapshot-id": 1160721244667940476
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "4",
               "total-files-size": "3707",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2877653363747437295,
             "timestamp-ms": 1731924048280,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-2877653363747437295-1-1ef16a0b-b41b-4a9c-b208-99a1a8a5dbf3.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 8359719989215943497
           },
           {
             "summary": {
               "operation": "replace",
               "added-records": "7",
               "total-records": "7",
               "deleted-records": "7",
               "added-data-files": "1",
               "added-files-size": "1132",
               "total-data-files": "1",
               "total-files-size": "1132",
               "deleted-data-files": "7",
               "removed-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 20551329865583446,
             "timestamp-ms": 1731924048895,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-20551329865583446-1-549ed677-d32b-4dea-931b-cd6fdc1ae923.avro",
             "sequence-number": 7,
             "parent-snapshot-id": 5755162637366086408
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "7",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "7",
               "total-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5755162637366086408,
             "timestamp-ms": 1731924048550,
             "manifest-list": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/snap-5755162637366086408-1-82880ce5-38ae-4400-bc50-fd01bd0f1f12.avro",
             "sequence-number": 6,
             "parent-snapshot-id": 3680733727833058977
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-b8a3-7482-be7d-d31194aaffb8",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924048035,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-b8a3-7482-be7d-d32fea6e3829.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048105,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-b8e9-7b50-be4c-c067d9b617ac.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048191,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-b940-7850-ad38-204dc4ee3648.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048280,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-b999-7c63-b7f7-c1c54e347d8b.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048365,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-b9ee-7b12-9406-b47d0ce4b681.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048458,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-ba4b-7040-8448-f3a708ef6d5b.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924048550,
             "metadata-file": "s3://tests/01933eb7-b89c-7551-94ab-178a5c9713e7/01933eb7-b8a3-7482-be7d-d31194aaffb8/metadata/01933eb7-baa8-77c3-a67f-480820e3512e.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2625683097944305430,
             "timestamp-ms": 1731924048105
           },
           {
             "snapshot-id": 8359719989215943497,
             "timestamp-ms": 1731924048191
           },
           {
             "snapshot-id": 2877653363747437295,
             "timestamp-ms": 1731924048280
           },
           {
             "snapshot-id": 1160721244667940476,
             "timestamp-ms": 1731924048365
           },
           {
             "snapshot-id": 3680733727833058977,
             "timestamp-ms": 1731924048458
           },
           {
             "snapshot-id": 5755162637366086408,
             "timestamp-ms": 1731924048550
           },
           {
             "snapshot-id": 20551329865583446,
             "timestamp-ms": 1731924048895
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924048895,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 20551329865583446,
         "last-sequence-number": 7,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:48.035629+00', '2024-11-18 10:00:48.895629+00'),
       ('01933eb7-c37b-7572-b694-c697e0f3b17e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6164117688037251583
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-c372-77e2-915c-5461686b0df2/01933eb7-c37b-7572-b694-c697e0f3b17e",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6164117688037251583,
             "timestamp-ms": 1731924050873,
             "manifest-list": "s3://tests/01933eb7-c372-77e2-915c-5461686b0df2/01933eb7-c37b-7572-b694-c697e0f3b17e/metadata/snap-6164117688037251583-1-f4afc8f0-12f2-4b2d-8222-175a2b2b643f.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-c37b-7572-b694-c697e0f3b17e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6164117688037251583,
             "timestamp-ms": 1731924050873
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924050873,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6164117688037251583,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:50.810871+00', '2024-11-18 10:00:50.880464+00'),
       ('01933eb7-c148-7c41-81fc-5cf20e0175e7', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8572619086682464516
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1211261935667030218,
             "timestamp-ms": 1731924050511,
             "manifest-list": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/snap-1211261935667030218-1-47a1b29a-1025-454f-8bcb-92610a2baeeb.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 5900617703291803617
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5900617703291803617,
             "timestamp-ms": 1731924050420,
             "manifest-list": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/snap-5900617703291803617-1-7b8ea0c5-4eb8-4f75-97b6-ad8153f06586.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 4187900568336267548
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4187900568336267548,
             "timestamp-ms": 1731924050314,
             "manifest-list": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/snap-4187900568336267548-1-3bded33f-62ea-4a33-a72d-b3a2511373a4.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8572619086682464516,
             "timestamp-ms": 1731924050596,
             "manifest-list": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/snap-8572619086682464516-1-9cc48e62-2a77-4b50-9950-ee61df12910b.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 1211261935667030218
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2",
           "write.metadata.delete-after-commit.enabled": "true"
         },
         "table-uuid": "01933eb7-c148-7c41-81fc-5cf20e0175e7",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924050420,
             "metadata-file": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/01933eb7-c1f4-73c1-ac14-5588652d2516.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924050511,
             "metadata-file": "s3://tests/01933eb7-c13f-71c0-9500-5e0659cf3f73/01933eb7-c148-7c41-81fc-5cf20e0175e7/metadata/01933eb7-c250-7011-916a-7d67ae2a4478.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4187900568336267548,
             "timestamp-ms": 1731924050314
           },
           {
             "snapshot-id": 5900617703291803617,
             "timestamp-ms": 1731924050420
           },
           {
             "snapshot-id": 1211261935667030218,
             "timestamp-ms": 1731924050511
           },
           {
             "snapshot-id": 8572619086682464516,
             "timestamp-ms": 1731924050596
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924050596,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8572619086682464516,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:50.248897+00', '2024-11-18 10:00:50.596591+00'),
       ('01933eb8-3111-7f43-9a07-17f3227c36d2', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-3106-7920-aa79-96cbd5b79a1f/01933eb8-3111-7f43-9a07-17f3227c36d2",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-3111-7f43-9a07-17f3227c36d2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924078865,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:18.865491+00', NULL),
       ('01933eb7-bc75-72e1-88f3-3dcc0a877152', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2726950153078061314
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2726950153078061314,
             "timestamp-ms": 1731924049084,
             "manifest-list": "s3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/custom_location/metadata/snap-2726950153078061314-1-871cd37e-76ee-4b79-b2ba-b2b9a9e88913.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bc75-72e1-88f3-3dcc0a877152",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924049013,
             "metadata-file": "s3://tests/01933eb7-bc55-7252-940f-dd7077b351a1/custom_location/metadata/01933eb7-bc75-72e1-88f3-3dd338b80306.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2726950153078061314,
             "timestamp-ms": 1731924049084
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924049084,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 2726950153078061314,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.012937+00', '2024-11-18 10:00:49.085504+00'),
       ('01933eb7-bd11-7ce3-ab67-4fabf59a4cc9', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bd0a-7732-abe5-e000d0902ca4/01933eb7-bd11-7ce3-ab67-4fabf59a4cc9",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bd11-7ce3-ab67-4fabf59a4cc9",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924049169,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.169513+00', NULL),
       ('01933eb7-bddd-7132-af84-267337163841', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3602925096129081488
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3602925096129081488,
             "timestamp-ms": 1731924049449,
             "manifest-list": "s3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/custom_location/metadata/snap-3602925096129081488-1-ee59d327-1dfd-4272-ab3e-5d7f9dd32a12.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-bddd-7132-af84-267337163841",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924049373,
             "metadata-file": "s3://tests/01933eb7-bdc1-7d13-b222-4b225553abe5/custom_location/metadata/01933eb7-bddd-7132-af84-26845a19da9e.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3602925096129081488,
             "timestamp-ms": 1731924049449
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924049449,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3602925096129081488,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.373671+00', '2024-11-18 10:00:49.450232+00'),
       ('01933eb7-bf3b-75e1-819f-94b12349de42', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4818117057678362476
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4429428390880046615,
             "timestamp-ms": 1731924050001,
             "manifest-list": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/snap-4429428390880046615-1-949e193f-887f-4612-809c-c2015f98f0c9.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 636545125162488381
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 636545125162488381,
             "timestamp-ms": 1731924049916,
             "manifest-list": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/snap-636545125162488381-1-9777b6da-6c0c-4f0c-827b-35f8b9d46cd9.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 3287749745082008837
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3287749745082008837,
             "timestamp-ms": 1731924049788,
             "manifest-list": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/snap-3287749745082008837-1-d34e5935-50f2-43ca-a8d6-b5d51b3d0a72.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4818117057678362476,
             "timestamp-ms": 1731924050095,
             "manifest-list": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/snap-4818117057678362476-1-de6b9f99-b5c9-4709-9904-b0b02ef8b829.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 4429428390880046615
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2"
         },
         "table-uuid": "01933eb7-bf3b-75e1-819f-94b12349de42",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924049916,
             "metadata-file": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/01933eb7-bffd-7001-aeef-9dfb38ea60d4.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924050001,
             "metadata-file": "s3://tests/01933eb7-bf2d-77c2-8c09-4a1940206f6f/01933eb7-bf3b-75e1-819f-94b12349de42/metadata/01933eb7-c051-73f2-b717-9adcca2fbe5b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3287749745082008837,
             "timestamp-ms": 1731924049788
           },
           {
             "snapshot-id": 636545125162488381,
             "timestamp-ms": 1731924049916
           },
           {
             "snapshot-id": 4429428390880046615,
             "timestamp-ms": 1731924050001
           },
           {
             "snapshot-id": 4818117057678362476,
             "timestamp-ms": 1731924050095
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924050095,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 4818117057678362476,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:49.723495+00', '2024-11-18 10:00:50.096409+00'),
       ('01933eb8-02a5-7610-a61a-43ff8c081518', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6032219594400106571
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "961",
               "total-data-files": "1",
               "total-files-size": "961",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6032219594400106571,
             "timestamp-ms": 1731924071501,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518/metadata/snap-6032219594400106571-1-237bfa54-6ea3-43e0-8792-bdda7c9e9483.avro",
             "sequence-number": 2
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6671263441148992666,
             "timestamp-ms": 1731924068911,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518/metadata/snap-6671263441148992666-1-16760bc1-3eff-422d-ac67-9f9ae50c1da9.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root",
           "write.parquet.compression-codec": "zstd"
         },
         "table-uuid": "01933eb8-02a5-7610-a61a-43ff8c081518",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924068911,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-0283-79c1-8852-3fc21025b598/01933eb8-02a5-7610-a61a-43ff8c081518/metadata/01933eb8-0b22-7231-be4d-4668b493a7df.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6032219594400106571,
             "timestamp-ms": 1731924071501
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924071501,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6032219594400106571,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:06.980987+00', '2024-11-18 10:01:12.035816+00'),
       ('01933eb7-c407-7520-9831-fc501240440e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8794866021800270953
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933eb7-c3ff-7ce0-aab6-ad5338d4955a/01933eb7-c407-7520-9831-fc501240440e",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924015162",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8794866021800270953,
             "timestamp-ms": 1731924051010,
             "manifest-list": "s3://tests/01933eb7-c3ff-7ce0-aab6-ad5338d4955a/01933eb7-c407-7520-9831-fc501240440e/metadata/snap-8794866021800270953-1-e2896528-404e-48e8-b077-fefb2be3bc86.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-c407-7520-9831-fc501240440e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8794866021800270953,
             "timestamp-ms": 1731924051010
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924051010,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8794866021800270953,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:00:50.951289+00', '2024-11-18 10:00:51.01645+00'),
       ('01933eb7-ec34-7bc2-aa0b-ffc3f2783457', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb7-ec08-7cc0-a665-79d9fed8c5d3/01933eb7-ec34-7bc2-aa0b-ffc3f2783457",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-ec34-7bc2-aa0b-ffc3f2783457",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924061236,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:01.236115+00', NULL),
       ('01933eb7-f416-7822-ad99-9d65c13e1168', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5155424093960951554
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "long",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eb7-f3b4-7743-861e-aacc8dfd1750/01933eb7-f416-7822-ad99-9d65c13e1168",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "983",
               "total-data-files": "1",
               "total-files-size": "983",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5155424093960951554,
             "timestamp-ms": 1731924066158,
             "manifest-list": "gs://ht-catalog-dev/01933eb7-f3b4-7743-861e-aacc8dfd1750/01933eb7-f416-7822-ad99-9d65c13e1168/metadata/snap-5155424093960951554-1-82140722-f6d2-4eed-b0b6-fa6127c8137f.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb7-f416-7822-ad99-9d65c13e1168",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5155424093960951554,
             "timestamp-ms": 1731924066158
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924066158,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5155424093960951554,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:03.254713+00', '2024-11-18 10:01:06.464574+00'),
       ('01933eb8-2201-7663-bb7d-956d2048159d', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-21f0-70e3-ae90-83a460633f13/01933eb8-2201-7663-bb7d-956d2048159d",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-2201-7663-bb7d-956d2048159d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924075009,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:15.009629+00', NULL),
       ('01933eb8-3d78-77f0-81df-55bab3ad74e4', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-3d70-7781-bcbe-cecc1747417a/01933eb8-3d78-77f0-81df-55bab3ad74e4",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-3d78-77f0-81df-55bab3ad74e4",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924082040,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:22.040608+00', NULL),
       ('01933eb8-4738-7eb1-a914-60ae6101b0a2', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-472c-7a53-83e1-1aee37f700fb/01933eb8-4738-7eb1-a914-60ae6101b0a2",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-4738-7eb1-a914-60ae6101b0a2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924084537,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:24.536251+00', NULL),
       ('01933eb8-d555-7500-a136-0eb2a3d2ffc5', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-d541-70d0-a5c8-26f258d3b57d/01933eb8-d555-7500-a136-0eb2a3d2ffc5",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-d555-7500-a136-0eb2a3d2ffc5",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924120917,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:00.917431+00', NULL),
       ('01933eb8-4f55-7833-b225-8ef9257fc646', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4900949324704220364
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "id",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1876",
               "total-data-files": "2",
               "total-files-size": "1876",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1300376143133654623,
             "timestamp-ms": 1731924089195,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646/metadata/snap-1300376143133654623-1-401ce5b4-3088-49e0-afc3-d7ade5499f1c.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "overwrite",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "deleted-records": "1",
               "added-data-files": "1",
               "added-files-size": "958",
               "total-data-files": "2",
               "total-files-size": "1896",
               "deleted-data-files": "1",
               "removed-files-size": "938",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4900949324704220364,
             "timestamp-ms": 1731924094387,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646/metadata/snap-4900949324704220364-1-b00e2b17-24aa-4290-ab99-b9b1c171fc40.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 1300376143133654623
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-4f55-7833-b225-8ef9257fc646",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924086613,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646/metadata/01933eb8-4f55-7833-b225-8f06f7a967b7.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924089195,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-4f4a-70a2-b2ec-d30b6237ff9b/01933eb8-4f55-7833-b225-8ef9257fc646/metadata/01933eb8-596f-7733-8caa-3a14d29070b7.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 1300376143133654623,
             "timestamp-ms": 1731924089195
           },
           {
             "snapshot-id": 4900949324704220364,
             "timestamp-ms": 1731924094387
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924094387,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 4900949324704220364,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:01:26.613252+00', '2024-11-18 10:01:34.39052+00'),
       ('01933eb8-e4ed-7ee0-ae88-81b77d872451', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8120687630386891666
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b/01933eb8-e4ed-7ee0-ae88-81b77d872451",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8120687630386891666,
             "timestamp-ms": 1731924127388,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b/01933eb8-e4ed-7ee0-ae88-81b77d872451/metadata/snap-8120687630386891666-1-9277ee1b-9098-4f53-bc15-dd28001618b7.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-e4ed-7ee0-ae88-81b77d872451",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924124909,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-e4e4-7f80-9471-d543ea2f8e1b/01933eb8-e4ed-7ee0-ae88-81b77d872451/metadata/01933eb8-e4ed-7ee0-ae88-81c3b902b508.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8120687630386891666,
             "timestamp-ms": 1731924127388
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924127388,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8120687630386891666,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:04.909726+00', '2024-11-18 10:02:07.39271+00'),
       ('01933eb8-f70b-7d00-b2f3-68a92a576801', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-f6fe-7dc0-9484-865f90b9de2b/01933eb8-f70b-7d00-b2f3-68a92a576801",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-f70b-7d00-b2f3-68a92a576801",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924129547,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:09.547162+00', NULL),
       ('01933eb8-db3e-7a32-8f02-c6391ce0ef83', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-db32-7ad3-81dc-b9c75dd674e0/01933eb8-db3e-7a32-8f02-c6391ce0ef83",
         "snapshots": [],
         "properties": {
           "key1": "value1",
           "key2": "value2",
           "owner": "root"
         },
         "table-uuid": "01933eb8-db3e-7a32-8f02-c6391ce0ef83",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924122430,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-db32-7ad3-81dc-b9c75dd674e0/01933eb8-db3e-7a32-8f02-c6391ce0ef83/metadata/01933eb8-db3e-7a32-8f02-c64254398766.gz.metadata.json"
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924122430,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:02.430192+00', '2024-11-18 10:02:04.393802+00'),
       ('01933eb8-fbc6-7910-8214-20ada70ab1f6', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4430476858694804015
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4/01933eb8-fbc6-7910-8214-20ada70ab1f6",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4430476858694804015,
             "timestamp-ms": 1731924133363,
             "manifest-list": "gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4/01933eb8-fbc6-7910-8214-20ada70ab1f6/metadata/snap-4430476858694804015-1-1b24080e-4a79-4b2c-973f-6f977927abd9.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb8-fbc6-7910-8214-20ada70ab1f6",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924130758,
             "metadata-file": "gs://ht-catalog-dev/01933eb8-fbb8-77a3-a202-7506067ac3f4/01933eb8-fbc6-7910-8214-20ada70ab1f6/metadata/01933eb8-fbc6-7910-8214-20b31ef31ca0.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4430476858694804015,
             "timestamp-ms": 1731924133363
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924133363,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 4430476858694804015,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:10.757948+00', '2024-11-18 10:02:13.364974+00'),
       ('01933eb9-1f71-7c60-91eb-d7748b04bfec', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4526156630859337214
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "m/y fl !? -_ä oats",
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
         "location": "gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497/01933eb9-1f71-7c60-91eb-d7748b04bfec",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "2009",
               "total-data-files": "2",
               "total-files-size": "2009",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4526156630859337214,
             "timestamp-ms": 1731924142541,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497/01933eb9-1f71-7c60-91eb-d7748b04bfec/metadata/snap-4526156630859337214-1-230a4328-d5c6-4813-af97-34b11fbe483a.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-1f71-7c60-91eb-d7748b04bfec",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924139889,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-1f5b-7852-88ec-8b3c0464d497/01933eb9-1f71-7c60-91eb-d7748b04bfec/metadata/01933eb9-1f71-7c60-91eb-d78c86ea839f.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4526156630859337214,
             "timestamp-ms": 1731924142541
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924142541,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "m/y fl !? -_ä oats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 4526156630859337214,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:19.889688+00', '2024-11-18 10:02:22.543562+00'),
       ('01933eba-0bb4-78a1-9f5b-5f236c87202b', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6308005459699490075
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 6308005459699490075,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6308005459699490075,
             "timestamp-ms": 1731924202785,
             "manifest-list": "gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b/metadata/snap-6308005459699490075-1-e41cd481-f99c-4e9c-a5a2-daed89cb2471.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-0bb4-78a1-9f5b-5f236c87202b",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924200372,
             "metadata-file": "gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b/metadata/01933eba-0bb4-78a1-9f5b-5f31b1cee1f4.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924202785,
             "metadata-file": "gs://ht-catalog-dev/01933eba-0ba5-7703-8d44-20a7a3f23cfa/01933eba-0bb4-78a1-9f5b-5f236c87202b/metadata/01933eba-1528-72e0-bed1-cbf82df6a76b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6308005459699490075,
             "timestamp-ms": 1731924202785
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924202785,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6308005459699490075,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:20.372245+00', '2024-11-18 10:03:24.644518+00'),
       ('01933eb9-0fd5-7961-9d9f-72e131f79898', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3258097484819596630
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "my floats",
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
         "location": "gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6/01933eb9-0fd5-7961-9d9f-72e131f79898",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1865",
               "total-data-files": "2",
               "total-files-size": "1865",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 3258097484819596630,
             "timestamp-ms": 1731924138611,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6/01933eb9-0fd5-7961-9d9f-72e131f79898/metadata/snap-3258097484819596630-1-a611e8bc-0ae9-4804-b331-fff30daed5ec.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-0fd5-7961-9d9f-72e131f79898",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924135893,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-0fc8-7e43-b604-cd16dd6d52e6/01933eb9-0fd5-7961-9d9f-72e131f79898/metadata/01933eb9-0fd5-7961-9d9f-72fe9c8444e5.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3258097484819596630,
             "timestamp-ms": 1731924138611
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924138611,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my floats",
                 "field-id": 1000,
                 "source-id": 2,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 3258097484819596630,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:15.893666+00', '2024-11-18 10:02:18.614773+00'),
       ('01933eb9-2ed2-7882-9461-3f9668f9611c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8933003251373007473
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 6218482799900120673,
             "timestamp-ms": 1731924146626,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/snap-6218482799900120673-1-07fc7025-c07f-4206-a3ab-bdec1e7c008f.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "3",
               "total-files-size": "2779",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8933003251373007473,
             "timestamp-ms": 1731924149846,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/snap-8933003251373007473-1-85b42172-8c07-4d42-939d-03f03fad87a2.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 6218482799900120673
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-2ed2-7882-9461-3f9668f9611c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924143826,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/01933eb9-2ed2-7882-9461-3fa410c3df44.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924146626,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/01933eb9-39c9-7ca1-91fe-e16ab21445d8.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924146626,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-2eb8-71f1-ad43-52c963f18210/01933eb9-2ed2-7882-9461-3f9668f9611c/metadata/01933eb9-3eec-7eb0-a404-632e1ad1776b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6218482799900120673,
             "timestamp-ms": 1731924146626
           },
           {
             "snapshot-id": 8933003251373007473,
             "timestamp-ms": 1731924149846
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 1,
         "last-updated-ms": 1731924149846,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           },
           {
             "fields": [],
             "spec-id": 1
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 8933003251373007473,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:23.826129+00', '2024-11-18 10:02:29.848439+00'),
       ('01933eb9-c324-7912-96ab-b4dc6421b0c2', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 1959174632724250911
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 5962868559162074089
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5962868559162074089,
             "timestamp-ms": 1731924184254,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/snap-5962868559162074089-1-d18859d4-80f5-482d-ad7b-2028071e46fc.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 1959174632724250911,
             "timestamp-ms": 1731924187993,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/snap-1959174632724250911-1-63a9efd5-5f33-4767-97dd-f183d44933c0.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 5962868559162074089
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-c324-7912-96ab-b4dc6421b0c2",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924181796,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/01933eb9-c324-7912-96ab-b4eb7888d489.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924184254,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/01933eb9-ccbf-75b3-bd96-5e4085b215d0.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924184254,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-c30d-7f02-ab54-da33a381ef5e/01933eb9-c324-7912-96ab-b4dc6421b0c2/metadata/01933eb9-d42a-7ac3-ba7a-742c257eee59.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5962868559162074089,
             "timestamp-ms": 1731924184254
           },
           {
             "snapshot-id": 1959174632724250911,
             "timestamp-ms": 1731924187993
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924187993,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 1959174632724250911,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:01.796153+00', '2024-11-18 10:03:08.001299+00'),
       ('01933eba-1eac-7463-88d4-20759a827b49', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5669577842761996724
           },
           "test_branch": {
             "type": "branch",
             "snapshot-id": 8375132428111857216,
             "max-ref-age-ms": 604800000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5669577842761996724,
             "timestamp-ms": 1731924207636,
             "manifest-list": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/snap-5669577842761996724-1-70496fe0-b7c6-431d-a342-078d3c2c4415.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "926",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8375132428111857216,
             "timestamp-ms": 1731924211384,
             "manifest-list": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/snap-8375132428111857216-1-b242ea30-8e3b-4d69-baac-9ab1ecf8ac45.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 5669577842761996724
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-1eac-7463-88d4-20759a827b49",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924205228,
             "metadata-file": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/01933eba-1eac-7463-88d4-208b568440b5.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924207636,
             "metadata-file": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/01933eba-2819-7db1-9dba-7c710780a4a0.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924207636,
             "metadata-file": "gs://ht-catalog-dev/01933eba-1ea1-7be1-8bc5-7454857f8431/01933eba-1eac-7463-88d4-20759a827b49/metadata/01933eba-2f60-7472-b1f3-3d55594b2002.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5669577842761996724,
             "timestamp-ms": 1731924207636
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924211384,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5669577842761996724,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:25.227971+00', '2024-11-18 10:03:31.388349+00'),
       ('01933eba-9ded-7fa0-b48a-6266c7bc50c3', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/01933eba-9ded-7fa0-b48a-6266c7bc50c3",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-9ded-7fa0-b48a-6266c7bc50c3",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924237805,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:57.805589+00', NULL),
       ('01933eba-d146-7032-9914-cdce96d2b36c', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/01933eba-d146-7032-9914-cdce96d2b36c",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-d146-7032-9914-cdce96d2b36c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924250950,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:10.950445+00', NULL),
       ('01933eb9-51f1-7450-b4bc-64acbf3333d1', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6372390749219946709
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be/01933eb9-51f1-7450-b4bc-64acbf3333d1",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "929",
               "total-data-files": "1",
               "total-files-size": "929",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6372390749219946709,
             "timestamp-ms": 1731924155370,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be/01933eb9-51f1-7450-b4bc-64acbf3333d1/metadata/snap-6372390749219946709-1-18b9a087-1eb5-4539-9503-5f2d3cd2aa63.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-51f1-7450-b4bc-64acbf3333d1",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924152817,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-51d5-7b71-a198-c0fa5d5550be/01933eb9-51f1-7450-b4bc-64acbf3333d1/metadata/01933eb9-51f1-7450-b4bc-64b962b554ec.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6372390749219946709,
             "timestamp-ms": 1731924155370
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924155370,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 6372390749219946709,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:32.816879+00', '2024-11-18 10:02:35.374322+00'),
       ('01933eb9-635b-71e0-8791-1b500fd4c82c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2411779885223657719
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 2
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
               },
               {
                 "id": 4,
                 "name": "my_bool",
                 "type": "boolean",
                 "required": false
               }
             ],
             "schema-id": 1
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6432236283688187444,
             "timestamp-ms": 1731924159687,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/snap-6432236283688187444-1-8c94e3c8-d4f3-4408-b5b6-36052d89021c.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "909",
               "total-data-files": "2",
               "total-files-size": "1836",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 2,
             "snapshot-id": 2411779885223657719,
             "timestamp-ms": 1731924164588,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/snap-2411779885223657719-1-5251348c-f31b-4fbc-8a8c-1c5772973c99.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 6432236283688187444
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-635b-71e0-8791-1b500fd4c82c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924157276,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/01933eb9-635c-76e0-880c-195104372d7f.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924159687,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/01933eb9-6ccc-7d10-b451-92ec4c00b05b.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924159687,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/01933eb9-748e-7272-b7ff-7ce8a4a2a6c1.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924159687,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-6343-7511-9ef4-90d0fdfe16b9/01933eb9-635b-71e0-8791-1b500fd4c82c/metadata/01933eb9-78cf-70d0-bab3-4da02df43bde.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6432236283688187444,
             "timestamp-ms": 1731924159687
           },
           {
             "snapshot-id": 2411779885223657719,
             "timestamp-ms": 1731924164588
           }
         ],
         "format-version": 2,
         "last-column-id": 4,
         "default-spec-id": 0,
         "last-updated-ms": 1731924164588,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 2,
         "last-partition-id": 999,
         "current-snapshot-id": 2411779885223657719,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:37.275748+00', '2024-11-18 10:02:44.592888+00'),
       ('01933eba-a3c9-70b3-aa9d-e71d4c8fd589', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7515680698723190808
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 7515680698723190808,
             "timestamp-ms": 1731924241729,
             "manifest-list": "gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/custom_location/metadata/snap-7515680698723190808-1-4872e2c5-15cb-4896-b941-a6b3e32c6be8.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-a3c9-70b3-aa9d-e71d4c8fd589",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924239305,
             "metadata-file": "gs://ht-catalog-dev/01933eba-9dd7-7e30-ab5b-8420b5d3d8b6/custom_location/metadata/01933eba-a3c9-70b3-aa9d-e72909f0ed56.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 7515680698723190808,
             "timestamp-ms": 1731924241729
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924241729,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 7515680698723190808,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:59.305164+00', '2024-11-18 10:04:01.731995+00'),
       ('01933eba-b7bf-7d91-8e79-2d9f4052b63d', '{
         "refs": {},
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/01933eba-b7bf-7d91-8e79-2d9f4052b63d",
         "snapshots": [],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-b7bf-7d91-8e79-2d9f4052b63d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924244416,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "last-sequence-number": 0,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:04.415667+00', NULL),
       ('01933eb9-8975-7c33-903f-5e93cc511d1e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 7807534850315414094
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "4",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "4",
               "total-files-size": "3706",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 3706726807239064451,
             "timestamp-ms": 1731924173085,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/snap-3706726807239064451-1-c39e07f0-0df8-43ad-bfab-d8db290bc4b7.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 2389136725482628759
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "6",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "6",
               "total-files-size": "5559",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 7807534850315414094,
             "timestamp-ms": 1731924178734,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/snap-7807534850315414094-1-92c898c9-6d45-4d14-be7d-7049141b5713.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 3706726807239064451
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2389136725482628759,
             "timestamp-ms": 1731924169435,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/snap-2389136725482628759-1-80f1d0ec-a70a-4beb-a2d5-bcd621a2af80.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-8975-7c33-903f-5e93cc511d1e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924167029,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-8975-7c33-903f-5eafe0fee5ea.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924169435,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-92e1-7a02-b0a2-26f3856db9dd.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924169435,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-9838-79c2-ac52-d4d55b0c4a67.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924173085,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-a126-7870-897c-a253eb9c008d.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924173085,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-acd9-7663-a9b8-be323da80872.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924173085,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-895b-7d51-b004-b5d32d8a8d96/01933eb9-8975-7c33-903f-5e93cc511d1e/metadata/01933eb9-aea8-73c0-9d8b-1ef22cd1a72f.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2389136725482628759,
             "timestamp-ms": 1731924169435
           },
           {
             "snapshot-id": 3706726807239064451,
             "timestamp-ms": 1731924173085
           },
           {
             "snapshot-id": 7807534850315414094,
             "timestamp-ms": 1731924178734
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 2,
         "last-updated-ms": 1731924178734,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "int_bucket",
                 "field-id": 1000,
                 "source-id": 1,
                 "transform": "bucket[16]"
               }
             ],
             "spec-id": 1
           },
           {
             "fields": [],
             "spec-id": 0
           },
           {
             "fields": [
               {
                 "name": "string_bucket",
                 "field-id": 1001,
                 "source-id": 3,
                 "transform": "truncate[4]"
               }
             ],
             "spec-id": 2
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1001,
         "current-snapshot-id": 7807534850315414094,
         "last-sequence-number": 3,
         "default-sort-order-id": 0
       }', '2024-11-18 10:02:47.029454+00', '2024-11-18 10:02:58.736626+00'),
       ('01933eb9-e738-7dd2-aa40-b4c36c0e198d', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5614379900471112780
           },
           "first_insert": {
             "type": "tag",
             "snapshot-id": 5100346450497744283,
             "max-ref-age-ms": 31536000000
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "2",
               "total-files-size": "1854",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5614379900471112780,
             "timestamp-ms": 1731924197086,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/snap-5614379900471112780-1-f3dc28fb-2252-4c10-8443-f9444c1e10ab.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 5100346450497744283
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "1",
               "total-files-size": "927",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5100346450497744283,
             "timestamp-ms": 1731924193387,
             "manifest-list": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/snap-5100346450497744283-1-7077a6ea-bc4d-487c-b79a-a278e2ce4cc7.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eb9-e738-7dd2-aa40-b4c36c0e198d",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924191033,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/01933eb9-e739-7971-9402-9677d935db66.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924193387,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/01933eb9-f06e-7171-97a9-6906bf740cba.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924193387,
             "metadata-file": "gs://ht-catalog-dev/01933eb9-e729-7e81-945e-f27ad0fdb595/01933eb9-e738-7dd2-aa40-b4c36c0e198d/metadata/01933eb9-f7a3-7d50-aa69-1654c4d78165.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5100346450497744283,
             "timestamp-ms": 1731924193387
           },
           {
             "snapshot-id": 5614379900471112780,
             "timestamp-ms": 1731924197086
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924197086,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5614379900471112780,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:11.032693+00', '2024-11-18 10:03:17.087785+00'),
       ('01933ebb-4891-75d3-a778-75f6bb307576', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3461735137509251799
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933ebb-4889-7693-b552-33e70732e655/01933ebb-4891-75d3-a778-75f6bb307576",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3461735137509251799,
             "timestamp-ms": 1731924283179,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-4889-7693-b552-33e70732e655/01933ebb-4891-75d3-a778-75f6bb307576/metadata/snap-3461735137509251799-1-5cc2dee5-55b6-4ebd-98f0-221743cd95b7.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933ebb-4891-75d3-a778-75f6bb307576",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3461735137509251799,
             "timestamp-ms": 1731924283179
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924283179,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3461735137509251799,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:41.489568+00', '2024-11-18 10:04:43.386331+00'),
       ('01933ebb-9361-7e33-9b6a-1212ff7f9739', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_ints",
               "type": "int",
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
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-9359-7e20-9a57-5e508051df61/01933ebb-9361-7e33-9b6a-1212ff7f9739",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "949",
               "total-data-files": "1",
               "total-files-size": "949",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6910721092725046868,
             "timestamp-ms": 1731924323524,
             "manifest-list": "s3://tests/01933ebb-9359-7e20-9a57-5e508051df61/01933ebb-9361-7e33-9b6a-1212ff7f9739/metadata/snap-6910721092725046868-1-ab46ded2-f92b-4f40-b4ef-1b051bb94f03.avro"
           }
         ],
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-9361-7e33-9b6a-1212ff7f9739",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924300641,
             "metadata-file": "s3://tests/01933ebb-9359-7e20-9a57-5e508051df61/01933ebb-9361-7e33-9b6a-1212ff7f9739/metadata/01933ebb-9361-7e33-9b6a-1224d034eb57.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6910721092725046868,
             "timestamp-ms": 1731924323524
           }
         ],
         "format-version": 1,
         "last-column-id": 3,
         "partition-spec": [],
         "default-spec-id": 0,
         "last-updated-ms": 1731924323524,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6910721092725046868,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:00.641418+00', '2024-11-18 10:05:23.596032+00'),
       ('01933ebb-efd9-7fe3-9ee8-2b5480a474cb', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_ints",
               "type": "int",
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
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-efd1-7221-9bee-dce604007b98/01933ebb-efd9-7fe3-9ee8-2b5480a474cb",
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-efd9-7fe3-9ee8-2b5480a474cb",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 1,
         "last-column-id": 3,
         "partition-spec": [],
         "default-spec-id": 0,
         "last-updated-ms": 1731924324313,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:24.313052+00', NULL),
       ('01933eba-4208-7250-9c68-107b11627db7', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 6183895632010991307
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "4",
               "total-files-size": "3707",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2979956359950141393,
             "timestamp-ms": 1731924221708,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-2979956359950141393-1-ae2f56da-3fb2-47fa-917b-cc6540a9dc94.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 6495031244029089914
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1853",
               "total-data-files": "2",
               "total-files-size": "1853",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 356831852765384899,
             "timestamp-ms": 1731924216694,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-356831852765384899-1-e11de8bb-2fc8-447e-9451-22cc8f0b0e21.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "3",
               "total-files-size": "2780",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6495031244029089914,
             "timestamp-ms": 1731924219150,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-6495031244029089914-1-8adadda9-a57d-41e4-b7ef-013a506c391b.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 356831852765384899
           },
           {
             "summary": {
               "operation": "replace",
               "added-records": "7",
               "total-records": "7",
               "deleted-records": "7",
               "added-data-files": "1",
               "added-files-size": "1132",
               "total-data-files": "1",
               "total-files-size": "1132",
               "deleted-data-files": "7",
               "removed-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6183895632010991307,
             "timestamp-ms": 1731924235869,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-6183895632010991307-1-4ae4937f-4451-4434-8b08-5d65b0fb940f.avro",
             "sequence-number": 7,
             "parent-snapshot-id": 4218616030706468680
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "6",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "6",
               "total-files-size": "5561",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4296348057059759659,
             "timestamp-ms": 1731924226668,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-4296348057059759659-1-a98ea944-5226-4eec-9906-d5c08a27c1c4.avro",
             "sequence-number": 5,
             "parent-snapshot-id": 5741177974093538789
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "7",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "7",
               "total-files-size": "6488",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4218616030706468680,
             "timestamp-ms": 1731924229035,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-4218616030706468680-1-b2abfe22-a730-40c8-b1bf-61c6c7a378e5.avro",
             "sequence-number": 6,
             "parent-snapshot-id": 4296348057059759659
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "5",
               "added-data-files": "1",
               "added-files-size": "927",
               "total-data-files": "5",
               "total-files-size": "4634",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5741177974093538789,
             "timestamp-ms": 1731924224225,
             "manifest-list": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/snap-5741177974093538789-1-c575f360-fea1-4857-9813-1ece9506f2c2.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 2979956359950141393
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-4208-7250-9c68-107b11627db7",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924214280,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-4208-7250-9c68-1083de3ae6c2.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924216694,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-4b78-7dc3-8a7a-605a6ca92c33.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924219150,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-550f-7383-9c20-982056a14b51.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924221708,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-5f10-75c1-b7ad-8f90733e55a3.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924224225,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-68e3-76e3-a139-d7c3108d4391.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924226668,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-7272-7a20-b042-0d7f19ff3915.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924229035,
             "metadata-file": "gs://ht-catalog-dev/01933eba-41f8-7790-903e-06e6422ecca2/01933eba-4208-7250-9c68-107b11627db7/metadata/01933eba-7bad-7002-89bd-6e8c9b95ef4a.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 356831852765384899,
             "timestamp-ms": 1731924216694
           },
           {
             "snapshot-id": 6495031244029089914,
             "timestamp-ms": 1731924219150
           },
           {
             "snapshot-id": 2979956359950141393,
             "timestamp-ms": 1731924221708
           },
           {
             "snapshot-id": 5741177974093538789,
             "timestamp-ms": 1731924224225
           },
           {
             "snapshot-id": 4296348057059759659,
             "timestamp-ms": 1731924226668
           },
           {
             "snapshot-id": 4218616030706468680,
             "timestamp-ms": 1731924229035
           },
           {
             "snapshot-id": 6183895632010991307,
             "timestamp-ms": 1731924235869
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924235869,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 6183895632010991307,
         "last-sequence-number": 7,
         "default-sort-order-id": 0
       }', '2024-11-18 10:03:34.280038+00', '2024-11-18 10:03:55.872006+00'),
       ('01933eba-bdbf-75b3-ae3e-f4f62d4cc418', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 5409906826222031642
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5409906826222031642,
             "timestamp-ms": 1731924248480,
             "manifest-list": "gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/custom_location/metadata/snap-5409906826222031642-1-32bcfbae-fea7-4e51-89a6-9a6b1d950684.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-bdbf-75b3-ae3e-f4f62d4cc418",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924245952,
             "metadata-file": "gs://ht-catalog-dev/01933eba-b7a6-7f12-aec3-7f57fa74a505/custom_location/metadata/01933eba-bdc0-7361-94cc-7eeaa251f578.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 5409906826222031642,
             "timestamp-ms": 1731924248480
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924248480,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 5409906826222031642,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:05.951771+00', '2024-11-18 10:04:08.4838+00'),
       ('01933eba-d748-7c80-b7ec-ebf3221e86ba', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8697670673502010734
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/custom_location",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "832",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8697670673502010734,
             "timestamp-ms": 1731924254898,
             "manifest-list": "gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/custom_location/metadata/snap-8697670673502010734-1-da0fa1fa-43ca-4ad5-9f06-f3e23c3bb4d7.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933eba-d748-7c80-b7ec-ebf3221e86ba",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924252488,
             "metadata-file": "gs://ht-catalog-dev/01933eba-d130-79c1-8c44-a166ebc0463b/custom_location/metadata/01933eba-d748-7c80-b7ec-ec044018df6d.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8697670673502010734,
             "timestamp-ms": 1731924254898
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924254898,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8697670673502010734,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:12.488144+00', '2024-11-18 10:04:14.899163+00'),
       ('01933eba-eaeb-7262-a801-d6ac38bd14bc', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 672347839420401346
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3305874482834352172,
             "timestamp-ms": 1731924260032,
             "manifest-list": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/snap-3305874482834352172-1-df1edbcc-1e01-4abc-9e94-df97370d0ee5.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 672347839420401346,
             "timestamp-ms": 1731924267502,
             "manifest-list": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/snap-672347839420401346-1-6b9346e9-b82b-4bdc-b0b5-c2d0e39ee98c.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 3901151765646847908
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3901151765646847908,
             "timestamp-ms": 1731924265102,
             "manifest-list": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/snap-3901151765646847908-1-f451255a-b606-4b48-a41b-73f5428d9a18.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 9092697118158360817
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 9092697118158360817,
             "timestamp-ms": 1731924262628,
             "manifest-list": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/snap-9092697118158360817-1-1944db0d-5aa1-4505-b1b1-b8ffea4cda88.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 3305874482834352172
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2"
         },
         "table-uuid": "01933eba-eaeb-7262-a801-d6ac38bd14bc",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924262628,
             "metadata-file": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/01933eba-fee9-7d93-a468-c64d48c384d1.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924265102,
             "metadata-file": "gs://ht-catalog-dev/01933eba-eacf-79e3-900a-32bef3afe08f/01933eba-eaeb-7262-a801-d6ac38bd14bc/metadata/01933ebb-0892-73f1-80ff-a8a10fec79b6.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3305874482834352172,
             "timestamp-ms": 1731924260032
           },
           {
             "snapshot-id": 9092697118158360817,
             "timestamp-ms": 1731924262628
           },
           {
             "snapshot-id": 3901151765646847908,
             "timestamp-ms": 1731924265102
           },
           {
             "snapshot-id": 672347839420401346,
             "timestamp-ms": 1731924267502
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924267502,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 672347839420401346,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:17.515544+00', '2024-11-18 10:04:27.503147+00'),
       ('01933ebb-555e-7010-acf7-07ef7dbd6dd6', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2996714831338831250
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933ebb-5550-73e1-9a52-1df0cfc2dab7/01933ebb-555e-7010-acf7-07ef7dbd6dd6",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2996714831338831250,
             "timestamp-ms": 1731924286386,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-5550-73e1-9a52-1df0cfc2dab7/01933ebb-555e-7010-acf7-07ef7dbd6dd6/metadata/snap-2996714831338831250-1-be0510d8-083f-4ec5-9c64-978b8e218535.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933ebb-555e-7010-acf7-07ef7dbd6dd6",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 2996714831338831250,
             "timestamp-ms": 1731924286386
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924286386,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 2996714831338831250,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:44.766172+00', '2024-11-18 10:04:46.600541+00'),
       ('01933ebb-1757-7550-9bad-ff4fa2769b83', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 2243772991896633769
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "4",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "4",
               "total-files-size": "1664",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 2243772991896633769,
             "timestamp-ms": 1731924279570,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/snap-2243772991896633769-1-524cf990-dfb9-4197-97f4-3dea762cdb40.avro",
             "sequence-number": 4,
             "parent-snapshot-id": 8963905025341810000
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "2",
               "total-files-size": "832",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 5162260303896580270,
             "timestamp-ms": 1731924274077,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/snap-5162260303896580270-1-3b452a2f-3fe8-4f33-bd41-f8cec146bb8d.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 44359308972377437
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "3",
               "total-files-size": "1248",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 8963905025341810000,
             "timestamp-ms": 1731924276619,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/snap-8963905025341810000-1-5dace1d7-62c1-4d28-a771-438cab2dca51.avro",
             "sequence-number": 3,
             "parent-snapshot-id": 5162260303896580270
           },
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "416",
               "total-data-files": "1",
               "total-files-size": "416",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 44359308972377437,
             "timestamp-ms": 1731924271577,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/snap-44359308972377437-1-707733f0-24e3-49f6-97ff-00eeb061a841.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root",
           "write.metadata.previous-versions-max": "2",
           "write.metadata.delete-after-commit.enabled": "true"
         },
         "table-uuid": "01933ebb-1757-7550-9bad-ff4fa2769b83",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924274077,
             "metadata-file": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/01933ebb-2b9f-7ea1-becd-0f208f1b05bd.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924276619,
             "metadata-file": "gs://ht-catalog-dev/01933ebb-174a-7861-8d9f-b5cf0f8a0d17/01933ebb-1757-7550-9bad-ff4fa2769b83/metadata/01933ebb-3590-74f0-ad98-8b43a75feb22.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 44359308972377437,
             "timestamp-ms": 1731924271577
           },
           {
             "snapshot-id": 5162260303896580270,
             "timestamp-ms": 1731924274077
           },
           {
             "snapshot-id": 8963905025341810000,
             "timestamp-ms": 1731924276619
           },
           {
             "snapshot-id": 2243772991896633769,
             "timestamp-ms": 1731924279570
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924279570,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 2243772991896633769,
         "last-sequence-number": 4,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:28.887443+00', '2024-11-18 10:04:39.573639+00'),
       ('01933ebb-6252-7022-89c9-82ac478186c3', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 3662924153235461692
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "a",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "gs://ht-catalog-dev/01933ebb-6243-72e3-94f4-0bc65081f5fd/01933ebb-6252-7022-89c9-82ac478186c3",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "spark.app.id": "local-1731924059142",
               "added-records": "1",
               "total-records": "1",
               "added-data-files": "1",
               "added-files-size": "398",
               "total-data-files": "1",
               "total-files-size": "398",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3662924153235461692,
             "timestamp-ms": 1731924289652,
             "manifest-list": "gs://ht-catalog-dev/01933ebb-6243-72e3-94f4-0bc65081f5fd/01933ebb-6252-7022-89c9-82ac478186c3/metadata/snap-3662924153235461692-1-e111e4b3-2c80-4a65-aba8-a84e0714b80d.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "owner": "root"
         },
         "table-uuid": "01933ebb-6252-7022-89c9-82ac478186c3",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3662924153235461692,
             "timestamp-ms": 1731924289652
           }
         ],
         "format-version": 2,
         "last-column-id": 1,
         "default-spec-id": 0,
         "last-updated-ms": 1731924289652,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3662924153235461692,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:48.082631+00', '2024-11-18 10:04:49.846526+00'),
       ('01933ebb-809a-7b62-a38f-e177eca4ff16', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8371176108759781105
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-807e-7750-8650-e43ef1b7d8d7/my_table-5810f0d2bb3940538c4721f6d883a0e1",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "total-records": "0",
               "trino_query_id": "20241118_100455_00010_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "total-data-files": "0",
               "total-files-size": "0",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "0"
             },
             "schema-id": 0,
             "snapshot-id": 8371176108759781105,
             "timestamp-ms": 1731924296046,
             "manifest-list": "s3://tests/01933ebb-807e-7750-8650-e43ef1b7d8d7/my_table-5810f0d2bb3940538c4721f6d883a0e1/metadata/snap-8371176108759781105-1-40f8e60c-6e22-4fc7-9e82-322596dcafa0.avro",
             "sequence-number": 1
           }
         ],
         "properties": {
           "write.format.default": "PARQUET"
         },
         "table-uuid": "01933ebb-809a-7b62-a38f-e177eca4ff16",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8371176108759781105,
             "timestamp-ms": 1731924296046
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924296046,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 8371176108759781105,
         "last-sequence-number": 1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:55.833995+00', '2024-11-18 10:04:56.108787+00'),
       ('01933ebb-81ee-7e82-b92f-1661a0a0b83c', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 4119261334322996757
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "total-records": "0",
               "trino_query_id": "20241118_100456_00012_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "total-data-files": "0",
               "total-files-size": "0",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "0"
             },
             "schema-id": 0,
             "snapshot-id": 483631937174526115,
             "timestamp-ms": 1731924296200,
             "manifest-list": "s3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce/metadata/snap-483631937174526115-1-ff6f1199-bc4d-43b8-a9f0-1ab375a2277f.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "trino_query_id": "20241118_100456_00013_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "added-data-files": "1",
               "added-files-size": "484",
               "total-data-files": "1",
               "total-files-size": "484",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 4119261334322996757,
             "timestamp-ms": 1731924296716,
             "manifest-list": "s3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce/metadata/snap-4119261334322996757-1-5ad2b911-56d4-4900-adc8-3bb0e83eeb18.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 483631937174526115
           }
         ],
         "properties": {
           "write.format.default": "PARQUET"
         },
         "table-uuid": "01933ebb-81ee-7e82-b92f-1661a0a0b83c",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924296200,
             "metadata-file": "s3://tests/01933ebb-81e1-7da2-8183-34caa584380b/my_table-873a13a656b54ead8f76e33173a138ce/metadata/01933ebb-8210-7ab2-ae78-0e196da44c29.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 483631937174526115,
             "timestamp-ms": 1731924296200
           },
           {
             "snapshot-id": 4119261334322996757,
             "timestamp-ms": 1731924296716
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924296716,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 4119261334322996757,
         "last-sequence-number": 2,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:56.174793+00', '2024-11-18 10:04:56.731906+00'),
       ('01933ebb-eeda-7410-95c3-76d15984f743', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_floats",
               "type": "double",
               "required": false
             },
             {
               "id": 2,
               "name": "strings",
               "type": "string",
               "required": false
             },
             {
               "id": 3,
               "name": "my_ints",
               "type": "int",
               "required": false
             },
             {
               "id": 4,
               "doc": "",
               "name": "my_new_column",
               "type": "int",
               "required": false
             }
           ],
           "schema-id": 1
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_floats",
                 "type": "double",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_floats",
                 "type": "double",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 4,
                 "doc": "",
                 "name": "my_new_column",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 1
           }
         ],
         "location": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1864",
               "total-data-files": "2",
               "total-files-size": "1864",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 4343511809769803432,
             "timestamp-ms": 1731924324130,
             "manifest-list": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/snap-4343511809769803432-1-e0cf8cb5-b1ef-4d17-b7de-3235aac0ff03.avro"
           },
           {
             "summary": {
               "operation": "append",
               "added-records": "1",
               "total-records": "3",
               "added-data-files": "1",
               "added-files-size": "1228",
               "total-data-files": "3",
               "total-files-size": "3092",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 1,
             "snapshot-id": 397199606314282997,
             "timestamp-ms": 1731924324242,
             "manifest-list": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/snap-397199606314282997-1-e602bb67-71cb-4359-ac1f-20ba75c157bc.avro",
             "parent-snapshot-id": 4343511809769803432
           }
         ],
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-eeda-7410-95c3-76d15984f743",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924324058,
             "metadata-file": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/01933ebb-eeda-7410-95c3-76eafaa6fd99.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924324130,
             "metadata-file": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/01933ebb-ef2d-7d63-b374-d7e02ab4fdd7.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924324130,
             "metadata-file": "s3://tests/01933ebb-eed3-7310-b70f-18ab74f8566a/01933ebb-eeda-7410-95c3-76d15984f743/metadata/01933ebb-ef46-7523-b8b9-7c31c88fc9dc.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 4343511809769803432,
             "timestamp-ms": 1731924324130
           },
           {
             "snapshot-id": 397199606314282997,
             "timestamp-ms": 1731924324242
           }
         ],
         "format-version": 1,
         "last-column-id": 4,
         "partition-spec": [
           {
             "name": "my_ints",
             "field-id": 1000,
             "source-id": 3,
             "transform": "identity"
           }
         ],
         "default-spec-id": 0,
         "last-updated-ms": 1731924324242,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 3,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 1,
         "last-partition-id": 1000,
         "current-snapshot-id": 397199606314282997,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:24.058666+00', '2024-11-18 10:05:24.253683+00'),
       ('01933ebb-8467-7260-80e3-7a203167b21e', '{
         "refs": {
           "main": {
             "type": "branch",
             "snapshot-id": 8398355224264403441
           }
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "my_floats",
                 "type": "double",
                 "required": false
               }
             ],
             "schema-id": 1
           },
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "total-records": "0",
               "trino_query_id": "20241118_100456_00015_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "total-data-files": "0",
               "total-files-size": "0",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "0"
             },
             "schema-id": 0,
             "snapshot-id": 9038017218095984104,
             "timestamp-ms": 1731924296825,
             "manifest-list": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/snap-9038017218095984104-1-3bfe0406-251d-449d-8bec-0598fd228aa0.avro",
             "sequence-number": 1
           },
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "trino_query_id": "20241118_100456_00016_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "added-data-files": "1",
               "added-files-size": "484",
               "total-data-files": "1",
               "total-files-size": "484",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 6568015682963263770,
             "timestamp-ms": 1731924296952,
             "manifest-list": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/snap-6568015682963263770-1-a6a402c4-5e47-4d71-a8f0-460b2cd75069.avro",
             "sequence-number": 2,
             "parent-snapshot-id": 9038017218095984104
           },
           {
             "summary": {
               "operation": "append",
               "total-records": "0",
               "trino_query_id": "20241118_100457_00017_ydbzw",
               "iceberg-version": "Apache Iceberg 1.6.1 (commit 8e9d59d299be42b0bca9461457cd1e95dbaad086)",
               "total-data-files": "0",
               "total-files-size": "0",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "0"
             },
             "schema-id": 1,
             "snapshot-id": 8398355224264403441,
             "timestamp-ms": 1731924297029,
             "manifest-list": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/snap-8398355224264403441-1-319aff8b-8ecd-4ac8-9564-04ed7bc29408.avro",
             "sequence-number": 3
           }
         ],
         "properties": {
           "write.format.default": "PARQUET",
           "write.parquet.compression-codec": "zstd"
         },
         "table-uuid": "01933ebb-8467-7260-80e3-7a203167b21e",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924296825,
             "metadata-file": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/01933ebb-8481-75e0-8634-12cc85a63b7a.gz.metadata.json"
           },
           {
             "timestamp-ms": 1731924296952,
             "metadata-file": "s3://tests/01933ebb-8459-78a2-ae42-b41dca360cd5/my_table-6c168c6f9d69414fa404e9d59f392bb5/metadata/01933ebb-8507-7d43-b043-f6a9ef1c1bc3.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 8398355224264403441,
             "timestamp-ms": 1731924297029
           }
         ],
         "format-version": 2,
         "last-column-id": 3,
         "default-spec-id": 0,
         "last-updated-ms": 1731924297029,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 1,
         "last-partition-id": 999,
         "current-snapshot-id": 8398355224264403441,
         "last-sequence-number": 3,
         "default-sort-order-id": 0
       }', '2024-11-18 10:04:56.807047+00', '2024-11-18 10:04:57.042743+00'),
       ('01933ebb-9249-75b2-9dc1-6370dbb6cda7', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_ints",
               "type": "int",
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
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-9227-7f32-a10f-f34cb33d7d17/01933ebb-9249-75b2-9dc1-6370dbb6cda7",
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-9249-75b2-9dc1-6370dbb6cda7",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "format-version": 1,
         "last-column-id": 3,
         "partition-spec": [],
         "default-spec-id": 0,
         "last-updated-ms": 1731924300362,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": -1,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:00.361788+00', NULL),
       ('01933ebb-edbb-7b40-b51c-700aec28733f', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_ints",
               "type": "int",
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
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_ints",
                 "type": "int",
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
         "location": "s3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2/01933ebb-edbb-7b40-b51c-700aec28733f",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "1",
               "added-files-size": "949",
               "total-data-files": "1",
               "total-files-size": "949",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "1"
             },
             "schema-id": 0,
             "snapshot-id": 3188368482794192079,
             "timestamp-ms": 1731924323847,
             "manifest-list": "s3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2/01933ebb-edbb-7b40-b51c-700aec28733f/metadata/snap-3188368482794192079-1-521bc81b-2031-4ed2-90b3-e5b4443499d8.avro"
           }
         ],
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-edbb-7b40-b51c-700aec28733f",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924323771,
             "metadata-file": "s3://tests/01933ebb-edab-7ea1-ad96-efada9261ae2/01933ebb-edbb-7b40-b51c-700aec28733f/metadata/01933ebb-edbb-7b40-b51c-70171f8c6a9b.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 3188368482794192079,
             "timestamp-ms": 1731924323847
           }
         ],
         "format-version": 1,
         "last-column-id": 3,
         "partition-spec": [],
         "default-spec-id": 0,
         "last-updated-ms": 1731924323847,
         "partition-specs": [
           {
             "fields": [],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 999,
         "current-snapshot-id": 3188368482794192079,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:23.770942+00', '2024-11-18 10:05:23.861698+00'),
       ('01933ebb-ee44-74a1-967b-f81fff985352', '{
         "schema": {
           "type": "struct",
           "fields": [
             {
               "id": 1,
               "name": "my_floats",
               "type": "double",
               "required": false
             },
             {
               "id": 2,
               "name": "strings",
               "type": "string",
               "required": false
             },
             {
               "id": 3,
               "name": "my_ints",
               "type": "int",
               "required": false
             }
           ],
           "schema-id": 0
         },
         "schemas": [
           {
             "type": "struct",
             "fields": [
               {
                 "id": 1,
                 "name": "my_floats",
                 "type": "double",
                 "required": false
               },
               {
                 "id": 2,
                 "name": "strings",
                 "type": "string",
                 "required": false
               },
               {
                 "id": 3,
                 "name": "my_ints",
                 "type": "int",
                 "required": false
               }
             ],
             "schema-id": 0
           }
         ],
         "location": "s3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b/01933ebb-ee44-74a1-967b-f81fff985352",
         "snapshots": [
           {
             "summary": {
               "operation": "append",
               "added-records": "2",
               "total-records": "2",
               "added-data-files": "2",
               "added-files-size": "1864",
               "total-data-files": "2",
               "total-files-size": "1864",
               "total-delete-files": "0",
               "total-equality-deletes": "0",
               "total-position-deletes": "0",
               "changed-partition-count": "2"
             },
             "schema-id": 0,
             "snapshot-id": 6707785916006581295,
             "timestamp-ms": 1731924323988,
             "manifest-list": "s3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b/01933ebb-ee44-74a1-967b-f81fff985352/metadata/snap-6707785916006581295-1-7b0f73de-ef9f-4229-b1d4-36fa0350732d.avro"
           }
         ],
         "properties": {
           "write.format.default": "parquet",
           "write.parquet.compression-codec": "gzip"
         },
         "table-uuid": "01933ebb-ee44-74a1-967b-f81fff985352",
         "sort-orders": [
           {
             "fields": [],
             "order-id": 0
           }
         ],
         "metadata-log": [
           {
             "timestamp-ms": 1731924323908,
             "metadata-file": "s3://tests/01933ebb-ee3b-72c2-b3a9-75e92f5d1f2b/01933ebb-ee44-74a1-967b-f81fff985352/metadata/01933ebb-ee44-74a1-967b-f82b6325d970.gz.metadata.json"
           }
         ],
         "snapshot-log": [
           {
             "snapshot-id": 6707785916006581295,
             "timestamp-ms": 1731924323988
           }
         ],
         "format-version": 1,
         "last-column-id": 3,
         "partition-spec": [
           {
             "name": "my_ints",
             "field-id": 1000,
             "source-id": 3,
             "transform": "identity"
           }
         ],
         "default-spec-id": 0,
         "last-updated-ms": 1731924323988,
         "partition-specs": [
           {
             "fields": [
               {
                 "name": "my_ints",
                 "field-id": 1000,
                 "source-id": 3,
                 "transform": "identity"
               }
             ],
             "spec-id": 0
           }
         ],
         "current-schema-id": 0,
         "last-partition-id": 1000,
         "current-snapshot-id": 6707785916006581295,
         "default-sort-order-id": 0
       }', '2024-11-18 10:05:23.908613+00', '2024-11-18 10:05:23.997999+00');

INSERT INTO public.view (view_id, view_format_version, created_at, updated_at)
VALUES ('01933eb6-c946-7122-bb89-62cb91175c86', 'v1', '2024-11-18 09:59:46.75799+00', NULL),
       ('01933eb6-c9a8-7250-af28-c011bdf03031', 'v1', '2024-11-18 09:59:46.941387+00', NULL),
       ('01933eb6-ca66-7a91-922a-8625500666e1', 'v1', '2024-11-18 09:59:47.046125+00', NULL),
       ('01933eb6-ccd0-7603-8c0d-d22e44da9c9f', 'v1', '2024-11-18 09:59:47.664422+00', NULL),
       ('01933eb7-4770-72b1-b4a6-369a0eae67f2', 'v1', '2024-11-18 10:00:19.055972+00', NULL),
       ('01933eb7-47ce-7880-a2b8-613531e5315a', 'v1', '2024-11-18 10:00:19.232096+00', NULL),
       ('01933eb7-4884-7c91-9006-f616216f92ba', 'v1', '2024-11-18 10:00:19.332056+00', NULL),
       ('01933eb7-4ac0-7d13-aa02-77472e809d63', 'v1', '2024-11-18 10:00:19.904347+00', NULL),
       ('01933eb8-1d7e-7a52-9a4f-7267d1f16f7a', 'v1', '2024-11-18 10:01:13.854162+00', NULL),
       ('01933eb8-2725-72f0-a1f7-52bc3286dbaa', 'v1', '2024-11-18 10:01:17.719835+00', NULL),
       ('01933eb8-35dc-7371-a7f8-bc6612db859e', 'v1', '2024-11-18 10:01:20.09161+00', NULL),
       ('01933eb8-4c09-7ae0-ba19-5a54ce4307a5', 'v1', '2024-11-18 10:01:25.769196+00', NULL);


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