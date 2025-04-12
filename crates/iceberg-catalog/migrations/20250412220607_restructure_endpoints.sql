TRUNCATE TABLE endpoint_statistics;
ALTER TYPE api_endpoints
RENAME value 'catalog-post-aws-s3-sign' TO 'sign-s3-request-global';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-prefix-aws-s3-sign' TO 'sign-s3-request-prefix';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-config' TO 'catalog-v1-get-config';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespaces' TO 'catalog-v1-list-namespaces';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespaces' TO 'catalog-v1-create-namespace';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace' TO 'catalog-v1-load-namespace-metadata';
ALTER TYPE api_endpoints
RENAME value 'catalog-head-namespace' TO 'catalog-v1-namespace-exists';
ALTER TYPE api_endpoints
RENAME value 'catalog-delete-namespace' TO 'catalog-v1-drop-namespace';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-properties' TO 'catalog-v1-update-namespace-properties';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace-tables' TO 'catalog-v1-list-tables';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-tables' TO 'catalog-v1-create-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace-table' TO 'catalog-v1-load-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-table' TO 'catalog-v1-update-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-delete-namespace-table' TO 'catalog-v1-drop-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-head-namespace-table' TO 'catalog-v1-table-exists';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace-table-credentials' TO 'catalog-v1-load-credentials';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-tables-rename' TO 'catalog-v1-rename-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-register' TO 'catalog-v1-register-table';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-table-metrics' TO 'catalog-v1-report-metrics';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-transactions-commit' TO 'catalog-v1-commit-transaction';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-views' TO 'catalog-v1-create-view';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace-views' TO 'catalog-v1-list-views';
ALTER TYPE api_endpoints
RENAME value 'catalog-get-namespace-view' TO 'catalog-v1-load-view';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace-view' TO 'catalog-v1-replace-view';
ALTER TYPE api_endpoints
RENAME value 'catalog-delete-namespace-view' TO 'catalog-v1-drop-view';
ALTER TYPE api_endpoints
RENAME value 'catalog-head-namespace-view' TO 'catalog-v1-view-exists';
ALTER TYPE api_endpoints
RENAME value 'catalog-post-views-rename' TO 'catalog-v1-rename-view';
ALTER TYPE api_endpoints
RENAME value 'management-get-info' TO 'management-v1-server-info';
ALTER TYPE api_endpoints
RENAME value 'management-post-bootstrap' TO 'management-v1-bootstrap';
ALTER TYPE api_endpoints
RENAME value 'management-post-role' TO 'management-v1-create-role';
ALTER TYPE api_endpoints
RENAME value 'management-get-role' TO 'management-v1-list-role';
ALTER TYPE api_endpoints
RENAME value 'management-post-role-id' TO 'management-v1-update-role';
ALTER TYPE api_endpoints
RENAME value 'management-get-role-id' TO 'management-v1-get-role';
ALTER TYPE api_endpoints
RENAME value 'management-delete-role-id' TO 'management-v1-delete-role';
ALTER TYPE api_endpoints
RENAME value 'management-post-search-role' TO 'management-v1-search-role';
ALTER TYPE api_endpoints
RENAME value 'management-get-whoami' TO 'management-v1-whoami';
ALTER TYPE api_endpoints
RENAME value 'management-post-search-user' TO 'management-v1-search-user';
ALTER TYPE api_endpoints
RENAME value 'management-post-user-id' TO 'management-v1-update-user';
ALTER TYPE api_endpoints
RENAME value 'management-get-user-id' TO 'management-v1-get-user';
ALTER TYPE api_endpoints
RENAME value 'management-delete-user-id' TO 'management-v1-delete-user';
ALTER TYPE api_endpoints
RENAME value 'management-post-user' TO 'management-v1-create-user';
ALTER TYPE api_endpoints
RENAME value 'management-get-user' TO 'management-v1-list-user';
ALTER TYPE api_endpoints
RENAME value 'management-post-project' TO 'management-v1-create-project';
ALTER TYPE api_endpoints
RENAME value 'management-get-default-project' TO 'management-v1-get-default-project';
ALTER TYPE api_endpoints
RENAME value 'management-delete-default-project' TO 'management-v1-delete-default-project';
ALTER TYPE api_endpoints
RENAME value 'management-post-rename-project' TO 'management-v1-rename-default-project';
ALTER TYPE api_endpoints
RENAME value 'management-get-project-id' TO 'management-v1-get-default-project-by-id';
ALTER TYPE api_endpoints
RENAME value 'management-get-endpoint-statistics' TO 'management-v1-load-endpoint-statistics';
ALTER TYPE api_endpoints
RENAME value 'management-delete-project-id' TO 'management-v1-delete-project-by-id';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse' TO 'management-v1-create-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-get-warehouse' TO 'management-v1-list-warehouses';
ALTER TYPE api_endpoints
RENAME value 'management-get-project-list' TO 'management-v1-list-projects';
ALTER TYPE api_endpoints
RENAME value 'management-get-warehouse-id' TO 'management-v1-get-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-delete-warehouse-id' TO 'management-v1-delete-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-rename' TO 'management-v1-rename-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-deactivate' TO 'management-v1-deactivate-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-activate' TO 'management-v1-activate-warehouse';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-storage' TO 'management-v1-update-storage-profile';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-storage-credential' TO 'management-v1-update-storage-credential';
ALTER TYPE api_endpoints
RENAME value 'management-get-warehouse-statistics' TO 'management-v1-get-warehouse-statistics';
ALTER TYPE api_endpoints
RENAME value 'management-get-warehouse-deleted-tabulars' TO 'management-v1-list-deleted-tabulars';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-deleted-tabulars-undrop1' TO 'management-v1-undrop-tabulars-deprecated';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-deleted-tabulars-undrop2' TO 'management-v1-undrop-tabulars';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-delete-profile' TO 'management-v1-update-warehouse-delete-profile';
ALTER TYPE api_endpoints
RENAME value 'management-get-permissions' TO 'permission-v1-get';
ALTER TYPE api_endpoints
RENAME value 'management-post-permissions' TO 'permission-v1-post';
ALTER TYPE api_endpoints
RENAME value 'management-head-permissions' TO 'permission-v1-head';
ALTER TYPE api_endpoints
RENAME value 'management-delete-permissions' TO 'permission-v1-delete';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-protection' TO 'management-v1-set-warehouse-protection';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-namespace-protection' TO 'management-v1-set-namespace-protection';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-table-protection' TO 'management-v1-set-table-protection';
ALTER TYPE api_endpoints
RENAME value 'management-post-warehouse-view-protection' TO 'management-v1-set-view-protection';
-- This endpoint never existed. Re-using it.
ALTER TYPE api_endpoints
RENAME value 'catalog-post-namespace' TO 'catalog-v1-fetch-scan-tasks';
-- Add new endpoints
ALTER TYPE api_endpoints
ADD value 'catalog-v1-cancel-planning';
ALTER TYPE api_endpoints
ADD value 'catalog-v1-fetch-planning-result';
ALTER TYPE api_endpoints
ADD value 'catalog-v1-plan-table-scan';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-get-view-protection';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-get-table-protection';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-get-namespace-protection';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-rename-default-project-deprecated';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-get-default-project-deprecated';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-delete-default-project-deprecated';
ALTER TYPE api_endpoints
ADD VALUE 'permission-v1-put';
ALTER TYPE api_endpoints
ADD VALUE 'management-v1-rename-project-by-id';