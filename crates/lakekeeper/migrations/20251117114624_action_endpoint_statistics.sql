ALTER TYPE api_endpoints RENAME value 'management-v1-get-default-project' TO 'management-v1-get-project';

ALTER TYPE api_endpoints RENAME value 'management-v1-delete-default-project' TO 'management-v1-delete-project';

ALTER TYPE api_endpoints RENAME value 'management-v1-rename-default-project' TO 'management-v1-rename-project';

ALTER TYPE api_endpoints RENAME value 'management-v1-rename-project-by-id' TO 'management-v1-rename-project-by-id-deprecated';

ALTER TYPE api_endpoints RENAME value 'management-v1-delete-project-by-id' TO 'management-v1-delete-project-by-id-deprecated';

ALTER TYPE api_endpoints RENAME value 'management-v1-get-default-project-by-id' TO 'management-v1-get-project-by-id-deprecated';

ALTER TYPE api_endpoints ADD value 'management-v1-get-server-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-user-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-role-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-warehouse-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-project-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-namespace-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-table-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-get-view-actions';

ALTER TYPE api_endpoints ADD value 'management-v1-batch-check-actions';