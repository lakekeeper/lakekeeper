-- Role-assignment management API: POST/GET/DELETE /management/v1/role-assignments

-- New management endpoints (tracked for endpoint statistics).
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-create-role-assignment';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-list-role-assignments';
ALTER TYPE api_endpoints ADD VALUE IF NOT EXISTS 'management-v1-delete-role-assignment';
