-- Add warehouse-level flags to control STS and Remote Signing
-- Issue #1498: Add Warehouse option to disable STS & Remote Signing on Warehouse level

-- Add sts_enabled flag (defaults to true for backward compatibility)
ALTER TABLE "warehouse" ADD COLUMN "sts_enabled" BOOLEAN NOT NULL DEFAULT true;

-- Add remote_signing_enabled flag (defaults to true for backward compatibility)
ALTER TABLE "warehouse" ADD COLUMN "remote_signing_enabled" BOOLEAN NOT NULL DEFAULT true;

-- Add comment to document the purpose
COMMENT ON COLUMN "warehouse"."sts_enabled" IS 'When false, disables all forms of vended credentials (STS for S3, SAS tokens for ADLS, downscoped tokens for GCS)';
COMMENT ON COLUMN "warehouse"."remote_signing_enabled" IS 'When false, disables remote signing for this warehouse';
