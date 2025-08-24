# Table Maintenance

## Metadata File Cleanup
Lakekeeper honors the `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max` Iceberg table properties. By default, `delete-after-commit` is enabled in Lakekeeper version 0.10.0 and newer (it was disabled in earlier versions). When a table commit occurs, Lakekeeper automatically removes excess Table Metadata files if the total count would exceed the `write.metadata.previous-versions-max` limit (default: 100).
