# Table Maintenance

## Metadata File Cleanup
Lakekeeper honors the Iceberg table properties `write.metadata.delete-after-commit.enabled` and `write.metadata.previous-versions-max`. Starting with Lakekeeper v0.10.0, `delete-after-commit` is enabled by default (it was disabled in earlier versions). On each table commit, when `delete-after-commit` is enabled, Lakekeeper keeps the current table metadata file plus up to `write.metadata.previous-versions-max` previous metadata files (default: 100) and deletes the oldest tracked metadata file from the metadata log once that limit is exceeded. This cleanup applies only to metadata files tracked in the metadata log; it does not remove orphaned metadata files.

For example: if `write.metadata.previous-versions-max=20`, Lakekeeper retains 21 files in total (the current plus 20 previous); committing a 22nd version deletes the oldest tracked metadata file.
