# Examples

This directory contains examples of how to run Lakekeeper in different configurations.

## Available Examples

- [**minimal**](./minimal/): Basic setup with minimal configuration
- [**access-control-simple**](./access-control-simple/): Simple access control with Keycloak
- [**access-control-advanced**](./access-control-advanced/): Advanced access control with OpenFGA and Trino

## Statistics Retention

All examples now include automatic statistics retention to prevent unbounded growth of statistics data. The retention policies can be configured through environment variables:

- `LAKEKEEPER__ENDPOINT_STAT_MAX_ENTRIES`: Maximum endpoint statistics entries per combination (default: 1000)
- `LAKEKEEPER__ENDPOINT_STAT_MAX_AGE`: Maximum age for endpoint statistics (default: 7d)
- `LAKEKEEPER__WAREHOUSE_STAT_MAX_ENTRIES`: Maximum warehouse statistics entries per warehouse (default: 1000)
- `LAKEKEEPER__WAREHOUSE_STAT_MAX_AGE`: Maximum age for warehouse statistics (default: 7d)
- `LAKEKEEPER__STAT_RETENTION_CLEANUP_INTERVAL`: How often to run cleanup (default: 1h)

Set any max_entries to 0 to disable count-based retention, or max_age to 0 to disable time-based retention.

## Getting Started

Each example includes:
- A `docker-compose.yaml` file for easy setup
- A `README.md` with specific instructions
- Jupyter notebooks demonstrating usage
- Configuration files for all components

Choose the example that best matches your use case and follow the instructions in its README.

