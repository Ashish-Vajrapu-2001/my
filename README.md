# Metadata-Driven CDC Pipeline

A complete, production-ready solution for ingesting data from Azure SQL to ADLS Gen2/Delta Lake using Azure Data Factory and Databricks.

## Architecture
[SQL Source] --(CDC)--> [ADF Copy] --> [ADLS Gen2 (Parquet)] --> [Databricks (Delta)]

## Key Features
- **Metadata Driven**: Add tables via SQL inserts, no pipeline changes.
- **Secure**: All credentials in Azure Key Vault.
- **Resilient**: Handles Schema Drift and Load Dependencies.
- **CDC Optimized**: Uses SQL Server Change Tracking.

## Quick Start
1. Configure Azure Key Vault secrets.
2. Deploy SQL scripts.
3. Import ADF code.
4. Run `PL_Master_Orchestrator`.

See `docs/DEPLOYMENT.md` for full details.
