# Deployment Guide

## 1. Prerequisites
- **Azure Resources**: Ensure the following exist (from extracted JSON):
  - Storage: `dbstorage5hbkzkgcon35m`
  - ADF: `ADF-Databricks456`
  - Databricks: `Adf-databricks456`
  - Key Vault: `ADF-Databricks1`
  - SQL: `adf-databricks`
- **Service Principal**:
  - ID: `40a941f4-f4f0-4ddd-9e42-7a816b76b8f2`
  - Must have `Storage Blob Data Contributor` on Storage.
  - Must have `Get/List` secrets permission on Key Vault.

## 2. Key Vault Configuration
Add the following secrets to `ADF-Databricks1`:
- `storage-account-key`: Access key for `dbstorage5hbkzkgcon35m`
- `sql-username`: SQL Admin user
- `sql-password`: SQL Admin password
- `sp-client-id`: `<YOUR-SERVICE-PRINCIPAL-CLIENT-ID>`
- `sp-client-secret`: `<YOUR-SERVICE-PRINCIPAL-CLIENT-SECRET>`
- `sp-tenant-id`: `<YOUR-TENANT-ID>`
- `databricks-token`: PAT Token from Databricks User Settings

## 3. SQL Setup
1. Open SSMS or Azure Query Editor.
2. Run `sql/control_tables/01_create_control_tables.sql`.
3. Run `sql/control_tables/02_populate_control_tables.sql`.
4. Run all stored procedures in `sql/stored_procedures/`.
5. Enable Change Tracking on source tables (if not already done).

## 4. Databricks Setup
1. Create Secret Scope `azure-key-vault` linked to `ADF-Databricks1`.
2. Import notebooks from `databricks/notebooks`.
3. Run `utilities/Setup_Secrets.py` to verify scope.
4. Run `setup/Mount_ADLS.py` to mount storage.

## 5. ADF Deployment
1. Import Linked Services (LS_KeyVault first).
2. Update `LS_AzureDatabricks` with the specific workspace URL.
3. Import Datasets.
4. Import Pipelines.
5. Publish All.

## 6. Execution
Trigger `PL_Master_Orchestrator`. It will:
1. Detect initial load (first run).
2. Perform full copy + Delta write.
3. Subsequent runs will use CDC.
