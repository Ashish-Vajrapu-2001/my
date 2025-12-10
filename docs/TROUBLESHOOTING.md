# Troubleshooting Guide

## Duplicate Columns in Parquet/Delta
- **Cause**: `sp_GetCDCChanges` selecting `CT.*` and `T.*` blindly.
- **Fix**: The generated SP explicitly selects `CT.SYS_CHANGE_OPERATION`, `CT.SYS_CHANGE_VERSION` and `T.*` to avoid duplication. Ensure you deployed the provided SP.

## "Primary Key not found" Error
- **Cause**: Control table metadata does not match actual Parquet file columns.
- **Fix**: Check `control.table_metadata` entry for the failing table. Ensure `primary_key_columns` matches the source schema exactly (case-sensitive).

## Databricks JDBC Error (Java)
- **Cause**: Using `DriverManager` instead of Spark JDBC.
- **Fix**: Ensure notebooks use `spark.read.jdbc` or `dataframe.write.jdbc` patterns provided in the code.

## Pipeline fails at "If_Rows_Found"
- **Cause**: No changes in source, but pipeline tried to run notebook.
- **Fix**: The pipeline checks `rowsCopied > 0`. If 0, it skips. If it fails, check the expression logic in ADF.
