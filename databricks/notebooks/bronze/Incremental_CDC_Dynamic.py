# Incremental_CDC_Dynamic.py
from pyspark.sql.functions import col, expr
from delta.tables import DeltaTable

# 1. Get Parameters
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("pipeline_run_id", "", "Run ID")
dbutils.widgets.text("rows_copied", "0", "Rows Copied")

schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
run_id = dbutils.widgets.get("pipeline_run_id")
rows_copied = int(dbutils.widgets.get("rows_copied"))

if rows_copied == 0:
    dbutils.notebook.exit("No rows to process")

# 2. Database Connection for Metadata
server_name = "adf-databricks.database.windows.net"
database_name = "ADF-Databricks"
jdbc_url = f"jdbc:sqlserver://{server_name}:1433;database={database_name}"
user = dbutils.secrets.get(scope="azure-key-vault", key="sql-username")
password = dbutils.secrets.get(scope="azure-key-vault", key="sql-password")
conn_props = {
    "user": user,
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# 3. Get Primary Keys
metadata_query = f"""
(SELECT primary_key_columns
 FROM control.table_metadata
 WHERE source_schema = '{schema_name}' AND table_name = '{table_name}') as meta
"""
meta_df = spark.read.jdbc(jdbc_url, metadata_query, properties=conn_props)
pk_string = meta_df.first()["primary_key_columns"]
pk_cols = [x.strip() for x in pk_string.split(",")]

# 4. Read CDC Data
source_path = f"/mnt/datalake/bronze/SRC-001/{schema_name}/{table_name}/incremental/{run_id}/*.parquet"
delta_path = f"/mnt/datalake/delta/{schema_name}/{table_name}"

cdc_df = spark.read.parquet(source_path)

# Validate PKs
for pk in pk_cols:
    if pk not in cdc_df.columns:
        raise ValueError(f"Primary Key {pk} not found in CDC data")

# 5. Merge Logic
delta_table = DeltaTable.forPath(spark, delta_path)

# Build Merge Condition
# "target.ID = source.ID AND target.ID2 = source.ID2"
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

# Perform Merge
# D = Delete, U = Update, I = Insert
delta_table.alias("target").merge(
    cdc_df.alias("source"),
    merge_condition
).whenMatchedDelete(
    condition = "source.SYS_CHANGE_OPERATION = 'D'"
).whenMatchedUpdateAll(
    condition = "source.SYS_CHANGE_OPERATION != 'D'"
).whenNotMatchedInsertAll(
    condition = "source.SYS_CHANGE_OPERATION != 'D'"
).execute()

# 6. Update Control Table
update_query = f"""
EXEC control.sp_UpdateTableMetadata
    @SchemaName = '{schema_name}',
    @TableName = '{table_name}',
    @Status = 'Success',
    @RowsProcessed = {rows_copied},
    @PipelineRunId = '{run_id}',
    @PipelineName = 'Incremental_CDC_Dynamic'
"""

spark.sql("SELECT 1 as dummy").write \
    .mode("append") \
    .jdbc(jdbc_url, f"({update_query}) as q", properties=conn_props)

print(f"Merged {rows_copied} rows successfully.")
