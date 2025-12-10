# Initial_Load_Dynamic.py
from pyspark.sql.functions import current_timestamp, lit

# 1. Get Parameters
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("pipeline_run_id", "", "Run ID")

schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
run_id = dbutils.widgets.get("pipeline_run_id")

# 2. Paths
# Note: Source system code is currently hardcoded to SRC-001 in logic for simplicity,
# in full prod fetch from control DB
source_path = f"/mnt/datalake/bronze/SRC-001/{schema_name}/{table_name}/initial_load/*/*.parquet"
delta_path = f"/mnt/datalake/delta/{schema_name}/{table_name}"

# 3. Read Parquet
print(f"Reading from {source_path}")
try:
    df = spark.read.parquet(source_path)
except Exception as e:
    print("No file found, pipeline likely failed at copy step.")
    raise e

# 4. Add Metadata
final_df = df.withColumn("_load_date", current_timestamp()) \
             .withColumn("_run_id", lit(run_id))

# 5. Write to Delta (Overwrite for Initial)
print(f"Writing to {delta_path}")
final_df.write.format("delta").mode("overwrite").save(delta_path)

# 6. Update Control Table (Spark JDBC)
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

# Execute SP via JDBC wrapper
update_query = f"""
EXEC control.sp_UpdateTableMetadata
    @SchemaName = '{schema_name}',
    @TableName = '{table_name}',
    @Status = 'Success',
    @RowsProcessed = {df.count()},
    @PipelineRunId = '{run_id}',
    @PipelineName = 'Initial_Load_Dynamic'
"""

spark.sql("SELECT 1 as dummy").write \
    .mode("append") \
    .jdbc(jdbc_url, f"({update_query}) as q", properties=conn_props)

print("Control table updated.")
