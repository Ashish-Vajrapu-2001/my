# Validate_Dependencies.py
# Checks if control tables and external resources are accessible

def check_jdbc():
    try:
        server_name = "adf-databricks.database.windows.net"
        database_name = "ADF-Databricks"
        jdbc_url = f"jdbc:sqlserver://{server_name}:1433;database={database_name}"
        user = dbutils.secrets.get(scope="azure-key-vault", key="sql-username")
        password = dbutils.secrets.get(scope="azure-key-vault", key="sql-password")

        conn_props = {
            "user": user, "password": password,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

        df = spark.read.jdbc(jdbc_url, "(SELECT count(*) as cnt FROM control.table_metadata) as q", properties=conn_props)
        count = df.first()['cnt']
        print(f"✅ SQL Connection Successful. Table Metadata Count: {count}")
    except Exception as e:
        print(f"❌ SQL Connection Failed: {str(e)}")

def check_mount():
    try:
        dbutils.fs.ls("/mnt/datalake")
        print("✅ ADLS Mount Successful")
    except Exception as e:
        print(f"❌ ADLS Mount Failed: {str(e)}")

check_mount()
check_jdbc()
