# Mount_ADLS.py
# Mounts Azure Data Lake Storage Gen2 to /mnt/datalake

# Get credentials from Databricks secrets (backed by Key Vault)
storage_account_name = "dbstorage5hbkzkgcon35m"
client_id = dbutils.secrets.get(scope="azure-key-vault", key="sp-client-id")
client_secret = dbutils.secrets.get(scope="azure-key-vault", key="sp-client-secret")
tenant_id = dbutils.secrets.get(scope="azure-key-vault", key="sp-tenant-id")

# Configuration
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Mount point
mount_point = "/mnt/datalake"
source = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net/"

# Check if already mounted
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    print(f"Already mounted at {mount_point}")
else:
    dbutils.fs.mount(
        source=source,
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted {source} to {mount_point}")

# Verify
display(dbutils.fs.ls(mount_point))
