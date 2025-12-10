# Setup_Secrets.py
# Run this once to configure secret scope backed by Key Vault

# Instructions for setting up Azure Key Vault backed secret scope
print("""
To configure Databricks secrets with Azure Key Vault:

1. Get your Key Vault URI and Resource ID from Azure Portal
   KV URI: https://ADF-Databricks1.vault.azure.net/

2. Create the secret scope (run in Databricks CLI or specific URL):
   https://<databricks-instance>#secrets/createScope

   Scope Name: azure-key-vault
   DNS Name: https://ADF-Databricks1.vault.azure.net/
   Resource ID: /subscriptions/1b199c7b-15bb-4533-b7f3-f157001f1520/resourceGroups/ADF-Databricks/providers/Microsoft.KeyVault/vaults/ADF-Databricks1

3. Verify secrets are accessible:
""")

# Test secret access
try:
    test_secret = dbutils.secrets.get(scope="azure-key-vault", key="sql-username")
    print("✅ Secret scope configured correctly!")
except:
    print("❌ Secret scope not configured. Follow instructions above.")
