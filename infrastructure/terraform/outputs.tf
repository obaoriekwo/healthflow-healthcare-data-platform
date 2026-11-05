output "resource_group" {
  value = azurerm_resource_group.main.name
}

output "storage_account" {
  description = "ADLS Gen2 account holding the Bronze layer."
  value       = azurerm_storage_account.lake.name
}

output "warehouse_host" {
  value = azurerm_postgresql_flexible_server.warehouse.fqdn
}

output "key_vault" {
  value = azurerm_key_vault.main.name
}

output "load_settings" {
  description = "Paste into your shell to point HealthFlow at Azure. Secrets are read from Key Vault, never printed by Terraform."
  value       = <<-EOT
    export STORAGE_BACKEND=azure
    export AZURE_STORAGE_CONTAINER=${azurerm_storage_container.bronze.name}
    export AZURE_STORAGE_CONNECTION_STRING="$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name storage-connection-string --query value -o tsv)"
    export WAREHOUSE_DSN="$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name warehouse-dsn --query value -o tsv)"
    export WAREHOUSE_HOST=${azurerm_postgresql_flexible_server.warehouse.fqdn} WAREHOUSE_PORT=5432 WAREHOUSE_SSLMODE=require
    export WAREHOUSE_USER=${azurerm_postgresql_flexible_server.warehouse.administrator_login} WAREHOUSE_DB=${azurerm_postgresql_flexible_server_database.warehouse.name}
    export WAREHOUSE_PASSWORD="$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name warehouse-password --query value -o tsv)"
  EOT
}
