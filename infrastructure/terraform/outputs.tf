output "resource_group" {
  value = azurerm_resource_group.main.name
}

output "storage_account" {
  description = "ADLS Gen2 account holding the Bronze layer."
  value       = azurerm_storage_account.lake.name
}

output "warehouse_host" {
  value = var.deploy_postgres ? azurerm_postgresql_flexible_server.warehouse[0].fqdn : null
}

output "key_vault" {
  value = azurerm_key_vault.main.name
}

locals {
  storage_settings = <<-EOT
    export STORAGE_BACKEND=azure
    export AZURE_STORAGE_CONTAINER=${azurerm_storage_container.bronze.name}
    export AZURE_STORAGE_CONNECTION_STRING="$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name storage-connection-string --query value -o tsv)"
  EOT

  warehouse_settings = var.deploy_postgres ? join("\n", [
    "export WAREHOUSE_DSN=\"$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name warehouse-dsn --query value -o tsv)\"",
    "export WAREHOUSE_HOST=${azurerm_postgresql_flexible_server.warehouse[0].fqdn} WAREHOUSE_PORT=5432 WAREHOUSE_SSLMODE=require",
    "export WAREHOUSE_USER=${azurerm_postgresql_flexible_server.warehouse[0].administrator_login} WAREHOUSE_DB=${azurerm_postgresql_flexible_server_database.warehouse[0].name}",
    "export WAREHOUSE_PASSWORD=\"$(az keyvault secret show --vault-name ${azurerm_key_vault.main.name} --name warehouse-password --query value -o tsv)\"",
  ]) : "# deploy_postgres = false: keep your existing WAREHOUSE_* settings (the warehouse stays outside Azure)"
}

output "load_settings" {
  description = "Run where the az CLI is signed in; it prints export lines with the secrets read from Key Vault."
  value       = "${local.storage_settings}${local.warehouse_settings}\n"
}
