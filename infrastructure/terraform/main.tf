# HealthFlow on Azure: the smallest deployment that proves the platform runs on real Azure.
#
#   Resource group
#   ├── Storage account, hierarchical namespace on (ADLS Gen2), container "healthflow-data"  ← Bronze
#   ├── PostgreSQL Flexible Server 16, Burstable B1ms, database healthflow_warehouse         ← warehouse
#   ├── Key Vault: warehouse DSN and storage connection string                              ← secrets
#   └── Budget with email alerts                                                            ← cost control
#
# Deliberately left out of the first deployment (see docs/azure_deployment.md): a VM for
# Airflow, a container registry, private networking. Airflow keeps running in GitHub
# Actions / Codespaces and talks to these services over TLS.

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  upper   = false
  special = false
}

resource "random_password" "warehouse" {
  length           = 32
  special          = true
  override_special = "-_.~" # URL-safe, so the DSN needs no escaping
}

locals {
  name    = "${var.project}-${random_string.suffix.result}"
  compact = "${var.project}${random_string.suffix.result}" # storage and vault names allow no hyphens / are short
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = var.tags
}

# --- Bronze: ADLS Gen2 -----------------------------------------------------------------

resource "azurerm_storage_account" "lake" {
  name                            = "st${local.compact}"
  resource_group_name             = azurerm_resource_group.main.name
  location                        = azurerm_resource_group.main.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true # hierarchical namespace = ADLS Gen2
  min_tls_version                 = "TLS1_2"
  https_traffic_only_enabled      = true
  allow_nested_items_to_be_public = false
  tags                            = var.tags
}

resource "azurerm_storage_container" "bronze" {
  name                  = "healthflow-data" # same container name the ingestion code uses locally
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# --- Warehouse: Azure Database for PostgreSQL ---------------------------------------------

resource "azurerm_postgresql_flexible_server" "warehouse" {
  count = var.deploy_postgres ? 1 : 0

  name                          = "psql-${local.name}"
  resource_group_name           = azurerm_resource_group.main.name
  location                      = azurerm_resource_group.main.location
  version                       = "16"
  sku_name                      = "B_Standard_B1ms" # smallest Burstable tier
  storage_mb                    = 32768
  backup_retention_days         = 7
  administrator_login           = "healthflow"
  administrator_password        = random_password.warehouse.result
  public_network_access_enabled = true # locked down by the firewall rule below; TLS is required
  tags                          = var.tags

  lifecycle {
    ignore_changes = [zone] # Azure picks a zone; don't fight it on later applies
  }
}

resource "azurerm_postgresql_flexible_server_database" "warehouse" {
  count = var.deploy_postgres ? 1 : 0

  name      = "healthflow_warehouse"
  server_id = azurerm_postgresql_flexible_server.warehouse[0].id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

resource "azurerm_postgresql_flexible_server_firewall_rule" "you" {
  count = var.deploy_postgres ? 1 : 0

  name             = "allow-deployer"
  server_id        = azurerm_postgresql_flexible_server.warehouse[0].id
  start_ip_address = var.allowed_ip
  end_ip_address   = var.allowed_ip
}

# --- Secrets: Key Vault ---------------------------------------------------------------------

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.compact}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false # demo only: lets `terraform destroy` clean up completely
  tags                       = var.tags

  access_policy {
    tenant_id          = data.azurerm_client_config.current.tenant_id
    object_id          = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
  }
}

resource "azurerm_key_vault_secret" "warehouse_dsn" {
  count = var.deploy_postgres ? 1 : 0

  name         = "warehouse-dsn"
  key_vault_id = azurerm_key_vault.main.id

  value = format(
    "postgresql://%s:%s@%s:5432/%s?sslmode=require",
    azurerm_postgresql_flexible_server.warehouse[0].administrator_login,
    random_password.warehouse.result,
    azurerm_postgresql_flexible_server.warehouse[0].fqdn,
    azurerm_postgresql_flexible_server_database.warehouse[0].name,
  )
}

resource "azurerm_key_vault_secret" "warehouse_password" {
  count = var.deploy_postgres ? 1 : 0

  name         = "warehouse-password"
  key_vault_id = azurerm_key_vault.main.id
  value        = random_password.warehouse.result
}

resource "azurerm_key_vault_secret" "storage_connection_string" {
  name         = "storage-connection-string"
  key_vault_id = azurerm_key_vault.main.id
  value        = azurerm_storage_account.lake.primary_connection_string
}

# --- Cost control -----------------------------------------------------------------------------

resource "azurerm_consumption_budget_resource_group" "main" {
  count = var.deploy_budget ? 1 : 0

  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.main.id
  amount            = var.budget_eur
  time_grain        = "Monthly"

  time_period {
    start_date = "${var.budget_start_date}T00:00:00Z"
  }

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"
      contact_emails = [var.budget_contact_email]
    }
  }
}
