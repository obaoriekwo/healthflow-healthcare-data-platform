terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.40"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }

  # State stays on your machine for a short-lived demo deployment. For anything longer-lived,
  # move it to an Azure Storage backend (see docs/azure_deployment.md) and never commit it:
  # *.tfstate is in .gitignore because state contains the database password.
}

provider "azurerm" {
  # Don't let Terraform register ~40 resource providers on its own: student and other
  # restricted subscriptions can't, and HealthFlow needs only Microsoft.Storage,
  # Microsoft.KeyVault and Microsoft.DBforPostgreSQL (see docs/azure_deployment.md).
  resource_provider_registrations = "none"

  features {
    key_vault {
      purge_soft_delete_on_destroy = true # demo: `terraform destroy` really removes the vault
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
  # Subscription comes from `az login` / ARM_SUBSCRIPTION_ID, never from the code.
}
