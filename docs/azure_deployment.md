# Deploying HealthFlow to Azure

A short-lived, low-cost deployment that proves the platform runs on real Azure services:
deploy, run the pipeline against Azure, capture the evidence, destroy. Plan on about an hour.

| Azure service | HealthFlow role | Replaces locally |
| --- | --- | --- |
| Storage account with hierarchical namespace (**ADLS Gen2**) | Bronze layer and quarantine | Azurite / local folder |
| **Azure Database for PostgreSQL** Flexible Server (Burstable B1ms) | Warehouse | Docker Postgres |
| **Key Vault** | Warehouse DSN, password, storage connection string | `.env` file |
| **Budget** with email alerts at 50 / 80 / 100% | Cost control | – |

Airflow keeps running where it already runs (GitHub Codespaces or GitHub Actions) and reaches
these services over TLS. A VM for Airflow, a container registry and private networking are
deliberately left out of this first deployment.

**Cost.** Billed per hour while the resources exist; the database is most of it. Deploying,
running and destroying on the same day costs well under the €10 budget. Check current prices on
the [Azure pricing calculator](https://azure.microsoft.com/pricing/calculator/) for your region.
A budget alert is an email, not a spending cap, so destroy everything when you finish.

## 1. One-time setup (in your Codespace)

```bash
# Azure CLI and Terraform
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
sudo apt-get update && sudo apt-get install -y gnupg software-properties-common
wget -qO- https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" \
  | sudo tee /etc/apt/sources.list.d/hashicorp.list
sudo apt-get update && sudo apt-get install -y terraform

az login --use-device-code          # sign in with the account that has your Azure subscription
az account show --query "{name:name, id:id}" -o table
```

## 2. Deploy

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your IP, email and this month's first day
terraform init
terraform plan -out tfplan                     # read it: 10 resources to add, nothing else
terraform apply tfplan                         # about 5-10 minutes (the database is the slow part)
```

## 3. Run the pipeline against Azure

```bash
eval "$(terraform output -raw load_settings)"  # secrets come from Key Vault, not from files
cd ../..
python scripts/demo_pipeline.py --synthea-csv-dir data/synthea/output/csv --reset
.dbt-venv/bin/dbt build --project-dir dbt --profiles-dir dbt
```

`demo_pipeline.py` prints `Bronze storage: AzureBlobStorageClient`: every delivery is written to
ADLS Gen2, and the warehouse is Azure Database for PostgreSQL.

## 4. Capture the evidence (for the README)

```bash
az storage fs file list --account-name "$(terraform -chdir=infrastructure/terraform output -raw storage_account)" \
  --file-system healthflow-data --path bronze/encounters --auth-mode key \
  --account-key "$(az storage account keys list -n "$(terraform -chdir=infrastructure/terraform output -raw storage_account)" --query [0].value -o tsv)" \
  --query "length(@)"                          # number of Bronze folders written
psql "$WAREHOUSE_DSN" -c "select source_name, count(*), sum(records_loaded) from audit.ingestion_file_audit group by 1"
```

Take screenshots of: the resource group in the Azure portal, the `bronze/` folders in the storage
account's **Storage browser**, the `dbt build` summary, and the two query results above.
Save them in `docs/images/azure-*.png`.

## 5. Destroy

```bash
cd infrastructure/terraform && terraform destroy
```

Then confirm in the portal that the resource group is gone.

## Known limitations of this deployment

- Storage is accessed with the account's connection string (kept in Key Vault). The next step is a
  managed identity with the *Storage Blob Data Contributor* role, so no key exists at all.
- The database is reachable from one IP address over TLS, not through a private network.
- Terraform state is local. For a long-lived environment, move it to an Azure Storage backend with
  state locking, and never commit it (`*.tfstate` is git-ignored because it contains the password).
