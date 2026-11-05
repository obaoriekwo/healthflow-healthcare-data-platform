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

## 2. Check the subscription can use the three services

```bash
for p in Microsoft.Storage Microsoft.KeyVault Microsoft.DBforPostgreSQL; do
  echo "$p: $(az provider show -n $p --query registrationState -o tsv)"
done
```

All three should say `Registered`. If one says `NotRegistered`, run
`az provider register --namespace <name>` and wait a minute. Restricted subscriptions (for
example some school-managed *Azure for Students* subscriptions) may not allow registering.
If `Microsoft.DBforPostgreSQL` stays unregistered, deploy without the database
(`deploy_postgres = false` in `terraform.tfvars`): Bronze goes to ADLS Gen2 and secrets to Key
Vault, and the warehouse stays in your Codespace's PostgreSQL. Ask your Azure administrator to
register the provider if you need the full deployment. If the budget fails to create on your
subscription type, set `deploy_budget = false`.

**If your organisation blocks `az login` from a Codespace** (device-code sign-in is often
restricted), use **Azure Cloud Shell** in the portal instead: it is already signed in and has
Terraform installed. Run steps 2-3 there, then copy the settings to your Codespace (step 4).

## 3. Deploy

```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in your IP, email and this month's first day
terraform init
terraform plan -out tfplan                     # read it: 10 resources to add, nothing else
terraform apply tfplan                         # about 5-10 minutes (the database is the slow part)
```

## 4. Run the pipeline against Azure

Where `az` is signed in (Codespace or Cloud Shell), load the settings from Key Vault:

```bash
eval "$(terraform output -raw load_settings)"  # secrets come from Key Vault, not from files
```

If the pipeline runs somewhere else (for example, Terraform ran in Cloud Shell and the pipeline
runs in your Codespace), print the settings, then paste them into the other terminal. They are
secrets: paste them only into your own terminal, never into chats, issues or commits.

```bash
for v in STORAGE_BACKEND AZURE_STORAGE_CONTAINER AZURE_STORAGE_CONNECTION_STRING WAREHOUSE_DSN \
         WAREHOUSE_HOST WAREHOUSE_PORT WAREHOUSE_SSLMODE WAREHOUSE_USER WAREHOUSE_DB WAREHOUSE_PASSWORD; do
  [ -n "${!v}" ] && printf "export %s='%s'\n" "$v" "${!v}"
done
```

Then, in the terminal that runs the pipeline:

```bash
cd ../..
python scripts/demo_pipeline.py --synthea-csv-dir data/synthea/output/csv --reset
.dbt-venv/bin/dbt build --project-dir dbt --profiles-dir dbt
```

`demo_pipeline.py` prints `Bronze storage: AzureBlobStorageClient`: every delivery is written to
ADLS Gen2, and the warehouse is Azure Database for PostgreSQL.

## 5. Capture the evidence (for the README)

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

## 6. Destroy

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
