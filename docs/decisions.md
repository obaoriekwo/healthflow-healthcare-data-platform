# Architecture decisions

Short records of the decisions that shape HealthFlow, so a reviewer can see why, not just what.

## ADR-001: ELT, not ETL

Python extracts and loads source data unchanged. All cleaning, typing, deduplication and
business logic happens inside PostgreSQL with dbt. Python never "fixes" data; it only
accepts it, rejects it, or quarantines it.

## ADR-002: Bronze is immutable; idempotency comes from delivery identity

Raw ingestion does not upsert on business keys. Instead:

- Each delivery is identified by (source, delivery date, sha256 of the file bytes), and
  `file_id = uuid5(source|date|checksum)`. That triple is UNIQUE in
  `audit.ingestion_file_audit`; a delivery already recorded is skipped.
- The date is part of the identity because legitimate files can be byte-identical on
  different days. A first version used the checksum alone, and testing with a year of
  real Synthea data showed every empty daily file after the first being skipped as a
  duplicate, so those deliveries were never recorded and freshness reporting would have
  been wrong. A regression test now covers it.
- Bronze object keys contain a checksum prefix and are written with "do not overwrite".
  Two different versions of a file are both preserved; identical re-uploads after a crash are accepted.
- The audit row and the raw rows for one file are committed in a single transaction,
  so a crash mid-load leaves nothing behind and the rerun starts clean.

Corrected records arrive in later files and are appended as new raw rows. dbt staging
selects the latest version per encounter (by `source_updated_at`, then `_available_date`).

## ADR-003: Three dates per record

| Column | Meaning |
| --- | --- |
| `_event_date` | When the encounter happened (Synthea `START`) |
| `source_updated_at` | When the source last changed the record |
| `_available_date` | When the source delivered it (the file's date) |
| `_ingested_at` | When HealthFlow loaded it |

Incremental processing and backfills key on `_available_date`, never on `_event_date`.
The simulator sends each record after discharge plus a 0–2 day lag, and re-sends about
2% of records 2–10 days later with a corrected claim cost.

## ADR-004: Storage behind an interface; Azurite for Blob only

Ingestion code only sees `StorageClient`. `LocalStorageClient` is used for fast tests,
`AzureBlobStorageClient` for Azurite and Azure. Azurite emulates Blob APIs, not the ADLS
Gen2 hierarchical namespace, so anything relying on hierarchical namespace (directory ACLs,
atomic renames) is tested against a real dev storage account in Phase 6.

## ADR-005: Pinned versions

Airflow 3.2.2 (installed with the official constraints file), PostgreSQL 16, Azurite 3.37.0.
Cosmos, dbt-core and dbt-postgres versions are pinned together when dbt is added in Phase 3.
Upgrade them as a set, in one pull request, with the test suite passing.

## ADR-006: Two-level validation and quarantine

- File level: the header must match the contract exactly. Schema drift sends the whole
  file to `quarantine/rejected_files/`.
- Row level: unusable rows (no id, bad timestamps, wrong field count) go to
  `quarantine/invalid_records/` with a reason; the rest load. If more than 10% of rows fail,
  the whole file is quarantined instead, because that usually means a broken extract.

Business rules (for example "discharge after admission" for inpatient stays) are dbt tests, not ingestion checks.

## ADR-007: No version-controlled database (Dolt / Doltgres)

Considered and rejected. Doltgres reached 1.0 in August 2026 and is optimised for
transactional workloads; it is not a managed Azure service, and dbt / Power BI support
against it would be an untested dependency. Data history is already covered by immutable
Bronze files, git for transformation code, and dbt snapshots for slowly changing attributes.

## ADR-008: The simulator removes nothing yet

Synthea's `encounters.csv` contains no direct identifiers, so it is passed through as-is.
When `patients.csv` is added, the simulator (playing the source system) will drop names,
addresses, SSN, driver's licence and passport columns before delivery, and the data
dictionary will say so.

## ADR-009: Patient history comes from source versions, not dbt snapshots

`raw_staging.patients` is append-only, so it already holds every version the source sent
(for example "alive", then "deceased"). `stg_patient_versions` turns those into
valid_from / valid_to periods and `dim_patient` is an SCD type 2 dimension built from them.

dbt snapshots were considered. They record what the table looked like each time dbt runs,
so history depends on the run schedule: a backfill, or two updates between runs, loses
versions. Building history from the versions the source delivered is complete and
reproducible from Bronze. Facts join to the patient version valid when the event happened.

## ADR-010: Business rules for readmissions live in seeds

Which admissions are planned, and which department each admission type belongs to, are in
`dbt/seeds/admission_types.csv` and `admission_reasons.csv`, not in SQL `CASE` statements.
A clinician or analyst can review and change them without touching model code, and they
are versioned and tested like everything else. The full definition, and why reason codes
were needed (chemotherapy cycles), is in `docs/readmission_definition.md`.

## ADR-011: What is incremental and what is rebuilt

`fact_encounters` is incremental (delete+insert on `encounter_id`), reprocessing encounters
whose newest version was loaded since the last run, minus a 3-day lookback. A correction to
an old encounter is loaded recently, so it replaces the earlier row.

`fact_hospital_stays` and the marts are rebuilt every run. A stay's readmission flag changes
when a later admission arrives, and whether it is an index stay changes as its follow-up
window becomes observable, so an incremental version would need to reprocess most rows anyway.

`scripts/check_incremental.py` (run in CI) loads data in two halves, builds after each, and
checks that the incremental result is identical to `dbt build --full-refresh`.

## ADR-012: The live demo is a nightly rebuild on GitHub Actions, published as a static site

Recruiters and reviewers need a link that always works, without logging in. Hosting Airflow and
Postgres permanently would cost money and expose admin tools to the internet. Instead,
`.github/workflows/live-demo.yml` rebuilds everything from an empty database every night on a
GitHub-hosted runner and publishes static pages to GitHub Pages (free for public repositories).

- The site is generated from the gold layer and the audit tables only (`scripts/build_site.py`),
  so every number on it was produced by the pipeline in that run.
- Each run injects two faults (a schema-drifted re-send and corrupt rows, `simulator/faults.py`),
  so the quarantine log always has something to show.
- Each run replays every day's ingestion a second time and fails if the warehouse changes.
- A failed step or test stops the deploy, so the site keeps the last good version.

Readmission rates are shown only for discharge months whose 30-day follow-up is complete. The model
judges completeness from the latest *event* date, not the latest *delivery* date: deliveries keep
arriving for days after the last event (late arrivals), and the first version of the live demo
showed a misleading 33% spike for the most recent month because of exactly this.

## ADR-013: Nothing is delivered after the simulated "today"

The simulator sends each encounter after discharge plus a delay. Stays still open when the
window ends (long hospice and nursing-home episodes, late arrivals, pending corrections) were
being written as deliveries dated weeks into the future, so the pipeline page showed deliveries
through late December while the dashboard ended in October. An external review spotted it.
A real source cannot send tomorrow's file: the window's last day is now the last delivery day,
and records due later are reported as "not sent yet". Covered by a regression test.

## ADR-014: Airflow runs dbt through Cosmos, one task per node

The `healthflow_daily` DAG renders the dbt project with Astronomer Cosmos (`DbtTaskGroup`):
each seed, model and test becomes its own Airflow task, so a failure shows exactly which model
or test broke and can be retried alone, and lineage is visible in the Airflow graph.

- Tests run right after their model (`TestBehavior.AFTER_EACH`); tests that span several models
  (the business rules) are detached into their own tasks; source tests and freshness are rendered.
- dbt runs from its own virtual environment (`DBT_EXECUTABLE`, subprocess invocation), because
  dbt's dependencies conflict with Airflow's. Airflow 3.2.2, Cosmos 1.16.0, dbt-core 1.12.6 and
  dbt-postgres 1.11.0 are pinned together.
- After dbt, `reconcile` compares layers (landing files → audit → raw → gold) and writes each check
  to `audit.reconciliation_audit`; a mismatch fails the run.
- The nightly live demo runs the final day through this DAG with `airflow dags test` (a complete
  DAG run in-process, no scheduler needed), so every published build is proof the DAG works.

## ADR-015: HealthFlow stays Azure-ready rather than Azure-deployed

A deployment was attempted with the Terraform in `infrastructure/terraform`. The school-managed
*Azure for Students* subscription available to the author blocked it: the organisation's sign-in
policy rejected device-code and Codespaces sign-ins, and the account had no role on the
subscription (it could not register `Microsoft.DBforPostgreSQL` or even read resource groups).
An Azure free account was then not offered. Nothing was created in Azure.

What came out of the attempt is kept in the code: automatic provider registration is turned off,
the database and budget can be switched off for restricted subscriptions, and the guide covers
deploying from Azure Cloud Shell when `az login` is blocked.

What is verified without an Azure subscription:
- Terraform formatting and validation on every change (Terraform CI).
- The Bronze storage code path through the Azure Blob API, end to end against Azurite.
- TLS settings for Azure Database for PostgreSQL in the dbt profile.

The deployment itself is about an hour of work with a subscription where the account has the
Contributor role; `docs/azure_deployment.md` has the steps.
