# DQ Automation

Production-style data quality automation pipeline for Collibra CDQ datasets.

This repository orchestrates a 9-stage workflow that:

- pulls dataset and DQ findings from Collibra CDQ,
- enriches and aggregates those findings,
- proposes and manages JIRA tickets,
- auto-invalidates stable adaptive-rule anomalies,
- diagnoses likely upstream causes,
- sends notification emails,
- and optionally publishes outputs to Tableau.

The pipeline is designed to run in Databricks (as a scheduled job bundle), while still supporting local execution for development and debugging.

## Notes for Deployment and Production

### Secret Management
Keep a local `.env` to save the environment variables.
Or even better to keep `.env.dev` and `.env.prod` to differentiate the dev and prod environments (Recommended).
Please refer to `.env.example`, all environment variables are saved to Databricks secret scope `collibra`.
For first-time secret scope set-up, please change `dotenv_path` in the first cell of databricks_notebook.ipynb, and the run all cells for the notebook.
You can use command line to manage the secrets which are stored in the secret scope `collibra`.
To check existing secret scopes, you can run:
```bash
databricks secrets list-scopes -t dev # for dev
# or
databricks secrets list-scopes -t prod # for prod
```
To create secret scope, you can run:
```bash
databricks secrets create-scope collibra -t <target> # replace <target> with dev or prod
```
To list all existing secrets in the secret scope, you can run:
```bash
databricks secrets list-secrets collibra -t <target>
```
To update/add a secret, you can use following format:
```bash
databricks secrets put-secret <scope> <secret-key> --string-value <new-secret-value> -t <target>
# for example
databricks secrets put-secret collibra cdq_base_url_cn --string-value "https://jnj-cn-comm-dq.myxjp.com" -t dev
```
To add a new secret, you need to update the python scripts and .env file first, then use above method to add the secret, or you update `databricks_notebook.ipynb` and run the notebook which uses API to update the secret instead.
To check a secret value, you can run:
```bash
databricks secrets get-secret <scope> <secret-key> -t <target> | jq -r .value | base64 --decode
# for example
databricks secrets get-secret collibra cdq_base_url_cn -t dev | jq -r .value | base64 --decode
```

### Pipeline Configuration
The pipeline configuration can be found in databricks.yml, take note that thefollwing variables are configured with respect to dev and prod targets.
- cluster_id
- databricks host

### Deployment
To deploy the pipeline to `dev` environment, set up a profile for `Service Principal`.
```bash
databricks configure --profile QA_SERVICE_PRINCIPAL
```
Edit .databrickscfg configuration file by:
```bash
vim ~/.databrickscfg
```
Then change the host and client_secret below and paste them into config file .databrickscfg:
```bash
[QA_SERVICE_PRINCIPAL]
host       = https://dbc-1a2fed98-ca15.cloud.databricks.com
client_id  = 9c060639-2782-4d7f-9606-841dfa9f85c3
client_secret = <your-sp-client-secret>
```
Save the .databrickscfg file by pressing Shift+":", following by typing "wq!"
After that, deploy to dev by running:
```bash
export DATABRICKS_CONFIG_PROFILE=QA_SERVICE_PRINCIPAL
databricks bundle deploy -t dev
```

To deploy the pipeline to `production`, create a prod profile `PROD_SERVICE_PRINCIPAL` using the same method above, and execute the following from command line:
```bash
export DATABRICKS_CONFIG_PROFILE=PROD_SERVICE_PRINCIPAL
databricks bundle deploy -t prod
```
After deployment of the bundle, run the automation pipeline manually or wait for the schedule job to run at scheduled time.
```bash
databricks bundle run dq_automation_pipeline -t prod
```

### Database Migration
Make sure to have DEV DB and PROD DB environmental variables configured in your .env file (refer to .env.example). Then run the python script postgres_dev_to_prod.py to migrate the contents from dev to prod
```bash
python postgres_io.py migrate-dev-to-prod
python postgres_dev_to_prod.py migrate-dev-to-prod
```


## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Layout](#repository-layout)
- [Pipeline Stages (S1-S9)](#pipeline-stages-s1-s9)
- [Data Inputs and Outputs](#data-inputs-and-outputs)
- [Configuration and Secrets](#configuration-and-secrets)
- [Local Development Setup](#local-development-setup)
- [Databricks Setup and Deployment](#databricks-setup-and-deployment)
- [How to Run](#how-to-run)
- [Operational Notes](#operational-notes)
- [Troubleshooting](#troubleshooting)
- [Git and Contribution Workflow](#git-and-contribution-workflow)

## Architecture Overview

```mermaid
flowchart TD
    S1[S1 Query Datasets and BU]
    S2[S2 Query Dataset Details]
    S3[S3 Prepare JJDMC Report]
    S4[S4 Publish to Tableau (Optional)]
    S5[S5 Generate Potential JIRA Tickets]
    S6[S6 Create/Reopen JIRA Tickets]
    S7[S7 Invalidate Adaptive Breaks]
    S8[S8 Diagnose JIRA Issues]
    S9[S9 Send Email Notifications]

    S1 --> S2
    S2 --> S3
    S2 --> S5
    S2 -. optional .-> S4
    S5 --> S6
    S5 --> S7
    S6 --> S7
    S6 --> S8
    S5 --> S9
    S7 --> S9
```

Execution target: Databricks Job defined in `databricks.yml`.

Primary integration surfaces:

- Collibra CDQ API (APAC + CN)
- Jira REST API
- PostgreSQL (historical state)
- Tableau Cloud (optional publishing)
- SMTP (notifications)
- J&J GenAI Gateway (S8 diagnosis)

## Repository Layout

### Core pipeline scripts

- `s1_query_dataset_and_bu.py`
- `s2_query_dataset_details.py`
- `s3_prepare_jjdmc_report.py`
- `s4_publish_report_to_tableau.py`
- `s5_generate_potential_jira_tickets.py`
- `s6_log_new_jira_tickets.py`
- `s7_invalidate_jira_tickets.py`
- `s8_diagnose_jira_issues.py`
- `s9_send_email_notification.py`

### Shared modules

- `config.py`: environment-driven configuration source.
- `pipeline_io.py`: unified CSV/PostgreSQL read-write abstraction.
- `token_manager.py`: Collibra token caching and refresh.
- `postgres_io.py`: reusable PostgreSQL helpers (shared credential loading, insert-only writes, change-aware upserts, stale-row pruning).
- `tableau_publisher.py`: Hyper extract + Tableau publish helpers.
- `jnj_strands_model.py`: J&J gateway model wrapper used in S8.
- `mwaa_dataset_reference.py`: dataset inclusion/exclusion reference list.

### Utility scripts and docs

- `link_id_finder.py`: PySpark utility to discover candidate link ID keys.
- `DATABRICKS_SETUP.md`: secret and Databricks setup guide.
- `databricks_notebook.ipynb`: notebook helper for secret/bootstrap operations.

### Runtime folders

- `data/`: static reference files used during enrichment.
- `outputs/`: local pipeline output artifacts (CSV/Hyper).

### Embedded Databricks bundle template project

The `dq_automation/` subfolder contains a generated Databricks bundle template (with `src/`, `resources/`, and `tests/`) that is separate from the top-level S1-S9 orchestration scripts.

## Pipeline Stages (S1-S9)

## Script dependencies

S1 (entry)
  ↓
S2 ─────────┬─── S3 (reporting)
  │         │
  │         └─── S4 (Tableau) ← deactivated
  │
  ├─── S5 ────→ S6 ────→ S8 (diagnosis)
  │              ↓
  └─── S7 ──────→ S9 ← now also depends on S5

## S1 - Query datasets and business units

Purpose:

- Fetch dataset inventory from APAC and CN Collibra CDQ regions.
- Resolve latest run IDs.
- Build dataset-to-business-unit mapping.

Main outputs:

- `dataset_apac`
- `dataset_cn`
- `business_unit_mapping`
- `dataset_runid`

## S2 - Query detailed dataset findings

Purpose:

- Read S1 outputs.
- Pull run findings, rules, profile deltas, definitions, outliers, duplicates, and patterns.
- Build enriched and aggregated DQ outputs.

Main outputs:

- `dataset_details`
- `dataset_rule_details`
- `dataset_adaptive_rule_details`
- `dataset_custom_rules`
- `dataset_outlier_details`
- `dataset_dupe_details`
- `dataset_pattern_details`
- `dataset_definitions`
- `dqm_dashboard_by_data_domain`

S2 also syncs four tables to PostgreSQL (guarded to skip when DB credentials are incomplete):

- `dqm_dashboard_by_data_domain` -> `dqm_hist_db_table`: insert-only, keyed on `(dataset, runId)`
- `business_unit_mapping` -> `bu_mapping_db_table`: upsert keyed on `dataset`, updates only when a column value changed
- `dataset_definitions` -> `dataset_def_db_table`: upsert keyed on `(dataset, col_name)`; `Run Id` always refreshes on update, other columns only update when changed, and column rows no longer present for a dataset are deleted
- `dataset_custom_rules` -> `dataset_custom_rules_db_table`: insert-only, keyed on `(dataset, ruleNm, runId)`; `ruleValue` is excluded (no corresponding DB column)

## S3 - Prepare quarterly JJDMC report artifacts (optional)

Purpose:

- Aggregate historical custom-rule and domain metrics over configurable lookback.
- Prepare domain-specific report outputs and trigger references.

Main outputs:

- `epic_list_prepared`
- `IM_APAC_Customer`
- `IM_APAC_Employee`
- `IM_APAC_HCP`
- `IM_APAC_Material`
- `mwaa_dq_trigger_datasets`

## S4 - Consolidate and publish to Tableau (optional / currently deactivated in default job)

Purpose:

- Consolidate S2 dashboard output with PostgreSQL history.
- Deduplicate by `(dataset, runId)`.
- Insert new records into PostgreSQL.
- Publish Hyper extract to Tableau Cloud.

Main output:

- `merged_historical_data`

## S5 - Generate potential JIRA tickets

Purpose:

- Fetch active Jira issues.
- Score and rank adaptive and non-adaptive DQ anomalies.
- Produce candidate ticket lists for creation/reuse.

Main outputs:

- `issue_list`
- `issue_list_prepared`
- `task_list_prepared`
- `epic_list_prepared`
- `potential_jira_ticket_list`
- `potential_adaptive_rule_jira_ticket_list`
- `potential_dupe_jira_ticket_list`
- `potential_pattern_jira_ticket_list`

## S6 - Create or reopen Jira tickets

Purpose:

- Create new tickets/subtasks from S5 candidates.
- Reopen or update existing adaptive tickets when appropriate.
- Preserve summary stability for adaptive ticket lifecycle.

Main output:

- `new_jira_ticket_list`

## S7 - Investigate and invalidate adaptive breaks

Purpose:

- Evaluate persistence and stability heuristics for adaptive breaks.
- Auto-invalidate selected breaks through CDQ.
- Comment on and potentially close related Jira issues.
- Flag high-score tickets for notifications.

Main outputs:

- `break_list_investigated`
- `break_list_to_invalidate`
- `invalidated_break_list`
- `notification_list`

## S8 - Diagnose adaptive Jira issues

Purpose:

- Analyze newly raised adaptive JGPV issues.
- Search upstream JEJQ evidence through Jira.
- Rank likely root causes.
- Use J&J model gateway for summarized diagnosis.

Main output:

- `issue_diagnosis_results`

## S9 - Send email notifications

Purpose:

- Read `notification_list` from S7.
- Resolve recipients from Jira + issue metadata.
- Send dataset-level notification emails for invalidations and high-score alerts.

Primary effect:

- Outbound SMTP notifications

## Data Inputs and Outputs

Outputs are written through a shared I/O layer and can target:

- CSV files under `./outputs`
- PostgreSQL tables under `public.dqm_auto_<output_name>`
- both (dual-write)

Mode is controlled by `PIPELINE_WRITE_MODE`.

Common output files currently present in this repo include:

- `dataset_runid.csv`
- `business_unit_mapping.csv`
- `dataset_details.csv`
- `dataset_rule_details.csv`
- `dataset_adaptive_rule_details.csv`
- `dataset_custom_rules.csv`
- `dqm_dashboard_by_data_domain.csv`
- `potential_adaptive_rule_jira_ticket_list.csv`
- `new_jira_ticket_list.csv`
- `invalidated_break_list.csv`
- `notification_list.csv`
- `issue_diagnosis_results.csv`

## Configuration and Secrets

All scripts use `config.py` as the local source of truth and attempt Databricks secrets first when running in Databricks.

## Core runtime settings

- `PIPELINE_WRITE_MODE`: `csv`, `postgres`, or `both`
- `PIPELINE_LOCAL_OUTPUT_DIR`: local output path (default `./outputs`)
- `DATABRICKS_SECRET_SCOPE`: secret scope name (default `collibra`)

## Collibra CDQ

- `CDQ_BASE_URL_APAC`
- `CDQ_BASE_URL_CN`
- `CDQ_USERNAME_APAC`, `CDQ_PASSWORD_APAC`
- `CDQ_USERNAME_CN`, `CDQ_PASSWORD_CN`
- `CDQ_VERIFY_SSL`, `CDQ_CA_BUNDLE`

## Jira

- `JIRA_URL`
- `JIRA_API_TOKEN`
- `JIRA_PROJECT_KEYS`
- `JIRA_VERIFY_SSL`, `JIRA_CA_BUNDLE`
- Retry knobs:
  - `JIRA_HTTP_TOTAL_RETRIES`
  - `JIRA_HTTP_CONNECT_RETRIES`
  - `JIRA_HTTP_READ_RETRIES`
  - `JIRA_HTTP_BACKOFF_FACTOR`

## PostgreSQL

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DQM_HIST_DB_TABLE`
- `BU_MAPPING_DB_TABLE`
- `DATASET_DEF_DB_TABLE`
- `DATASET_CUSTOM_RULES_DB_TABLE`

Connection credentials (`db_host`/`db_port`/`db_name`/`db_user`/`db_password`) are loaded once per script via `postgres_io.load_db_credentials()`, then combined with a table name through `postgres_io.settings_for_table()` for each write.

Pipeline handoff snapshots use transactional full refreshes. `business_unit_mapping` uses the existing `public.dqm_business_unit_mapping` upsert table, while the existing historical dashboard, dataset-definition, and custom-rule tables retain their specialized write policies.

S3 writes its report-shaped custom-rule output to `public.dqm_auto_jjdmc_dataset_custom_rules`, leaving `public.dqm_auto_dataset_custom_rules` as the current-run S2-to-S5 handoff.

## Tableau Cloud (S4)

- `TABLEAU_CLOUD_PROD_URL`
- `TABLEAU_CLOUD_PROD_TOKEN_NAME`
- `TABLEAU_CLOUD_PROD_TOKEN_VALUE`
- `TABLEAU_CLOUD_SITE_ID`
- `TABLEAU_CLOUD_PROJECT_PATH`
- `TABLEAU_CLOUD_DATASOURCE_NAME`
- `TABLEAU_CLOUD_PUBLISH_MODE`
- `TABLEAU_VERIFY_SSL`

## Notifications (S9)

- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_FROM`
- `SMTP_USE_TLS`
- `SMTP_USE_SSL`
- `ALLOWED_EMAIL_DOMAINS`
- `DQ_DL_ON_CLOSE`

## Diagnosis / GenAI (S8)

- `JNJ_GENAI_API_KEY`
- `MCP_ATLASSIAN_URL`
- `X_ATLASSIAN_JIRA_URL`
- `X_ATLASSIAN_JIRA_PERSONAL_TOKEN`
- `X_ATLASSIAN_USERNAME`
- `X_ATLASSIAN_READ_ONLY_MODE`
- `X_ATLASSIAN_ENABLE_XRAY`

For Databricks secret initialization commands and examples, see `DATABRICKS_SETUP.md`.

## Local Development Setup

## 1) Create and activate environment

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 2) Configure local credentials

Set environment variables (recommended) or provide values in `config.py` for local-only development.

Example minimal local env file:

```bash
cat > .env <<'EOF'
PIPELINE_WRITE_MODE=csv
PIPELINE_LOCAL_OUTPUT_DIR=./outputs

CDQ_BASE_URL_APAC=https://jnj-apac-comm-dq.collibra.jnj.com
CDQ_BASE_URL_CN=https://jnj-cn-comm-dq.collibra.jnj.com
CDQ_USERNAME_APAC=<apac-user>
CDQ_PASSWORD_APAC=<apac-pass>
CDQ_USERNAME_CN=<cn-user>
CDQ_PASSWORD_CN=<cn-pass>

JIRA_URL=https://jira.jnj.com/rest/api/2/search
JIRA_API_TOKEN=<jira-token>
JIRA_PROJECT_KEYS=JGPV
EOF
```

`config.py` calls `dotenv.load_dotenv()` if available, so `.env` values are loaded automatically.

## 3) Run one stage at a time

```bash
python s1_query_dataset_and_bu.py
python s2_query_dataset_details.py
python s5_generate_potential_jira_tickets.py
python s6_log_new_jira_tickets.py
python s7_invalidate_jira_tickets.py
python s8_diagnose_jira_issues.py
python s9_send_email_notification.py
```

Run optional/reporting stages as needed:

```bash
python s3_prepare_jjdmc_report.py
python s4_publish_report_to_tableau.py
```

## Databricks Setup and Deployment

This repo includes a Databricks Asset Bundle manifest at top level: `databricks.yml`.

## 1) Configure Databricks auth

```bash
databricks auth profiles
databricks auth login --profile <profile-name>
```

## 2) Create/update secret scope and required keys

Follow the full setup in `DATABRICKS_SETUP.md`.

## 3) Deploy bundle

```bash
export DATABRICKS_CONFIG_PROFILE=<profile-name>
databricks bundle deploy -t dev
```

## 4) Run pipeline

```bash
databricks bundle run dq_automation_pipeline -t dev
```

By default, `databricks.yml` schedules the job daily in `Asia/Singapore` time.

## Jenkins CI/CD

The root `Jenkinsfile` is intended for a Jenkins multibranch pipeline. The Jenkins agent must provide:

- Python 3 with `venv`
- Databricks CLI with Asset Bundle support
- Network access to the target Databricks workspace and package repository

Create these Jenkins Username/Password credentials:

| Jenkins credential ID | Username | Password |
|---|---|---|
| `dq-automation-databricks-dev` | Dev service-principal application/client ID | Dev OAuth client secret |
| `dq-automation-databricks-prod` | Prod service-principal application/client ID | Prod OAuth client secret |

The credentials are exposed to the Databricks CLI as `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`; they must never be stored in the repository. Configure the Bitbucket checkout credential on the Jenkins multibranch job itself.

Pipeline behavior:

| Branch | Python checks | Databricks validation | Deployment | Job run |
|---|---:|---:|---:|---:|
| Feature / pull request | Yes | No credentials exposed | No | No |
| `dev` | Yes | Dev | Automatic to dev | Only when `RUN_AFTER_DEPLOY=true` |
| `main` | Yes | Prod | Requires Jenkins approval | Only when `RUN_AFTER_DEPLOY=true` |

The optional run is disabled by default because the pipeline can create or update Jira tickets and send email notifications.

### Service-principal access

The service principal authenticating Jenkins should normally be the same principal configured in `databricks.yml` under `run_as`. It needs:

- Workspace access in both Databricks workspaces
- `CAN_ATTACH_TO` on the existing cluster configured for the target
- `READ` on the `collibra` Databricks secret scope
- Permission to create and manage the deployed workflow job and bundle workspace files
- Network/DNS access from the cluster to Collibra, PostgreSQL, Jira, MCP, and SMTP endpoints

If Jenkins deploys as a different principal from `run_as`, the deployer also needs the Databricks service-principal user role for the configured run identity. PostgreSQL access is provided by the database credentials in the Databricks secret scope; the Databricks principal does not replace PostgreSQL grants.

The Jenkins preflight verifies the authenticated identity, secret-scope visibility, and bundle configuration before deployment. Cluster, endpoint, and database connectivity are exercised only when the Databricks job runs.

A workspace administrator can grant the configured service principal access to an existing scope with:

```bash
databricks secrets put-acl collibra \
  9c060639-2782-4d7f-9606-841dfa9f85c3 READ \
  -p <workspace-admin-profile>
```

If `collibra` does not exist in a workspace, create it and populate all keys listed in `DATABRICKS_SETUP.md` before applying the ACL:

```bash
databricks secrets create-scope collibra -p <workspace-admin-profile>
```

Grant `CAN_ATTACH_TO` on the target's existing cluster through the Databricks permissions UI or API. Being able to view a cluster does not by itself prove that the principal can attach workloads to it.

## How to Run

## Recommended execution order

1. S1
2. S2
3. S5
4. S6
5. S7
6. S8
7. S9

Optional branches:

- S2 -> S3 (quarterly reporting)
- S2 -> S4 (Tableau publish)

## Output mode behavior

- `csv`: read/write only local CSV files in `outputs/`
- `postgres`: read/write PostgreSQL pipeline tables
- `both`: write both; read CSV first then PostgreSQL fallback

## Operational Notes

- Token handling:
  - `CollibraTokenManager` caches tokens in-memory.
  - refreshes early with a 5-minute expiry buffer.
  - retries once after HTTP 401.

- Jira API behavior:
  - S5 and S6 include retry/rate-limit handling.
  - SSL verification can be toggled with `JIRA_VERIFY_SSL` and optional CA bundle.

- Databricks/local dual mode:
  - scripts attempt secret loading via Databricks SDK.
  - on failure, they fall back to `config.py` values.

- PostgreSQL writes:
  - insert-only tables (`dqm_dashboard_by_data_domain`, `dataset_custom_rules`) are deduplicated via `ON CONFLICT DO NOTHING` on their key columns.
  - upsert tables (`business_unit_mapping`, `dataset_definitions`) use `postgres_io.upsert_dataframe()`, only firing the `UPDATE` when a tracked column actually changed.
  - `dataset_definitions` also prunes stale `col_name` rows per dataset via `postgres_io.delete_rows_not_in_keys()`.
  - pipeline handoff tables use `dqm_auto_` names and transaction-local staging tables for atomic full refreshes.
  - active pipeline stages fail when DB credentials are incomplete.

- CN exclusions:
  - ticket generation and management logic intentionally excludes selected CN flows.

## Troubleshooting

## Common startup errors

`PIPELINE_WRITE_MODE must be one of: csv, postgres, both`

- Verify `PIPELINE_WRITE_MODE` value.

`PostgreSQL configuration is incomplete`

- Set `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` or their Databricks secret equivalents.

`JIRA_URL and JIRA_API_TOKEN must be configured`

- Required for S5-S9 depending on stage.

`Token not found in response`

- Check CDQ credentials and endpoint values.

`Spark session not found`

- For UC reads/writes, run in Databricks context with Spark available.

## Stage-level debugging tips

- Start with S1 and validate `dataset_runid.csv` exists.
- Validate S2 outputs before running ticket logic.
- For S7/S8/S9, inspect `issue_list_prepared.csv`, `new_jira_ticket_list.csv`, and `notification_list.csv`.
- Check Databricks job logs for API payload and retry diagnostics.

## Git and Contribution Workflow

Use the following procedure when making repository changes.

## 1) Check branch and status

```bash
git branch --show-current
git status
```

## 2) Stage changes

```bash
git add <files>
# or
git add .
```

## 3) Use Jira-linked commit messages

- Feature: `feat(JGPV-1234): short description`
- Bug fix: `fix(JGPV-1234): short description`
- Docs: `docs(JGPV-1234): short description`

## 4) Push branch

```bash
git push origin <branch-name>
```

## Suggested sequence for feature work

1. Create Jira ticket.
2. Implement changes.
3. Run local validation.
4. Commit using Jira key format.
5. Push and open PR.

## Additional Notes

- Keep commits small and focused.
- Include Jira keys in commit and PR titles.
- Avoid committing secrets and environment files.
