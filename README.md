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
- `pipeline_io.py`: unified CSV/Unity Catalog read-write abstraction.
- `token_manager.py`: Collibra token caching and refresh.
- `postgres_io.py`: reusable PostgreSQL helpers.
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
- Unity Catalog tables
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

- `PIPELINE_WRITE_MODE`: `csv`, `uc`, or `both`
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
- `DB_TABLE`

## Unity Catalog (when using `uc` or `both`)

- `UC_CATALOG`
- `UC_SCHEMA`

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
- `uc`: read/write only Unity Catalog tables
- `both`: write both; read CSV first then UC fallback

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
  - deduplicated by `(dataset, runId)` patterns.
  - guarded to skip when DB credentials are incomplete.

- CN exclusions:
  - ticket generation and management logic intentionally excludes selected CN flows.

## Troubleshooting

## Common startup errors

`PIPELINE_WRITE_MODE must be one of: csv, uc, both`

- Verify `PIPELINE_WRITE_MODE` value.

`UC_CATALOG/UC_SCHEMA not configured`

- Required when mode is `uc` or `both`.

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
