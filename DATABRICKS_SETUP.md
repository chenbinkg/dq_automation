# Databricks Token Manager Setup Guide

## Overview
This guide explains how to set up and use the new `CollibraTokenManager` pattern for your Databricks pipeline scripts.

## Components

### 1. `token_manager.py`
A utility module that handles:
- Token generation and caching
- Automatic token refresh on expiry (1-hour default)
- Retry logic for API calls that return 401 (Unauthorized)
- 5-minute buffer before expiry to prevent mid-step failures

### 2. `s1_query_dataset_and_bu.py` (Refactored)
Updated to use the token manager with:
- Automatic credential loading from Databricks Secrets or local `config.py`
- Per-region token managers (APAC and CN)
- Enhanced error handling with automatic token refresh
- Intermediate output writes to Unity Catalog managed Delta tables

## Setup Instructions

### For Databricks Environment

#### Step 1: Create Databricks Secret Scope

`dbutils.secrets` is read-only for lookup and cannot create scopes or put secrets.
Create/update scopes and secrets using Databricks CLI or REST API.

**Option A: Using Databricks CLI (Recommended)**

```bash
# List profile
databricks auth profiles

# Set up profile
databricks auth switch --profile <profile-name>

# Create secret scope (idempotent: ignore if already exists)
databricks secrets create-scope --scope collibra

# List secret scope
databricks secrets list-scopes

# Store credentials
databricks secrets put-secret --scope collibra --key cdq_base_url_apac --string-value "https://jnj-apac-comm-dq.collibra.jnj.com"
databricks secrets put --scope collibra --key cdq_base_url_cn --string-value "https://jnj-cn-comm-dq.collibra.jnj.com"
databricks secrets put-secret --scope collibra --key username_apac --string-value "your_apac_username"
databricks secrets put-secret --scope collibra --key password_apac --string-value "your_apac_password"
databricks secrets put-secret --scope collibra --key username_cn --string-value "your_cn_username"
databricks secrets put-secret --scope collibra --key password_cn --string-value "your_cn_password"

# Unity Catalog target for table writes
databricks secrets put-secret --scope collibra --key uc_catalog --string-value "main"
databricks secrets put-secret --scope collibra --key uc_schema --string-value "dq_automation"

# JIRA credentials and search configuration for S5
databricks secrets put-secret --scope collibra --key jira_user_email --string-value "your.name@jnj.com"
databricks secrets put-secret --scope collibra --key jira_api_token --string-value "your_jira_api_token"
databricks secrets put-secret --scope collibra --key jira_url --string-value "https://your-jira-host/rest/api/2/search"
databricks secrets put-secret --scope collibra --key jira_project_keys --string-value "JGPV,OTHERPROJECT"
databricks secrets put-secret --scope collibra --key jira_verify_ssl --string-value "false"
databricks secrets put-secret --scope collibra --key jira_ca_bundle --string-value "/path/to/corporate-ca.pem"
databricks secrets put-secret --scope collibra --key jira_http_total_retries --string-value "5"
databricks secrets put-secret --scope collibra --key jira_http_connect_retries --string-value "5"
databricks secrets put-secret --scope collibra --key jira_http_read_retries --string-value "5"
databricks secrets put-secret --scope collibra --key jira_http_backoff_factor --string-value "1.0"

# Tableau Cloud publish target for S4
databricks secrets put-secret --scope collibra --key tableau_cloud_prod_url --string-value "https://online.tableau.com"
databricks secrets put-secret --scope collibra --key tableau_cloud_site_id --string-value "your_site_content_url"
databricks secrets put-secret --scope collibra --key tableau_cloud_prod_token_name --string-value "your_pat_name"
databricks secrets put-secret --scope collibra --key tableau_cloud_prod_token_value --string-value "your_pat_secret"
databricks secrets put-secret --scope collibra --key tableau_cloud_project_path --string-value "Parent/Child/SubProject"
databricks secrets put-secret --scope collibra --key tableau_cloud_datasource_name --string-value "DQM_CDQ_DATASET_BY_DATA_DOMAIN"
databricks secrets put-secret --scope collibra --key tableau_cloud_publish_mode --string-value "Overwrite"

# JNJ GenAI Gateway key (used by jnj_strands_model.py)
databricks secrets put-secret --scope collibra --key JNJ_GENAI_API_KEY --string-value "your_jnj_genai_api_key"

# SMTP / email notification config for S9
databricks secrets put-secret --scope collibra --key smtp_host --string-value "smtp.office365.com"
databricks secrets put-secret --scope collibra --key smtp_port --string-value "587"
databricks secrets put-secret --scope collibra --key smtp_user --string-value "your.name@jnj.com"
databricks secrets put-secret --scope collibra --key smtp_password --string-value "your_smtp_password_or_token"
databricks secrets put-secret --scope collibra --key smtp_from --string-value "noreply@jnj.com"
databricks secrets put-secret --scope collibra --key smtp_use_tls --string-value "true"
databricks secrets put-secret --scope collibra --key smtp_use_ssl --string-value "false"
databricks secrets put-secret --scope collibra --key allowed_email_domains --string-value "jnj.com,its.jnj.com"
databricks secrets put-secret --scope collibra --key dq_dl_on_close --string-value "dl@example.com"

# MCP Atlassian / JEJQ diagnosis config for S8
databricks secrets put-secret --scope collibra --key mcp_atlassian_url --string-value "https://atlassian-mcp.xena.dev/mcp/"
databricks secrets put-secret --scope collibra --key x_atlassian_jira_url --string-value "https://jira.jnj.com"
databricks secrets put-secret --scope collibra --key x_atlassian_jira_personal_token --string-value "your_jira_personal_token"
databricks secrets put-secret --scope collibra --key x_atlassian_username --string-value "your_username"
databricks secrets put-secret --scope collibra --key x_atlassian_read_only_mode --string-value "false"
databricks secrets put-secret --scope collibra --key x_atlassian_enable_xray --string-value "false"

# Get Secrets 
databricks secrets get-secret <scope-name> <key-name> | jq -r .value | base64 --decode

```

**Option B: Using Databricks REST API (from notebook or local script)**

please run databricks_notebook.ipynb to save the configurations and credentials to the secret_scope.

```python
import requests

DATABRICKS_HOST = "https://dbc-1a2fed98-ca15.cloud.databricks.com"
DATABRICKS_TOKEN = "<your_pat>"
SCOPE = "collibra"

api = f"{DATABRICKS_HOST}/api/2.0"
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# Create scope if missing
scopes = requests.get(f"{api}/secrets/scopes/list", headers=headers)
scopes.raise_for_status()
existing = [s["name"] for s in scopes.json().get("scopes", [])]
if SCOPE not in existing:
    r = requests.post(
        f"{api}/secrets/scopes/create",
        headers=headers,
        json={"scope": SCOPE, "initial_manage_principal": "users"},
    )
    r.raise_for_status()

secrets = {
    "cdq_base_url_apac": "https://jnj-apac-comm-dq.collibra.jnj.com",
    "cdq_base_url_cn": "https://jnj-cn-comm-dq.collibra.jnj.com",
    "username_apac": "your_apac_username",
    "password_apac": "your_apac_password",
    "username_cn": "your_cn_username",
    "password_cn": "your_cn_password",
    "uc_catalog": "main",
    "uc_schema": "dq_automation",
    "jira_user_email": "your.name@jnj.com",
    "jira_api_token": "your_jira_api_token",
    "jira_url": "https://your-jira-host/rest/api/2/search",
    "jira_project_keys": "JGPV,OTHERPROJECT",
    "jira_verify_ssl": "false",
    "jira_ca_bundle": "/path/to/corporate-ca.pem",
    "jira_http_total_retries": "5",
    "jira_http_connect_retries": "5",
    "jira_http_read_retries": "5",
    "jira_http_backoff_factor": "1.0",
    "tableau_cloud_prod_url": "https://online.tableau.com",
    "tableau_cloud_site_id": "your_site_content_url",
    "tableau_cloud_prod_token_name": "your_pat_name",
    "tableau_cloud_prod_token_value": "your_pat_secret",
    "tableau_cloud_project_path": "Parent/Child/SubProject",
    "tableau_cloud_datasource_name": "DQM_CDQ_DATASET_BY_DATA_DOMAIN",
    "tableau_cloud_publish_mode": "Overwrite",
    "JNJ_GENAI_API_KEY": "your_jnj_genai_api_key",
    "smtp_host": "smtp.office365.com",
    "smtp_port": "587",
    "smtp_user": "your.name@jnj.com",
    "smtp_password": "your_smtp_password_or_token",
    "smtp_from": "noreply@jnj.com",
    "smtp_use_tls": "true",
    "smtp_use_ssl": "false",
    "allowed_email_domains": "jnj.com,its.jnj.com",
    "dq_dl_on_close": "dl@example.com",
    "mcp_atlassian_url": "https://atlassian-mcp.xena.dev/mcp/",
    "x_atlassian_jira_url": "https://jira.jnj.com",
    "x_atlassian_jira_personal_token": "your_jira_personal_token",
    "x_atlassian_username": "your_username",
    "x_atlassian_read_only_mode": "false",
    "x_atlassian_enable_xray": "false",
}

for key, value in secrets.items():
    r = requests.post(
        f"{api}/secrets/put",
        headers=headers,
        json={"scope": SCOPE, "key": key, "string_value": value},
    )
    r.raise_for_status()
```

**Verify secrets were created:**

```bash
databricks secrets list-secrets --scope collibra
```

#### Step 2: Configure Databricks Job
Update your `databricks.yml` (apply after secrets are configured via CLI or notebook):

```yaml
bundle:
  name: dq_automation

targets:
  dev:
    mode: development
    workspace:
      host: https://dbc-1a2fed98-ca15.cloud.databricks.com

jobs:
  - name: collibra_dq_pipeline
    tasks:
      - task_key: s1_query_dataset
        notebook_task:
          notebook_path: ./s1_query_dataset_and_bu
        cluster:
          spark_version: "13.3.x-scala2.12"
          node_type_id: "i3.xlarge"
          num_workers: 1
```

#### Step 3: Deploy to Databricks
```bash
export DATABRICKS_CONFIG_PROFILE=DEFAULT # set your databricks profile
databricks bundle deploy -t dev
databricks bundle run dq_automation_pipeline -t dev
```

#### Edit the dev target to add scheduling:
```bash
targets:
  dev:
    mode: development
    workspace:
      host: https://dbc-1a2fed98-ca15.cloud.databricks.com
    jobs:
      dq_automation_pipeline:
        schedule:
          quartz_cron_expression: "0 0 * * * ?" # Daily at midnight
          timezone_id: "UTC"
```

### For Local Development

Keep your `config.py` with credentials and Unity Catalog target:

```python
# config.py
CDQ_BASE_URL_APAC = "https://jnj-apac-comm-dq.collibra.jnj.com"
CDQ_BASE_URL_CN = "https://jnj-cn-comm-dq.collibra.jnj.com"
COLLIBRA_USERNAME_APAC = "your_username"
COLLIBRA_PASSWORD_APAC = "your_password"
COLLIBRA_USERNAME_CN = "your_username_cn"
COLLIBRA_PASSWORD_CN = "your_password_cn"

# Required for Unity Catalog writes
UC_CATALOG = "main"
UC_SCHEMA = "dq_automation"

# JIRA (used by S5 issue fetch and ticket generation)
JIRA_USER_EMAIL = "your.name@jnj.com"
JIRA_API_TOKEN = "your_jira_api_token"
JIRA_URL = "https://your-jira-host/rest/api/2/search"
JIRA_PROJECT_KEYS = "JGPV,OTHERPROJECT"
JIRA_VERIFY_SSL = "false"
JIRA_CA_BUNDLE = "/path/to/corporate-ca.pem"
JIRA_HTTP_TOTAL_RETRIES = 5
JIRA_HTTP_CONNECT_RETRIES = 5
JIRA_HTTP_READ_RETRIES = 5
JIRA_HTTP_BACKOFF_FACTOR = 1.0

# Tableau Cloud (used by S4 publish step)
TABLEAU_CLOUD_PROD_URL = "https://online.tableau.com"
TABLEAU_CLOUD_SITE_ID = "your_site_content_url"
TABLEAU_CLOUD_PROD_TOKEN_NAME = "your_pat_name"
TABLEAU_CLOUD_PROD_TOKEN_VALUE = "your_pat_secret"
TABLEAU_CLOUD_PROJECT_PATH = "Parent/Child/SubProject"
TABLEAU_CLOUD_DATASOURCE_NAME = "DQM_CDQ_DATASET_BY_DATA_DOMAIN"
TABLEAU_CLOUD_PUBLISH_MODE = "Overwrite"

# JNJ GenAI Gateway (used by jnj_strands_model.py)
JNJ_GENAI_API_KEY = "your_jnj_genai_api_key"

# SMTP / email notifications (used by S9)
SMTP_HOST = "smtp.na.jnj.com"
SMTP_PORT = "25"
# SMTP_USER = "your.name@jnj.com"
# SMTP_PASSWORD = "your_smtp_password_or_token"
SMTP_FROM = "noreply@jnj.com"
SMTP_USE_TLS = "false"
SMTP_USE_SSL = "false"
ALLOWED_EMAIL_DOMAINS = "jnj.com,its.jnj.com"
DQ_DL_ON_CLOSE = "dl@example.com"

# MCP Atlassian / JEJQ diagnosis settings (used by S8)
MCP_ATLASSIAN_URL = "https://atlassian-mcp.xena.dev/mcp/"
X_ATLASSIAN_JIRA_URL = "https://jira.jnj.com"
X_ATLASSIAN_JIRA_PERSONAL_TOKEN = "your_jira_personal_token"
X_ATLASSIAN_USERNAME = "your_username"
X_ATLASSIAN_READ_ONLY_MODE = "false"
X_ATLASSIAN_ENABLE_XRAY = "false"
```

The refactored script will automatically detect the environment and use the appropriate credential source.

## How It Works

### Token Lifecycle

```
Pipeline Start
    ↓
[Initialize Token Managers]
    ↓
Step 1: Call API
    ├─ Get token (auto-refresh if expired)
    ├─ Request with token
    ├─ Got 401? → Force refresh → Retry
    └─ Success → Continue
    ↓
Step 2: Call API
    ├─ Check token expiry
    ├─ Still valid? → Use cached token
    ├─ Expired? → Auto-refresh
    └─ Make request
    ↓
[Continue until pipeline ends]
```

### Key Features

1. **Automatic Refresh**: Tokens are automatically refreshed if they expire
2. **5-Minute Buffer**: Prevents failures by refreshing 5 minutes before actual expiry
3. **401 Handling**: If API returns 401, the token is refreshed and request is retried
4. **Per-Region Managers**: Each region (APAC/CN) has its own token manager
5. **Dual Environment Support**: Works with both Databricks and local dev

## Usage in Other Pipeline Steps

To use the token manager in other pipeline steps:

```python
from token_manager import CollibraTokenManager

# Initialize
token_mgr = CollibraTokenManager(
    base_url="https://your-collibra-url",
    username="your_username",
    password="your_password",
    region="apac"
)

# Get valid token (auto-refreshes if needed)
token = token_mgr.get_token()

# OR get auth header directly
headers = token_mgr.get_auth_header()

# Use in requests
response = requests.get(url, headers=headers)

# Force refresh if needed
token = token_mgr.get_token(force_refresh=True)
```

## Monitoring & Debugging

The scripts include detailed logging:

```
[APAC] Refreshing token from https://...
[APAC] ✓ Token refreshed for user: john.doe
[APAC] Expires at: 2026-06-19 16:30:45
[APAC] Fetching datasets...
[APAC] Total datasets: 150
```

Check these logs in:
- **Databricks**: Job runs page → Output tab
- **Local**: Console output

## Troubleshooting

### Problem: "Token not found in response"
- **Cause**: Authentication failed or API changed
- **Fix**: Verify credentials in Databricks Secrets or config.py

### Problem: "Call failed after token refresh"
- **Cause**: Token refreshed but API still returns 401
- **Fix**: Check if credentials are correct and have API permissions

### Problem: "Request failed: Connection timeout"
- **Cause**: Network issue or Collibra endpoint is down
- **Fix**: Verify connectivity to Collibra URL and check firewall rules

## Best Practices

1. **Don't hardcode credentials** → Use Databricks Secrets or config.py
2. **Test locally first** → Use config.py before pushing to Databricks
3. **Monitor token usage** → Check logs to understand refresh patterns
4. **Handle errors gracefully** → The script includes retry logic; don't add more
5. **Use consistent regions** → Each region has its own token manager

## Next Steps

- Test the refactored script locally with `config.py`
- Set up Databricks Secrets and deploy to a dev environment
- Monitor token refresh behavior in logs
- Extend this pattern to other pipeline steps (S2, S3, etc.)

