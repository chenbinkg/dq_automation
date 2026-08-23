import requests
import os

DATABRICKS_HOST = "https://dbc-1a2fed98-ca15.cloud.databricks.com" # your workspace URL
DATABRICKS_TOKEN = "" # paste your PAT here — do NOT commit this to git

if not DATABRICKS_TOKEN:
    raise ValueError("Set DATABRICKS_TOKEN before running this cell.")

API_BASE = f"{DATABRICKS_HOST}/api/2.0"
HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

SCOPE = "collibra"

# Verify the token is valid by making a test request
test = requests.get(f"{API_BASE}/token/list", headers=HEADERS)
if test.status_code == 401:
    raise ValueError("DATABRICKS_TOKEN is invalid or expired. Please generate a new PAT from your Databricks workspace: Settings → Developer → Access Tokens.")
test.raise_for_status()
print("Token is valid.")

existing = requests.get(f"{API_BASE}/secrets/scopes/list", headers=HEADERS)
existing.raise_for_status()
existing_scopes = [s["name"] for s in existing.json().get("scopes", [])]

if SCOPE in existing_scopes:
    print(f"Secret scope '{SCOPE}' already exists — skipping creation.")
else:
    r = requests.post(
        f"{API_BASE}/secrets/scopes/create",
        headers=HEADERS,
        json={"scope": SCOPE, "initial_manage_principal": "users"}
    )
    r.raise_for_status()
    print(f"Secret scope '{SCOPE}' created successfully.")

import os
from dotenv import load_dotenv

# override=True forces os.environ to update with the newest .env values
load_dotenv(override=True) 


secrets = {
"cdq_base_url_apac": os.environ["CDQ_BASE_URL_APAC"],
"cdq_base_url_cn": os.environ["CDQ_BASE_URL_CN"],
"username_apac": os.environ["CDQ_USERNAME_APAC"],
"password_apac": os.environ["CDQ_PASSWORD_APAC"],
"username_cn": os.environ["CDQ_USERNAME_CN"],
"password_cn": os.environ["CDQ_PASSWORD_CN"],
"uc_catalog": os.environ["UC_CATALOG"],
"uc_schema": os.environ["UC_SCHEMA"],
"jira_user_email": os.environ["JIRA_USER_EMAIL"],
"jira_api_token": os.environ["JIRA_API_TOKEN"],
"jira_url": os.environ["JIRA_URL"],
"jira_project_keys": os.environ["JIRA_PROJECT_KEYS"],
"jira_verify_ssl": os.environ.get("JIRA_VERIFY_SSL", "false"),
"jira_ca_bundle": os.environ.get("JIRA_CA_BUNDLE", ""),
"jira_http_total_retries": os.environ.get("JIRA_HTTP_TOTAL_RETRIES", "5"),
"jira_http_connect_retries": os.environ.get("JIRA_HTTP_CONNECT_RETRIES", "5"),
"jira_http_read_retries": os.environ.get("JIRA_HTTP_READ_RETRIES", "5"),
"jira_http_backoff_factor": os.environ.get("JIRA_HTTP_BACKOFF_FACTOR", "1.0"),
"tableau_cloud_prod_url": os.environ["TABLEAU_CLOUD_PROD_URL"],
"tableau_cloud_site_id": os.environ["TABLEAU_CLOUD_SITE_ID"],
"tableau_cloud_prod_token_name": os.environ["TABLEAU_CLOUD_PROD_TOKEN_NAME"],
"tableau_cloud_prod_token_value": os.environ["TABLEAU_CLOUD_PROD_TOKEN_VALUE"],
"tableau_cloud_project_path": os.environ["TABLEAU_CLOUD_PROJECT_PATH"],
"tableau_cloud_datasource_name": os.environ.get("TABLEAU_CLOUD_DATASOURCE_NAME", "DQM_CDQ_DATASET_BY_DATA_DOMAIN"),
"tableau_cloud_publish_mode": os.environ.get("TABLEAU_CLOUD_PUBLISH_MODE", "Overwrite"),
"tableau_verify_ssl": os.environ.get("TABLEAU_VERIFY_SSL", "false"),
"JNJ_GENAI_API_KEY": os.environ["JNJ_GENAI_API_KEY"],
"smtp_host": os.environ["SMTP_HOST"],
"smtp_port": os.environ.get("SMTP_PORT", "587"),
# "smtp_user": os.environ["SMTP_USER"],
# "smtp_password": os.environ["SMTP_PASSWORD"],
"smtp_from": os.environ["SMTP_FROM"],
# "smtp_use_tls": os.environ.get("SMTP_USE_TLS", "false"),
# "smtp_use_ssl": os.environ.get("SMTP_USE_SSL", "false"),
"allowed_email_domains": os.environ.get("ALLOWED_EMAIL_DOMAINS", ""),
"dq_dl_on_close": os.environ.get("DQ_DL_ON_CLOSE", ""),
"mcp_atlassian_url": os.environ.get("MCP_ATLASSIAN_URL", ""),
"x_atlassian_jira_url": os.environ.get("X_ATLASSIAN_JIRA_URL", "https://jira.jnj.com"),
"x_atlassian_jira_personal_token": os.environ.get("X_ATLASSIAN_JIRA_PERSONAL_TOKEN", ""),
"x_atlassian_username": os.environ.get("X_ATLASSIAN_USERNAME", ""),
"x_atlassian_read_only_mode": os.environ.get("X_ATLASSIAN_READ_ONLY_MODE", "false"),
"x_atlassian_enable_xray": os.environ.get("X_ATLASSIAN_ENABLE_XRAY", "false"),
}

for key, value in secrets.items():
    r = requests.post(
        f"{API_BASE}/secrets/put",
        headers=HEADERS,
        json={"scope": SCOPE, "key": key, "string_value": value}
    )
    r.raise_for_status()
    print(f"  Stored: {key}")

print("\nAll secrets stored successfully.")

# Store database credentials in the same Databricks Secrets scope
db_secrets = {
    "db_host":     os.environ["DB_HOST"],
    "db_port":     os.environ.get("DB_PORT", "5432"),
    "db_name":     os.environ["DB_NAME"],
    "db_user":     os.environ["DB_USER"],
    "db_password": os.environ["DB_PASSWORD"],
    "db_table":    os.environ.get("DB_TABLE", "public.dqm_dashboard_history_apac"),
}

for key, value in db_secrets.items():
    r = requests.post(
        f"{API_BASE}/secrets/put",
        headers=HEADERS,
        json={"scope": SCOPE, "key": key, "string_value": value}
    )
    r.raise_for_status()
    print(f"  Stored: {key}")

print("\nDatabase credentials stored successfully.")

r = requests.get(
    f"{API_BASE}/secrets/list",
    headers=HEADERS,
    params={"scope": SCOPE}
)
r.raise_for_status()

print(f"Secrets in scope '{SCOPE}':")
for secret in r.json().get("secrets", []):
    print(f" - {secret['key']} (last updated: {secret.get('last_updated_timestamp', 'N/A')})")

# import base64

# for secret in r.json().get("secrets", []):
#     r = requests.get(
#         f"{API_BASE}/secrets/get",
#         headers=HEADERS,
#         params={"scope": SCOPE, "key": secret["key"]}
#         )
#     r.raise_for_status()
#     secret_bytes_b64 = r.json().get("value", "")
#     secret_value = base64.b64decode(secret_bytes_b64).decode("utf-8")
#     print(f" - {secret['key']}: {secret_value} (last updated: {secret.get('last_updated_timestamp', 'N/A')})")
