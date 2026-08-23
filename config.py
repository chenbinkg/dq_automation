"""
Configuration file for the DQM Automation.

This file is the SINGLE SOURCE OF TRUTH for all configuration.
It reads settings from environment variables and provides them as constants to the application.
This ensures that settings are managed in one place, making the application easier to configure and deploy.
"""

# Import the 'os' module to interact with the operating system, specifically for reading environment variables.
import os
# Load .env file for local development only; in Databricks env vars come from cluster/secrets config.
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# --- PostgreSQL RDS Database Configuration ---
# Retrieve the database host address from environment variables.
DB_HOST = os.environ.get("DB_HOST")
# Retrieve the database port, defaulting to "5432" if not specified.
DB_PORT = os.environ.get("DB_PORT", "5432")
# Retrieve the name of the database.
DB_NAME = os.environ.get("DB_NAME")
# Retrieve the database username.
DB_USER = os.environ.get("DB_USER")
# Retrieve the database password. This MUST be set in the production environment.
DB_PASSWORD = os.environ.get("DB_PASSWORD")
# Retrieve the name of the table containing the data assets.
DB_TABLE = os.environ.get("DB_TABLE", "dqm_dashboard_history_apac")

# --- Collibra DQ Configuration ---
CDQ_USERNAME_APAC = os.environ.get("CDQ_USERNAME_APAC")
CDQ_PASSWORD_APAC = os.environ.get("CDQ_PASSWORD_APAC")
CDQ_USERNAME_CN = os.environ.get("CDQ_USERNAME_CN")
CDQ_PASSWORD_CN = os.environ.get("CDQ_PASSWORD_CN")

# Backward-compatible aliases used by existing scripts.
COLLIBRA_USERNAME_APAC = CDQ_USERNAME_APAC
COLLIBRA_PASSWORD_APAC = CDQ_PASSWORD_APAC
COLLIBRA_USERNAME_CN = CDQ_USERNAME_CN
COLLIBRA_PASSWORD_CN = CDQ_PASSWORD_CN

CDQ_BASE_URL_APAC = os.environ.get("CDQ_BASE_URL_APAC")
CDQ_BASE_URL_CN = os.environ.get("CDQ_BASE_URL_CN")
CDQ_ENV = os.environ.get("CDQ_ENV")
CDQ_TOKEN_APAC = os.environ.get("CDQ_TOKEN_APAC")
CDQ_TOKEN_CN = os.environ.get("CDQ_TOKEN_CN")
CDQ_VERIFY_SSL = os.environ.get("CDQ_VERIFY_SSL", "false")
CDQ_CA_BUNDLE = os.environ.get("CDQ_CA_BUNDLE")

# --- JIRA Configuration ---
JIRA_USER_EMAIL = os.environ.get("JIRA_USER_EMAIL")
JIRA_API_TOKEN = os.environ.get("JIRA_API_TOKEN")
JIRA_URL = os.environ.get("JIRA_URL")
JIRA_PROJECT_KEYS = os.environ.get("JIRA_PROJECT_KEYS")
JIRA_VERIFY_SSL = os.environ.get("JIRA_VERIFY_SSL", "false")
JIRA_CA_BUNDLE = os.environ.get("JIRA_CA_BUNDLE")
JIRA_HTTP_TOTAL_RETRIES = int(os.environ.get("JIRA_HTTP_TOTAL_RETRIES", "5"))
JIRA_HTTP_CONNECT_RETRIES = int(os.environ.get("JIRA_HTTP_CONNECT_RETRIES", "5"))
JIRA_HTTP_READ_RETRIES = int(os.environ.get("JIRA_HTTP_READ_RETRIES", "5"))
JIRA_HTTP_BACKOFF_FACTOR = float(os.environ.get("JIRA_HTTP_BACKOFF_FACTOR", "1.0"))

# --- Notification Configuration ---
DQ_DL_ON_CLOSE = os.environ.get("DQ_DL_ON_CLOSE")
SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = os.environ.get("SMTP_PORT", "587")
SMTP_FROM = os.environ.get("SMTP_FROM")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true")
SMTP_USE_SSL = os.environ.get("SMTP_USE_SSL", "false")
ALLOWED_EMAIL_DOMAINS = os.environ.get("ALLOWED_EMAIL_DOMAINS")

# --- TABLEAU Configuration ---
TABLEAU_CLOUD_PROD_URL = os.environ.get("TABLEAU_CLOUD_PROD_URL")
TABLEAU_CLOUD_PROD_TOKEN_NAME = os.environ.get("TABLEAU_CLOUD_PROD_TOKEN_NAME")
TABLEAU_CLOUD_PROD_TOKEN_VALUE = os.environ.get("TABLEAU_CLOUD_PROD_TOKEN_VALUE")
TABLEAU_CLOUD_SITE_ID = os.environ.get("TABLEAU_CLOUD_SITE_ID")
TABLEAU_CLOUD_PROJECT_PATH = os.environ.get("TABLEAU_CLOUD_PROJECT_PATH")
TABLEAU_CLOUD_DATASOURCE_NAME = os.environ.get(
	"TABLEAU_CLOUD_DATASOURCE_NAME", "DQM_CDQ_DATASET_BY_DATA_DOMAIN"
)
TABLEAU_CLOUD_PUBLISH_MODE = os.environ.get("TABLEAU_CLOUD_PUBLISH_MODE", "Overwrite")
TABLEAU_VERIFY_SSL = os.environ.get("TABLEAU_VERIFY_SSL", "false")

# --- GENAI API KEY ---
JNJ_GENAI_API_KEY = os.environ.get("JNJ_GENAI_API_KEY")
MCP_ATLASSIAN_URL = os.environ.get("MCP_ATLASSIAN_URL")
X_ATLASSIAN_JIRA_URL = os.environ.get("X_ATLASSIAN_JIRA_URL")
X_ATLASSIAN_JIRA_PERSONAL_TOKEN = os.environ.get("X_ATLASSIAN_JIRA_PERSONAL_TOKEN")
X_ATLASSIAN_USERNAME = os.environ.get("X_ATLASSIAN_USERNAME")
X_ATLASSIAN_READ_ONLY_MODE = os.environ.get("X_ATLASSIAN_READ_ONLY_MODE", "false")
X_ATLASSIAN_ENABLE_XRAY = os.environ.get("X_ATLASSIAN_ENABLE_XRAY", "false")
