# -*- coding: utf-8 -*-
"""
S9: Send Email Notifications for DQ Rule Events
=====================================================

Sends email notifications for two types of rule events:
1. Invalidated breaks (S7 successfully auto-invalidated stable anomalies breaking adaptive rules)
2. High-score ticket alerts (S7 detected new high-score breaks requiring attention)

Reads notification list from S7 (notification_list), retrieves recipient
email addresses from JIRA issue metadata, and sends customized email notifications
to dataset owners and stakeholders.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                  | Produced by | Description                              |
|-----------------------------|-------------|------------------------------------------|
| notification_list           | S7          | Invalidated breaks + high-score alerts   |
| issue_list_prepared         | S5          | Issue inventory (for recipient lookup)   |

Fallback Inputs (for backward compatibility)
----------------------------------------------
| Table name              | Produced by | Description                              |
|------------------------|-------------|------------------------------------------|
| invalidated_break_list  | S7          | Legacy invalidation results (if exists)  |

Outputs
-------
- Emails sent to dataset owners and assignees
- Console logs showing recipient list, subject, notification type

External APIs Called
--------------------
- SMTP Server
    - SMTP AUTH + TLS/SSL
    - SMTP.sendmail  – Send email notifications
- Jira REST API v2
    - GET /rest/api/2/issue/{key}  – Fetch issue assignee/reporter
- Collibra CDQ
    - GET /v3/profile/deltas  – (Optional) Dataset profile context

Email Notification Types
------------------------
1. invalidated_break (from S7):
   Subject: "[Data Quality] Adaptive rule invalidated, no further action needed - {dataset}"
   Message: Lists invalidated breaks, confirms stable anomalies, no action needed
   
2. high_score_ticket (from S7):
   Subject: "[Data Quality] High-score rule breaks detected - {dataset} requires attention"
   Message: Alerts to new high-score rule breaks, requests review/action

Recipients
-----------
- Primary: Issue assignees and reporters (from JIRA issue_list_prepared)
- Secondary: Distribution list (DL_ON_CLOSE) added
- Filters: Only valid email addresses within allowed domains (ALLOWED_DOMAINS)

Email Template Structure
------------------------
Each email includes:
- Dataset name and CDQ profile link
- List of detected or invalidated breaks
- Related JIRA tickets and their status
- Reason/context (invalidation rationale or alert reason)
- Call-to-action (depends on notification type)

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- smtp_host / smtp_port / smtp_from  (SMTP server)
- smtp_username / smtp_password  (SMTP auth, optional)
- smtp_use_tls / smtp_use_ssl  (TLS/SSL flags)
- smtp_ca_bundle  (CA cert bundle for SSL)
- jira_url / jira_api_token / jira_ca_bundle  (JIRA for recipient lookup)
- jira_verify_ssl  (SSL verification flag)
- cdq_base_url_apac  (CDQ dataset profile links)
- uc_catalog / uc_schema

Configuration Parameters
------------------------
- ALLOWED_DOMAINS: Email domain whitelist (space/comma-separated)
- DL_ON_CLOSE: Distribution list for ticket closure notifications
- EMAIL_RE: Regex for email validation

Processing Flow
---------------
1. Load notification_list (fallback to invalidated_break_list)
2. Group by dataset
3. For each group:
   a. Extract issue keys and notification type
   b. Look up recipients from issue_list_prepared
   c. Apply DL recipients if ticket closed
   d. Filter invalid emails (domain check, format validation)
   e. Build notification message (template based on type)
   f. Send via SMTP to all recipients
4. Log send results (success/failure per dataset)

Recipient Resolution Strategy
------------------------------
- Primary: From JIRA issue (assignee, reporter)
- Secondary: From issue_list_prepared normalized issue data
- Fallback: Dataset business unit owner (if available)
- Validation: Only addresses matching ALLOWED_DOMAINS

Backward Compatibility
----------------------
- Reads notification_list if available
- Falls back to invalidated_break_list for older S7 outputs
- Adds notification_type="invalidated_break" to legacy data
- Handles missing notification_type gracefully

Error Handling
--------------
- Continues on individual email send failures
- Logs recipients and subject for manual retry
- Validates recipients before sending (no silent failures)
- Graceful degradation if JIRA connectivity lost (uses fallback recipients)
"""

from __future__ import annotations

import logging
import os
import re
import smtplib
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
import urllib3

import config
from pipeline_io import PipelineIO

try:
    from databricks.sdk import WorkspaceClient

    dbutils = WorkspaceClient().dbutils
except Exception:
    dbutils = None


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WRITE_MODE = os.getenv("PIPELINE_WRITE_MODE", "csv").strip().lower()
LOCAL_OUTPUT_DIR = os.getenv("PIPELINE_LOCAL_OUTPUT_DIR", "./outputs")
SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")

def _load_secret_or_default(key: str, default: Any = None) -> Any:
    if dbutils is None:
        return default
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        return default


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}

uc_catalog = _load_secret_or_default("uc_catalog", getattr(config, "UC_CATALOG", None))
uc_schema = _load_secret_or_default("uc_schema", getattr(config, "UC_SCHEMA", None))

pipeline_io = PipelineIO(
    write_mode=WRITE_MODE,
    local_output_dir=LOCAL_OUTPUT_DIR,
    dbutils=dbutils,
    spark=globals().get("spark"),
    config_module=config,
    secret_scope=SECRET_SCOPE,
    uc_catalog=uc_catalog,
    uc_schema=uc_schema,
    logger=logger,
    sanitize_uc_table_names=True,
)
read_input = pipeline_io.read_input

raw_jira_url = _load_secret_or_default("jira_url", config.JIRA_URL)
jira_token = _load_secret_or_default("jira_api_token", config.JIRA_API_TOKEN)
jira_verify_ssl_raw = _load_secret_or_default("jira_verify_ssl", config.JIRA_VERIFY_SSL)
jira_ca_bundle = _load_secret_or_default("jira_ca_bundle", config.JIRA_CA_BUNDLE)

smtp_host = _load_secret_or_default("smtp_host", config.SMTP_HOST)
smtp_port = int(_load_secret_or_default("smtp_port", config.SMTP_PORT) or 587)
# smtp_user = _load_secret_or_default("smtp_user", config.SMTP_USER)
# smtp_password = _load_secret_or_default("smtp_password", config.SMTP_PASSWORD)
smtp_from = _load_secret_or_default("smtp_from", config.SMTP_FROM or os.getenv("SMTP_FROM"))
smtp_use_tls = parse_bool(_load_secret_or_default("smtp_use_tls", config.SMTP_USE_TLS), default=True)
smtp_use_ssl = parse_bool(_load_secret_or_default("smtp_use_ssl", config.SMTP_USE_SSL), default=False)

DL_ON_CLOSE = _load_secret_or_default("dq_dl_on_close", config.DQ_DL_ON_CLOSE)
ALLOWED_DOMAINS = [
    d.strip().lower()
    for d in str(_load_secret_or_default("allowed_email_domains", config.ALLOWED_EMAIL_DOMAINS or "")).split(",")
    if d.strip()
]

if not smtp_host or not smtp_from:
    raise ValueError(
        "SMTP configuration missing. Require smtp_host and smtp_from (via Databricks secrets or env vars)."
    )

if not raw_jira_url or not jira_token:
    logger.warning("JIRA_URL or JIRA_API_TOKEN missing; JIRA recipient fallback will be disabled")

jira_match = re.match(r"(https?://[^/]+)", str(raw_jira_url or "").strip())
JIRA_BASE = jira_match.group(1) if jira_match else ""
JIRA_VERIFY_SSL = parse_bool(jira_verify_ssl_raw, default=False)
JIRA_CA_BUNDLE = jira_ca_bundle

if not JIRA_VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _jira_verify_value() -> Any:
    if JIRA_VERIFY_SSL:
        return JIRA_CA_BUNDLE or True
    return False


SESSION = requests.Session()
if jira_token:
    SESSION.headers.update(
        {
            "Authorization": f"Bearer {jira_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )

EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def is_valid_email(addr: Any) -> bool:
    if not isinstance(addr, str):
        return False
    email = addr.strip()
    if not email or not EMAIL_RE.match(email):
        return False
    if ALLOWED_DOMAINS:
        try:
            domain = email.split("@", 1)[1].lower()
            return domain in ALLOWED_DOMAINS
        except Exception:
            return False
    return True


def jira_get_issue(issue_key: str, fields: str = "assignee,reporter,status") -> Dict[str, Any]:
    if not (JIRA_BASE and jira_token):
        return {}
    response = SESSION.get(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}",
        params={"fields": fields},
        timeout=30,
        verify=_jira_verify_value(),
    )
    if response.status_code >= 400:
        raise RuntimeError(f"JIRA GET failed for {issue_key}: HTTP {response.status_code}")
    return response.json()


def get_issue_recipients(issue_key: str, issue_list_df: pd.DataFrame) -> List[str]:
    group = issue_list_df[issue_list_df.get("key", pd.Series([], dtype="object")).astype(str).eq(issue_key)]
    emails = (
        group.get("assignee_email", pd.Series([], dtype="object"))
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )
    emails = [email for email in emails if is_valid_email(email)]

    if not group.empty and "project" in group.columns:
        try:
            if str(group["project"].iloc[0]).strip() == "ANGen":
                emails.extend(["KSee1@ITS.JNJ.com", "athangar@ITS.JNJ.com", "SSreede1@ITS.JNJ.com"])
        except Exception:
            pass

    if emails:
        out = []
        seen = set()
        for email in emails:
            if email not in seen:
                out.append(email)
                seen.add(email)
        return out

    try:
        issue = jira_get_issue(issue_key, fields="assignee,reporter")
        fields = issue.get("fields") or {}
        candidates = []

        assignee = fields.get("assignee") or {}
        if assignee.get("emailAddress"):
            candidates.append(assignee["emailAddress"])

        reporter = fields.get("reporter") or {}
        if reporter.get("emailAddress"):
            candidates.append(reporter["emailAddress"])

        out = []
        seen = set()
        for email in candidates:
            email = str(email).strip()
            if is_valid_email(email) and email not in seen:
                out.append(email)
                seen.add(email)
        if out:
            return out
    except Exception as exc:
        logger.warning("JIRA fallback failed for %s: %s", issue_key, exc)

    return []


def build_message(
    dataset_name: str,
    breaks: List[str],
    issue_keys: List[str],
    status_map: Dict[str, str],
    cdq_base_url: Optional[str],
    notification_type: str = "invalidated_break",
) -> str:
    def fmt_break(break_key: str) -> str:
        if "__" in break_key:
            col, typ = break_key.split("__", 1)
            return f"- {col} ({typ})"
        return f"- {break_key}"

    break_lines = [fmt_break(b) for b in sorted(set(b for b in breaks if isinstance(b, str) and b.strip()))]

    jira_lines = []
    for key in issue_keys:
        status = status_map.get(key, "Updated")
        if JIRA_BASE:
            jira_lines.append(f"- {JIRA_BASE}/browse/{key} ({status})")
        else:
            jira_lines.append(f"- {key} ({status})")

    cdq_line = "- None"
    if cdq_base_url:
        cdq_line = f"- {cdq_base_url.rstrip('/')}/dq/finding?dataset={dataset_name}"

    # Build message based on notification type
    if notification_type == "high_score_ticket":
        return f"""High-score rule breaks detected and require attention.

Dataset:
{dataset_name}
{cdq_line}

Detected breaks:
{chr(10).join(break_lines) if break_lines else '- None'}

JIRA tickets:
{chr(10).join(jira_lines) if jira_lines else '- None'}

Action Required:
These high-score rule breaks have been detected with high confidence score (>10).
Please review the dataset changes and assess whether these represent genuine
data quality issues that need to be addressed.

---
This is an automated message from the Databricks DQ monitoring workflow.
"""
    else:  # invalidated_break
        return f"""Data quality rule breaks for Adaptive rules have been invalidated.

Dataset:
{dataset_name}
{cdq_line}

Invalidated breaks:
{chr(10).join(break_lines) if break_lines else '- None'}

JIRA tickets:
{chr(10).join(jira_lines) if jira_lines else '- None'}

Reason:
DQ data profile changes persisted across multiple runs while row count
remained within normal fluctuation.

---
This is an automated message from the Databricks DQ invalidation workflow.
"""


def send_email(recipients: List[str], subject: str, message: str) -> None:
    msg = MIMEText(message, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = ", ".join(recipients)

    if smtp_use_ssl:
        with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
            # if smtp_user and smtp_password:
            #     server.login(smtp_user, smtp_password)
            server.sendmail(smtp_from, recipients, msg.as_string())
        return

    with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
        server.ehlo()
        if smtp_use_tls:
            server.starttls()
            server.ehlo()
        # if smtp_user and smtp_password:
        #     server.login(smtp_user, smtp_password)
        server.sendmail(smtp_from, recipients, msg.as_string())


def run() -> None:
    logger.info("Starting S9 email notification step")

    # Try reading from the new notification_list first, fall back to invalidated_break_list
    try:
        notification_list_df = read_input("notification_list")
    except Exception as e:
        logger.info(f"notification_list not found ({e}), falling back to invalidated_break_list")
        notification_list_df = read_input("invalidated_break_list")
        # Ensure notification_type is set for backward compatibility
        if "notification_type" not in notification_list_df.columns:
            notification_list_df["notification_type"] = "invalidated_break"
    
    issue_list_prepared_df = read_input("issue_list_prepared")

    if notification_list_df.empty:
        logger.info("No notifications found. Skipping email notifications.")
        return

    for dataset_name, group in notification_list_df.groupby("dataset", dropna=True):
        # Determine notification type for this group (assume all records in group have same type)
        notification_type = (
            group.get("notification_type", pd.Series(["invalidated_break"])).iloc[0]
            if "notification_type" in group.columns
            else "invalidated_break"
        )
        
        issue_keys = (
            group.get("issue_key", pd.Series([], dtype="object"))
            .dropna()
            .astype(str)
            .str.strip()
            .unique()
            .tolist()
        )
        
        # For high_score_ticket, we may not have issue keys yet, so use dataset-based recipients
        recipients: Set[str] = set()
        
        if issue_keys:
            for issue_key in issue_keys:
                for email in get_issue_recipients(issue_key, issue_list_prepared_df):
                    if is_valid_email(email):
                        recipients.add(email)
        else:
            # Fallback: use business unit or dataset owner info if available
            logger.debug(f"No issue keys found for dataset={dataset_name}, using default recipients")
        
        # should_close = False
        # try:
        #     should_close = bool(group.get("should_close", pd.Series([False])).astype(bool).any())
        # except Exception:
        #     should_close = False

        if DL_ON_CLOSE:
            for email in [x.strip() for x in str(DL_ON_CLOSE).split(",") if x.strip()]:
                if is_valid_email(email):
                    recipients.add(email)

        recipients_list = sorted(recipients)
        if not recipients_list:
            logger.info("Skipping dataset=%s: no valid recipients for %s", dataset_name, notification_type)
            continue

        # Extract breaks based on notification type
        # For invalidated_break: use break_key column
        # For high_score_ticket: use detected_breaks column (contains pipe-separated list)
        if notification_type == "high_score_ticket":
            detected_breaks_raw = group.get("detected_breaks", pd.Series([], dtype="object")).dropna().astype(str).tolist()
            # detected_breaks contains pipe-separated values like "col1__TYPE1 | col2__TYPE2"
            breaks = []
            for item in detected_breaks_raw:
                if isinstance(item, str) and item.strip():
                    # Split by pipe and clean up
                    for break_item in item.split("|"):
                        break_item = break_item.strip()
                        if break_item:
                            breaks.append(break_item)
            if not breaks:
                breaks.append(group.get("suggested_title", pd.Series(["No breaks detected"])).iloc[0])
        else:  # invalidated_break
            breaks = group.get("break_key", pd.Series([], dtype="object")).dropna().astype(str).tolist()

        status_map: Dict[str, str] = {}
        for issue_key in issue_keys:
            if notification_type == "high_score_ticket":
                # For high_score_ticket, we assume the ticket is newly created and thus "Updated"
                status_map[issue_key] = "Updated"
            else:
                # For invalidated_break, check if the ticket should be closed based on should_close column
                closed = bool(
                    group[group.get("issue_key", pd.Series([], dtype="object")).astype(str).eq(issue_key)]
                    .get("should_close", pd.Series([False]))
                    .astype(bool)
                    .any()
                )
                status_map[issue_key] = "Closed" if closed else "Updated"

        # Customize subject and message based on notification type
        if notification_type == "high_score_ticket":
            subject = f"[Data Quality] High-score rule breaks detected - {dataset_name} requires attention"
        else:  # invalidated_break
            subject = f"[Data Quality] Adaptive rule invalidated, no further action needed - {dataset_name}"
        
        message = build_message(
            dataset_name=str(dataset_name),
            breaks=breaks,
            issue_keys=issue_keys,
            status_map=status_map,
            cdq_base_url=_load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC),
            notification_type=notification_type,
        )

        try:
            logger.info("SMTP Host=%s", smtp_host)
            logger.info("SMTP Port=%s", smtp_port)
            logger.info("Recipients=%s", recipients_list)
            send_email(recipients_list, subject, message)
            logger.info("Sent dataset=%s (type=%s) to %s recipient(s)", dataset_name, notification_type, len(recipients_list))
        except Exception as exc:
            logger.error("Failed to send for dataset=%s: %s", dataset_name, exc)


if __name__ == "__main__":
    run()
