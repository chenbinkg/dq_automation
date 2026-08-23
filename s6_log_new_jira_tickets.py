# -*- coding: utf-8 -*-
"""
S6: Create or Reopen JIRA Tickets from Candidate Outputs
=========================================================

Creates new DQ JIRA tickets or reopens existing tickets based on candidate lists
from S5. Implements smart subtask reuse for adaptive tickets to keep summaries
stable across runs while updating details in descriptions/comments.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                              | Produced by | Description                    |
|-----------------------------------------|-------------|--------------------------------|
| potential_jira_ticket_list              | S5          | Non-adaptive ticket candidates |
| potential_adaptive_rule_jira_ticket_list| S5          | Adaptive ticket candidates     |
| potential_dupe_jira_ticket_list         | S5          | Duplicate ticket candidates    |
| potential_pattern_jira_ticket_list      | S5          | Pattern ticket candidates      |
| issue_list_prepared                     | S5          | Active issue inventory         |
| business_unit_mapping                   | S2          | Dataset → BU metadata          |

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name           | Description                                          |
|----------------------|------------------------------------------------------|
| new_jira_ticket_list | Tickets created/reopened in this run (for S7, S8)    |

External APIs Called
--------------------
- Jira REST API v2/v3
    - POST /rest/api/2/issue  – Create new tickets
    - GET /rest/api/2/issue/{key}  – Fetch issue details
    - POST /rest/api/2/issue/{key}/comment  – Add comments
    - POST /rest/api/2/issue/{key}/transitions  – Reopen/transition
    - GET /rest/api/2/issue/createmeta  – Fetch field metadata

Ticket Creation Logic
---------------------
1. Parse candidate lists from S5
2. For each candidate:
   a. Check if related subtask already exists (for adaptive)
   b. If adaptive + active subtask exists: Update description/comment (don't change summary)
   c. If new: Create new subtask with generated title and description
   d. If closed: Reopen with comment
3. Populate standard DQ fields:
   - Summary: From candidate title
   - Description: Formatted with dataset, business unit, metric breakdown
   - Labels: "collibra", "dq", "adaptive" (for adaptive)
   - Priority: Mapped from candidate severity score
4. Link to parent dataset task if exists
5. Write new_jira_ticket_list for downstream stages

Adaptive Ticket Reuse Strategy
------------------------------
- Prefers updating existing active adaptive subtask over creating duplicate
- Keeps ticket summary stable across multiple runs
- Updates details via comments tagged with [AUTO_UPDATE]
- Searches by dataset parent + "adaptive" label + active status
- Falls back to creating new if no active match found

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- jira_url
- jira_api_token
- jira_ca_bundle  (optional, for SSL verification)
- uc_catalog / uc_schema  (required when PIPELINE_WRITE_MODE != csv)

Ticket Exclusions
-----------------
- Data-Product project tickets excluded during creation (APAC only for now)
- China datasets excluded

TO-DO
-----
- Add ticket field customization for different business units
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
import urllib3

import config
from pipeline_io import PipelineIO
from token_manager import CollibraTokenManager

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


def _jira_verify_value() -> Any:
    if JIRA_VERIFY_SSL:
        return JIRA_CA_BUNDLE or True
    return False


def _cdq_verify_value() -> Any:
    if CDQ_VERIFY_SSL:
        return CDQ_CA_BUNDLE or True
    return False


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
write_output = pipeline_io.write_output


raw_jira_url = _load_secret_or_default("jira_url", config.JIRA_URL)
jira_token = _load_secret_or_default("jira_api_token", config.JIRA_API_TOKEN)
project_key = _load_secret_or_default("jira_project_keys", config.JIRA_PROJECT_KEYS)
jira_verify_ssl_raw = _load_secret_or_default("jira_verify_ssl", config.JIRA_VERIFY_SSL)
jira_ca_bundle = _load_secret_or_default("jira_ca_bundle", config.JIRA_CA_BUNDLE)

cdq_base_url = _load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC)
cdq_username = _load_secret_or_default("username_apac", config.CDQ_USERNAME_APAC)
cdq_password = _load_secret_or_default("password_apac", config.CDQ_PASSWORD_APAC)
cdq_verify_ssl_raw = _load_secret_or_default("cdq_verify_ssl", config.CDQ_VERIFY_SSL)
cdq_ca_bundle = _load_secret_or_default("cdq_ca_bundle", config.CDQ_CA_BUNDLE)

if not raw_jira_url or not jira_token:
    raise ValueError("JIRA_URL and JIRA_API_TOKEN must be configured")
if not cdq_base_url or not cdq_username or not cdq_password:
    raise ValueError("CDQ_BASE_URL_APAC, CDQ_USERNAME_APAC, and CDQ_PASSWORD_APAC must be configured")

match = re.match(r"(https?://[^/]+)", str(raw_jira_url).strip())
if not match:
    raise ValueError("Invalid JIRA URL format")

JIRA_BASE = match.group(1)
API_TOKEN = str(jira_token)
PROJECT_KEY = str(project_key)
DEFAULT_TASK_TYPE_ID = str(os.getenv("JIRA_TASK_TYPE_ID", "3"))
DEFAULT_SUBTASK_TYPE_ID = str(os.getenv("JIRA_SUBTASK_TYPE_ID", "5"))

JIRA_VERIFY_SSL = parse_bool(jira_verify_ssl_raw, default=False)
JIRA_CA_BUNDLE = jira_ca_bundle
CDQ_VERIFY_SSL = parse_bool(cdq_verify_ssl_raw, default=False)
CDQ_CA_BUNDLE = cdq_ca_bundle

if not JIRA_VERIFY_SSL or not CDQ_VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

TIMEOUT = (5, 30)

cdq_token_mgr = CollibraTokenManager(
    base_url=str(cdq_base_url),
    username=str(cdq_username),
    password=str(cdq_password),
    region="apac",
)


def _cdq_get(path: str, params: Optional[Dict[str, Any]] = None, timeout: tuple = TIMEOUT) -> requests.Response:
    url = f"{str(cdq_base_url).rstrip('/')}{path}"
    headers = cdq_token_mgr.get_auth_header()
    response = requests.get(url, headers=headers, params=params, verify=_cdq_verify_value(), timeout=timeout)
    if response.status_code == 401:
        logger.warning("CDQ token expired. Refreshing token and retrying request: %s", path)
        cdq_token_mgr.get_token(force_refresh=True)
        headers = cdq_token_mgr.get_auth_header()
        response = requests.get(url, headers=headers, params=params, verify=_cdq_verify_value(), timeout=timeout)
    return response

# Existing JIRA Epic names
EPIC_CATALOG = {
    "E.AI": "Engagement.AI (E.AI)",
    "MDM": "Product MDM (Maestro)",
    "DATA ENRICHMENT": "Data Enrichment",
    "ANGEN": "Analytics Next Generation (ANGen)",
    "PATHFINDER": "Pathfinder",
    "DATA PRODUCTS": "Data Products",
    "OTHER": "Other",
}

# Assignee by topic
ASSIGNEE_CATALOG = {
    "E.AI": "YShao12",
    "MDM": "ylee103",
    "DATA ENRICHMENT": "YShao12",
    "ANGEN": "PChua1",
    "ANGEN MAF": "XWang510",
    "PATHFINDER": "PChua1",
    "DATA PRODUCT": "JIshika5",
    "Market Definition": "ylee103",
    "CDE": "nsakamot",
    "CAE": "ahsieh5",
    "IT": "TToyama1",
    "OTHER": "DDenny1",
}

# Threshold for raising JIRA
DQ_SCORE_THRESH_DEFAULT = 1
DQ_SCORE_THRESH = {
    "ds_ods_mdm_itg_mdm_indication_split_ratio": 0,
    "ds_ods_mdm_itg_mdm_market_definition": 0,
}

ACTIVE_JIRA_STATUSES = {"open", "to do", "in progress", "reopened"}

SESSION = requests.Session()
SESSION.headers.update(
    {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    }
)

_JIRA_RETRY_ATTEMPTS = 5
_JIRA_RETRY_BASE_DELAY = 10.0  # seconds
# 429 = rate limit, 5xx = transient gateway/proxy failures in front of Jira
_JIRA_RETRY_STATUSES = {429, 500, 502, 503, 504}


def _handle_retryable_response(response: requests.Response, attempt: int) -> None:
    """Sleep before retrying a rate-limited or transient server error, honouring Retry-After if present."""
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            delay = float(retry_after)
        except ValueError:
            delay = _JIRA_RETRY_BASE_DELAY * (2 ** attempt)
    else:
        delay = _JIRA_RETRY_BASE_DELAY * (2 ** attempt)
    logger.warning(
        "Jira returned %s. Retrying in %.1fs (attempt %d/%d)...",
        response.status_code,
        delay,
        attempt + 1,
        _JIRA_RETRY_ATTEMPTS,
    )
    time.sleep(delay)


def _post(url: str, payload: Dict[str, Any]) -> Any:
    for attempt in range(_JIRA_RETRY_ATTEMPTS):
        response = SESSION.post(
            f"{JIRA_BASE}{url}",
            data=json.dumps(payload),
            timeout=60,
            verify=_jira_verify_value(),
        )
        if response.status_code in _JIRA_RETRY_STATUSES and attempt < _JIRA_RETRY_ATTEMPTS - 1:
            _handle_retryable_response(response, attempt)
            continue
        if response.status_code >= 400:
            logger.error("Jira error: %s %s", response.status_code, response.text[:1000])
        response.raise_for_status()
        if response.text and response.text.strip():
            return response.json()
        return None
    return None  # unreachable but satisfies type checkers


def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    for attempt in range(_JIRA_RETRY_ATTEMPTS):
        response = SESSION.get(
            f"{JIRA_BASE}{url}",
            params=params,
            timeout=30,
            verify=_jira_verify_value(),
        )
        if response.status_code in _JIRA_RETRY_STATUSES and attempt < _JIRA_RETRY_ATTEMPTS - 1:
            _handle_retryable_response(response, attempt)
            continue
        if response.status_code >= 400:
            logger.error("Jira error: %s %s", response.status_code, response.text[:1000])
        response.raise_for_status()
        if response.text and response.text.strip():
            return response.json()
        return None
    return None


def _put(url: str, payload: Dict[str, Any]) -> Any:
    for attempt in range(_JIRA_RETRY_ATTEMPTS):
        response = SESSION.put(
            f"{JIRA_BASE}{url}",
            data=json.dumps(payload),
            timeout=60,
            verify=_jira_verify_value(),
        )
        if response.status_code in _JIRA_RETRY_STATUSES and attempt < _JIRA_RETRY_ATTEMPTS - 1:
            _handle_retryable_response(response, attempt)
            continue
        if response.status_code >= 400:
            logger.error("Jira error: %s %s", response.status_code, response.text[:1000])
        response.raise_for_status()
        if response.text and response.text.strip():
            return response.json()
        return None
    return None


def _search_jql(jql: str, fields: Optional[Union[List[str], str]] = None, max_results: int = 50) -> Any:
    if fields is None:
        fields_list: Union[str, List[str]] = ["summary", "key"]
    elif isinstance(fields, str):
        if fields.strip() in ("*all", "*navigable"):
            fields_list = fields.strip()
        else:
            fields_list = [field.strip() for field in fields.split(",") if field.strip()]
    else:
        fields_list = list(fields)

    payload = {"jql": jql, "maxResults": max_results, "fields": fields_list}
    return _post("/rest/api/2/search", payload)


# ---------- Field discovery ----------
def get_epic_link_customfield_id() -> Optional[str]:
    data = _get("/rest/api/2/field")
    for field in data:
        if field.get("name") == "Epic Link" and field.get("schema", {}).get("custom"):
            return field.get("id")
    return None


def get_versions_map(project_key: str) -> Dict[str, str]:
    versions = _get(f"/rest/api/2/project/{project_key}/versions")
    return {version["name"]: version["id"] for version in versions if not version.get("archived", False)}


def get_components_map(project_key: str) -> Dict[str, str]:
    components = _get(f"/rest/api/2/project/{project_key}/components")
    return {component["name"]: component["id"] for component in components}


# ---------- BU Mapping ----------
def region_from_business_unit(bu_name: Any) -> Optional[str]:
    if not isinstance(bu_name, str):
        return None
    return bu_name.split(" - ")[0].strip().upper()


def _capability_from_bu_name(bu_name: Optional[str]) -> str:
    if not isinstance(bu_name, str) or not bu_name.strip():
        return ""
    parts = [part.strip() for part in bu_name.split(" - ", 1)]
    capability = parts[1] if len(parts) == 2 else parts[0]
    return capability.upper()


def map_business_unit_to_component_name(bu_name: Optional[str]) -> str:
    cap = _capability_from_bu_name(bu_name)
    cap_norm = cap.replace("_", " ").replace("-", " ").strip()

    if "ANGEN" in cap and "MAF" in cap:
        return "ANGEN MAF"
    if "ANGEN" in cap:
        return "ANGen"
    if "E.AI" in cap or "EAI" in cap_norm:
        return "E.AI"
    if "MARKET DEFINITION" in cap_norm:
        return "MDM"
    if "MDM" in cap:
        return "MDM"
    if "DATA ENRICHMENT" in cap_norm:
        return "Data-Enrichment"
    if "PATHFINDER" in cap:
        return "Pathfinder"
    if "DATA PRODUCT" in cap_norm:
        return "Data-Product"
    if "ACE MEDICAL" in cap_norm:
        return "ACE-Medical"

    return "Other"


def map_business_unit_to_epic_name(bu_name: Optional[str]) -> str:
    cap = _capability_from_bu_name(bu_name)
    cap_norm = cap.replace("_", " ").replace("-", " ")

    if "E.AI" in cap or "EAI" in cap_norm:
        return EPIC_CATALOG["E.AI"]
    if "MDM" in cap:
        return EPIC_CATALOG["MDM"]
    if "DATA ENRICHMENT" in cap_norm:
        return EPIC_CATALOG["DATA ENRICHMENT"]
    if "ANGEN" in cap:
        return EPIC_CATALOG["ANGEN"]
    if "PATHFINDER" in cap:
        return EPIC_CATALOG["PATHFINDER"]
    if "DATA PRODUCT" in cap_norm:
        return EPIC_CATALOG["DATA PRODUCTS"]
    if "MARKET DEFINITION" in cap_norm:
        return EPIC_CATALOG["MDM"]

    return EPIC_CATALOG["OTHER"]


def map_business_unit_to_assignee(bu_name: Optional[str]) -> Optional[str]:
    cap = _capability_from_bu_name(bu_name)
    cap_norm = cap.replace("_", " ").replace("-", " ").strip()

    if "ANGEN" in cap and "MAF" in cap:
        return ASSIGNEE_CATALOG["ANGEN MAF"]
    if "ANGEN" in cap:
        return ASSIGNEE_CATALOG["ANGEN"]
    if "E.AI" in cap or "EAI" in cap_norm:
        return ASSIGNEE_CATALOG["E.AI"]
    if "MDM" in cap:
        return ASSIGNEE_CATALOG["MDM"]
    if "DATA ENRICHMENT" in cap_norm:
        return ASSIGNEE_CATALOG["DATA ENRICHMENT"]
    if "PATHFINDER" in cap:
        return ASSIGNEE_CATALOG["PATHFINDER"]
    if "DATA PRODUCT" in cap_norm:
        return ASSIGNEE_CATALOG["DATA PRODUCT"]
    if "MARKET DEFINITION" in cap_norm:
        return ASSIGNEE_CATALOG["Market Definition"]
    if "CDE" in cap:
        return ASSIGNEE_CATALOG["CDE"]
    if "CAE" in cap:
        return ASSIGNEE_CATALOG["CAE"]
    if "IT" in cap:
        return ASSIGNEE_CATALOG["IT"]

    return ASSIGNEE_CATALOG.get("OTHER")


# ---------- Parent Task ----------
def _sanitize_for_text_query(phrase: str) -> str:
    return re.sub(r"[+\-&|!(){}\[\]^~*?\\:]", " ", phrase).strip()


def find_parent_task_key(project_key: str, dataset_name: str) -> Optional[str]:
    safe_phrase = _sanitize_for_text_query(dataset_name)
    jql = (
        f'project = "{project_key}" AND issuetype = Task '
        f'AND summary ~ "{safe_phrase}" ORDER BY created DESC'
    )

    result = _get("/rest/api/2/search", params={"jql": jql, "maxResults": 20, "fields": "summary,key"})

    for issue in (result or {}).get("issues", []):
        if issue.get("fields", {}).get("summary") == dataset_name:
            return issue.get("key")
    return None


def build_labels_for_task() -> List[str]:
    return ["quality"]


def find_epic_by_exact_name(project_key: str, epic_name: str) -> Optional[str]:
    epic_name_clean = epic_name.replace('"', '\\"')
    jql = (
        f'project = "{project_key}" AND issuetype = Epic '
        f'AND "Epic Name" = "{epic_name_clean}" '
        f"ORDER BY updated DESC"
    )
    response = _search_jql(jql, fields=["key", "summary"], max_results=3)
    issues = response.get("issues", [])
    return issues[0]["key"] if issues else None


def create_parent_task(
    project_key: str,
    dataset_name: str,
    epic_link_field_id: Optional[str],
    version_id: Optional[str],
    component_obj: Optional[Dict[str, Any]],
    business_unit_by_dataset: Dict[str, str],
) -> str:
    fields: Dict[str, Any] = {
        "project": {"key": project_key},
        "issuetype": {"id": DEFAULT_TASK_TYPE_ID},
        "summary": dataset_name,
        "description": f"Auto-created parent Task for dataset '{dataset_name}' (Collibra DQ).",
        "labels": build_labels_for_task(),
    }
    if version_id:
        fields["versions"] = [{"id": version_id}]
    if component_obj:
        fields["components"] = [component_obj]

    bu_name = business_unit_by_dataset.get(dataset_name)
    epic_name = map_business_unit_to_epic_name(bu_name) if bu_name else "Other"
    epic_key = find_epic_by_exact_name(project_key, epic_name)
    if epic_link_field_id and epic_key:
        fields[epic_link_field_id] = epic_key

    payload = {"fields": fields}
    response = _post("/rest/api/2/issue", payload)
    return response["key"]


# ---------- Sub-task ----------
def find_subtask_keys_by_parent(
    project_key: str,
    parent_key: str,
    subtask_summary_hint: Optional[str] = None,
    max_results: int = 50,
) -> List[str]:
    jql = f'project = "{project_key}" AND issuetype = "Sub-task" AND parent = "{parent_key}"'
    if subtask_summary_hint:
        safe = _sanitize_for_text_query(subtask_summary_hint)
        jql += f' AND summary ~ "{safe}"'
    jql += " ORDER BY created DESC"

    result = _get("/rest/api/2/search", params={"jql": jql, "maxResults": max_results, "fields": "summary,key,parent"})
    return [item["key"] for item in (result or {}).get("issues", [])]


def _labels_to_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text.replace("'", '"'))
                if isinstance(parsed, list):
                    return [str(item).strip().lower() for item in parsed if str(item).strip()]
            except Exception:
                pass
        return [part.strip().lower() for part in text.split(",") if part.strip()]
    return []


def find_active_subtask_by_parent(
        project_key: str, 
        parent_key: str, 
        ticket_source: str
        ) -> Optional[Dict[str, str]]:
    jql = (
        f'project = "{project_key}" AND issuetype = "Sub-task" AND parent = "{parent_key}" '
        f'AND status in ("Open", "To Do", "In Progress", "Reopened") ORDER BY updated DESC'
    )
    result = _get("/rest/api/2/search", params={"jql": jql, "maxResults": 50, "fields": "summary,key,labels,status"})

    for issue in (result or {}).get("issues", []):
        fields = issue.get("fields", {})
        labels = _labels_to_list(fields.get("labels"))
        status = str((fields.get("status") or {}).get("name", "")).strip().lower()
        if ticket_source in labels and status in ACTIVE_JIRA_STATUSES:
            return {"key": issue.get("key", ""), "summary": str(fields.get("summary") or "").strip()}
    return None


def format_subtask_summary(row: pd.Series) -> str:
    rule_type = str(row.get("ruleType", "")).strip() or "RULE"
    rule_name = str(row.get("ruleNm", "")).strip()
    return f"[{rule_type}] {rule_name}" if rule_name else rule_type


def create_subtask(
    project_key: str,
    parent_key: str,
    summary: str,
    description: str,
    version_id: Optional[str],
    labels: List[str],
    component_obj: Optional[Dict[str, Any]] = None,
    assignee: Optional[str] = None,
) -> str:
    fields: Dict[str, Any] = {
        "project": {"key": project_key},
        "issuetype": {"id": DEFAULT_SUBTASK_TYPE_ID},
        "parent": {"key": parent_key},
        "summary": summary[:254],
        "description": description,
        "labels": labels,
    }
    if version_id:
        fields["versions"] = [{"id": version_id}]
    if component_obj:
        fields["components"] = [component_obj]
    if assignee:
        fields["assignee"] = {"name": assignee}

    payload = {"fields": fields}
    response = _post("/rest/api/2/issue", payload)
    return response["key"]


def get_pushdown_job_flag(dataset_name: str):
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        return None, None, "dataset_name must be a non-empty string"

    params = {"datasetName": dataset_name}

    try:
        response = _cdq_get("/v2/get-is-pushdown", params=params, timeout=TIMEOUT)
        status = response.status_code
        if status == 200:
            try:
                data = response.json()
            except ValueError:
                return None, status, "Invalid JSON"
            return data, status, None
        error = response.text[:500] if isinstance(response.text, str) else str(response.content)[:500]
        return None, status, error
    except requests.exceptions.RequestException as exc:
        return None, None, f"RequestException: {exc}"


def get_breaking_records(dataset_name: str, rule_name: str, run_id: str, limit: int = 6):
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        return None, None, "dataset_name must be a non-empty string"
    if not isinstance(rule_name, str) or not rule_name.strip():
        return None, None, "rule_name must be a non-empty string"
    if not isinstance(run_id, str) or not run_id.strip():
        return None, None, "run_id must be a non-empty string"

    params = {
        "dataset": dataset_name,
        "rowkey": rule_name,
        "runId": run_id,
        "length": str(int(limit) if limit and limit > 0 else 6),
        "draw": str(int(1)),
        "start": str(int(0)),
    }

    try:
        response = _cdq_get("/v2/getrulesdatapreviewpaging", params=params, timeout=TIMEOUT)
        status = response.status_code
        if status == 200:
            try:
                data = response.json()
                data1 = data.get("dataAssetList", [])
            except ValueError:
                return None, status, "Invalid JSON"
            if not isinstance(data1, list):
                return None, status, "Unexpected payload (expected a list)"
            return data1, status, None
        error = response.text[:500] if isinstance(response.text, str) else str(response.content)[:500]
        return None, status, error
    except requests.exceptions.RequestException as exc:
        return None, None, f"RequestException: {exc}"


def format_breaking_records_dynamic(
    records: List[Dict[str, Any]],
    *,
    break_columns: Optional[str] = None,
    max_rows: int = 6,
    max_cols: int = 10,
    drop_keys: Optional[List[str]] = None,
    ensure_keys: Optional[List[str]] = None,
    max_cell_len: int = 80,
    output: str = "table",
) -> str:
    if not records:
        return "_No sample breaking records returned._"

    drop_keys = list(set((drop_keys or ["dataset", "runId", "ruleName"])))
    ensure_keys = list(ensure_keys or ["linkId"])

    rows = [row for row in records if isinstance(row, dict)][:max_rows]
    if not rows:
        return "_No sample breaking records returned._"

    def fmt_cell(value: Any) -> str:
        text = str(value).replace("\n", " ").strip()
        if len(text) > max_cell_len:
            text = text[: max_cell_len - 3] + "..."
        return text.replace("|", "\\|")

    all_cols = list(rows[0].keys())
    dynamic_cols = [column for column in all_cols if column not in (ensure_keys + drop_keys)]
    cols = ensure_keys + dynamic_cols[:max_cols]

    breaking_header = ""
    if break_columns and str(break_columns).strip():
        breaking_header = f"*Break column(s):* {str(break_columns).strip()}\n\n"

    if output == "bullets":
        blocks = [breaking_header.rstrip()] if breaking_header else []
        for idx, row in enumerate(rows, 1):
            blocks.append(f"- *#{idx}*")
            for column in cols:
                blocks.append(f"  - *{column}*: {fmt_cell(row.get(column))}")
        return "\n".join([block for block in blocks if block])

    header = "| # | " + " | ".join(cols) + " |"
    sep = "|---|" + "|".join(["---"] * len(cols)) + "|"
    lines = [breaking_header.rstrip()] if breaking_header else []
    lines += [header, sep]

    for idx, row in enumerate(rows, 1):
        vals = [fmt_cell(row.get(column)) for column in cols]
        lines.append("| " + str(idx) + " | " + " | ".join(vals) + " |")

    return "\n".join([line for line in lines if line])


def make_invalid_sample_table(dataset: str, rule_name: str, run_id: str, break_columns: Optional[str] = None) -> str:
    records, status, err = get_breaking_records(dataset, rule_name, run_id, limit=6)
    if not records:
        if err:
            return f"_No sample breaking records returned._ (API: {status}, {err})"
        return "_No sample breaking records returned._"

    return format_breaking_records_dynamic(
        records,
        break_columns=break_columns,
        max_rows=6,
        max_cols=10,
        drop_keys=["dataset", "runId", "ruleName", "owl_id"],
        ensure_keys=[break_columns] if break_columns else None,
        output="table",
    )


def summarize_break_summary(
    break_count: Optional[Union[int, float]],
    break_percentage: Optional[float],
    *,
    decimals: int = 4,
) -> str:
    if break_count is None or pd.isna(break_count):
        return "_No breaking records._"

    try:
        cnt = int(break_count)
    except Exception:
        return "_No breaking records._"

    if break_percentage is None or pd.isna(break_percentage):
        return f"{cnt} (-)"

    pct = round(float(break_percentage), decimals)
    fmt = f"{{:.{decimals}f}}"
    return f"{cnt} ({fmt.format(pct)}%)"


def get_finding_report(dataset_name: str, run_id: Optional[str] = None) -> Any:
    if not isinstance(dataset_name, str) or not dataset_name.strip():
        raise ValueError("dataset_name must be a non-empty string")

    params: Dict[str, str] = {"dataset": dataset_name}
    if run_id:
        params["runId"] = run_id

    try:
        response = _cdq_get("/v2/getDatasetFindingsReport", params=params, timeout=TIMEOUT)
    except requests.RequestException as exc:
        raise requests.RequestException(f"Error calling Collibra API: {exc}") from exc

    if response.status_code >= 400:
        snippet = response.text[:1000] if isinstance(response.text, str) else str(response.content)[:1000]
        raise requests.HTTPError(
            f"Collibra API returned HTTP {response.status_code} for {response.url}.\n"
            f"Body (truncated): {snippet}"
        )

    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(f"Invalid JSON returned by Collibra API for {response.url}: {exc}") from exc


def get_job_schedule(dataset_name: str):
    encoded = quote(dataset_name, safe="")

    try:
        response = _cdq_get(f"/v3/datasetDefs/{encoded}", timeout=TIMEOUT)
        status = response.status_code

        if status == 200:
            try:
                data = response.json()
            except ValueError:
                logger.warning("[CDQ] Invalid JSON for /v3/datasetDefs/%s (HTTP 200)", encoded)
                return None, status, "Invalid JSON"
            return data, status, None

        body = ""
        try:
            body = response.text if isinstance(response.text, str) else ""
        except Exception:
            body = ""
        if not body:
            body = f"(empty body) reason={getattr(response, 'reason', '')}"

        err = body[:500]
        logger.warning("[CDQ] Request failed: HTTP %s %s", status, getattr(response, "reason", ""))
        logger.warning("[CDQ] URL: %s", response.url)
        logger.warning("[CDQ] Body (trunc): %s", err)
        return None, status, err
    except requests.exceptions.RequestException as exc:
        logger.warning("[CDQ] RequestException calling /v3/datasetDefs/%s: %s", encoded, exc)
        return None, None, f"RequestException: {exc}"


def get_table_info(dataset: str, run_id: Optional[str]) -> Dict[str, Any]:
    info = {"kind": "Unknown", "name": None, "db": None, "host": None, "path": None}

    try:
        finding_report = get_finding_report(dataset, run_id=run_id)
        if isinstance(finding_report, list) and finding_report:
            for item in finding_report:
                dataset_report = item.get("datasetReport") or {}
                if dataset_report:
                    info["db"] = dataset_report.get("db_nm")
                    info["name"] = dataset_report.get("table_nm")
                    info["host"] = dataset_report.get("host")
                    info["kind"] = "Table" if info["name"] else "Table/View"
                    break
    except Exception:
        pass

    try:
        if not info["name"]:
            payload, _, _ = get_job_schedule(dataset)
            if payload:
                source = (payload or {}).get("source") or {}
                file_path = source.get("filePath")
                if file_path:
                    info["kind"] = "File"
                    info["path"] = file_path
                    info["name"] = file_path.split("/")[-1]
                if not info["db"]:
                    info["db"] = source.get("connectionName") or source.get("dataset")
    except Exception:
        pass

    return info


def make_cdq_link(dataset: str) -> str:
    base = cdq_base_url.rstrip("/")
    return f"{base}/dq/finding?dataset={dataset}"


def make_subtask_description(
    dataset: str,
    run_id: str,
    rule_name: str,
    rule_value: Optional[str],
    score: Optional[float],
    perc: Optional[float],
    dim_name: Optional[str],
    break_count: Optional[int],
    break_columns: Optional[str],
) -> str:
    table_info = get_table_info(dataset, run_id)
    table_lines: List[str] = []
    if table_info["kind"] == "File":
        table_lines.append("- *Type*: File")
        if table_info["path"]:
            table_lines.append(f"- *Path*: `{table_info['path']}`")
    else:
        table_lines.append(f"- *Type*: {table_info['kind']}")
        if table_info["db"]:
            table_lines.append(f"- *DB*: `{table_info['db']}`")
        if table_info["name"]:
            table_lines.append(f"- *Object*: `{table_info['name']}`")
        if table_info["host"]:
            table_lines.append(f"- *Host*: `{table_info['host']}`")

    # pushdown_result, _, _ = get_pushdown_job_flag(dataset)
    # pushdown_flag = bool(pushdown_result and pushdown_result.get("result"))

    # if pushdown_flag:
    #     invalid_samples = "_Pushdown Job: No sample breaking records available._"
    # else:
    invalid_samples = make_invalid_sample_table(dataset, rule_name, run_id, break_columns)

    break_summary = summarize_break_summary(break_count, perc)
    cdq_link = make_cdq_link(dataset)

    header: List[str] = [
        f"*Dataset:* {dataset}",
        f"*Run ID:* {run_id}",
        f"*Rule Name:* {rule_name}",
    ]
    if rule_value:
        header.append(f"*Rule Value:* {rule_value}")
    if score is not None:
        header.append(f"*Penalty Score:* {score}")
    if perc is not None:
        header.append(f"*Perc:* {perc}%")
    if dim_name:
        header.append(f"*Dimension:* {dim_name}")

    desc: List[str] = []
    desc.append("h3. Description")
    desc.append("")
    desc.extend(header)
    desc.append("")
    desc.append("---")
    desc.append("")
    desc.append("h3. Table / View / File")
    desc.append("")
    desc.extend(table_lines if table_lines else ["_Unknown_"])
    desc.append("")
    desc.append("h3. Invalid Sample Data")
    desc.append("")
    desc.append(invalid_samples)
    desc.append("")
    desc.append("h3. Breaking Records")
    desc.append("")
    desc.append(break_summary)
    desc.append("")
    desc.append("h3. Investigation")
    desc.append("")
    desc.append("_Owner to add investigation notes here_")
    desc.append("")
    desc.append("h3. Expected Outcome / Solution")
    desc.append("")
    desc.append("_Owner to propose fix / logic change / data correction_")
    desc.append("")
    desc.append("h3. Status / Next Steps")
    desc.append("")
    desc.append("None")
    desc.append("")
    desc.append("h3. Links")
    desc.append("")
    desc.append(f"- *CDQ Dataset*: {cdq_link}")
    desc.append("")
    desc.append("h3. Comments")
    desc.append("")
    desc.append("None")
    return "\n".join(desc)


def build_labels_for_subtask(dim_name: Optional[str], extra: Optional[List[str]] = None) -> List[str]:
    labels = ["dq", "collibra", "end-users"]
    if not pd.isna(dim_name):
        labels.append(str(dim_name).strip().lower())
    if extra:
        labels.extend(extra)

    seen = set()
    out: List[str] = []
    for label in labels:
        if label and label not in seen:
            seen.add(label)
            out.append(label)
    return out


# ---------- Reopen and update ----------
def is_issue_already_open(issue_key: str) -> bool:
    issue = _get(f"/rest/api/2/issue/{issue_key}?fields=status")
    status = str(issue["fields"]["status"]["name"]).lower()
    return status in {"open", "reopened", "to do", "in progress"}


def reopen_issue(issue_key: str, transition_id: str) -> None:
    payload = {"transition": {"id": str(transition_id)}}
    _post(f"/rest/api/2/issue/{issue_key}/transitions", payload)


def add_comment(issue_key: str, body: str) -> None:
    payload = {"body": body}
    _post(f"/rest/api/2/issue/{issue_key}/comment", payload)


def update_description(issue_key: str, description: str) -> None:
    payload = {"fields": {"description": description}}
    _put(f"/rest/api/2/issue/{issue_key}", payload)


def get_reopen_transition_id(issue_key: str) -> Optional[str]:
    response = _get(f"/rest/api/2/issue/{issue_key}/transitions")
    for transition in response.get("transitions", []):
        name = str(transition["name"]).lower()
        target = str(transition["to"]["name"]).lower()
        if "reopen" in name or target == "open":
            return transition["id"]
    return None

def reopen_and_comment_adaptive(issue_key: str, new_description_text: str) -> None:
    '''
    reopen jira issue if it is closed and 
    update description with the new description text. 
    if the issue is already open, just update the comment.
    used mainly for adaptive issues
    return the subtask status if it was reopened, otherwise None
    '''
    subtask_status = None
    if is_issue_already_open(issue_key):
        add_comment(issue_key, new_description_text)
        logger.info("Added comment for issue %s", issue_key)
        return subtask_status

    reopen_transition_id = get_reopen_transition_id(issue_key)
    if not reopen_transition_id:
        logger.warning("No reopen transition available for %s", issue_key)
        return subtask_status

    reopen_issue(issue_key, reopen_transition_id)
    logger.info("Reopened issue %s", issue_key)
    update_description(issue_key, new_description_text)
    logger.info("Updated description for issue %s", issue_key)
    return "Reopened"

def reopen_and_comment(issue_key: str, new_description_text: str) -> None:
    '''
    reopen jira issue if it is closed and 
    add a comment with the new description text. 
    if the issue is already open, just update the description.
    used mainly for non-adaptive issues
    return the subtask status if it was reopened, otherwise None
    '''
    subtask_status = None
    if is_issue_already_open(issue_key):
        update_description(issue_key, new_description_text)
        logger.info("Updated description for issue %s", issue_key)
        return subtask_status

    reopen_transition_id = get_reopen_transition_id(issue_key)
    if not reopen_transition_id:
        logger.warning("No reopen transition available for %s", issue_key)
        return subtask_status

    reopen_issue(issue_key, reopen_transition_id)
    logger.info("Reopened issue %s", issue_key)
    add_comment(issue_key, new_description_text)
    logger.info("Added comment for issue %s", issue_key)
    return "Reopened"

def find_thresh(row: pd.Series, dq_score_thresh: Dict[str, float]) -> float:
    dataset = row.get("dataset")
    if dataset in dq_score_thresh:
        return dq_score_thresh[dataset]
    return DQ_SCORE_THRESH_DEFAULT


def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    df_custom = read_input("potential_jira_ticket_list").copy()
    df_adapt = read_input("potential_adaptive_rule_jira_ticket_list").copy()
    df_dupe = read_input("potential_dupe_jira_ticket_list").copy()
    df_pattern = read_input("potential_pattern_jira_ticket_list").copy()
    df_bu = read_input("business_unit_mapping").copy()

    df_custom["ticket_source"] = "custom"
    df_adapt["ticket_source"] = "adaptive"
    df_dupe["ticket_source"] = "dupes"
    df_pattern["ticket_source"] = "pattern"

    if "dimName" not in df_adapt.columns:
        df_adapt["dimName"] = np.nan

    df = pd.concat([df_custom, df_adapt, df_dupe, df_pattern], ignore_index=True, sort=False)
    return df, df_bu


def run() -> None:
    logger.info("Starting S6 JIRA logging step")

    df, df_bu = load_inputs()
    if df.empty:
        logger.info("No candidate rows found. Writing empty output.")
        write_output(pd.DataFrame(), "new_jira_ticket_list")
        return

    epic_link_cf = get_epic_link_customfield_id()
    versions_by_name = get_versions_map(PROJECT_KEY)
    components_by_name = get_components_map(PROJECT_KEY)

    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["jira_dq_threshold"] = df.apply(lambda row: find_thresh(row, DQ_SCORE_THRESH), axis=1)
    df_subset = df[df["score"] > df["jira_dq_threshold"]].copy()

    if df_subset.empty:
        logger.info("No rows passed DQ threshold. Writing empty output.")
        write_output(pd.DataFrame(), "new_jira_ticket_list")
        return

    business_unit_by_dataset = (
        df_bu.set_index("dataset")["business_unit"].to_dict() if not df_bu.empty and "dataset" in df_bu.columns else {}
    )

    created_rows: List[pd.DataFrame] = []
    errors: List[str] = []

    for _, row in df_subset.iterrows():
        dataset_name = str(row.get("dataset", "")).strip()
        if not dataset_name:
            continue

        matched_bu = None
        if not df_bu.empty and "dataset" in df_bu.columns and "business_unit" in df_bu.columns:
            bu_rows = df_bu[df_bu["dataset"] == dataset_name]
            if len(bu_rows) > 0:
                matched_bu = bu_rows["business_unit"].values[0]

        bu_name = matched_bu if matched_bu is not None else row.get("business_unit")
        region = region_from_business_unit(bu_name) if bu_name else None
        version_id = versions_by_name.get(region) if region else None

        component_name = map_business_unit_to_component_name(bu_name) if bu_name else "Other"
        if component_name == "Data-Product":
            logger.info("Skipping Data-Product ticket for dataset %s", dataset_name)
            continue

        component_id = components_by_name.get(component_name)
        component_obj = {"id": component_id} if component_id else None
        assignee = map_business_unit_to_assignee(bu_name) if bu_name else None

        try:
            parent_key = find_parent_task_key(PROJECT_KEY, dataset_name)
        except Exception as exc:
            errors.append(f"{dataset_name}: find_parent_task_key failed: {exc}")
            logger.warning("find_parent_task_key failed for %s: %s", dataset_name, exc)
            continue

        if not parent_key:
            try:
                parent_key = create_parent_task(
                    project_key=PROJECT_KEY,
                    dataset_name=dataset_name,
                    epic_link_field_id=epic_link_cf,
                    version_id=version_id,
                    component_obj=component_obj,
                    business_unit_by_dataset=business_unit_by_dataset,
                )
                logger.info("Created parent task %s for %s", parent_key, dataset_name)
            except Exception as exc:
                errors.append(f"{dataset_name}: create_parent_task failed: {exc}")
                logger.warning("create_parent_task failed for %s: %s", dataset_name, exc)
                continue

        try:
            title = str(row.get("suggested_title") or "").strip() or format_subtask_summary(row)
            subtask_status = "Old"
            if row.get("ticket_source") in ["adaptive", "dupes", "pattern"]:
                sub_desc = str(row.get("jira_description") or "").strip()
                if not sub_desc:
                    sub_desc = (
                        "h3. Description\n\n"
                        f"*Dataset:* {row.get('dataset')}\n"
                        f"*Run ID:* {row.get('runId')}\n\n"
                        f"_No jira_description provided in potential_{row.get('ticket_source')}_jira_ticket_list._"
                    )
                labels = _labels_to_list(row.get("suggested_labels"))
                ticket_source_label = str(row.get("ticket_source") or "").strip().lower()
                if ticket_source_label and ticket_source_label not in labels:
                    labels.append(ticket_source_label)
                if not labels:
                    labels = build_labels_for_subtask(
                        row.get("dimName"),
                        extra=[ticket_source_label] if ticket_source_label else None,
                    )

                active = find_active_subtask_by_parent(
                    project_key=PROJECT_KEY,
                    parent_key=parent_key,
                    ticket_source=row.get("ticket_source")
                )

                if active and active.get("key"):
                    sub_key = active["key"]
                    if active.get("summary"):
                        title = active["summary"]
                    if row.get("ticket_source") == "adaptive":
                        # for adaptive tickets, we only add a comment if the issue is already open; 
                        # otherwise, we reopen and update the description
                        # the adaptive description is used for AI diagnostics, we want to be 
                        # consistent unless it's closed and reopened
                        status = reopen_and_comment_adaptive(issue_key=sub_key, new_description_text=sub_desc)
                    else:
                        status = reopen_and_comment(issue_key=sub_key, new_description_text=sub_desc)

                    if status:
                        subtask_status = status
                    logger.info(f"Updated active {row.get('ticket_source')} subtask {sub_key}")
                else:
                    sub_key = create_subtask(
                        project_key=PROJECT_KEY,
                        parent_key=parent_key,
                        summary=title,
                        description=sub_desc,
                        version_id=version_id,
                        labels=labels,
                        component_obj=component_obj,
                        assignee=assignee,
                    )
                    subtask_status = "New"
                    logger.info(f"Created {row.get('ticket_source')} subtask {sub_key}")
            else:
                sub_desc = make_subtask_description(
                    dataset=dataset_name,
                    run_id=str(row.get("runId") or ""),
                    rule_name=str(row.get("ruleNm") or ""),
                    rule_value=row.get("ruleValue"),
                    score=row.get("score"),
                    perc=row.get("perc"),
                    dim_name=row.get("dimName"),
                    break_count=row.get("violation_count"),
                    break_columns=row.get("violation_columns"),
                )
                labels = build_labels_for_subtask(row.get("dimName"), extra=None)

                existing_subs = find_subtask_keys_by_parent(
                    project_key=PROJECT_KEY,
                    parent_key=parent_key,
                    subtask_summary_hint=title,
                )

                if existing_subs:
                    sub_key = existing_subs[0]
                    status = reopen_and_comment(issue_key=sub_key, new_description_text=sub_desc)
                    if status:
                        subtask_status = status
                    logger.info("Updated existing subtask %s", sub_key)
                else:
                    sub_key = create_subtask(
                        project_key=PROJECT_KEY,
                        parent_key=parent_key,
                        summary=title,
                        description=sub_desc,
                        version_id=version_id,
                        labels=labels,
                        component_obj=component_obj,
                        assignee=assignee,
                    )
                    subtask_status = "New"
                    logger.info("Created subtask %s", sub_key)

            row_with_subtask = row.copy()
            row_with_subtask["SUBTASK"] = sub_key
            row_with_subtask["SUBTASK_STATUS"] = subtask_status
            created_rows.append(pd.DataFrame(row_with_subtask).T)
        except Exception as exc:
            errors.append(f"{dataset_name}: create_or_update_subtask failed: {exc}")
            logger.warning("create_or_update_subtask failed for %s: %s", dataset_name, exc)

        time.sleep(1.0)

    if errors:
        logger.warning("S6 completed with %s errors", len(errors))
        for item in errors:
            logger.warning("  - %s", item)

    if created_rows:
        new_jira_ticket_list_df = pd.concat(created_rows, axis=0).reset_index(drop=True)
    else:
        new_jira_ticket_list_df = pd.DataFrame()

    write_output(new_jira_ticket_list_df, "new_jira_ticket_list")
    logger.info("Wrote new_jira_ticket_list rows: %s", len(new_jira_ticket_list_df))


if __name__ == "__main__":
    run()
