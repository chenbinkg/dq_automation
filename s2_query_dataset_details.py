# -*- coding: utf-8 -*-
"""
S2: Query Dataset Details from Collibra CDQ
===========================================

Reads intermediate outputs produced by S1 (dataset_runid, business_unit_mapping,
and dataset_cn) and calls the Collibra CDQ REST API to retrieve detailed DQ run
findings, dataset definitions, column profiles, custom rules, and outlier data
for every dataset/runId pair.  Results are written back to the configured
output store (CSV, Unity Catalog, or both) for consumption by downstream stages.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name             | Produced by | Description                                      |
|------------------------|-------------|--------------------------------------------------|
| dataset_runid          | S1          | One row per dataset with its latest run_date     |
| business_unit_mapping  | S1          | Dataset → business unit / market / project / CDE |
| dataset_cn             | S1          | List of datasets hosted on the CN Collibra region|

External APIs Called
--------------------
- Collibra CDQ (APAC region): cdq_base_url_apac
    - GET /v3/jobs/{dataset}/{run_date}/findings   – DQ run findings (rules, outliers,
                                                     adaptive items, shape/dupe scores)
    - GET /v3/datasetDefs/{dataset}               – Full dataset definition (profile flags,
                                                     shape settings, outlier/dupe/patterns
                                                     config, SQL query, job schedule)
    - GET /v3/rules                               – All active rules
    - GET /v2/templateRules                        – Template rule definitions
    - GET /v2/getDatasetReport                    – Table-level db_nm / table_nm
    - GET /v3/profile/deltas                       – Column-level null % deltas
- Collibra CDQ (CN region):  cdq_base_url_cn      – Same endpoints as APAC for CN datasets
- PostgreSQL (optional):      db_host / db_name   – Historical DQ dashboard data (currently
                                                     disabled; activate write_to_postgres()
                                                     before deployment)

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name                      | Description                                                  |
|---------------------------------|--------------------------------------------------------------|
| business_unit_mapping           | Enriched BU mapping (adds db_nm, table_nm, Data Domain etc.) |
| dataset_details                 | One row per dataset run with all DQ dimension scores         |
| dataset_outlier_details         | Raw outlier records for each dataset run                     |
| dataset_rule_details            | Per-rule score / exception / pass results for each run       |
| dataset_adaptive_rule_details   | Adaptive (behaviour) rule items (transposed dqItems)         |
| dqm_dashboard_by_data_domain    | Aggregated DQ scores joined with Data Domain / BU metadata   |
| dataset_definitions             | Column-level DQ configuration: checks enabled, data types,   |
|                                 | scheduler settings, date-filter key, current null %          |
| dataset_custom_rules            | Custom rule definitions enriched with template values,       |
|                                 | run-level metrics (score/perc/exception) and BU metadata     |
| dataset_dupe_details             | Raw duplicate records for each dataset run                  |
| dataset_pattern_details          | Raw pattern violation records for each dataset run          |

Environment Variables
---------------------
- PIPELINE_WRITE_MODE            : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR      : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- cdq_base_url_apac / cdq_base_url_cn
- username_apac / password_apac / username_cn / password_cn
- db_host / db_port / db_name / db_user / db_password / db_table  (PostgreSQL)
- uc_catalog / uc_schema  (Unity Catalog, required when PIPELINE_WRITE_MODE != csv)

TO-DO
-----
- Activate write_to_postgres() before deployment.
- Replace all data domain tags in the database with the updated ones from Collibra
  prior to deployment.
"""
import os
import json
import logging
import re
from datetime import datetime
import pandas as pd
import numpy as np
import requests
import urllib3
import config
try:
    import psycopg2
    import psycopg2.extras as extras
except ImportError:
    psycopg2 = None  # postgres writes are optional; guarded by write_to_postgres()
    extras = None
from typing import Optional, Dict, Any, Tuple, List
from requests.utils import quote
from pipeline_io import PipelineIO
from token_manager import CollibraTokenManager
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Configuration
WRITE_MODE = os.getenv("PIPELINE_WRITE_MODE", "csv").strip().lower()
LOCAL_OUTPUT_DIR = os.getenv("PIPELINE_LOCAL_OUTPUT_DIR", "./outputs")
SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")

logger.info(f"Pipeline write mode: {WRITE_MODE}")

# Credentials
try:
    from databricks.sdk import WorkspaceClient
    dbutils = WorkspaceClient().dbutils
except Exception:
    dbutils = None

def _load_secret_or_default(key: str, default: Any = None) -> Any:
    if dbutils is None:
        return default
    try:
        return dbutils.secrets.get(scope=SECRET_SCOPE, key=key)
    except Exception:
        return default

cdq_url_apac = _load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC)
cdq_url_cn = _load_secret_or_default("cdq_base_url_cn", config.CDQ_BASE_URL_CN)
username_apac = _load_secret_or_default("username_apac", config.COLLIBRA_USERNAME_APAC)
password_apac = _load_secret_or_default("password_apac", config.COLLIBRA_PASSWORD_APAC)
username_cn = _load_secret_or_default("username_cn", config.COLLIBRA_USERNAME_CN)
password_cn = _load_secret_or_default("password_cn", config.COLLIBRA_PASSWORD_CN)
uc_catalog = _load_secret_or_default("uc_catalog", getattr(config, "UC_CATALOG", None))
uc_schema = _load_secret_or_default("uc_schema", getattr(config, "UC_SCHEMA", None))

# DB credentials (loaded separately so they fail independently of Collibra creds)
db_host = _load_secret_or_default("db_host", config.DB_HOST)
db_port = _load_secret_or_default("db_port", config.DB_PORT)
db_name = _load_secret_or_default("db_name", config.DB_NAME)
db_user = _load_secret_or_default("db_user", config.DB_USER)
db_password = _load_secret_or_default("db_password", config.DB_PASSWORD)
db_table = _load_secret_or_default("db_table", config.DB_TABLE)

# Token managers
token_mgr_apac = CollibraTokenManager(
    base_url=cdq_url_apac, username=username_apac, password=password_apac, region="apac"
)
token_mgr_cn = CollibraTokenManager(
    base_url=cdq_url_cn, username=username_cn, password=password_cn, region="cn"
)

pipeline_io = PipelineIO(
    write_mode=WRITE_MODE,
    local_output_dir=LOCAL_OUTPUT_DIR,
    dbutils=dbutils,
    spark=globals().get("spark"),
    secret_scope=SECRET_SCOPE,
    uc_catalog=uc_catalog,
    uc_schema=uc_schema,
    logger=logger,
)

read_input = pipeline_io.read_input
write_output = pipeline_io.write_output


# DB write columns (must match table schema)
DB_COLS = [
    "dataset", "runId", "runDate", "rows", "passFail", "passFailLimit",
    "peak", "dayOfWeek", "timeZone", "avgRows", "cols", "activeRules",
    "activeAlerts", "runTime", "shapeScore", "dupeScore", "patternScore",
    "outlierScore", "schemaScore", "recordScore", "ruleScore", "sourceScore",
    "behaviorScore", "DQScore", "jobSchedule", "business_unit",
    "Data Domain", "CDE", "Project", "Market", "report_time",
]


def _to_bool(v):
    """Convert jobSchedule value to proper Python bool or None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    if isinstance(v, bool):
        return v
    s = str(v).strip().upper()
    if s == "TRUE":
        return True
    if s == "FALSE":
        return False
    return None


def write_to_postgres(df: pd.DataFrame) -> None:
    """
    Deduplicate against existing rows in the DB and INSERT only new records.
    Uses ON CONFLICT (dataset, runId) DO NOTHING as a safety net.
    Skips gracefully when DB credentials are not configured.
    """
    if psycopg2 is None:
        logger.warning("psycopg2 not available — skipping PostgreSQL write")
        return

    if not all([db_host, db_name, db_user, db_password]):
        logger.warning("DB credentials not configured — skipping PostgreSQL write")
        return

    if df.empty:
        logger.info("dqm_dashboard_by_data_domain is empty — skipping DB write")
        return

    def _make_conn():
        return psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            sslmode="require",
        )

    # ---- 1) Dedup: fetch existing (dataset, runId) pairs ----
    try:
        with _make_conn() as conn:
            df_hist = pd.read_sql(
                f'SELECT "dataset","runId" FROM {db_table}', conn
            )
    except Exception as e:
        logger.warning(f"Could not read history from DB ({e}) — will insert all rows")
        df_hist = pd.DataFrame(columns=["dataset", "runId"])

    df_new = df[
        ~df.set_index(["dataset", "runId"]).index.isin(
            df_hist.set_index(["dataset", "runId"]).index
        )
    ].copy()

    logger.info(f"DB dedup: {len(df)} total, {len(df_new)} new rows to insert")
    if df_new.empty:
        logger.info("No new rows to insert — skipping DB write")
        return

    # ---- 2) Normalise columns for psycopg2 ----
    if "runDate" in df_new.columns:
        df_new["runDate"] = (
            df_new["runDate"]
            .astype(str)
            .str.replace(r"\+0000$", "+00:00", regex=True)
            .replace("NaT", None)
        )

    if "jobSchedule" in df_new.columns:
        df_new["jobSchedule"] = df_new["jobSchedule"].map(_to_bool)

    # Enforce column order; fill missing cols with None
    for col in DB_COLS:
        if col not in df_new.columns:
            df_new[col] = None
    df_new = df_new[DB_COLS]

    # ---- 3) Build INSERT SQL ----
    quoted_cols = ",".join([f'"{c}"' for c in DB_COLS])
    sql = f"""
        INSERT INTO {db_table} ({quoted_cols})
        VALUES %s
        ON CONFLICT ("dataset","runId") DO NOTHING
    """
    rows = [tuple(r) for r in df_new.to_numpy()]
    logger.info(f"Prepared {len(rows)} rows for insertion into {db_table}")

    # ---- 4) Execute ----
    try:
        with _make_conn() as conn:
            with conn.cursor() as cur:
                extras.execute_values(cur, sql, rows, page_size=500)
        logger.info(f"Inserted {len(rows)} rows into {db_table}")
    except Exception as e:
        logger.error(f"DB write failed: {e}")
        raise

def get_job_findings(
    token_mgr: CollibraTokenManager, dataset: str, run_date: str
) -> Tuple[Optional[Dict], Optional[int], Optional[str]]:
    """Get DQ Job findings from Collibra CDQ."""
    cdq_base_url = token_mgr.base_url
    url_request = f"{cdq_base_url}/v3/jobs/{dataset}/{run_date}/findings"
    logger.debug(f"Request URL: {url_request}")
    
    try:
        headers = token_mgr.get_auth_header()
        response = requests.get(url_request, headers=headers, verify=False, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            logger.debug(f"Retrieved findings for {dataset}")
            return data, response.status_code, None
        elif response.status_code == 401:
            logger.warning(f"Token expired. Refreshing...")
            token_mgr.get_token(force_refresh=True)
            headers = token_mgr.get_auth_header()
            response = requests.get(url_request, headers=headers, verify=False, timeout=30)
            if response.status_code == 200:
                return response.json(), response.status_code, None
            return None, response.status_code, f"Still unauthorized: {response.text[:200]}"
        else:
            err = response.text[:500] if isinstance(response.text, str) else str(response.content)[:500]
            return None, response.status_code, err
    except requests.exceptions.RequestException as e:
        return None, None, f"RequestException: {str(e)}"

def get_nested(d: Dict, path: str, default=None) -> Any:
    """Safely traverse nested dicts."""
    cur = d
    try:
        for p in path.split("."):
            if cur is None:
                return default
            cur = cur.get(p)
        return cur if cur is not None else default
    except Exception:
        return default

def get_job_schedule(token_mgr: CollibraTokenManager, dataset: str) -> Tuple[Optional[Dict], Optional[int], Optional[str]]:
    """Call /v3/datasetDefs/{dataset} and parse selected fields."""
    cdq_base_url = token_mgr.base_url
    encoded = quote(dataset, safe="")
    url = f"{cdq_base_url}/v3/datasetDefs/{encoded}"
    try:
        headers = token_mgr.get_auth_header()
        response = requests.get(url, headers=headers, verify=False, timeout=30)
        if response.status_code == 200:
            payload = response.json()

            # CDQ datasetDefs provides metaTags where the 2nd entry is Data Domain.
            meta_tags = payload.get("metaTags")
            data_domain = None
            sub_domain = None
            if isinstance(meta_tags, list) and len(meta_tags) > 1:
                second_tag = meta_tags[1]
                third_tag = meta_tags[2] if len(meta_tags) > 2 else None
                if isinstance(second_tag, str) and second_tag.strip():
                    data_domain = second_tag.strip()
                if isinstance(third_tag, str) and third_tag.strip():
                    sub_domain = third_tag.strip()

            parsed_payload = {
                "load": payload.get("load"),
                "jobSchedule": payload.get("jobSchedule"),
                "pushdown": payload.get("pushdown"),
                "dataDomain": data_domain,
                "subDomain": sub_domain,
                "metaTags": meta_tags,
            }
            return parsed_payload, response.status_code, None
        else:
            err = response.text[:500] if isinstance(response.text, str) else str(response.content)[:500]
            return None, response.status_code, err
    except requests.exceptions.RequestException as e:
        return None, None, f"RequestException: {e}"

def get_table_info(token_mgr: CollibraTokenManager, dataset: str, run_id: str) -> Dict[str, Any]:
    """Extract table info and job schedule."""
    info = {
        "kind": "Unknown",
        "name": None,
        "db": None,
        "host": None,
        "path": None,
        "jobSchedule": None,
        "Data Domain": None,
        "subDomain": None,
        "connectionName": None,
        "scheduleTime": None,
        "timeZone": None,
    }
    try:
        result = get_dataset_report_first_row(token_mgr, dataset, run_id)
        # Check if API succeeded
        if result.get("db_nm") and result.get("table_nm"):
            info["db"] = result.get("db_nm")
            info["name"] = result.get("table_nm")
    except Exception as e:
        logger.debug(f"Error extracting table info for {dataset}/{run_id}: {e}")
    try:
        payload, status, err = get_job_schedule(token_mgr, dataset)
        if payload is not None:
            js = get_nested(payload, "jobSchedule", default={})
            enabled = get_nested(js, "enabled")
            schedule_time = get_nested(js, "scheduleTime")
            schedule_timezone = get_nested(js, "timeZone")
            info["jobSchedule"] = enabled
            info["scheduleTime"] = schedule_time
            info["timeZone"] = schedule_timezone
            load = get_nested(payload, "load", default={})
            # get connectionName from load.connectionName if present
            conn_name_load = get_nested(load, "connectionName")
            if conn_name_load:
                info["connectionName"] = conn_name_load
            else:
                pushdown = get_nested(payload, "pushdown", default={})
                conn_name_pushdown = get_nested(pushdown, "connectionName")
                info["connectionName"] = conn_name_pushdown

            data_domain = payload.get("dataDomain")
            if data_domain:
                info["Data Domain"] = data_domain
            sub_domain = payload.get("subDomain")
            if sub_domain:
                info["subDomain"] = sub_domain
    except Exception as e:
        logger.debug(f"Error extracting job schedule for {dataset}: {e}")
    return info

def safe_get(url: str, token_mgr: CollibraTokenManager, timeout: int = 60) -> Tuple[Optional[Any], Optional[int], Optional[str]]:
    """GET with one token-refresh retry on 401."""
    try:
        headers = token_mgr.get_auth_header()
        r = requests.get(url, headers=headers, verify=False, timeout=timeout)

        if r.status_code == 401:
            token_mgr.get_token(force_refresh=True)
            headers = token_mgr.get_auth_header()
            r = requests.get(url, headers=headers, verify=False, timeout=timeout)

        if r.status_code == 200:
            try:
                return r.json(), 200, None
            except ValueError:
                return None, 200, "Invalid JSON"

        err = r.text[:500] if isinstance(r.text, str) else str(r.content)[:500]
        return None, r.status_code, err

    except requests.exceptions.RequestException as e:
        return None, None, f"RequestException: {e}"

def normalize_rules_payload(payload: Any) -> List[Dict[str, Any]]:
    """Normalize v3/rules response payload."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("rules", "data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list):
                return [x for x in payload[key] if isinstance(x, dict)]
    return []

def normalize_template_payload(payload: Any) -> List[Dict[str, Any]]:
    """Normalize v2/templateRules response payload."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("templates", "data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list):
                return [x for x in payload[key] if isinstance(x, dict)]
    return []

def get_all_rules(token_mgr: CollibraTokenManager) -> List[Dict[str, Any]]:
    """Fetch all rules from v3/rules."""
    url = f"{token_mgr.base_url}/v3/rules"
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch all rules ({token_mgr.region}): status={status}, err={err}")
        return []
    return normalize_rules_payload(payload)

def get_template_rules(token_mgr: CollibraTokenManager) -> List[Dict[str, Any]]:
    """Fetch all template rules from v2/templateRules."""
    url = f"{token_mgr.base_url}/v2/templateRules"
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch template rules ({token_mgr.region}): status={status}, err={err}")
        return []
    return normalize_template_payload(payload)

def get_dataset_report_first_row(token_mgr: CollibraTokenManager, dataset: str, run_id: str) -> Dict[str, Any]:
    """Extract db_nm and table_nm from v2/getDatasetReport first row."""
    url = f"{token_mgr.base_url}/v2/getDatasetReport?dataset={dataset}&runId={run_id}"
    payload, status, err = safe_get(url, token_mgr)
    
    if status != 200:
        logger.debug(f"getDatasetReport failed for {dataset}/{run_id}: status={status}, err={err}")
        return {"db_nm": None, "table_nm": None}

    # Handle empty or None payload
    if not payload:
        logger.debug(f"getDatasetReport returned empty payload for {dataset}/{run_id}")
        return {"db_nm": None, "table_nm": None}

    first = None
    payload_type = None
    
    # Try to extract first element from various payload structures
    if isinstance(payload, list):
        payload_type = "list"
        if len(payload) > 0 and isinstance(payload[0], dict):
            first = payload[0]
        else:
            logger.debug(f"getDatasetReport returned empty list for {dataset}/{run_id}")
            return {"db_nm": None, "table_nm": None}
    elif isinstance(payload, dict):
        payload_type = "dict"
        # Try common wrapper keys
        for key in ("data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list) and payload[key]:
                if isinstance(payload[key][0], dict):
                    first = payload[key][0]
                    break
        
        # If no wrapper found, payload itself might be the data
        if not first:
            first = payload

    if not first:
        logger.debug(f"getDatasetReport: no data structure found for {dataset}/{run_id}, payload_type={payload_type}")
        return {"db_nm": None, "table_nm": None}

    # Extract db_nm and table_nm
    db_nm = first.get("db_nm")
    table_nm = first.get("table_nm")
    
    # Log if we got the data or if they're missing
    if db_nm and table_nm:
        logger.debug(f"getDatasetReport: extracted {db_nm}.{table_nm} for {dataset}/{run_id}")
    else:
        available_keys = list(first.keys()) if isinstance(first, dict) else []
        logger.debug(f"getDatasetReport: db_nm/table_nm not found in first element for {dataset}/{run_id}. Available keys: {available_keys}")
    
    return {
        "db_nm": db_nm,
        "table_nm": table_nm,
    }

def build_template_lookup(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Build template lookup by ruleName."""
    out: Dict[str, Dict[str, Any]] = {}
    for r in items:
        rn = r.get("ruleName")
        if isinstance(rn, str) and rn.strip():
            out[rn] = {
                "ruleValue": r.get("ruleValue"),
                "ruleDescription": r.get("ruleDescription"),
            }
    return out

def replace_col_placeholder(rule_value: Any, col_name: Optional[str]) -> Any:
    """Replace $colNm placeholder in rule value with actual column name."""
    if not isinstance(rule_value, str):
        return rule_value
    if not col_name:
        return rule_value
    return rule_value.replace("$colNm", str(col_name))

def apply_template_enrichment(row: pd.Series, cn_datasets: set, lookup_apac: Dict, lookup_cn: Dict) -> pd.Series:
    """Enrich CUSTOM rules from template rules."""
    dataset = str(row.get("dataset") or "")
    is_cn = dataset in cn_datasets
    tmpl_lookup = lookup_cn if is_cn else lookup_apac

    rule_type = row.get("ruleType")
    rule_repo = row.get("ruleRepo")
    col_name = row.get("columnName")

    if isinstance(rule_type, str) and rule_type.upper() == "CUSTOM":
        if isinstance(rule_repo, str) and rule_repo in tmpl_lookup:
            tmpl = tmpl_lookup[rule_repo]
            tmpl_value = tmpl.get("ruleValue")
            tmpl_desc = tmpl.get("ruleDescription")

            if tmpl_value:
                row["ruleValue"] = replace_col_placeholder(tmpl_value, col_name)

            if tmpl_desc and (not row.get("businessDesc")):
                row["businessDesc"] = tmpl_desc

    return row

def get_profile_deltas(token_mgr: CollibraTokenManager, dataset: str, run_id: Optional[str]) -> Tuple[Optional[List[Dict]], Optional[int], Optional[str]]:
    """
    Fetch /v3/profile/deltas for a dataset + runId.
    Returns: (list_json, status_code, error_text)
    """
    if not run_id:
        return [], None, "No runId provided"

    ds_enc = quote(dataset, safe="")
    rid_enc = quote(str(run_id), safe="")
    url = f"{token_mgr.base_url}/v3/profile/deltas?dataset={ds_enc}&runId={rid_enc}"

    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch profile deltas for {dataset}/{run_id}: status={status}, err={err}")
        return None, status, err

    # expect a list; tolerate dict-wrapped list
    if isinstance(payload, list):
        return payload, 200, None
    if isinstance(payload, dict):
        for k in ("items", "results", "data", "content"):
            if k in payload and isinstance(payload[k], list):
                return payload[k], 200, None
    
    logger.warning(f"Unexpected JSON shape for profile deltas")
    return None, 200, "Unexpected JSON shape"

def _extract_null_ratio(delta_item: Dict[str, Any]) -> Optional[float]:
    """
    Extract null ratio from profile delta item.
    Prefer datasetField.nullRatio, fallback to computing from profile counts.
    Returns ratio in [0,1].
    """
    nr = get_nested(delta_item, "datasetField.nullRatio")
    if isinstance(nr, (int, float)) and not pd.isna(nr):
        nr_f = float(nr)
        if nr_f > 1.0:
            return nr_f / 100.0  # assume it's percent
        return nr_f
    return None

def collect_included_columns_from_section(section: Any) -> set:
    """Extract column names from outliers/patterns/dupe sections."""
    cols = set()
    if isinstance(section, dict):
        include = section.get("include") or section.get("columns")
        if isinstance(include, list):
            cols.update([str(c).lower() for c in include if c])
        settings = section.get("settings") or section.get("items")
        if isinstance(settings, list):
            for s in settings:
                inc = s.get("include") or s.get("columns")
                if isinstance(inc, list):
                    cols.update([str(c).lower() for c in inc if c])
                col = s.get("column")
                if isinstance(col, str):
                    cols.add(col.lower())
    elif isinstance(section, list):
        for item in section:
            if isinstance(item, dict):
                inc = item.get("include") or item.get("columns")
                if isinstance(inc, list):
                    cols.update([str(c).lower() for c in inc if c])
                col = item.get("column")
                if isinstance(col, str):
                    cols.add(col.lower())
    return cols

def collect_excluded_columns_from_section(section: Any) -> set:
    """Extract excluded column names from outliers/patterns/dupe sections."""
    cols = set()
    if isinstance(section, dict):
        exclude = section.get("exclude") or section.get("columns")
        if isinstance(exclude, list):
            cols.update([str(c).lower() for c in exclude if c])
        settings = section.get("settings") or section.get("items")
        if isinstance(settings, list):
            for s in settings:
                exclude = s.get("exclude") or s.get("columns")
                if isinstance(exclude, list):
                    cols.update([str(c).lower() for c in exclude if c])
                col = s.get("column")
                if isinstance(col, str):
                    cols.add(col.lower())
    elif isinstance(section, list):
        for item in section:
            if isinstance(item, dict):
                exclude = item.get("exclude") or item.get("columns")
                if isinstance(exclude, list):
                    cols.update([str(c).lower() for c in exclude if c])
                col = item.get("column")
                if isinstance(col, str):
                    cols.add(col.lower())
    return cols

def collect_patterns_map(section: Any) -> Dict[str, List[str]]:
    """Build mapping: column -> [patternKey1, patternKey2, ...]"""
    out: Dict[str, List[str]] = {}
    if isinstance(section, dict):
        items = section.get("settings") or section.get("items") or section.get("patterns") or []
    elif isinstance(section, list):
        items = section
    else:
        items = []

    for it in items:
        if not isinstance(it, dict):
            continue
        key = it.get("key") or it.get("pattern") or it.get("name")
        include = it.get("include") or it.get("columns") or []
        if isinstance(include, list) and key:
            for c in include:
                cl = str(c).lower()
                out.setdefault(cl, []).append(str(key))
    return out

def _is_truthy(v) -> bool:
    """Convert value to boolean."""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "yes", "y", "1")
    return bool(v)

def extract_db_and_table_from_query(sql: Optional[str]) -> Dict[str, Optional[str]]:
    """
    Extract db_nm and table_nm from FROM clause in SQL query.
    Handles patterns like: db_name.table_name, "db"."table", db_name.table_name AS t, etc.
    Returns {"db_nm": value, "table_nm": value} or both None if not found.
    """
    if not sql or not isinstance(sql, str):
        return {"db_nm": None, "table_nm": None}
    
    s = sql.strip()
    s = re.sub(r"\s+", " ", s)
    
    # Match FROM clause: captures the table/schema reference up to alias, WHERE, or end
    m = re.search(r"\bfrom\s+([^\s,;]+(?:\.[^\s,;]+)?)\b", s, flags=re.IGNORECASE)
    if not m:
        return {"db_nm": None, "table_nm": None}
    
    table_ref = m.group(1).strip()
    
    # Remove alias if present (e.g., "table_name AS t" -> "table_name")
    table_ref = re.sub(r"\s+(?:AS\s+)?[A-Za-z_]\w*$", "", table_ref, flags=re.IGNORECASE)
    
    # Handle quoted identifiers: "db"."table" or 'db'.'table' or `db`.`table`
    quoted_pattern = r'^["`](.+?)["`]\.["`](.+?)["`]$'
    m_quoted = re.match(quoted_pattern, table_ref)
    if m_quoted:
        return {"db_nm": m_quoted.group(1), "table_nm": m_quoted.group(2)}
    
    # Handle unquoted: db.table
    if "." in table_ref:
        parts = [p.strip().strip("`").strip('"') for p in table_ref.split(".")]
        if len(parts) >= 2:
            # Last part is table_nm, everything else is db_nm (in case of schema.db.table)
            return {"db_nm": parts[-2], "table_nm": parts[-1]}
    
    # Single name only (no db prefix found)
    return {"db_nm": None, "table_nm": table_ref.strip().strip("`").strip('"')}

def parse_select_columns(sql: str) -> Optional[List[str]]:
    """Extract SELECT list columns from simple SELECT ... FROM query."""
    if not sql:
        return None
    s = sql.strip()
    s = re.sub(r"\s+", " ", s)
    m = re.search(r"select\s+(.*?)\s+from\s", s, flags=re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    select_part = m.group(1).strip()
    if select_part == "*" or select_part.lower() == "distinct *":
        return None

    cols = []
    buf = []
    depth = 0
    in_single = False
    in_double = False
    for ch in select_part:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "(" and not in_single and not in_double:
            depth += 1
        elif ch == ")" and not in_single and not in_double:
            depth = max(0, depth - 1)
        if ch == "," and depth == 0 and not in_single and not in_double:
            cols.append("".join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    if buf:
        cols.append("".join(buf).strip())

    cleaned = []
    for c in cols:
        m_as = re.search(r"^(.*?)(\s+as\s+.+)$", c, flags=re.IGNORECASE)
        if m_as:
            c = m_as.group(1).strip()
        else:
            m_alias = re.search(r"^(.*?)[\s]+[A-Za-z_][A-Za-z0-9_]*$", c)
            if m_alias and "(" in c:
                c = m_alias.group(1).strip()
        cleaned.append(c)
    return [x.strip().strip("`").strip('"').lower() for x in cleaned if x]

def detect_date_filter_and_field(sql: Optional[str]) -> Tuple[bool, Optional[str]]:
    """Detect date filter in SQL and extract date column name."""
    if not sql:
        return False, None

    DATE_TOKENS = ["dateadd", "to_date", "date_trunc", "current_date", "${rd}", "${run_date}", "interval"]
    DATE_FIELD_HINTS = ["date", "dt", "time", "timestamp"]
    
    s = re.sub(r"\s+", " ", sql.lower())
    has_where = " where " in f" {s} "
    has_date_fn = any(tok in s for tok in DATE_TOKENS)

    if not (has_where and has_date_fn):
        return False, None

    FIELD_REGEX = re.compile(
        r"(?:where|and)\s+"
        r"(?:"
        r"to_date\s*\(\s*([a-zA-Z_][\w\.]*)\s*,[^)]*\)"
        r"|date\s*\(\s*([a-zA-Z_][\w\.]*)\s*\)"
        r"|([a-zA-Z_][\w\.]*)"
        r")\s*"
        r"(&amp;gt;=|&gt;=|>=|&amp;gt;|&gt;|>|&amp;lt;=|&lt;=|<=|&amp;lt;|&lt;|<|=|between|in)",
        re.IGNORECASE,
    )

    def _looks_like_datetime_field(field: str) -> bool:
        f = field.lower()
        return any(h in f for h in DATE_FIELD_HINTS)

    def _extract_field_and_wrapped(m: re.Match) -> Tuple[Optional[str], bool]:
        if m.group(1):
            return m.group(1), True
        if m.group(2):
            return m.group(2), True
        return m.group(3), False

    match = FIELD_REGEX.search(sql)
    if not match:
        return True, None

    field, wrapped = _extract_field_and_wrapped(match)
    if field and (wrapped or _looks_like_datetime_field(field)):
        return True, field

    tail = sql[match.end():]
    and_match = re.search(r"\band\b", tail, re.IGNORECASE)
    if not and_match:
        return True, None

    tail_sql = tail[and_match.start():]
    match2 = FIELD_REGEX.search(tail_sql)
    if match2:
        field2, _wrapped2 = _extract_field_and_wrapped(match2)
        return True, field2

    return True, None

# Main
logger.info("=== S2: Query Dataset Details ===")
logger.info("Loading input datasets from S1...")
df_input = read_input("dataset_runid")
df_bu = read_input("business_unit_mapping")
df_ds_cn = read_input("dataset_cn")
logger.info(f"Loaded {len(df_input)} datasets and {len(df_bu)} business units")

results = []
outliers = []
adaptive = []
rules = []
dupes = []
patterns = []

logger.info("Fetching findings from Collibra CDQ...")
for index, row in df_input.iterrows():
    dataset = row["dataset"]
    run_date = row["run_date"]
    
    if pd.isna(run_date):
        logger.warning(f"Skipping {dataset} due to missing run_date")
        continue
    
    token_mgr = token_mgr_apac if dataset not in df_ds_cn['dataset'].values else token_mgr_cn
    data, status, error = get_job_findings(token_mgr, dataset, run_date)
    
    if data:
        if "dqItems" in data and len(data["dqItems"]) > 0 and dataset not in df_ds_cn['dataset'].values:
            df = pd.DataFrame(data["dqItems"])
            df_t = df.T.reset_index(drop=True)
            df_t.insert(0, "dataset", dataset)
            df_t.insert(1, "runId", run_date)
            adaptive.append(df_t)
        
        if "outliers" in data and isinstance(data["outliers"], list) and dataset not in df_ds_cn['dataset'].values:
            for outlier in data["outliers"]:
                outliers.append(outlier)
        
        if "rules" in data and isinstance(data["rules"], list):
            for rule in data["rules"]:
                rules.append(rule)

        if "dupes" in data and isinstance(data["dupes"], list):
            for dupe in data["dupes"]:
                dupes.append(dupe)

        if "patterns" in data and isinstance(data["patterns"], list):
            for pattern in data["patterns"]:
                patterns.append(pattern)
        
        for key in ["observations", "patterns", "datashapes", "sources", "dupes", "alerts", "dqItems"]:
            if key in data and isinstance(data[key], list):
                data[key] = json.dumps(data[key])
        
        tinfo = get_table_info(token_mgr, dataset, run_date)
        if tinfo.get("jobSchedule"):
            data["jobSchedule"] = tinfo["jobSchedule"]
        if tinfo.get("Data Domain"):
            data["Data Domain"] = tinfo["Data Domain"]
        if tinfo.get("subDomain"):
            data["subDomain"] = tinfo["subDomain"]
        if tinfo.get("connectionName"):
            data["connectionName"] = tinfo["connectionName"]
        if tinfo.get("db"):
            data["db_nm"] = tinfo["db"]
        if tinfo.get("name"):
            data["table_nm"] = tinfo["name"]
        if tinfo.get("scheduleTime"):
            data["scheduleTime"] = tinfo["scheduleTime"]
        if tinfo.get("timeZone"):
            data["timeZone"] = tinfo["timeZone"]
        
        results.append(data)
    else:
        logger.error(f"Failed to fetch findings for {dataset}: {error} (HTTP {status})")

logger.info(f"Results: {len(results)}, Outliers: {len(outliers)}, Adaptive: {len(adaptive)}, Rules: {len(rules)}")

# Create DataFrames
df_output = pd.DataFrame(results) if results else pd.DataFrame()
df_outliers = pd.DataFrame(outliers) if outliers else pd.DataFrame()
df_rules = pd.DataFrame(rules) if rules else pd.DataFrame()
df_adaptive = pd.concat(adaptive, ignore_index=True) if adaptive else pd.DataFrame()
df_dupes = pd.DataFrame(dupes) if dupes else pd.DataFrame()
df_patterns = pd.DataFrame(patterns) if patterns else pd.DataFrame()

# Identify CN datasets for regional logic
cn_datasets = set(df_ds_cn["dataset"].dropna().astype(str).tolist()) if "dataset" in df_ds_cn.columns else set()

# Enrich business_unit_mapping with dataset details fields from df_output
if not df_output.empty and not df_bu.empty:
    details_cols = ["dataset", "jobSchedule", "Data Domain", "subDomain", "connectionName", "db_nm", "table_nm", "scheduleTime", "timeZone"]
    available_details_cols = [c for c in details_cols if c in df_output.columns]
    details_for_bu = (
        df_output[available_details_cols]
        .drop_duplicates(subset=["dataset"], keep="first")
        if "dataset" in available_details_cols
        else pd.DataFrame(columns=details_cols)
    )

    df_bu = pd.merge(
        df_bu,
        details_for_bu,
        how="left",
        on=["dataset"],
        suffixes=("", "_from_details"),
    )

    for field in details_cols:
        incoming_col = f"{field}_from_details"
        if incoming_col in df_bu.columns:
            if field in df_bu.columns:
                df_bu[field] = df_bu[incoming_col].combine_first(df_bu[field])
            else:
                df_bu[field] = df_bu[incoming_col]
            df_bu = df_bu.drop(columns=[incoming_col])

# Build dataset_definitions using comprehensive Dataiku logic
# ============================================================
logger.info("Building dataset_definitions with detailed column-level metadata...")

def_records: List[Dict[str, Any]] = []
delta_records: List[Dict[str, Any]] = []

for index, row in df_input.iterrows():
    dataset = str(row.get("dataset") or "")
    if not dataset.strip():
        logger.warning(f"Row {index} has no dataset name; skipping")
        continue
    dataset = dataset.strip()
    run_id = row.get("run_date")

    # Fetch full dataset definition from CDQ (contains profile, shape, outliers, dupe, patterns, rule, etc.)
    token_mgr = token_mgr_cn if dataset in cn_datasets else token_mgr_apac
    url = f"{token_mgr.base_url}/v3/datasetDefs/{quote(dataset, safe='')}"
    dataset_def, def_status, def_err = safe_get(url, token_mgr)
    if not isinstance(dataset_def, dict):
        dataset_def = {}

    # Parse dataDomain / subDomain from metaTags inline (same logic as get_job_schedule)
    if dataset_def:
        meta_tags = dataset_def.get("metaTags")
        if isinstance(meta_tags, list) and len(meta_tags) > 1:
            second_tag = meta_tags[1]
            third_tag = meta_tags[2] if len(meta_tags) > 2 else None
            dataset_def.setdefault("dataDomain", second_tag.strip() if isinstance(second_tag, str) and second_tag.strip() else None)
            dataset_def.setdefault("subDomain", third_tag.strip() if isinstance(third_tag, str) and third_tag.strip() else None)

    if dataset_def:
        # Extract dataset-level fields
        link_id = dataset_def.get("linkId")
        
        # Load/query
        query = (dataset_def.get("load", {}).get("query") or 
                dataset_def.get("load", {}).get("sql") or 
                dataset_def.get("query"))

        # Profile behavior flags
        profile = dataset_def.get("profile", {}) if isinstance(dataset_def.get("profile", {}), dict) else {}
        row_count = _is_truthy(profile.get("behaviorRowCheck"))
        exec_time = _is_truthy(profile.get("behaviorTimeCheck"))
        min_check = _is_truthy(profile.get("behaviorMinValueCheck"))
        max_check = _is_truthy(profile.get("behaviorMaxValueCheck"))
        mean_check = _is_truthy(profile.get("behaviorMeanValueCheck"))
        null_check = _is_truthy(profile.get("behaviorNullCheck"))
        empty_check = _is_truthy(profile.get("behaviorEmptyCheck"))
        uniq_check = _is_truthy(profile.get("behaviorUniqueCheck"))
        dtype_check = _is_truthy(profile.get("behaviorShiftCheck"))
        schema_change = _is_truthy(profile.get("detectStringNumerics"))

        # Rule toggle
        rule_on = _is_truthy(get_nested(dataset_def, "rule.on"))

        # Outliers / Dupe / Patterns
        outlier_section = dataset_def.get("outliers", {})
        dupe_section = dataset_def.get("dupe", {})
        patterns_section = dataset_def.get("patterns", {})

        outliers_on = _is_truthy(outlier_section.get("on")) if isinstance(outlier_section, dict) else False
        dupe_on = _is_truthy(dupe_section.get("on")) if isinstance(dupe_section, dict) else False

        outlier_cols = collect_included_columns_from_section(outlier_section)
        dupe_cols = collect_included_columns_from_section(dupe_section)
        patterns_map = collect_patterns_map(patterns_section)

        # Shape & columns
        shape = dataset_def.get("shape", {}) if isinstance(dataset_def.get("shape", {}), dict) else {}
        col_settings = shape.get("columnSettings", [])
        data_types: Dict[str, Any] = {}
        col_descs: Dict[str, Any] = {}
        shape_enabled_cols: set = set()

        if isinstance(col_settings, list):
            for cs in col_settings:
                if not isinstance(cs, dict):
                    continue
                cn = cs.get("name")
                if not isinstance(cn, str):
                    continue
                cn_l = cn.lower()
                data_types[cn_l] = cs.get("type")
                col_descs[cn_l] = cs.get("description") or cs.get("comment") or ""
                if _is_truthy(cs.get("enabled", True)):
                    shape_enabled_cols.add(cn_l)

        # Identify columns from SQL or shape
        cols_from_sql = parse_select_columns(query) if isinstance(query, str) else None
        all_columns = cols_from_sql if cols_from_sql else list(data_types.keys()) if data_types else []

        # Handle cases where dupe_cols only has exclude list
        if not dupe_cols and dupe_on:
            excluded_dupe_cols = collect_excluded_columns_from_section(dupe_section)
            dupe_cols = set(data_types.keys()) - excluded_dupe_cols if data_types else set()

        # Detect date filter
        date_filter_enabled, date_filter_key = detect_date_filter_and_field(query)

        # Job schedule
        job_schedule = dataset_def.get("jobSchedule", {}) if isinstance(dataset_def.get("jobSchedule", {}), dict) else {}
        scheduler_enabled = _is_truthy(job_schedule.get("enabled"))
        sched_freq = job_schedule.get("scheduleFrequency")
        sched_time = job_schedule.get("scheduleTime")

        # Build definition records – one row per column
        if not all_columns and isinstance(col_settings, list) and col_settings:
            all_columns = [str(cs.get("name", "")).lower() for cs in col_settings if cs.get("name")]

        if not all_columns:
            # Dataset-level record (no columns)
            rec = {
                "Table Name": dataset,
                "Rule Id": run_id,
                "Link Id": link_id,
                "Date Filter": bool(date_filter_enabled),
                "Date Filter Key": date_filter_key,
                "Scheduler": bool(scheduler_enabled),
                "Scheduled Freq": sched_freq,
                "Scheduled Time": sched_time,
                "Column Name": None,
                "Data Type": None,
                "Column Description": None,
                "Row Count": bool(row_count),
                "Execution Time": bool(exec_time),
                "Data Type Check": bool(dtype_check),
                "Schema Change": bool(schema_change),
                "Dupes": bool(dupe_on or len(dupe_cols) > 0),
                "Custom Rules": bool(rule_on),
                "Null Values": bool(null_check),
                "Empty Fields": bool(empty_check),
                "Uniqueness": bool(uniq_check),
                "Min": bool(min_check),
                "Max": bool(max_check),
                "Mean": bool(mean_check),
                "Outliers": bool(outliers_on or len(outlier_cols) > 0),
                "Shapes": bool(len(shape_enabled_cols) > 0),
                "Patterns": None,
                "Current Null %": None,
            }
            def_records.append(rec)
        else:
            for col in all_columns:
                col_l = str(col).lower()
                rec = {
                    "Table Name": dataset,
                    "Rule Id": run_id,
                    "Link Id": link_id,
                    "Date Filter": bool(date_filter_enabled),
                    "Date Filter Key": date_filter_key,
                    "Scheduler": bool(scheduler_enabled),
                    "Scheduled Freq": sched_freq,
                    "Scheduled Time": sched_time,
                    "Column Name": col_l,
                    "Data Type": data_types.get(col_l),
                    "Column Description": col_descs.get(col_l),
                    "Row Count": bool(row_count),
                    "Execution Time": bool(exec_time),
                    "Data Type Check": bool(dtype_check),
                    "Schema Change": bool(schema_change),
                    "Dupes": bool(col_l in dupe_cols or (dupe_on and not dupe_cols)),
                    "Custom Rules": bool(rule_on),
                    "Null Values": bool(null_check),
                    "Empty Fields": bool(empty_check),
                    "Uniqueness": bool(uniq_check),
                    "Min": bool(min_check),
                    "Max": bool(max_check),
                    "Mean": bool(mean_check),
                    "Outliers": bool(col_l in outlier_cols or (outliers_on and not outlier_cols)),
                    "Shapes": bool(col_l in shape_enabled_cols or (not shape_enabled_cols and col_l in data_types)),
                    "Patterns": ";".join(patterns_map.get(col_l, [])) if col_l in patterns_map else None,
                    "Current Null %": None,
                }
                def_records.append(rec)
    else:
        logger.warning(f"Dataset definition fetch failed for '{dataset}': status={def_status}, err={def_err}")

    # Fetch profile deltas for current dataset/runId
    if run_id:
        deltas, d_status, d_err = get_profile_deltas(token_mgr, dataset, run_id)
        if d_status == 200 and isinstance(deltas, list):
            for it in deltas:
                if not isinstance(it, dict):
                    continue
                col = (it.get("colName") or 
                      get_nested(it, "datasetField.fieldNm") or 
                      get_nested(it, "runProfile.colName"))
                if not isinstance(col, str) or not col.strip():
                    continue
                col_l = col.strip().lower()

                null_ratio = _extract_null_ratio(it)
                null_pct = (null_ratio * 100.0) if isinstance(null_ratio, (int, float)) else None

                delta_records.append({
                    "Table Name": dataset,
                    "Rule Id": run_id,
                    "Column Name": col_l,
                    "Current Null %": null_pct,
                })
        elif d_status is not None:
            logger.debug(f"Profile deltas fetch failed for '{dataset}': status={d_status}, err={d_err}")

# Build DataFrame
cols_def = [
    "Table Name", "Rule Id", "Link Id", "Date Filter", "Date Filter Key", "Scheduler", 
    "Scheduled Freq", "Scheduled Time", "Column Name", "Data Type", "Column Description",
    "Row Count", "Execution Time", "Data Type Check", "Schema Change", "Dupes", "Custom Rules",
    "Null Values", "Empty Fields", "Uniqueness", "Min", "Max", "Mean", "Outliers", "Shapes", 
    "Patterns", "Current Null %"
]

df_dataset_definitions = pd.DataFrame(def_records) if def_records else pd.DataFrame(columns=cols_def)
for c in cols_def:
    if c not in df_dataset_definitions.columns:
        df_dataset_definitions[c] = None
df_dataset_definitions = df_dataset_definitions[cols_def]

# Build and merge deltas
df_deltas = pd.DataFrame(delta_records) if delta_records else pd.DataFrame(
    columns=["Table Name", "Rule Id", "Column Name", "Current Null %"]
)

if not df_deltas.empty:
    df_deltas["Table Name"] = df_deltas["Table Name"].astype(str)
    df_deltas["Rule Id"] = df_deltas["Rule Id"].astype(str)
    df_deltas["Column Name"] = df_deltas["Column Name"].astype(str).str.lower()

    df_dataset_definitions = pd.merge(
        df_dataset_definitions,
        df_deltas[["Table Name", "Rule Id", "Column Name", "Current Null %"]],
        how="left",
        on=["Table Name", "Rule Id", "Column Name"]
    )

    # Ensure no duplicate columns after merge
    for col in df_dataset_definitions.columns:
        if col.endswith("_x"):
            df_dataset_definitions = df_dataset_definitions.drop(columns=[col])
        elif col.endswith("_y"):
            df_dataset_definitions = df_dataset_definitions.rename(columns={col: col[:-2]})

logger.info(f"Built dataset_definitions with {len(df_dataset_definitions)} records")

# Merge with business_unit_mapping (only columns that exist at this point)
if not df_dataset_definitions.empty and not df_bu.empty and "dataset" in df_bu.columns:
    bu_df = df_bu.rename(columns={"dataset": "Table Name"})
    
    # Only select columns that actually exist in df_bu at this point
    # db_nm and table_nm will be added later after df_custom_rules is built
    cols_to_merge = ["Table Name"]
    for col in ["db_nm", "table_nm", "business_unit", "Market", "Project", "CDE"]:
        if col in bu_df.columns:
            cols_to_merge.append(col)
    
    df_dataset_definitions = pd.merge(
        df_dataset_definitions,
        bu_df[cols_to_merge],
        how="left",
        on="Table Name"
    )
    logger.info(f"Enriched dataset_definitions with business_unit_mapping")


# Build dataset_custom_rules using original S3 logic
logger.info("Building dataset_custom_rules...")

# Fetch all rules + template rules once per region
rules_apac = get_all_rules(token_mgr_apac)
rules_cn = get_all_rules(token_mgr_cn)
templates_apac = get_template_rules(token_mgr_apac)
templates_cn = get_template_rules(token_mgr_cn)

# Build template lookups
lookup_apac = build_template_lookup(templates_apac)
lookup_cn = build_template_lookup(templates_cn)

# Keep only datasets that appear in dataset_rule_details
wanted_datasets = set(df_rules["dataset"].dropna().astype(str).tolist()) if "dataset" in df_rules.columns else set()

all_rules = []
for rec in rules_apac + rules_cn:
    ds = rec.get("dataset")
    if isinstance(ds, str) and ds in wanted_datasets:
        all_rules.append(rec)

if not all_rules:
    logger.warning("No matching rules found from v3/rules for datasets in dataset_rule_details")

df_rules_from_api = pd.DataFrame(all_rules)

# Ensure required fields exist
required_rule_cols = [
    "dataset", "ruleNm", "ruleType", "ruleValue", "ruleRepo", "columnName",
    "businessDesc", "dimId", "dimName", "points", "isActive", "suppressed",
]
for col in required_rule_cols:
    if col not in df_rules_from_api.columns:
        df_rules_from_api[col] = None

# Apply template enrichment to CUSTOM rules
if not df_rules_from_api.empty:
    df_rules_from_api = df_rules_from_api.apply(
        lambda row: apply_template_enrichment(row, cn_datasets, lookup_apac, lookup_cn),
        axis=1
    )
    logger.info(f"Applied template enrichment to {len(df_rules_from_api)} rules")

# Drop perc/exception/score columns that may come from v3/rules (use dataset_rule_details instead)
for metric_col in ["perc", "exception", "score"]:
    if metric_col in df_rules_from_api.columns:
        df_rules_from_api = df_rules_from_api.drop(columns=[metric_col])

# Merge with df_rules (dataset_rule_details) to bring run-level metrics
join_cols = ["dataset", "ruleNm"]
available_rule_detail_cols = [c for c in ["dataset", "ruleNm", "runId", "score", "perc", "exception"] if c in df_rules.columns]

df_rule_metrics = df_rules[available_rule_detail_cols].copy() if available_rule_detail_cols else pd.DataFrame()

if not df_rule_metrics.empty:
    # Keep latest by runId if duplicates exist
    if "runId" in df_rule_metrics.columns:
        df_rule_metrics = df_rule_metrics.sort_values(by=["runId"]).drop_duplicates(subset=join_cols, keep="last")
    else:
        df_rule_metrics = df_rule_metrics.drop_duplicates(subset=join_cols, keep="last")

    df_custom_rules = pd.merge(df_rules_from_api, df_rule_metrics, how="left", on=join_cols)
else:
    df_custom_rules = df_rules_from_api.copy()

# Ensure runId column exists
if "runId" not in df_custom_rules.columns:
    df_custom_rules["runId"] = None

# Attach runId from dataset_runid if missing after merge
if "dataset" in df_input.columns and "run_date" in df_input.columns and not df_custom_rules.empty:
    df_custom_rules = pd.merge(
        df_custom_rules,
        df_input[["dataset", "run_date"]].rename(columns={"run_date": "runId_from_runid"}),
        how="left",
        on=["dataset"],
    )
    df_custom_rules["runId"] = df_custom_rules["runId"].combine_first(df_custom_rules["runId_from_runid"])
    df_custom_rules = df_custom_rules.drop(columns=["runId_from_runid"])


# ============================================================
# FINAL STEP: Merge all output dataframes with enriched df_bu (at end to avoid duplicates)
# ============================================================
logger.info("Enriching all outputs with business_unit_mapping...")

if not df_custom_rules.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_custom_rules.columns]
    df_custom_rules = pd.merge(
        df_custom_rules,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_custom_rules with {len(bu_enrich_cols)} BU columns")

if not df_output.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_output.columns]
    df_output = pd.merge(
        df_output,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_details with {len(bu_enrich_cols)} BU columns")

if not df_rules.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_rules.columns]
    df_rules = pd.merge(
        df_rules,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_rule_details with {len(bu_enrich_cols)} BU columns")

if not df_adaptive.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_adaptive.columns]
    df_adaptive = pd.merge(
        df_adaptive,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_adaptive_rule_details with {len(bu_enrich_cols)} BU columns")

if not df_dupes.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_dupes.columns]
    df_dupes = pd.merge(
        df_dupes,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_dupe_details with {len(bu_enrich_cols)} BU columns")

if not df_patterns.empty and not df_bu.empty:
    # Define which columns from df_bu to merge into each output
    bu_enrich_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_patterns.columns]
    df_patterns = pd.merge(
        df_patterns,
        df_bu[["dataset"] + bu_enrich_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on="dataset",
    )
    logger.info(f"Enriched dataset_pattern_details with {len(bu_enrich_cols)} BU columns")

# Build df_dataset_custom_rules using df_custom_rules with preferred column order
preferred_order = [
    "dataset", "runId", "db_nm", "table_nm",
    "ruleNm", "ruleType", "ruleRepo", "columnName", "ruleValue", "businessDesc",
    "dimId", "dimName", "score", "perc", "exception",
    "business_unit", "Market", "Project", "CDE", "jobSchedule", "Data Domain", "subDomain", "connectionName",
]

remaining_cols = [c for c in df_custom_rules.columns if c not in preferred_order]
final_cols = [c for c in preferred_order if c in df_custom_rules.columns] + remaining_cols

df_dataset_custom_rules = df_custom_rules[final_cols] if not df_custom_rules.empty else pd.DataFrame(columns=final_cols)

logger.info(f"Generated dataset_custom_rules with {len(df_dataset_custom_rules)} rows")

# Build dqm_dashboard_by_data_domain using df_output + enriched df_bu
sub_scores = [
    "shapeScore",
    "dupeScore",
    "patternScore",
    "outlierScore",
    "schemaScore",
    "recordScore",
    "ruleScore",
    "sourceScore",
    "behaviorScore",
]

if not df_output.empty:
    for col in sub_scores:
        if col not in df_output.columns:
            df_output[col] = 0

    df_output["DQScore"] = 100 - df_output[sub_scores].fillna(0).sum(axis=1)

    dataset_cols = [
        "dataset",
        "runId",
        "runDate",
        "rows",
        "passFail",
        "passFailLimit",
        "peak",
        "dayOfWeek",
        "timeZone",
        "avgRows",
        "cols",
        "activeRules",
        "activeAlerts",
        "runTime",
        "shapeScore",
        "dupeScore",
        "patternScore",
        "outlierScore",
        "schemaScore",
        "recordScore",
        "ruleScore",
        "sourceScore",
        "behaviorScore",
        "DQScore",
        "jobSchedule",
        "business_unit",
    ]
    data_domain_cols = ["dataset", "Data Domain", "CDE", "Project", "Market"]

    existing_dataset_cols = [c for c in dataset_cols if c in df_output.columns]
    existing_data_domain_cols = [c for c in data_domain_cols if c in df_bu.columns]

    dqm_dashboard_by_data_domain_df = pd.merge(
        df_output[existing_dataset_cols],
        df_bu[existing_data_domain_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on=["dataset"],
    )
    dqm_dashboard_by_data_domain_df["report_time"] = datetime.now().strftime("%Y-%m")
else:
    dqm_dashboard_by_data_domain_df = pd.DataFrame()

# Write outputs
logger.info("Writing output tables...")
write_output(df_bu, "business_unit_mapping")
write_output(df_output, "dataset_details")
write_output(df_outliers, "dataset_outlier_details")
write_output(df_rules, "dataset_rule_details")
write_output(df_adaptive, "dataset_adaptive_rule_details")
write_output(dqm_dashboard_by_data_domain_df, "dqm_dashboard_by_data_domain")
write_output(df_dataset_definitions, "dataset_definitions")
write_output(df_dataset_custom_rules, "dataset_custom_rules")
write_output(df_dupes, "dataset_dupe_details")
write_output(df_patterns, "dataset_pattern_details")

# Write dqm_dashboard_by_data_domain to PostgreSQL database
logger.info("Writing dqm_dashboard_by_data_domain to PostgreSQL...")
write_to_postgres(dqm_dashboard_by_data_domain_df)

logger.info("\n" + "=" * 60)
logger.info("S2 Pipeline completed successfully!")
logger.info("=" * 60)
