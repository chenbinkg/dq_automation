# -*- coding: utf-8 -*-
"""
S7: Investigate and Invalidate Adaptive JIRA Tickets
====================================================

Investigates adaptive rule breaking events to determine persistence, applies
heuristics to identify false positives (stable breaks), and auto-invalidates
breaks via Collibra API. Updates JIRA tickets with invalidation comments and
auto-closes when all breaks are resolved. Also identifies high-scoring adaptive
tickets for notification (score > 10).

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                              | Produced by | Description                    |
|-----------------------------------------|-------------|--------------------------------|
| dataset_adaptive_rule_details           | S2          | Breaking adaptive rule items   |
| issue_list_prepared                     | S5          | Active issue inventory         |
| new_jira_ticket_list                    | S6          | Tickets created/reopened       |
| potential_adaptive_rule_jira_ticket_list| S5          | Adaptive ticket candidates     |

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name                    | Description                                    |
|-------------------------------|------------------------------------------------|
| break_list_to_invalidate      | Breaks identified for Collibra invalidation    |
| break_list_investigated       | Full investigation report (all breaks analyzed)|
| invalidated_break_list        | Breaks successfully invalidated via API        |
| notification_list            | Combined invalidated breaks + high-score alerts|

External APIs Called
--------------------
- Collibra CDQ
    - GET /v3/jobs/{dataset}/{runId}/findings  – Fetch breaking items
    - POST /v2/pass-field  – Invalidate specific breaks
- Jira REST API v2
    - GET /rest/api/2/issue/{key}  – Fetch issue details
    - POST /rest/api/2/issue/{key}/comment  – Add invalidation comments
    - GET /rest/api/2/issue/{key}/transitions  – Get close transitions
    - POST /rest/api/2/issue/{key}/transitions  – Close ticket
- PostgreSQL
    - SELECT latest runs from dqm_dashboard_history_apac

Investigation Algorithm
------------------------
1. Load all breaking adaptive items from S2
2. For each dataset, fetch last 3+ runs from PostgreSQL history
3. Check persistence: Count runs where break appears (must be ≥3 to auto-invalidate)
4. Apply heuristics:
   - h_rc_small_change: Row count break with ±20% fluctuation → invalidate
   - h_null_reduces: NULL % dropped + row count stable → invalidate
   - h_profile_with_row_ok: Profile break + row count stable → invalidate
5. Candidates = breaks meeting persistence + heuristics
6. For each candidate: Call Collibra pass-field to invalidate
7. Update JIRA ticket with [AUTO_INVALIDATE] comment
8. If all breaks invalidated: Auto-close ticket

High-Score Ticket Alert Detection
----------------------------------
1. Load new_jira_ticket_list (S6 output)
2. Filter for all new tickets with score > 10.0
3. Exclude already-invalidated datasets
4. Mark with notification_type="high_score_ticket" for S9
5. Include in notification_list

Notification Types
------------------
- invalidated_break: Breaks that were successfully invalidated (sent to S9)
- high_score_ticket: New tickets with score > 10 requiring attention (sent to S9)

Configuration Parameters
------------------------
- PERSIST_RUNS = 3: Minimum runs to confirm persistence
- ROW_FLUCT_PCT = 20.0: Row count stability threshold
- PROFILE_TYPES = {MIN, MAX, CARDINALITY, DATA_TYPE, UNIQUE, UNIQUENESS}
- Score threshold for high-alert notifications = 10.0

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- cdq_base_url_apac / cdq_base_url_cn
- username_apac / password_apac / username_cn / password_cn
- jira_url / jira_api_token / jira_ca_bundle
- db_host / db_port / db_name / db_user / db_password / db_table
- uc_catalog / uc_schema

Auto-Invalidation Criteria
--------------------------
- Break must persist across ≥3 recent runs
- Row count must be within ±20% (or row count break not present)
- NULL % reduced (for NULL breaks) OR profile stable (for profile breaks)
- Not all breaks require same conditions; each type has specific heuristics
"""

from __future__ import annotations

import ast
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

import numpy as np
import pandas as pd
import requests
import urllib3

import config
from pipeline_io import PipelineIO
from postgres_io import build_settings, read_sql
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

# -------- Credential loading --------
raw_jira_url = _load_secret_or_default("jira_url", config.JIRA_URL)
jira_token = _load_secret_or_default("jira_api_token", config.JIRA_API_TOKEN)
jira_verify_ssl_raw = _load_secret_or_default("jira_verify_ssl", config.JIRA_VERIFY_SSL)
jira_ca_bundle = _load_secret_or_default("jira_ca_bundle", config.JIRA_CA_BUNDLE)

cdq_base_url = _load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC)
cdq_username = _load_secret_or_default("username_apac", config.CDQ_USERNAME_APAC)
cdq_password = _load_secret_or_default("password_apac", config.CDQ_PASSWORD_APAC)
cdq_verify_ssl_raw = _load_secret_or_default("cdq_verify_ssl", config.CDQ_VERIFY_SSL)
cdq_ca_bundle = _load_secret_or_default("cdq_ca_bundle", config.CDQ_CA_BUNDLE)

postgres_settings = build_settings(
    host=_load_secret_or_default("db_host", config.DB_HOST),
    port=_load_secret_or_default("db_port", config.DB_PORT),
    dbname=_load_secret_or_default("db_name", config.DB_NAME),
    user=_load_secret_or_default("db_user", config.DB_USER),
    password=_load_secret_or_default("db_password", config.DB_PASSWORD),
    table=_load_secret_or_default("db_table", config.DB_TABLE),
)

if not raw_jira_url or not jira_token:
    raise ValueError("JIRA_URL and JIRA_API_TOKEN must be configured")
if not cdq_base_url or not cdq_username or not cdq_password:
    raise ValueError("CDQ_BASE_URL_APAC, CDQ_USERNAME_APAC, and CDQ_PASSWORD_APAC must be configured")

jira_match = re.match(r"(https?://[^/]+)", str(raw_jira_url).strip())
if not jira_match:
    raise ValueError("Invalid JIRA URL format")

JIRA_BASE = jira_match.group(1)
JIRA_VERIFY_SSL = parse_bool(jira_verify_ssl_raw, default=False)
JIRA_CA_BUNDLE = jira_ca_bundle

CDQ_VERIFY_SSL = parse_bool(cdq_verify_ssl_raw, default=False)
CDQ_CA_BUNDLE = cdq_ca_bundle

if not JIRA_VERIFY_SSL or not CDQ_VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _jira_verify_value() -> Any:
    if JIRA_VERIFY_SSL:
        return JIRA_CA_BUNDLE or True
    return False


def _cdq_verify_value() -> Any:
    if CDQ_VERIFY_SSL:
        return CDQ_CA_BUNDLE or True
    return False


TIMEOUT = (10, 60)
cdq_token_mgr = CollibraTokenManager(
    base_url=str(cdq_base_url),
    username=str(cdq_username),
    password=str(cdq_password),
    region="apac",
)


SESSION = requests.Session()
SESSION.headers.update(
    {
        "Authorization": f"Bearer {jira_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
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


def _cdq_post(
    path: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: tuple = TIMEOUT,
) -> requests.Response:
    url = f"{str(cdq_base_url).rstrip('/')}{path}"
    headers = cdq_token_mgr.get_auth_header()
    headers = {**headers, "Content-Type": "application/json", "Accept": "application/json"}
    response = requests.post(
        url,
        headers=headers,
        data=json.dumps(payload) if payload is not None else None,
        params=params,
        verify=_cdq_verify_value(),
        timeout=timeout,
    )
    if response.status_code == 401:
        logger.warning("CDQ token expired. Refreshing token and retrying request: %s", path)
        cdq_token_mgr.get_token(force_refresh=True)
        headers = cdq_token_mgr.get_auth_header()
        headers = {**headers, "Content-Type": "application/json", "Accept": "application/json"}
        response = requests.post(
            url,
            headers=headers,
            data=json.dumps(payload) if payload is not None else None,
            params=params,
            verify=_cdq_verify_value(),
            timeout=timeout,
        )
    return response


# -------- Heuristic config --------
PERSIST_RUNS = 3
ROW_FLUCT_PCT = 20.0
PROFILE_TYPES = {"MIN VALUE", "MAX VALUE", "CARDINALITY", "DATA_TYPE", "UNIQUE", "UNIQUENESS"}
AUTO_TAG = "[AUTO_INVALIDATE]"
ACTIVE_STATUSES = {"Open", "Reopened", "In Progress", "To Do"}


# -------- Utility helpers --------
def safe_float(value: Any) -> float:
    try:
        out = float(value)
        if not np.isfinite(out):
            return np.nan
        return out
    except Exception:
        return np.nan


def parse_assignment(value: Any) -> Tuple[Optional[int], Optional[str]]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None, None
    if isinstance(value, dict):
        return value.get("id"), value.get("uuid")

    text = str(value).strip()
    if not text or text.lower() in ("nan", "none", "null"):
        return None, None

    try:
        parsed = ast.literal_eval(text)
        if isinstance(parsed, dict):
            return parsed.get("id"), parsed.get("uuid")
    except Exception:
        pass

    try:
        parsed = json.loads(text)
        if isinstance(parsed, dict):
            return parsed.get("id"), parsed.get("uuid")
    except Exception:
        pass

    return None, None


def infer_break_type(row: pd.Series) -> str:
    break_type = row.get("type")
    if pd.notna(break_type) and str(break_type).strip():
        return str(break_type).strip().upper()

    key = str(row.get("key", "") or "")
    if "__" in key:
        return key.split("__", 1)[1].strip().upper()
    return ""


def is_row_count_break(row: pd.Series) -> bool:
    break_type = str(row.get("type", "") or "").upper().strip()
    key = str(row.get("key", "") or "").upper()
    name = str(row.get("name", "") or "").upper().strip()
    return ("ROW_COUNT" in break_type) or ("__ROW_COUNT" in key) or (name == "ROW COUNT")


def _run_key_from_value(value: Any) -> Optional[str]:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None

    dt = pd.to_datetime(text, errors="coerce")
    if pd.notna(dt):
        return dt.strftime("%Y-%m-%d")
    return text[:10]


def fetch_last_runs(settings, datasets: List[str], n_runs: int = 3) -> pd.DataFrame:
    if settings is None or not datasets:
        return pd.DataFrame(columns=["dataset", "runId", "runDate", "rows"])

    escaped = []
    for ds in datasets:
        escaped.append("'" + str(ds).replace("'", "''") + "'")
    ds_sql = ", ".join(escaped)
    q = f"""
    SELECT *
    FROM (
        SELECT
            h."dataset",
            h."runId",
            h."runDate",
            h."rows",
            ROW_NUMBER() OVER (PARTITION BY h."dataset" ORDER BY h."runDate" DESC) AS rn
        FROM {settings.table} h
        WHERE h."dataset" IN ({ds_sql})
    ) t
    WHERE t.rn <= {int(n_runs)}
    ORDER BY t."dataset", t."runDate" DESC
    """
    try:
        return read_sql(q, settings)
    except Exception as exc:
        logger.warning("Failed to read history from PostgreSQL: %s", exc)
        return pd.DataFrame(columns=["dataset", "runId", "runDate", "rows"])


def get_findings(dataset: str, run_id: str):
    run_enc = quote(str(run_id), safe="")
    response = _cdq_get(f"/v3/jobs/{dataset}/{run_enc}/findings", timeout=TIMEOUT)
    if response.status_code >= 400:
        return None, response.status_code, response.text[:500]
    try:
        return response.json(), response.status_code, None
    except Exception as exc:
        return None, response.status_code, f"Invalid JSON: {exc}"


def extract_breaking_dqitem_keys(payload: Any) -> Set[str]:
    if not isinstance(payload, dict):
        return set()
    dqitems = payload.get("dqItems") or {}
    out: Set[str] = set()
    if isinstance(dqitems, dict):
        for key, value in dqitems.items():
            if isinstance(value, dict) and str(value.get("status", "")).lower() == "breaking":
                out.add(str(key))
    return out


def build_annotation(row: pd.Series) -> str:
    reasons = [f"persist_cnt={int(row['persist_cnt'])}/{PERSIST_RUNS} across runs [{row.get('checked_runs', '')}]"]
    if bool(row.get("h_null_reduces")):
        reasons.append(f"NULL% reduced (perChange={safe_float(row.get('perChange')):.2f}%) and row_ok (±{ROW_FLUCT_PCT:.0f}%)")
    if bool(row.get("h_profile_with_row_ok")):
        reasons.append(f"{row.get('break_type')} break but row_ok (±{ROW_FLUCT_PCT:.0f}%)")
    if bool(row.get("h_rc_small_change")):
        reasons.append(
            f"ROW_COUNT change within ±{ROW_FLUCT_PCT:.0f}% (row_pct_change={safe_float(row.get('row_pct_change')):.2f}%)"
        )
    reasons.append(
        f"mean={safe_float(row.get('mean')):.6g}, value={safe_float(row.get('value')):.6g}, z={safe_float(row.get('zscore')):.3g}"
    )
    return " | ".join(reasons)


# -------- JIRA helpers --------
def jira_get_issue(issue_key: str, fields: str = "summary,status,description,comment") -> Dict[str, Any]:
    response = SESSION.get(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}",
        params={"fields": fields},
        timeout=30,
        verify=_jira_verify_value(),
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"Jira GET {issue_key} failed {response.status_code}: {response.text[:500]}")
    return response.json()


def jira_add_comment(issue_key: str, body: str) -> None:
    response = SESSION.post(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}/comment",
        data=json.dumps({"body": body}),
        timeout=30,
        verify=_jira_verify_value(),
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"Jira comment {issue_key} failed {response.status_code}: {response.text[:500]}")


def jira_get_transitions(issue_key: str) -> List[Dict[str, Any]]:
    response = SESSION.get(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}/transitions",
        timeout=30,
        verify=_jira_verify_value(),
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"Jira transitions {issue_key} failed {response.status_code}: {response.text[:500]}")
    return response.json().get("transitions", [])


def jira_transition(issue_key: str, transition_id: str) -> None:
    response = SESSION.post(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}/transitions",
        data=json.dumps({"transition": {"id": str(transition_id)}}),
        timeout=30,
        verify=_jira_verify_value(),
    )
    if response.status_code >= 400:
        raise requests.HTTPError(f"Jira transition {issue_key} failed {response.status_code}: {response.text[:500]}")


def find_transition_id(
    transitions: List[Dict[str, Any]],
    *,
    to_status: Optional[str] = None,
    name_contains: Optional[str] = None,
) -> Optional[str]:
    to_status_lower = (to_status or "").strip().lower()
    name_contains_lower = (name_contains or "").strip().lower()

    for transition in transitions or []:
        t_name = str(transition.get("name", "")).strip().lower()
        t_to = str((transition.get("to") or {}).get("name", "")).strip().lower()

        if to_status_lower and t_to == to_status_lower:
            return transition.get("id")
        if name_contains_lower and name_contains_lower in t_name:
            return transition.get("id")
    return None


def get_status_name(issue_json: Dict[str, Any]) -> str:
    try:
        return str(((issue_json.get("fields") or {}).get("status") or {}).get("name") or "")
    except Exception:
        return ""


def close_issue_with_path(issue_key: str) -> Tuple[bool, str]:
    issue = jira_get_issue(issue_key, fields="status")
    status_name = get_status_name(issue)

    if status_name.lower().strip() != "in progress":
        transitions = jira_get_transitions(issue_key)
        transition_id = (
            find_transition_id(transitions, to_status="In Progress")
            or find_transition_id(transitions, name_contains="start progress")
            or find_transition_id(transitions, name_contains="start")
        )
        if transition_id:
            jira_transition(issue_key, transition_id)
        else:
            return False, f"No transition found to move into In Progress from '{status_name}'."

    transitions = jira_get_transitions(issue_key)
    for target in ["Completed", "Done", "Closed", "Resolved"]:
        transition_id = find_transition_id(transitions, to_status=target)
        if transition_id:
            jira_transition(issue_key, transition_id)
            return True, f"Transitioned to {target}."

    for token in ["complete", "done", "close", "resolve"]:
        transition_id = find_transition_id(transitions, name_contains=token)
        if transition_id:
            jira_transition(issue_key, transition_id)
            return True, f"Transitioned via '{token}'."

    return False, "No close/complete transition available after moving to In Progress."


BREAK_LINE_RE = re.compile(
    r"\*{1,2}(?P<col>[^*\n]+?)\*{1,2}\s*[—–]\s*\*{1,2}(?P<typ>[^*\n]+?)\*{1,2}",
    flags=re.IGNORECASE,
)
AUTO_INV_RE = re.compile(r"\[AUTO_INVALIDATE\].*?invalidated:\s*(.+)", flags=re.IGNORECASE | re.DOTALL)


def norm_break(col: str, typ: str) -> str:
    c = re.sub(r"\s+", " ", str(col).strip())
    t = re.sub(r"\s+", " ", str(typ).strip()).upper()
    return f"{c}__{t}"


def extract_breaks_from_text(text: str) -> Set[str]:
    if not isinstance(text, str) or not text.strip():
        return set()
    out: Set[str] = set()
    for match in BREAK_LINE_RE.finditer(text):
        out.add(norm_break(match.group("col"), match.group("typ")))
    return out


def extract_invalidated_from_comments(comments: List[Dict[str, Any]]) -> Set[str]:
    out: Set[str] = set()
    for comment in comments or []:
        body = comment.get("body") or ""
        match = AUTO_INV_RE.search(body)
        if not match:
            continue
        for token in re.split(r"[, \n\r\t]+", match.group(1)):
            token = token.strip()
            if "__" in token:
                out.add(token)
    return out


def pick_issue_for_dataset(dataset: str, df_tasks: pd.DataFrame, df_new: pd.DataFrame) -> Optional[str]:
    if not isinstance(dataset, str) or not dataset.strip():
        return None

    if not df_tasks.empty:
        subset = df_tasks.copy()
        subset = subset[subset.get("issue_type", "").astype(str).str.lower().eq("sub-task")]
        subset = subset[subset.get("parent_summary", "").astype(str).eq(dataset)]

        active = subset[subset.get("status", "").astype(str).isin(list(ACTIVE_STATUSES))]
        if not active.empty:
            if "date_updated" in active.columns:
                active = active.sort_values("date_updated", ascending=False)
            return str(active.iloc[0].get("key"))

        if not subset.empty:
            if "date_updated" in subset.columns:
                subset = subset.sort_values("date_updated", ascending=False)
            return str(subset.iloc[0].get("key"))

    if not df_new.empty and "dataset" in df_new.columns and "SUBTASK" in df_new.columns:
        fallback = df_new[df_new["dataset"].astype(str).eq(dataset)]
        if not fallback.empty:
            return str(fallback.iloc[0]["SUBTASK"])

    return None


def cdq_pass_field(dataset: str, run_id: str, item: str, metric_type: str) -> Tuple[bool, int, str]:
    params = {
        "dataset": dataset,
        "runId": run_id,
        "item": item,
        "metricType": metric_type,
    }
    response = _cdq_post("/v2/pass-field", params=params, timeout=TIMEOUT)
    if response.status_code >= 400:
        return False, response.status_code, response.text[:800]
    return True, response.status_code, response.text[:800]


def investigate_breaks(df_breaks: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_b = df_breaks.copy()
    if df_b.empty:
        return pd.DataFrame(), pd.DataFrame()

    if "status" in df_b.columns:
        df_b = df_b[df_b["status"].astype(str).str.lower().eq("breaking")].copy()

    if df_b.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_b["assignment_id"], df_b["assignment_uuid"] = zip(*df_b["assignmentId"].map(parse_assignment))
    df_b = df_b[df_b["assignment_id"].notna() & df_b["assignment_uuid"].notna()].copy()

    if df_b.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_b["break_key"] = df_b["key"].astype(str)
    df_b["break_type"] = df_b.apply(infer_break_type, axis=1)
    df_b["is_row_count"] = df_b.apply(is_row_count_break, axis=1)

    for col in ["perChange", "score", "mean", "value", "zscore", "lbAbs", "ubAbs"]:
        if col in df_b.columns:
            df_b[col] = pd.to_numeric(df_b[col], errors="coerce")

    datasets = sorted(df_b["dataset"].dropna().astype(str).unique().tolist())

    df_hist = fetch_last_runs(postgres_settings, datasets, n_runs=PERSIST_RUNS + 1)
    df_hist["runDate_dt"] = pd.to_datetime(df_hist.get("runDate"), errors="coerce", utc=True)
    df_hist["rows_num"] = pd.to_numeric(df_hist.get("rows"), errors="coerce")

    if not df_hist.empty:
        df_hist = df_hist.sort_values(["dataset", "runDate_dt"], ascending=[True, True])
        df_hist["rows_prev"] = df_hist.groupby("dataset")["rows_num"].shift(1)
        df_hist["row_pct_change"] = (df_hist["rows_num"] - df_hist["rows_prev"]) / df_hist["rows_prev"] * 100.0
        df_hist["run_key"] = df_hist["runId"].map(_run_key_from_value)
    else:
        df_hist = pd.DataFrame(columns=["dataset", "runId", "runDate_dt", "rows_num", "rows_prev", "row_pct_change", "run_key"])

    df_last_runs = (
        df_hist.sort_values(["dataset", "runDate_dt"], ascending=[True, False])
        .groupby("dataset", as_index=False)
        .head(PERSIST_RUNS)
    )

    runs_by_dataset = (
        df_last_runs.groupby("dataset")["runId"].apply(lambda s: [str(x) for x in s.dropna().tolist()]).to_dict()
    )

    rowchg_map = (
        df_hist[["dataset", "run_key", "row_pct_change", "rows_num", "rows_prev"]]
        .dropna(subset=["run_key"])
        .set_index(["dataset", "run_key"])
        .to_dict("index")
    )

    breaking_keys_cache: Dict[Tuple[str, str], Set[str]] = {}
    errors: List[Tuple[str, str, Any, str]] = []

    for ds, run_list in runs_by_dataset.items():
        for rid in run_list:
            cache_key = (ds, rid)
            if cache_key in breaking_keys_cache:
                continue
            payload, status, err = get_findings(ds, rid)
            if payload is None:
                errors.append((ds, rid, status, str(err)))
                breaking_keys_cache[cache_key] = set()
            else:
                breaking_keys_cache[cache_key] = extract_breaking_dqitem_keys(payload)

    def persistence_count(ds: str, break_key: str) -> Tuple[int, List[str], List[str]]:
        run_list = runs_by_dataset.get(ds, [])
        present_runs: List[str] = []
        for rid in run_list:
            if break_key in breaking_keys_cache.get((ds, rid), set()):
                present_runs.append(rid)
        return len(present_runs), present_runs, run_list

    df_b["persist_cnt"] = 0
    df_b["persist_runs"] = ""
    df_b["checked_runs"] = ""

    for idx, row in df_b.iterrows():
        cnt, present_runs, checked = persistence_count(str(row["dataset"]), str(row["break_key"]))
        df_b.at[idx, "persist_cnt"] = cnt
        df_b.at[idx, "persist_runs"] = ", ".join(present_runs)
        df_b.at[idx, "checked_runs"] = ", ".join(checked)

    df_b["persist_ok"] = df_b["persist_cnt"] >= PERSIST_RUNS

    has_rc_break = (
        df_b.groupby(["dataset", "runId"], as_index=False)["is_row_count"]
        .any()
        .rename(columns={"is_row_count": "has_row_count_break"})
    )
    df_b = df_b.merge(has_rc_break, on=["dataset", "runId"], how="left")
    df_b["run_key"] = df_b["runId"].map(_run_key_from_value)

    def get_rowchg(ds: str, run_key: Optional[str]) -> Tuple[float, float, float]:
        if not run_key:
            return np.nan, np.nan, np.nan

        ds_breaks = df_b[df_b["dataset"].astype(str).eq(ds)]
        if "type" in ds_breaks.columns and ds_breaks["type"].astype(str).str.upper().eq("ROW_COUNT").any():
            row_count_rows = ds_breaks[ds_breaks["type"].astype(str).str.upper().eq("ROW_COUNT")]
            if not row_count_rows.empty:
                first = row_count_rows.iloc[0]
                return first.get("perChange", np.nan), first.get("value", np.nan), first.get("mean", np.nan)

        info = rowchg_map.get((str(ds), str(run_key)))
        if not info:
            return np.nan, np.nan, np.nan
        return info.get("row_pct_change", np.nan), info.get("rows_num", np.nan), info.get("rows_prev", np.nan)

    df_b[["row_pct_change", "rows_current", "rows_prev"]] = pd.DataFrame(
        df_b.apply(lambda r: get_rowchg(str(r["dataset"]), r.get("run_key")), axis=1).tolist(), index=df_b.index
    )

    df_b["row_ok"] = np.where(
        df_b["has_row_count_break"] == False,
        True,
        df_b["row_pct_change"].abs() < ROW_FLUCT_PCT,
    )

    df_b["is_null_break"] = df_b["break_type"].str.contains("NULL", na=False)
    df_b["h_rc_small_change"] = df_b["is_row_count"] & (df_b["row_pct_change"].abs() < ROW_FLUCT_PCT)
    df_b["h_null_reduces"] = df_b["is_null_break"] & (df_b["perChange"] < 0) & (df_b["row_ok"])
    df_b["h_profile_with_row_ok"] = df_b["break_type"].isin(PROFILE_TYPES) & (df_b["row_ok"])

    df_b["to_invalidate"] = df_b["persist_ok"] & (
        df_b["h_rc_small_change"] | df_b["h_null_reduces"] | df_b["h_profile_with_row_ok"]
    )

    candidates = df_b[df_b["to_invalidate"]].copy()
    if candidates.empty:
        if errors:
            logger.warning("Findings API errors: %s", errors[:20])
        return pd.DataFrame(), df_b

    invalidated_break_list_df = pd.DataFrame(
        {
            "dataset": candidates["dataset"],
            "runId": candidates["runId"],
            "break_key": candidates["break_key"],
            "break_type": candidates["break_type"],
            "assignment_id": candidates["assignment_id"].astype("Int64"),
            "assignment_uuid": candidates["assignment_uuid"].astype(str),
            "includeKey": True,
            "annotation": candidates.apply(build_annotation, axis=1),
            "persist_cnt": candidates["persist_cnt"],
            "persist_runs": candidates["persist_runs"],
            "checked_runs": candidates["checked_runs"],
            "row_pct_change": candidates["row_pct_change"],
            "rows_current": candidates["rows_current"],
            "rows_prev": candidates["rows_prev"],
        }
    )

    if errors:
        logger.warning("Findings API errors: %s", errors[:20])

    return invalidated_break_list_df, df_b


def identify_high_score_adaptive_tickets(
    df_new_jira: pd.DataFrame,
    df_breaks: pd.DataFrame,
    invalidated_break_list: pd.DataFrame,
    score_threshold: float = 10.0,
) -> pd.DataFrame:
    """
    Identify all new tickets with high scores that should trigger notifications.
    
    Filters for:
    - Score > score_threshold (default: 10)
    - Not already in the invalidated list (to avoid duplicate notifications)
    
    Includes break details (break_key, break_type) by matching with df_breaks using
    dataset and runId when available. This allows S9 to display actual breaking rules
    in notifications for tickets tied to known findings.
    
    Returns a dataframe with columns suitable for combining with invalidated_break_list,
    including issue_key from SUBTASK column for email recipient lookup in S9.
    """
    if df_new_jira.empty:
        return pd.DataFrame()
    
    df = df_new_jira.copy()
    
    # Ensure score is numeric
    if "score" in df.columns:
        df["score"] = pd.to_numeric(df["score"], errors="coerce")
    else:
        return pd.DataFrame()
    
    # Filter for all new tickets with high scores
    high_score = df[df["score"] > score_threshold].copy()
    
    if high_score.empty:
        logger.info(f"No high-scoring tickets found (threshold: {score_threshold})")
        return pd.DataFrame()
    
    # Filter out datasets already in the invalidated list (avoid duplicate notifications)
    if not invalidated_break_list.empty:
        invalidated_datasets = set(invalidated_break_list["dataset"].dropna().astype(str).unique())
        high_score = high_score[~high_score["dataset"].astype(str).isin(invalidated_datasets)].copy()
    
    if high_score.empty:
        logger.info("All high-scoring tickets already have invalidation notifications")
        return pd.DataFrame()
    
    # Join with breaking rules to get break details for each high-score ticket when available
    # Match by dataset and runId to find the actual breaks when the data is available
    if not df_breaks.empty and "dataset" in df_breaks.columns and "runId" in df_breaks.columns:
        df_breaks_filtered = df_breaks.copy()
        
        # Filter breaking rules to only those in high_score datasets
        high_score_datasets = set(high_score["dataset"].astype(str).unique())
        df_breaks_filtered = df_breaks_filtered[
            df_breaks_filtered["dataset"].astype(str).isin(high_score_datasets)
        ].copy()
        
        # Prepare break summaries: group by dataset/runId to get list of breaking rules
        if not df_breaks_filtered.empty:
            # Create a break summary field for each row
            df_breaks_filtered["break_key"] = df_breaks_filtered.get("key", "")
            df_breaks_filtered["break_type"] = df_breaks_filtered.apply(infer_break_type, axis=1)
            
            # Group breaks by dataset and runId
            break_summary = (
                df_breaks_filtered
                .groupby(["dataset", "runId"], as_index=False, dropna=False)
                .agg({
                    "break_key": lambda x: " | ".join(x.dropna().astype(str).unique().tolist()),
                    "break_type": lambda x: " | ".join(x.dropna().astype(str).unique().tolist()),
                })
                .rename(columns={"break_key": "detected_breaks", "break_type": "break_types"})
            )
            
            # Join high_score with break_summary
            high_score = high_score.merge(
                break_summary,
                on=["dataset", "runId"],
                how="left",
            )
        else:
            high_score["detected_breaks"] = ""
            high_score["break_types"] = ""
    else:
        high_score["detected_breaks"] = ""
        high_score["break_types"] = ""
    
    # Format for notification output (handle optional columns gracefully)
    result_data = {
        "dataset": high_score["dataset"],
        "runId": high_score["runId"],
        "score": high_score["score"],
        "suggested_title": high_score["suggested_title"],
        "suggested_priority": high_score["suggested_priority"],
        "detected_breaks": high_score.get("detected_breaks", ""),
        "break_types": high_score.get("break_types", ""),
        "notification_type": "high_score_ticket",
        "notification_title": "High-Scoring Tickets Requiring Attention",
    }
    
    # Add issue_key from SUBTASK column for S9 recipient lookup
    # SUBTASK contains the JIRA issue key created in S6
    if "SUBTASK" in high_score.columns:
        result_data["issue_key"] = high_score["SUBTASK"]
    else:
        # Fallback: try to find issue_key column or set to None
        if "issue_key" in high_score.columns:
            result_data["issue_key"] = high_score["issue_key"]
        else:
            result_data["issue_key"] = None
            logger.warning("No SUBTASK or issue_key column found in df_new_jira; S9 may not be able to send emails")
    
    # Add optional columns if they exist
    if "business_unit" in high_score.columns:
        result_data["business_unit"] = high_score["business_unit"]
    if "rows_current" in high_score.columns:
        result_data["rows_current"] = high_score["rows_current"]
    if "rows_prev" in high_score.columns:
        result_data["rows_prev"] = high_score["rows_prev"]
    if "rows_pct_change_vs_prev" in high_score.columns:
        result_data["rows_pct_change_vs_prev"] = high_score["rows_pct_change_vs_prev"]
    
    result = pd.DataFrame(result_data)
    
    logger.info(f"Identified {len(result)} high-scoring tickets for notification (score > {score_threshold})")
    return result


def invalidate_and_update_jira(
    df_inv: pd.DataFrame,
    df_tasks: pd.DataFrame,
    df_new: pd.DataFrame,
) -> pd.DataFrame:
    if df_inv.empty:
        return pd.DataFrame()

    required_cols = {"dataset", "assignment_id", "assignment_uuid", "annotation", "break_key", "runId"}
    missing = required_cols - set(df_inv.columns)
    if missing:
        raise ValueError(f"break_list_to_invalidate missing columns: {missing}")

    results: List[pd.DataFrame] = []

    for dataset, group in df_inv.groupby("dataset", dropna=True):
        logger.info("Invalidating adaptive rules for %s", dataset)
        issue_key = pick_issue_for_dataset(str(dataset), df_tasks=df_tasks, df_new=df_new)

        inv_rows: List[Dict[str, Any]] = []
        for _, row in group.iterrows():
            break_key = str(row.get("break_key") or "").strip()
            item = ""
            metric_type = ""
            if "__" in break_key:
                item, metric_type = break_key.split("__", 1)

            run_id = str(row.get("runId") or "").strip()
            if run_id and item and metric_type:
                ok_pf, status_pf, msg_pf = cdq_pass_field(
                    dataset=str(dataset),
                    run_id=run_id,
                    item=item,
                    metric_type=metric_type,
                )
                logger.info(
                    "pass-field dataset=%s item=%s metric=%s runId=%s ok=%s status=%s",
                    dataset,
                    item,
                    metric_type,
                    run_id,
                    ok_pf,
                    status_pf,
                )
            else:
                ok_pf, status_pf, msg_pf = (
                    False,
                    0,
                    f"Missing required pass-field input(s): runId='{run_id}', break_key='{break_key}'",
                )

            inv_rows.append(
                {
                    "dataset": dataset,
                    "issue_key": issue_key,
                    "runId": row.get("runId"),
                    "break_key": row.get("break_key"),
                    "assignment_id": row.get("assignment_id"),
                    "assignment_uuid": row.get("assignment_uuid"),
                    "annotation": row.get("annotation"),
                    "pass_field_ok": ok_pf,
                    "pass_field_http_status": status_pf,
                    "pass_field_http_msg": msg_pf,
                }
            )

        df_inv_res = pd.DataFrame(inv_rows)
        if not issue_key:
            logger.info("No JIRA issue found for dataset %s", dataset)
            results.append(df_inv_res)
            continue

        ok_breaks = df_inv_res[df_inv_res["pass_field_ok"] == True]["break_key"].dropna().astype(str).tolist()
        if not ok_breaks:
            results.append(df_inv_res)
            continue

        try:
            issue = jira_get_issue(issue_key, fields="summary,status,description,comment")
        except Exception as exc:
            df_inv_res["jira_error"] = str(exc)
            results.append(df_inv_res)
            continue

        fields = issue.get("fields", {}) or {}
        description = fields.get("description", "") or ""
        comments = ((fields.get("comment") or {}).get("comments") or [])

        raised_breaks = extract_breaks_from_text(description)
        already_invalidated = extract_invalidated_from_comments(comments)

        friendly = []
        for break_key in ok_breaks:
            if "__" in break_key:
                col, typ = break_key.split("__", 1)
                friendly.append(f"{col} — {typ}")
            else:
                friendly.append(break_key)

        comment_body = (
            f"{AUTO_TAG} Adaptive findings invalidated via Collibra API.\n\n"
            f"{AUTO_TAG} invalidated: " + ", ".join(ok_breaks) + "\n\n"
            "Break(s):\n- " + "\n- ".join(friendly) + "\n\n"
            "Note: If all breaks in this ticket have been invalidated, the ticket will be auto-closed."
        )

        try:
            jira_add_comment(issue_key, comment_body)
        except Exception as exc:
            df_inv_res["jira_comment_error"] = str(exc)

        now_invalidated = set(ok_breaks)
        invalidated_total = already_invalidated.union(now_invalidated)
        outstanding = set()
        should_close = False

        if raised_breaks:
            outstanding = raised_breaks - invalidated_total
            should_close = len(outstanding) == 0

        if should_close:
            try:
                ok_close, msg = close_issue_with_path(issue_key)
                if ok_close:
                    jira_add_comment(issue_key, f"{AUTO_TAG} All breaks invalidated. Auto-closing ticket. ({msg})")
                else:
                    jira_add_comment(issue_key, f"{AUTO_TAG} All breaks invalidated, but could not auto-close. ({msg})")
            except Exception as exc:
                df_inv_res["jira_close_error"] = str(exc)

        df_inv_res["raised_breaks_cnt"] = len(raised_breaks)
        df_inv_res["already_invalidated_cnt"] = len(already_invalidated)
        df_inv_res["newly_invalidated_cnt"] = len(now_invalidated)
        df_inv_res["outstanding_cnt"] = len(outstanding)
        df_inv_res["should_close"] = should_close

        results.append(df_inv_res)

    return pd.concat(results, axis=0, ignore_index=True) if results else pd.DataFrame()


def format_invalidated_for_notification(invalidated_df: pd.DataFrame) -> pd.DataFrame:
    """Add notification metadata to invalidated breaks for consistent processing in s9."""
    if invalidated_df.empty:
        return invalidated_df
    
    result = invalidated_df.copy()
    result["notification_type"] = "invalidated_break"
    result["notification_title"] = "Adaptive Rules Invalidated"
    return result


def run() -> None:
    logger.info("Starting S7 investigation/invalidation step")

    df_new_jira = read_input("new_jira_ticket_list")
    df_breaks = read_input("dataset_adaptive_rule_details")
    df_tasks = read_input("issue_list_prepared")

    break_list_to_invalidate, break_list_investigated = investigate_breaks(df_breaks)

    write_output(break_list_to_invalidate, "break_list_to_invalidate")
    write_output(break_list_investigated, "break_list_investigated")

    invalidated_break_list = invalidate_and_update_jira(
        break_list_to_invalidate,
        df_tasks=df_tasks,
        df_new=df_new_jira,
    )
    
    # Add notification metadata to invalidated breaks
    invalidated_break_list = format_invalidated_for_notification(invalidated_break_list)
    
    # Identify high-scoring tickets that need attention
    high_score_tickets = identify_high_score_adaptive_tickets(
        df_new_jira,
        df_breaks,
        invalidated_break_list,
        score_threshold=10.0,
    )
    
    # Combine invalidated breaks and high-score tickets for notification
    # Invalidated: for notifying about invalidation results
    # High-score: for notifying about potential issues that need attention
    notification_list = pd.concat(
        [invalidated_break_list, high_score_tickets],
        axis=0,
        ignore_index=True,
    ) if not invalidated_break_list.empty or not high_score_tickets.empty else pd.DataFrame()
    
    write_output(invalidated_break_list, "invalidated_break_list")
    write_output(notification_list, "notification_list")

    logger.info(
        "S7 complete. investigated=%s, to_invalidate=%s, invalidated=%s, high_score_notifications=%s",
        len(break_list_investigated),
        len(break_list_to_invalidate),
        len(invalidated_break_list),
        len(high_score_tickets),
    )


if __name__ == "__main__":
    run()
