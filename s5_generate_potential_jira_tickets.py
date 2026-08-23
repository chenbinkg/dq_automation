# -*- coding: utf-8 -*-
"""
S5: Generate Potential JIRA Tickets from Adaptive Rule Breaks
=============================================================

Fetches all active JIRA issues, analyzes adaptive rule breaking events, and
generates candidate DQ tickets ranked by severity. Excludes CN datasets.
Produces issue inventories and curated ticket candidates for S6 creation/update.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                    | Produced by | Description                                 |
|-------------------------------|-------------|---------------------------------------------|
| dataset_adaptive_rule_details | S2          | Column-level breaking adaptive rule items   |
| dataset_rule_details          | S2          | Run-level custom rule metrics (score/perc)  |
| business_unit_mapping         | S2          | Dataset → BU, market, project, CDE metadata |
| dataset_dupe_details          | S2          | Datasets breaking duplication check         |
| dataset_pattern_details       | S2          | Datasets breaking pattern check             |

External APIs Called
--------------------
- Jira REST API
    - GET /rest/api/2/search  – Fetch all active issues (via JQL)
    - GET /rest/api/3/issues/search  – Alternative issue search (v3)

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name                              | Description                                        |
|-----------------------------------------|----------------------------------------------------|
| issue_list                              | All active Jira issues (raw from API)              |
| issue_list_prepared                     | Processed issue inventory with normalized metadata |
| task_list_prepared                      | Parent task/story records for linking              |
| epic_list_prepared                      | Epic records for impact analysis                   |
| mwaa_dq_trigger_datasets                | Reference: Datasets that trigger MWAA workflows    |
| potential_jira_ticket_list              | Non-adaptive DQ ticket candidates (standard rules) |
| potential_adaptive_rule_jira_ticket_list| Adaptive rule break ticket candidates (scored)     |

Adaptive Ticket Candidate Criteria
-----------------------------------
- Source: dataset_adaptive_rule_details (breaking items only)
- Filtered: Excludes datasets in MWAA trigger reference list
- Status: Only "BREAKING" status items (when status column exists)
- Granularity: One row per (dataset, runId) group
- Title: Generated from strongest break signals:
  * Row count drops (highest severity)
  * NULL % changes
  * Profile anomalies (cardinality, data type, uniqueness)
  * Min/max value shifts
- Stability: If active adaptive subtask exists, reuses existing title to keep summary stable
- Scoring: Multi-factor scoring considers:
  * Row count change magnitude and direction
  * NULL percentage shifts
  * Z-score deviation
  * Number of concurrent breaking adaptive items
  * Historical change patterns

Processing Flow
---------------
1. Fetch all active Jira issues via REST API
2. Process and normalize issue metadata
3. Load adaptive rule breaking events from S2
4. Generate candidate titles and descriptions
5. Score candidates by severity and impact
6. Dedup by (dataset, runId), keep highest-scoring
7. Produce issue lists and ticket candidates
8. Write outputs for S6 to consume

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

External Integrations
----------------------
- Jira: Fetches active issues, generates candidates for S6
- PostgreSQL: Optional historical context (currently disabled)
"""

from __future__ import annotations

import ast
import json
import logging
import math
import os
import re
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
import numpy as np
import pandas as pd
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from rapidfuzz import fuzz
except Exception:
    fuzz = None

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
except Exception:
    TfidfVectorizer = None
    cosine_similarity = None

from mwaa_dataset_reference import MWAADatasetReference
from pipeline_io import PipelineIO
from token_manager import CollibraTokenManager
from postgres_io import build_settings, read_sql
import config


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

WRITE_MODE = os.getenv("PIPELINE_WRITE_MODE", "csv").strip().lower()
LOCAL_OUTPUT_DIR = os.getenv("PIPELINE_LOCAL_OUTPUT_DIR", "./outputs")
SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")

logger.info("Pipeline write mode: %s", WRITE_MODE)

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

jira_url = _load_secret_or_default("jira_url", config.JIRA_URL)
jira_token = _load_secret_or_default("jira_api_token", config.JIRA_API_TOKEN)
jira_projects_raw = _load_secret_or_default("jira_project_keys", config.JIRA_PROJECT_KEYS)
jira_verify_ssl_raw = _load_secret_or_default("jira_verify_ssl", config.JIRA_VERIFY_SSL)
jira_ca_bundle = _load_secret_or_default("jira_ca_bundle", config.JIRA_CA_BUNDLE)
cdq_base_url = _load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC)
cdq_url_apac = _load_secret_or_default("cdq_base_url_apac", config.CDQ_BASE_URL_APAC)
cdq_url_cn = _load_secret_or_default("cdq_base_url_cn", config.CDQ_BASE_URL_CN)
username_apac = _load_secret_or_default("username_apac", config.COLLIBRA_USERNAME_APAC)
password_apac = _load_secret_or_default("password_apac", config.COLLIBRA_PASSWORD_APAC)
username_cn = _load_secret_or_default("username_cn", config.COLLIBRA_USERNAME_CN)
password_cn = _load_secret_or_default("password_cn", config.COLLIBRA_PASSWORD_CN)
uc_catalog = _load_secret_or_default("uc_catalog", getattr(config, "UC_CATALOG", None))
uc_schema = _load_secret_or_default("uc_schema", getattr(config, "UC_SCHEMA", None))


postgres_settings = build_settings(
    host=_load_secret_or_default("db_host", config.DB_HOST),
    port=_load_secret_or_default("db_port", config.DB_PORT),
    dbname=_load_secret_or_default("db_name", config.DB_NAME),
    user=_load_secret_or_default("db_user", config.DB_USER),
    password=_load_secret_or_default("db_password", config.DB_PASSWORD),
    table=_load_secret_or_default("db_table", config.DB_TABLE),
)

def parse_project_keys(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw).strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    return [part.strip() for part in text.split(",") if part.strip()]


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


jira_projects = parse_project_keys(jira_projects_raw)
jira_verify_ssl = parse_bool(jira_verify_ssl_raw, default=False)
if not jira_url or not jira_token or not jira_projects:
    raise ValueError("JIRA_URL, JIRA_API_TOKEN, and JIRA_PROJECT_KEYS must be configured")

if not jira_verify_ssl:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
    config_module=config,
    secret_scope=SECRET_SCOPE,
    uc_catalog=uc_catalog,
    uc_schema=uc_schema,
    logger=logger,
    sanitize_uc_table_names=True,
)

read_input = pipeline_io.read_input
write_output = pipeline_io.write_output

def get_jira_verify_value() -> Any:
    if jira_verify_ssl:
        return jira_ca_bundle or True
    return False

def build_jira_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=int(os.getenv("JIRA_HTTP_TOTAL_RETRIES", "5")),
        connect=int(os.getenv("JIRA_HTTP_CONNECT_RETRIES", "5")),
        read=int(os.getenv("JIRA_HTTP_READ_RETRIES", "5")),
        backoff_factor=float(os.getenv("JIRA_HTTP_BACKOFF_FACTOR", "1.0")),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"Authorization": f"Bearer {jira_token}", "Accept": "application/json"})
    return session

def normalize_jira_search_url(url: str) -> str:
    base = str(url).strip().rstrip("/")
    if "/rest/api/" in base and base.endswith("/search"):
        return base
    if "/rest/api/" in base:
        return f"{base}/search"
    return f"{base}/rest/api/2/search"


def fetch_jira_issue_list() -> pd.DataFrame:
    search_url = normalize_jira_search_url(jira_url)
    project_query = ",".join(jira_projects)
    fields = (
        "summary,status,priority,issuetype,labels,components,versions,assignee,created,"
        "resolution,reporter,resolutiondate,updated,parent"
    )
    session = build_jira_session()

    start_at = 0
    max_results = 1000
    issues: List[Dict[str, Any]] = []

    while True:
        params = {
            "jql": f"project in ({project_query})",
            "fields": fields,
            "startAt": start_at,
            "maxResults": max_results,
        }
        response = session.get(
            search_url,
            params=params,
            timeout=(10, 60),
            verify=get_jira_verify_value(),
        )
        response.raise_for_status()
        payload = response.json()
        batch = payload.get("issues", [])
        issues.extend(batch)

        total = int(payload.get("total", len(issues)))
        logger.info("Fetched JIRA issues: %s/%s", len(issues), total)
        if not batch or len(issues) >= total:
            break
        start_at += len(batch)

    if not issues:
        return pd.DataFrame()
    return pd.json_normalize(issues).sort_index(axis=1)


def parse_listish(cell: Any) -> List[Any]:
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str):
        try:
            value = ast.literal_eval(cell)
            return value if isinstance(value, list) else []
        except Exception:
            return []
    return []


def to_date(value: Any):
    if pd.isna(value):
        return None
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def ym_from_date(value: Any) -> Optional[str]:
    if value is None or pd.isna(value):
        return None
    return str(value)[:7]


def extract_version_name(values: List[Dict[str, Any]]) -> Optional[str]:
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, dict):
            return first.get("name")
    return None


def extract_market(values: List[Dict[str, Any]]) -> Optional[str]:
    if isinstance(values, list) and values:
        first = values[0]
        if isinstance(first, dict):
            return first.get("description")
    return None


def get_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column]
    return pd.Series([None] * len(df), index=df.index, dtype=object)


def prepare_jira_outputs(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df.empty:
        empty = pd.DataFrame()
        return empty, empty, empty

    out = pd.DataFrame()
    out["assignee"] = get_series(df, "fields.assignee.displayName")
    out["assignee_email"] = get_series(df, "fields.assignee.emailAddress")

    components = get_series(df, "fields.components").apply(parse_listish)
    out["project"] = components.apply(
        lambda values: values[0].get("name") if isinstance(values, list) and values and isinstance(values[0], dict) else None
    )

    out["date_created"] = get_series(df, "fields.created").apply(to_date)
    out["date_created_yyyymm"] = out["date_created"].apply(ym_from_date)
    out["date_updated"] = get_series(df, "fields.updated").apply(to_date)
    out["issue_duration_days"] = (
        out["date_updated"] - out["date_created"]
    ).apply(lambda value: value.days if pd.notnull(value) else None)
    out["date_updated_yyyymm"] = out["date_updated"].apply(ym_from_date)
    out["labels"] = get_series(df, "fields.labels")
    out["parent_summary"] = get_series(df, "fields.parent.fields.summary")
    out["parent_key"] = get_series(df, "fields.parent.key")
    out["priority"] = get_series(df, "fields.priority.name")
    out["reporter"] = get_series(df, "fields.reporter.displayName")
    out["resolution_name"] = get_series(df, "fields.resolution.name")
    out["date_resolved"] = get_series(df, "fields.resolutiondate").apply(to_date)
    out["date_resolved_yyyymm"] = out["date_resolved"].apply(ym_from_date)
    out["status"] = get_series(df, "fields.status.name")
    out["title"] = get_series(df, "fields.summary")
    out["issue_type"] = get_series(df, "fields.issuetype.name")

    versions = get_series(df, "fields.versions").apply(parse_listish)
    out["version_name"] = versions.apply(extract_version_name)
    out["market"] = versions.apply(extract_market)
    out["key"] = get_series(df, "key")
    out["report_date"] = out["date_updated"].apply(ym_from_date)

    out = out[out["version_name"] != "DELETED"].copy()

    task_df = out[(out["issue_type"] == "Task") & (out["title"].fillna("").str.startswith("ds_"))].copy()
    subtask_df = out[(out["issue_type"] == "Sub-task") & (out["parent_summary"].fillna("").str.startswith("ds_"))].copy()
    epic_df = out[out["issue_type"] == "Epic"].copy()
    return subtask_df, task_df, epic_df


_COL_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")
MAX_RULEVALUE_LEN = 80
ACTIVE_STATUSES = {"Open", "To Do", "In Progress", "Reopened"}


class TitleMatcher:
    def __init__(self):
        self.vec = (
            TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1, lowercase=True)
            if TfidfVectorizer is not None
            else None
        )
        self.vec_word = (
            TfidfVectorizer(
                analyzer="word", ngram_range=(1, 2), token_pattern=r"(?u)\b\w+\b", lowercase=True, min_df=1
            )
            if TfidfVectorizer is not None
            else None
        )
        self.titles: List[str] = []
        self.X_char = None
        self.X_word = None
        self.colsets: List[set[str]] = []

    def fit(self, titles: Sequence[str]) -> None:
        kept_titles: List[str] = []
        normalized: List[str] = []
        for title in titles:
            norm = normalize_title(title)
            if norm:
                kept_titles.append(str(title))
                normalized.append(norm)

        if not normalized:
            self.titles = []
            self.X_char = None
            self.X_word = None
            self.colsets = []
            return

        self.titles = kept_titles
        if self.vec is not None:
            self.X_char = self.vec.fit_transform(normalized)
        else:
            self.X_char = None
        if self.vec_word is not None:
            try:
                self.X_word = self.vec_word.fit_transform(normalized)
            except ValueError:
                self.X_word = None
        else:
            self.X_word = None
        self.colsets = [extract_column_tokens(title) for title in kept_titles]

    def best_match(self, new_title: str) -> Tuple[Optional[str], float]:
        if not self.titles:
            return None, 0.0

        normalized = normalize_title(new_title)
        if self.vec is not None and self.X_char is not None and cosine_similarity is not None:
            x_char = self.vec.transform([normalized])
            s_char = cosine_similarity(x_char, self.X_char).ravel()
        else:
            s_char = np.array([
                SequenceMatcher(None, normalized, normalize_title(title)).ratio() for title in self.titles
            ])

        if self.vec_word is not None and self.X_word is not None and cosine_similarity is not None:
            x_word = self.vec_word.transform([normalized])
            s_word = cosine_similarity(x_word, self.X_word).ravel()
        else:
            s_word = np.zeros_like(s_char)

        if fuzz is not None:
            fuzz_scores = np.array([fuzz.token_set_ratio(new_title, title) / 100.0 for title in self.titles])
        else:
            fuzz_scores = np.array([
                SequenceMatcher(None, normalize_title(new_title), normalize_title(title)).ratio()
                for title in self.titles
            ])
        new_cols = extract_column_tokens(new_title)
        overlap = np.array([len(new_cols & colset) for colset in self.colsets], dtype=float)
        if overlap.size and overlap.max() > 0:
            overlap = overlap / overlap.max()
        else:
            overlap = np.zeros_like(s_char)

        blended = 0.35 * s_char + 0.35 * s_word + 0.20 * fuzz_scores + 0.10 * overlap
        idx = int(blended.argmax())
        return self.titles[idx], float(blended[idx])


def extract_column_tokens(text: Any) -> set[str]:
    if not isinstance(text, str):
        return set()
    tokens = {token.lower() for token in _COL_TOKEN_RE.findall(text)}
    return {token for token in tokens if "_" in token or token.endswith("id") or token.endswith("code")}


def normalize_title(text: Any) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text.strip().lower())


def _norm(text: Any) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", str(text)).strip().lower()


def _labels_to_list(value: Any) -> List[str]:
    if isinstance(value, list):
        return [str(item).strip().lower() for item in value if str(item).strip()]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        parsed = parse_listish(text)
        if parsed:
            return [str(item).strip().lower() for item in parsed if str(item).strip()]
        return [part.strip().lower() for part in text.split(",") if part.strip()]
    return []


def _dim_to_upper(dim_name: Any) -> str:
    if dim_name is None:
        return ""
    try:
        if isinstance(dim_name, float) and math.isnan(dim_name):
            return ""
    except Exception:
        pass
    value = str(dim_name).strip()
    return value.upper() if value else ""


def _extract_col_from_rule_name(rule_name: Any) -> Optional[str]:
    if not isinstance(rule_name, str):
        return None
    match = re.match(r"^if_([^_]+(?:_[^_]+)*)_is_", rule_name)
    return match.group(1) if match else None


def _map_rule_to_title(rule_name: Any, rule_value: Any, dim_name: Any) -> str:
    dim = _dim_to_upper(dim_name)
    rule_name_text = str(rule_name or "").strip().lower()
    column = str(rule_value or "").strip() or (_extract_col_from_rule_name(rule_name_text) or "")

    if "brick_code_name_pair" in rule_name_text:
        return "brick_code_name_pair: mismatch"
    if "tr_jnj_check_null_empty" in rule_name_text:
        return f"{column}: NULL" if column else "NULL values"
    if "tr_jnj_anz_code_brick4" in rule_name_text:
        return f"{column}: Invalid" if column else "code: Invalid"
    if "tr_jnj_id_sfdcid" in rule_name_text:
        return f"{column or 'source_id'}: invalid Salesforce ID"

    if dim == "COMPLETENESS":
        return f"{column}: NULL" if column else f"Completeness: NULL values {rule_name}"
    if dim == "VALIDITY":
        return f"{column}: Invalid" if column else f"Validity: Invalid values {rule_name}"
    if dim == "CONSISTENCY":
        return f"{column}: Inconsistent" if column else f"Consistency: Inconsistencies {rule_name}"
    if dim == "ACCURACY":
        return f"{column}: Inaccurate" if column else f"Accuracy: Inaccurate values {rule_name}"
    if dim == "UNIQUENESS":
        return f"{column}: Duplicates" if column else f"Uniqueness: Duplicates {rule_name}"
    if dim == "TIMELINESS":
        return f"{column}: Out-of-date" if column else f"Timeliness: Out-of-date values {rule_name}"

    if column:
        return f"{column}: {rule_name_text}"
    return f"{rule_name_text}: Issue"


def _labels_for_dim(dim_name: Any) -> List[str]:
    dim = _dim_to_upper(dim_name)
    labels = ["Data", "Rule"]
    if dim:
        labels.insert(0, dim.capitalize())
    return labels


def _priority_from_perc(perc: Any) -> str:
    try:
        value = float(perc)
    except Exception:
        return "Medium"
    if value >= 10:
        return "Critical"
    if value >= 1:
        return "High"
    return "Medium"


def _priority_from_total_score(total_score: Any) -> str:
    try:
        value = float(total_score)
        if not np.isfinite(value):
            raise ValueError
    except Exception:
        return "Medium"
    if value >= 20:
        return "Critical"
    if value >= 10:
        return "High"
    return "Medium"


def _value_for_custom_title(row: pd.Series) -> str:
    column_name = row.get("columnName")
    if pd.notna(column_name) and str(column_name).strip():
        return str(column_name).strip()
    rule_value = row.get("ruleValue")
    if pd.notna(rule_value):
        rule_value_text = str(rule_value).strip()
        if rule_value_text and len(rule_value_text) <= MAX_RULEVALUE_LEN:
            return rule_value_text
    return str(row.get("ruleNm", "")).strip()


def build_matcher_for_dataset(df_existing: pd.DataFrame, dataset: str) -> TitleMatcher:
    if df_existing.empty:
        matcher = TitleMatcher()
        matcher.fit([])
        return matcher
    mask = (
        df_existing["parent_summary"].astype(str).eq(dataset)
        & df_existing["issue_type"].astype(str).str.lower().eq("sub-task")
        & df_existing["status"].astype(str).isin(list(ACTIVE_STATUSES))
    )
    titles = df_existing.loc[mask, "title"].dropna().astype(str).tolist()
    matcher = TitleMatcher()
    matcher.fit(titles)
    return matcher


def _boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or pd.isna(value):
        return False
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


def _extract_dataset_exclusions(df_dataset_cn: pd.DataFrame) -> set[str]:
    if df_dataset_cn is None or df_dataset_cn.empty or "dataset" not in df_dataset_cn.columns:
        return set()
    return {
        str(dataset).strip()
        for dataset in df_dataset_cn["dataset"].dropna().astype(str).tolist()
        if str(dataset).strip()
    }


def _exclude_datasets(df: pd.DataFrame, excluded_datasets: set[str], dataset_col: str = "dataset") -> pd.DataFrame:
    if df is None or df.empty or not excluded_datasets or dataset_col not in df.columns:
        return df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame()
    return df[~df[dataset_col].astype(str).isin(excluded_datasets)].copy()


def build_custom_break_candidates(
    df_rule_details: pd.DataFrame,
    df_custom_rules: pd.DataFrame,
    df_existing: pd.DataFrame,
    df_business_unit_mapping: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if df_rule_details.empty or df_custom_rules.empty:
        return pd.DataFrame()

    break_cols = [
        column
        for column in ["dataset", "runId", "ruleNm", "breakMsg", "score", "perc", "exception"]
        if column in df_rule_details.columns
    ]
    breaks = df_rule_details[break_cols].copy()
    custom = df_custom_rules.copy()

    if "runId" not in breaks.columns and "run_date" in breaks.columns:
        breaks = breaks.rename(columns={"run_date": "runId"})

    merged = pd.merge(
        custom,
        breaks,
        how="left",
        on=[column for column in ["dataset", "runId", "ruleNm"] if column in custom.columns and column in breaks.columns],
        suffixes=("", "_break"),
    )

    if "breakMsg" in merged.columns:
        merged = merged[merged["breakMsg"].astype(str).str.upper().eq("BREAKING")].copy()
    else:
        merged = merged[pd.to_numeric(merged.get("perc"), errors="coerce") > 0].copy()

    if "jobSchedule" in merged.columns:
        merged = merged[merged["jobSchedule"].map(_boolish)].copy()

    if merged.empty:
        return pd.DataFrame()

    matchers: Dict[str, TitleMatcher] = {}
    records: List[Dict[str, Any]] = []

    for _, row in merged.iterrows():
        dataset = str(row.get("dataset", ""))
        title = _map_rule_to_title(row.get("ruleNm"), _value_for_custom_title(row), row.get("dimName"))

        if dataset not in matchers:
            matchers[dataset] = build_matcher_for_dataset(df_existing, dataset)
        _, score = matchers[dataset].best_match(title)
        if score >= 0.72:
            continue

        perc = pd.to_numeric(row.get("perc"), errors="coerce")
        issue_lines = [
            f"*Dataset:* {row.get('dataset')}",
            f"*DataTable:* {row.get('db_nm')}.{row.get('table_nm')}" if pd.notna(row.get("db_nm")) and pd.notna(row.get("table_nm")) else "*DataTable:* NA",
            f"*RunId:* {row.get('runId')}",
            f"*Project:* {row.get('Project')}",
            f"*Rule:* {row.get('ruleNm')}",
            f"*Dimension:* {row.get('dimName')}",
            f"*Affected percentage:* {perc:.2f}%" if pd.notna(perc) else "*Affected percentage:* NA",
        ]
        if pd.notna(row.get("columnName")):
            issue_lines.append(f"*Column:* {row.get('columnName')}")
        if pd.notna(row.get("businessDesc")) and str(row.get("businessDesc")).strip():
            issue_lines.append("")
            issue_lines.append(str(row.get("businessDesc")).strip())

        # Build table_name from db_nm and table_nm
        table_name = None
        db_nm = row.get("db_nm")
        table_nm = row.get("table_nm")
        if pd.notna(db_nm) and pd.notna(table_nm):
            table_name = f"{db_nm}.{table_nm}"

        records.append(
            {
                "dataset": row.get("dataset"),
                "table_name": table_name,
                "runId": row.get("runId"),
                "ruleNm": row.get("ruleNm"),
                "ruleType": row.get("ruleType"),
                "ruleValue": row.get("ruleValue"),
                "dimName": row.get("dimName"),
                "score": row.get("score"),
                "perc": row.get("perc"),
                "business_unit": row.get("Project"),
                "violation_columns": row.get("columnName"),
                "suggested_title": title,
                "suggested_labels": _labels_for_dim(row.get("dimName")),
                "suggested_priority": _priority_from_perc(row.get("perc")),
                "jira_description": "\n".join(issue_lines).strip(),
            }
        )

    output = pd.DataFrame(records)
    if output.empty:
        return output
    output = output[pd.to_numeric(output["perc"], errors="coerce") > 0.0].copy()
    return output.sort_values(by=["perc", "dataset", "suggested_title"], ascending=[False, True, True])


def _safe_float(value: Any) -> float:
    try:
        number = float(value)
        if not np.isfinite(number):
            return np.nan
        return number
    except Exception:
        return np.nan


def _format_pct(value: Any, digits: int = 2) -> str:
    number = _safe_float(value)
    if np.isnan(number):
        return "NA"
    return f"{number:.{digits}f}%"


def _format_num(value: Any) -> str:
    number = _safe_float(value)
    if np.isnan(number):
        return "NA"
    if abs(number - int(number)) < 1e-9:
        return str(int(number))
    return f"{number:,.6g}"


def _is_row_count_break(row: pd.Series) -> bool:
    break_type = str(row.get("type", "")).upper().strip()
    key = str(row.get("key", "")).upper().strip()
    name = str(row.get("name", "")).upper().strip()
    return "ROW_COUNT" in break_type or "ROW_COUNT" in key or name == "ROW COUNT"


def _extract_column_from_key(key: Any, fallback: str = "") -> str:
    if not isinstance(key, str) or not key.strip():
        return fallback
    return key.split("__", 1)[0].strip() or fallback


def _extract_break_type(row: pd.Series) -> str:
    break_type = row.get("type")
    if pd.notna(break_type) and str(break_type).strip():
        return str(break_type).strip().upper()
    key = str(row.get("key", "")).strip()
    if "__" in key:
        return key.split("__", 1)[1].strip().upper()
    return ""


def _build_breaks_markdown(df_run: pd.DataFrame) -> str:
    """
    Build a markdown-formatted string summarizing the adaptive profile changes detected in a run.
    """
    lines = ["*Adaptive profile changes detected in this run:*", ""]
    ranked = df_run.copy()
    ranked["score_num"] = ranked["score"].apply(_safe_float)
    ranked = ranked.sort_values(["score_num"], ascending=False)

    for _, row in ranked.iterrows():
        column = str(row.get("name", "")).strip() or _extract_column_from_key(row.get("key"), "")
        break_type = _extract_break_type(row)
        verbose = str(row.get("verbose", "")).strip()

        lines.append(f"- *{column}* — *{break_type}*")
        if verbose:
            lines.append(f"  - Anomaly: {verbose}")
        lines.append(
            "  - Past (mean): "
            f"{_format_num(row.get('mean'))} | Current (value): {_format_num(row.get('value'))} | "
            f"Z-score: {_format_num(row.get('zscore'))}"
        )
        lines.append(
            "  - Bands: "
            f"[{_format_num(row.get('lbAbs'))}, {_format_num(row.get('ubAbs'))}] | "
            f"% change: {_format_pct(row.get('perChange'))} | Score impact: {_format_pct(row.get('score'))}"
        )
        if pd.notna(row.get("assignmentId")) and str(row.get("assignmentId")).strip():
            lines.append(f"  - assignmentId: {row.get('assignmentId')}")
        lines.append("")

    return "\n".join(lines).strip()


def _suggest_title_for_run(df_run: pd.DataFrame) -> str:
    """
    Suggest a title for the run based on the top scoring breaks and row count changes.
    Used for adaptive rule breaks only.
    """
    ranked = df_run.copy()
    ranked["score_num"] = ranked["score"].apply(_safe_float)
    ranked["is_row_count"] = ranked.apply(_is_row_count_break, axis=1)

    non_row = ranked[~ranked["is_row_count"]].copy()
    non_row["column_for_title"] = non_row.apply(
        lambda row: str(row.get("name", "")).strip() or _extract_column_from_key(row.get("key"), ""),
        axis=1,
    )
    non_row = non_row[non_row["column_for_title"].astype(str).str.strip().ne("")]
    non_row = non_row.sort_values("score_num", ascending=False)

    columns = non_row["column_for_title"].drop_duplicates().head(2).tolist() or ["Row Count"]
    columns_part = ", ".join(columns)

    if bool(ranked["is_row_count"].any()):
        row_count = ranked[ranked["is_row_count"]].sort_values("score_num", ascending=False).head(1)
        suffix = "Row Count Changed"
        if not row_count.empty:
            mean_value = _safe_float(row_count.iloc[0].get("mean"))
            current_value = _safe_float(row_count.iloc[0].get("value"))
            pct_change = _safe_float(row_count.iloc[0].get("perChange"))
            if np.isfinite(current_value) and np.isfinite(mean_value) and current_value != mean_value:
                suffix = "Row Count Increased" if current_value > mean_value else "Row Count Dropped"
            elif np.isfinite(pct_change) and pct_change != 0:
                suffix = "Row Count Increased" if pct_change > 0 else "Row Count Dropped"
        return f"{columns_part}: {suffix}"

    return f"{columns_part}: Profile Change"


def fetch_row_history_last7(datasets: Sequence[str]) -> pd.DataFrame:
    if not datasets or postgres_settings is None:
        return pd.DataFrame()

    ds_list_sql = ", ".join(["'" + str(dataset).replace("'", "''") + "'" for dataset in datasets])
    query = f"""
    SELECT *
    FROM (
        SELECT
            h.*,
            ROW_NUMBER() OVER (PARTITION BY h."dataset" ORDER BY h."runDate" DESC) AS rn
        FROM {postgres_settings.table} h
        WHERE h."dataset" IN ({ds_list_sql})
    ) t
    WHERE t.rn <= 7
    ORDER BY t."dataset", t."runDate" DESC
    """

    try:
        return read_sql(query, postgres_settings)
    except Exception as exc:
        logger.warning("Could not read row history from PostgreSQL: %s", exc)
        return pd.DataFrame()


def build_row_history_markdown(df_hist_ds: pd.DataFrame, run_dt: Optional[pd.Timestamp]) -> Tuple[str, Dict[str, Any]]:
    if df_hist_ds is None or df_hist_ds.empty:
        return "*Row count history (last 7 runs):* NA (no history found)", {
            "rows_current": np.nan,
            "rows_prev": np.nan,
            "rows_pct_change_vs_prev": np.nan,
        }

    history = df_hist_ds.copy()
    history["runDate_dt"] = pd.to_datetime(history["runDate"], errors="coerce", utc=True)
    history["rows_num"] = pd.to_numeric(history["rows"], errors="coerce")
    history_asc = history.sort_values("runDate_dt", ascending=True)
    history_asc["pct_change_vs_prev"] = history_asc["rows_num"].pct_change() * 100.0

    rows_current = np.nan
    rows_prev = np.nan
    pct_vs_prev = np.nan

    if run_dt is not None and pd.notna(run_dt):
        matched = history_asc[history_asc["runDate_dt"] == run_dt]
        if not matched.empty:
            idx = matched.index[-1]
            rows_current = history_asc.loc[idx, "rows_num"]
            pct_vs_prev = history_asc.loc[idx, "pct_change_vs_prev"]
            prev_values = history_asc.loc[history_asc["runDate_dt"] < run_dt, "rows_num"]
            if not prev_values.empty:
                rows_prev = prev_values.iloc[-1]
        else:
            rows_current = history_asc["rows_num"].iloc[-1]
            pct_vs_prev = history_asc["pct_change_vs_prev"].iloc[-1]
            if len(history_asc) >= 2:
                rows_prev = history_asc["rows_num"].iloc[-2]
    else:
        rows_current = history_asc["rows_num"].iloc[-1]
        pct_vs_prev = history_asc["pct_change_vs_prev"].iloc[-1]
        if len(history_asc) >= 2:
            rows_prev = history_asc["rows_num"].iloc[-2]

    display = history_asc.sort_values("runDate_dt", ascending=False).head(7)
    display["runDate_sgt"] = display["runDate_dt"].dt.tz_convert("Asia/Singapore")

    lines = [
        "*Row count history (last 7 runs)*",
        "",
        "| runDate | rows | % change vs prev | behaviorScore | DQScore |",
        "|---|---:|---:|---:|---:|",
    ]
    for _, row in display.iterrows():
        run_date = row.get("runDate_sgt", pd.NaT)
        run_date_text = run_date.strftime("%Y-%m-%d %H:%M:%S SGT") if pd.notna(run_date) else "NA"
        lines.append(
            f"| {run_date_text} | {_format_num(row.get('rows_num'))} | {_format_pct(row.get('pct_change_vs_prev'))} | "
            f"{_format_num(row.get('behaviorScore'))} | {_format_num(row.get('DQScore'))} |"
        )

    return "\n".join(lines), {
        "rows_current": rows_current,
        "rows_prev": rows_prev,
        "rows_pct_change_vs_prev": pct_vs_prev,
    }


def make_cdq_link(dataset: Any) -> str:
    if not cdq_base_url:
        return ""
    return f"{str(cdq_base_url).rstrip('/')}/dq/finding?dataset={dataset}"


def build_adaptive_break_candidates(
        df_adaptive: pd.DataFrame, 
        df_existing: pd.DataFrame, 
        mwaa_reference: MWAADatasetReference, 
        df_business_unit_mapping: Optional[pd.DataFrame] = None
        ) -> pd.DataFrame:
    if df_adaptive.empty:
        return pd.DataFrame()

    adaptive = mwaa_reference.filter_dataframe(df_adaptive, dataset_col="dataset", include=False)
    adaptive["runId_dt"] = pd.to_datetime(adaptive["runId"], errors="coerce", utc=True)

    for column in ["stndDev", "zscore", "mean", "value", "score", "perChange", "lbAbs", "ubAbs"]:
        if column in adaptive.columns:
            adaptive[column] = pd.to_numeric(adaptive[column], errors="coerce")

    adaptive = adaptive.replace([np.inf, -np.inf], np.nan)
    if "status" in adaptive.columns:
        df_breaks = adaptive[adaptive["status"].astype(str).str.lower().eq("breaking")].copy()
    else:
        df_breaks = adaptive.copy()

    if df_breaks.empty:
        return pd.DataFrame()

    df_existing = df_existing.copy()
    if not df_existing.empty:
        df_existing["__ds_norm"] = df_existing["parent_summary"].map(_norm)
        df_existing["__title_norm"] = df_existing["title"].map(_norm)
        df_existing["__is_subtask"] = df_existing["issue_type"].astype(str).str.lower().eq("sub-task")
        df_existing["__is_active"] = df_existing["status"].isin(list(ACTIVE_STATUSES))
        df_existing["__labels_norm"] = df_existing["labels"].apply(_labels_to_list)

        # Dedup keyset from existing JIRA (dataset + title) for active sub-tasks
        existing_active_pairs = set(
            tuple(x) for x in df_existing.loc[df_existing["__is_subtask"] & df_existing["__is_active"], ["__ds_norm", "__title_norm"]]
                            .itertuples(index=False, name=None)
        )
        active_adaptive_rows = df_existing[
            df_existing["__is_subtask"]
            & df_existing["__is_active"]
            & df_existing["__ds_norm"].astype(str).str.strip().ne("")
            & df_existing["__labels_norm"].apply(lambda labels: "adaptive" in labels)
        ].copy()

        if not active_adaptive_rows.empty:
            active_adaptive_rows = active_adaptive_rows.sort_values("date_updated", ascending=False, na_position="last")
            active_adaptive_title_by_dataset = (
                active_adaptive_rows.drop_duplicates(subset=["__ds_norm"], keep="first")
                .set_index("__ds_norm")["title"]
                .astype(str)
                .to_dict()
            )
        else:
            active_adaptive_title_by_dataset = {}
            existing_active_pairs = set()
    else:
        active_adaptive_title_by_dataset = {}
        existing_active_pairs = set()

    # # Merge with business_unit_mapping to get db_nm and table_nm if available
    # if df_business_unit_mapping is not None and not df_business_unit_mapping.empty and "dataset" in df_business_unit_mapping.columns:
    #     bu_cols = [c for c in df_business_unit_mapping.columns if c in ["dataset", "db_nm", "table_nm"]]
    #     df_breaks = pd.merge(
    #         df_breaks,
    #         df_business_unit_mapping[bu_cols].drop_duplicates(subset=["dataset"], keep="first"),
    #         how="left",
    #         on="dataset",
    #         suffixes=("", "_bu"),
    #     )

    df_breaks["__is_row_count"] = df_breaks.apply(_is_row_count_break, axis=1)
    # runs_without_row_count = (
    #     df_breaks.groupby(["dataset", "runId"], as_index=False)["__is_row_count"].any()
    # )
    # runs_without_row_count = runs_without_row_count[~runs_without_row_count["__is_row_count"]]
    # datasets_need_history = sorted(runs_without_row_count["dataset"].dropna().astype(str).unique().tolist())
    # get all datasets that have any breaking adaptive profile changes, regardless of row count
    datasets_need_history = sorted(df_breaks["dataset"].dropna().astype(str).unique().tolist())
    df_hist_all = fetch_row_history_last7(datasets_need_history) if datasets_need_history else pd.DataFrame()

    tickets: List[Dict[str, Any]] = []
    for (dataset, run_id), group in df_breaks.groupby(["dataset", "runId"], dropna=True):
        current_group = group.copy()
        suggested_title = _suggest_title_for_run(current_group)
        stable_title = active_adaptive_title_by_dataset.get(_norm(dataset))
        if isinstance(stable_title, str) and stable_title.strip():
            suggested_title = stable_title

        # # dedup check vs existing active issues
        # if (_norm(dataset), _norm(suggested_title)) in existing_active_pairs:
        #     logger.info(
        #         "Skipping dataset=%s, suggested_title=%s as it already exists",
        #         dataset,
        #         suggested_title,
        #     )
        #     continue

        total_score = pd.to_numeric(current_group["score"], errors="coerce").sum() if "score" in current_group.columns else np.nan

        row_metrics = {
            "rows_current": np.nan,
            "rows_prev": np.nan,
            "rows_pct_change_vs_prev": np.nan,
        }
        row_count = current_group[current_group["name"].astype(str).eq("Row Count")] if "name" in current_group.columns else pd.DataFrame()
        if not row_count.empty:
            row_metrics = {
                "rows_current": row_count.iloc[0].get("value"),
                "rows_prev": row_count.iloc[0].get("mean"),
                "rows_pct_change_vs_prev": row_count.iloc[0].get("perChange"),
            }

        row_history_markdown = ""
        need_history = not bool(current_group["__is_row_count"].any())
        if need_history and not df_hist_all.empty:
            history_for_dataset = df_hist_all[df_hist_all["dataset"].astype(str) == str(dataset)]
            row_history_markdown, row_metrics = build_row_history_markdown(
                history_for_dataset,
                pd.to_datetime(run_id, errors="coerce", utc=True),
            )

        description_parts = [
            f"*Dataset:* {dataset}",
            f"*DataTable:* {current_group.iloc[0].get('db_nm', '')}.{current_group.iloc[0].get('table_nm', '')}" if 'db_nm' in current_group.columns and 'table_nm' in current_group.columns else "",
            f"*RunId (runDate):* {run_id}",
        ]

        if "Project" in current_group.columns:
            project = current_group.iloc[0].get("Project", "")
            if project:
                description_parts.append(f"*Project:* {project}")
        description_parts.append(f"*Total adaptive score impact:* {_format_pct(total_score)}")

        if row_history_markdown:
            description_parts.extend(["", row_history_markdown])
        description_parts.extend(["", _build_breaks_markdown(current_group), ""])

        cdq_link = make_cdq_link(dataset)
        if cdq_link:
            description_parts.append(f"*CDQ Dataset:* {cdq_link}")

        # Build table_name from db_nm and table_nm
        table_name = None
        db_nm = current_group.get("db_nm")
        if not db_nm.empty:
            db_nm_val = db_nm.iloc[0]
            table_nm = current_group.get("table_nm")
            table_nm_val = table_nm.iloc[0] if not table_nm.empty else None
            if pd.notna(db_nm_val) and pd.notna(table_nm_val):
                table_name = f"{db_nm_val}.{table_nm_val}"

        tickets.append(
            {
                "dataset": dataset,
                "table_name": table_name,
                "runId": run_id,
                "business_unit": project,
                "suggested_title": suggested_title,
                "suggested_priority": _priority_from_total_score(total_score),
                "suggested_labels": ["collibra", "dq", "adaptive", "profile-change", "end-users"],
                "score": total_score,
                "rows_current": row_metrics.get("rows_current"),
                "rows_prev": row_metrics.get("rows_prev"),
                "rows_pct_change_vs_prev": row_metrics.get("rows_pct_change_vs_prev"),
                "jira_description": "\n".join(description_parts).strip(),
            }
        )

    return pd.DataFrame(tickets)

def safe_get(
        url: str, 
        token_mgr: CollibraTokenManager, 
        params:Optional[Dict[str, Any]] = None, 
        timeout: int=60
        ) -> Tuple[Optional[Any], Optional[int], Optional[str]]:
    """GET with one token-refresh retry on 401."""
    refreshed_token = False
    last_request_exception: Optional[Exception] = None

    for attempt in range(1, 4):
        try:
            headers = token_mgr.get_auth_header()
            r = requests.get(url, headers=headers, params=params, verify=False, timeout=timeout)
        except requests.exceptions.RequestException as exc:
            last_request_exception = exc
            if attempt == 3:
                return None, None, f"RequestException: {exc}"
            continue

        if r.status_code == 401 and not refreshed_token and attempt < 3:
            token_mgr.get_token(force_refresh=True)
            refreshed_token = True
            continue

        if r.status_code == 200:
            try:
                return r.json(), 200, None
            except ValueError:
                return None, 200, "Invalid JSON"

        err = r.text[:500] if isinstance(r.text, str) else str(r.content)[:500]
        return None, r.status_code, err

    if last_request_exception is not None:
        return None, None, f"RequestException: {last_request_exception}"
    return None, 401, "Unauthorized after retrying with token refresh"

def get_observation_details(
        token_mgr: CollibraTokenManager,
        dataset: str,
        rowkey: str,
        run_id: str,
        ) -> List[Dict[str, Any]]:
    """Fetch pattern observation details from v2/getobservationdetails."""
    url = f"{token_mgr.base_url}/v2/getobservationdetails"
    params = {
            "dataset": dataset,
            "rowKey": rowkey,
            "runId": run_id,
        }
    payload, status, err = safe_get(url, token_mgr, params=params)
    if status != 200:
        logger.warning(
            f"Failed to fetch all observation details "
            f"({token_mgr.region}): status={status}, err={err}"
        )
        return []
    return payload

def get_dupe_opt_include_keys(token_mgr: CollibraTokenManager, dataset: str) -> List[str]:
    """Fetch fallback dupes key columns from v2/dupe-opt/get include list."""
    url = f"{token_mgr.base_url}/v2/dupe-opt/get"
    params = {"dataset": dataset}
    payload, status, err = safe_get(url, token_mgr, params=params)
    if status != 200:
        logger.warning(
            "Failed to fetch dupe-opt config (%s): dataset=%s, status=%s, err=%s",
            token_mgr.region,
            dataset,
            status,
            err,
        )
        return []

    if not isinstance(payload, dict):
        return []
    result = payload.get("result")
    if not isinstance(result, dict):
        return []

    include = result.get("include")
    if not isinstance(include, list):
        return []
    return [str(key).strip() for key in include if str(key).strip()]

def build_pattern_markdown(data: list, pattern_count: int) -> str:
    """
    Build a markdown-formatted string summarizing the pattern observations.

    where data is a list containing details about a pattern observation with 2 records, 
    pattern_counts is the number of occurrences of that anti-pattern observation,
    usually the first observation in the list pair, while second is the majority observation.
    seq is the index of the observation in the group.
    e.g. data:
    [
        {
            "bu_name": "ST UC 前立腺癌ﾏｰｹﾃｨﾝｸﾞ・ｽﾄﾗﾃｼﾞｯｸﾘｴｿﾞﾝ部",
            "bu_code": "00002477"
        },
        {
            "bu_name": "ST UC ﾏｰｹﾃｨﾝｸﾞ部",
            "bu_code": "00002477"
        }
    ]
    """
    if not data:
        return "*Pattern observations:* NA (no details found)"

    first_obs = list(data[0].keys()) # minority/abnormal pattern
    header = " | ".join(["PatternCount"] + first_obs)
    lines = [ 
        f"| {header} |",
    ]
    for i, obs in enumerate(data):
        values = list(data[i].values())
        if i == 0:
            lines.append(f"| {' | '.join([str(pattern_count)] + [str(v) for v in values])} |")
        else:
            lines.append(f"| {' | '.join(["Majority Pattern"] + [str(v) for v in values])} |")

    return "\n".join(lines)

def build_pattern_break_candidates(
        df_pattern: pd.DataFrame, 
        df_dataset_cn: pd.DataFrame, 
        ) -> pd.DataFrame:
    """
    Building potential JIRA tickets for pattern breaks based on the 
    provided DataFrame of pattern observations.
    """
    if df_pattern.empty:
        return pd.DataFrame()

    df_pattern["runId_dt"] = pd.to_datetime(df_pattern["runId"], errors="coerce", utc=True)

    if "jobSchedule" in df_pattern.columns:
        # Keep only rows where jobSchedule is TRUE (case-insensitive)
        df_breaks = df_pattern[df_pattern["jobSchedule"].astype(str).str.upper().eq("TRUE")].copy()
    else:
        df_breaks = df_pattern.copy()

    if df_breaks.empty:
        return pd.DataFrame()

    # split obs column into title and pattern colums by the first occurrence of " ~~ "
    df_breaks[["title", "pattern"]] = df_breaks["obs"].astype(str).str.split(" ~~ ", n=1, expand=True)

    tickets: List[Dict[str, Any]] = []
    for (dataset, run_id, pattern), group in df_breaks.groupby(["dataset", "runId", "pattern"], dropna=True):
        current_group = group.copy()
        # find out total score for this pattern break
        total_score = pd.to_numeric(current_group["obsScore"], errors="coerce").sum()

        description_parts = [
            f"*Dataset:* {dataset}",
            f"*DataTable:* {
                current_group.iloc[0].get('db_nm', '')
                }.{current_group.iloc[0].get('table_nm', '')}" 
                if 'db_nm' in current_group.columns and 'table_nm' in current_group.columns else "",
            f"*RunId (runDate):* {run_id}",
        ]

        if "Project" in current_group.columns:
            project = current_group.iloc[0].get("Project", "")
            if project:
                description_parts.append(f"*Project:* {project}")
        description_parts.append(f"*Total pattern score impact:* {_format_pct(total_score)}")
        description_parts.extend(["", f"*Pattern: {pattern}*", ""])

        # find out detailed observations for this pattern break using v2/getobservationdetails
        token_mgr = token_mgr_apac if dataset not in df_dataset_cn['dataset'].values else token_mgr_cn
        for i, rowkey in enumerate(current_group["obsKey"].dropna().astype(str).unique()):
            data = get_observation_details(token_mgr, dataset, rowkey, run_id)
            if data:
                pattern_count = current_group[current_group["obsKey"]==rowkey]["owlRank"].values[0]
                description_parts.extend([build_pattern_markdown(data, pattern_count), ""])

        cdq_link = make_cdq_link(dataset)
        if cdq_link:
            description_parts.append(f"*CDQ Dataset:* {cdq_link}")

        # Build table_name from db_nm and table_nm
        table_name = None
        db_nm = current_group.get("db_nm")
        if not db_nm.empty:
            db_nm_val = db_nm.iloc[0]
            table_nm = current_group.get("table_nm")
            table_nm_val = table_nm.iloc[0] if not table_nm.empty else None
            if pd.notna(db_nm_val) and pd.notna(table_nm_val):
                table_name = f"{db_nm_val}.{table_nm_val}"

        tickets.append(
            {
                "dataset": dataset,
                "table_name": table_name,
                "runId": run_id,
                "business_unit": project,
                "suggested_title": f"Anti-Pattern: {pattern}",
                "suggested_priority": _priority_from_total_score(total_score),
                "suggested_labels": ["collibra", "dq", "pattern", "end-users"],
                "score": total_score,
                "jira_description": "\n".join(description_parts).strip(),
            }
        )

    return pd.DataFrame(tickets)

def build_dupes_markdown(df_dupes_records: list) -> str:
    """
    Build a markdown-formatted string summarizing the dupes observations.

    where df_dupes_records is a dataframe containing details about a dupes 
    observation with 2 or more records, occurs is one of the header columns 
    indicating the number of dupes.
    e.g. data:
    [
        {
            "zip": "null",
            "effective_end_date": "9999-12-31",
            "affiliation": "graduate school of medical sciences, kyushu university",
            "effective_start_date": "2026-08-01",
            "se_id": "4740623",
            "hcp_key": "b6ed5bec00f56498e1b1b6cdb8a1eb28",
            "occurs": "2"
        },
        {
            "zip": "null",
            "effective_end_date": "9999-12-31",
            "affiliation": "graduate school of medical sciences, kyushu university",
            "effective_start_date": "2026-08-01",
            "hcp_key": "b6ed5bec00f56498e1b1b6cdb8a1eb28",
            "se_id": "4740623",
            "occurs": "2"
        }
    ]
    """
    if df_dupes_records.empty:
        return "*Dupes observations:* NA (no details found)"

    headers = list(df_dupes_records.columns) # get header
    headers = [h for h in headers if h != "occurs"] + ["occurs"]  # move "occurs" to the end
    header = " | ".join(headers)
    lines = [ 
        f"| {header} |",
    ]
    for i, row in df_dupes_records.iterrows():
        lines.append(f"| {' | '.join([str(row[h]) for h in headers])} |")

    return "\n".join(lines)

def build_dupe_break_candidates(
        df_dupes: pd.DataFrame, 
        df_dataset_cn: pd.DataFrame, 
        ) -> pd.DataFrame:
    """
    Building potential JIRA tickets for dupes breaks based on the 
    provided DataFrame of dupes observations.
    """
    if df_dupes.empty:
        return pd.DataFrame()

    df_dupes["runId_dt"] = pd.to_datetime(df_dupes["runId"], errors="coerce", utc=True)

    if "jobSchedule" in df_dupes.columns:
        # Keep only rows where jobSchedule is TRUE (case-insensitive)
        df_breaks = df_dupes[df_dupes["jobSchedule"].astype(str).str.upper().eq("TRUE")].copy()
    else:
        df_breaks = df_dupes.copy()

    if df_breaks.empty:
        return pd.DataFrame()

    tickets: List[Dict[str, Any]] = []
    for (dataset, run_id), group in df_breaks.groupby(["dataset", "runId"], dropna=True):
        current_group = group.copy()
        # find out total score for this pattern break
        total_score = pd.to_numeric(current_group["owlRank"], errors="coerce").sum()

        description_parts = [
            f"*Dataset:* {dataset}",
            f"*DataTable:* {
                current_group.iloc[0].get('db_nm', '')
                }.{current_group.iloc[0].get('table_nm', '')}" 
                if 'db_nm' in current_group.columns and 'table_nm' in current_group.columns else "",
            f"*RunId (runDate):* {run_id}",
        ]

        if "Project" in current_group.columns:
            project = current_group.iloc[0].get("Project", "")
            if project:
                description_parts.append(f"*Project:* {project}")
        description_parts.append(f"*Total dupes score impact:* {_format_pct(total_score)}")
        description_parts.extend(["", f"*Dupes Records:*"])

        # find out detailed observations for this pattern break using v2/getobservationdetails
        token_mgr = token_mgr_apac if dataset not in df_dataset_cn['dataset'].values else token_mgr_cn
        dupes_records = []
        obs_keys = current_group["obsKey"].dropna().astype(str).unique()
        for i, rowkey in enumerate(obs_keys):
            data = get_observation_details(token_mgr, dataset, rowkey, run_id)
            if data:
                dupes_records.extend(data)

        if dupes_records:
            df_dupes_records = pd.DataFrame(dupes_records).drop_duplicates()
            description_parts.extend(["", build_dupes_markdown(df_dupes_records), ""])
            dupes_keys = [e for e in df_dupes_records.columns if e not in ["occurs"]]
        else:
            description_parts.extend([f"No details found for obs keys: {', '.join(obs_keys)}", ""])
            # fall back to use v2/dupe-opt/get?dataset= to get the dupes keys
            dupes_keys = get_dupe_opt_include_keys(token_mgr, str(dataset))

        cdq_link = make_cdq_link(dataset)
        if cdq_link:
            description_parts.append(f"*CDQ Dataset:* {cdq_link}")

        # Build table_name from db_nm and table_nm
        table_name = None
        db_nm = current_group.get("db_nm")
        if not db_nm.empty:
            db_nm_val = db_nm.iloc[0]
            table_nm = current_group.get("table_nm")
            table_nm_val = table_nm.iloc[0] if not table_nm.empty else None
            if pd.notna(db_nm_val) and pd.notna(table_nm_val):
                table_name = f"{db_nm_val}.{table_nm_val}"
    
        tickets.append(
            {
                "dataset": dataset,
                "table_name": table_name,
                "runId": run_id,
                "business_unit": project,
                "suggested_title": f"Dupes: {','.join(dupes_keys)}",
                "suggested_priority": _priority_from_total_score(total_score),
                "suggested_labels": ["collibra", "dq", "dupes", "end-users"],
                "score": total_score,
                "jira_description": "\n".join(description_parts).strip(),
            }
        )

    return pd.DataFrame(tickets)

def main() -> None:
    logger.info("=== S5: Generate potential JIRA tickets ===")

    mwaa_reference = MWAADatasetReference.default()

    df_issue_list = fetch_jira_issue_list()
    df_issue_prepared, df_task_prepared, df_epic_prepared = prepare_jira_outputs(df_issue_list)
    write_output(df_issue_list, "issue_list")
    write_output(df_issue_prepared, "issue_list_prepared")
    write_output(df_task_prepared, "task_list_prepared")
    write_output(df_epic_prepared, "epic_list_prepared")
    write_output(mwaa_reference.to_dataframe(), "mwaa_dq_trigger_datasets")

    df_rule_details = read_input("dataset_rule_details")
    df_custom_rules = read_input("dataset_custom_rules")
    df_adaptive_rule_details = read_input("dataset_adaptive_rule_details")
    df_dupe_details = read_input("dataset_dupe_details")
    df_pattern_details = read_input("dataset_pattern_details")
    df_business_unit_mapping = read_input("business_unit_mapping")

    try:
        df_dataset_cn = read_input("dataset_cn")
    except Exception:
        df_dataset_cn = pd.DataFrame()

    # Exclude CN dataset due to potential violation of data privacy regulations. 
    # This is a temporary measure until we have a proper solution for handling CN datasets.
    excluded_datasets = _extract_dataset_exclusions(df_dataset_cn)
    if excluded_datasets:
        logger.info("Excluding %s CN datasets from potential JIRA candidate generation", len(excluded_datasets))
    before_custom_rules = len(df_custom_rules)
    before_rule_details = len(df_rule_details)
    before_adaptive = len(df_adaptive_rule_details)
    before_dupe = len(df_dupe_details)
    before_pattern = len(df_pattern_details)

    df_custom_rules = _exclude_datasets(df_custom_rules, excluded_datasets, dataset_col="dataset")
    df_rule_details = _exclude_datasets(df_rule_details, excluded_datasets, dataset_col="dataset")
    df_adaptive_rule_details = _exclude_datasets(df_adaptive_rule_details, excluded_datasets, dataset_col="dataset")
    df_dupe_details = _exclude_datasets(df_dupe_details, excluded_datasets, dataset_col="dataset")
    df_pattern_details = _exclude_datasets(df_pattern_details, excluded_datasets, dataset_col="dataset")

    if excluded_datasets:
        logger.info(
            "Filtered rows by dataset_cn exclusion - custom_rules: %s->%s, rule_details: %s->%s, "
            "adaptive_rule_details: %s->%s, dupe_details: %s->%s, pattern_details: %s->%s",
            before_custom_rules,
            len(df_custom_rules),
            before_rule_details,
            len(df_rule_details),
            before_adaptive,
            len(df_adaptive_rule_details),
            before_dupe,
            len(df_dupe_details),
            before_pattern,
            len(df_pattern_details),
        )

    df_custom_candidates = build_custom_break_candidates(
        df_rule_details=df_rule_details,
        df_custom_rules=df_custom_rules,
        df_existing=df_issue_prepared,
        df_business_unit_mapping=df_business_unit_mapping,
    )
    write_output(df_custom_candidates, "potential_jira_ticket_list")

    df_adaptive_candidates = build_adaptive_break_candidates(
        df_adaptive=df_adaptive_rule_details,
        df_existing=df_issue_prepared,
        mwaa_reference=mwaa_reference,
        df_business_unit_mapping=df_business_unit_mapping,
    )
    write_output(df_adaptive_candidates, "potential_adaptive_rule_jira_ticket_list")

    df_pattern_candidates = build_pattern_break_candidates(
        df_pattern=df_pattern_details,
        df_dataset_cn=df_dataset_cn,
    )
    write_output(df_pattern_candidates, "potential_pattern_jira_ticket_list")

    df_dupe_candidates = build_dupe_break_candidates(
        df_dupes=df_dupe_details,
        df_dataset_cn=df_dataset_cn,
    )
    write_output(df_dupe_candidates, "potential_dupe_jira_ticket_list")

    logger.info("Wrote issue_list: %s rows", len(df_issue_list))
    logger.info("Wrote issue_list_prepared: %s rows", len(df_issue_prepared))
    logger.info("Wrote task_list_prepared: %s rows", len(df_task_prepared))
    logger.info("Wrote epic_list_prepared: %s rows", len(df_epic_prepared))
    logger.info("Wrote potential_jira_ticket_list: %s rows", len(df_custom_candidates))
    logger.info("Wrote potential_adaptive_rule_jira_ticket_list: %s rows", len(df_adaptive_candidates))
    logger.info("Wrote potential_pattern_jira_ticket_list: %s rows", len(df_pattern_candidates))
    logger.info("Wrote potential_dupe_jira_ticket_list: %s rows", len(df_dupe_candidates))


if __name__ == "__main__":
    main()
