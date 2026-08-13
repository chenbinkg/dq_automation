# -*- coding: utf-8 -*-
"""
S8: Diagnose Adaptive JIRA Tickets with Upstream Evidence
=========================================================

Diagnoses newly raised JGPV (Janssen Global Platform for Validation) adaptive-rule
DQ tickets by searching for upstream JEJQ (Janssen Extract/ETL JIRA) changes that
explain the DQ issues. Uses multi-factor scoring to rank potential root causes and
provides LLM-assisted reasoning for ticket resolution.

DISCLAIMER
----------
- Currently diagnoses only adaptive-rule failures (JGPV tickets)
- Non-adaptive tickets in new_jira_ticket_list are skipped by design

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                  | Produced by | Description                     |
|-----------------------------|-------------|----------------------------------|
| new_jira_ticket_list        | S6          | Adaptive rule JIRA tickets      |

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name                | Description                                         |
|---------------------------|----------------------------------------------------|
| issue_diagnosis_results   | Diagnosis for each JGPV with upstream JEJQ evidence|

External APIs Called
--------------------
- Jira REST API v2
    - POST /rest/api/2/search  – Search JEJQ issues (text ~ operator)
    - GET /rest/api/2/issue/{key}  – Fetch JEJQ issue details
- LLM Gateway (JNJClaudeGatewayModel)
    - POST /predict  – Generate root cause reasoning with evidence context

Evidence Ranking Algorithm
--------------------------
1. Extract DQ signals from JGPV ticket:
   - Dataset name, table name, run date/ID, business unit
   - Row count direction (dropped/increased)
   - Breaking rule type
2. Build multi-strategy JEJQ search:
   - Strategy 1: Table/component based (full name, prefixes, components)
   - Strategy 2: Business unit tokens (market, project names)
   - Executes JQL with OR operator over 8 terms per strategy
   - 45-day lookback window
   - Returns up to 30 candidates per strategy
3. Merge and rank all candidates by multi-factor score:
   * Table Match (45 pts exact summary, 35 pts exact text, 15 pts short name 
                  in summary, 8 pts short name in text)
   * Row Count Symptom (15 pts if direction matches)
   * Business Unit tokens (4 pts per matching token in text)
   * Temporal Proximity (MAX(0, 18 - delta_days*3) where delta measured from 
                         Collibra Development field merge time or created date)
   * Issue Type (6 pts Story, 3 pts Task)
   * Graph Depth (10 - depth*3 bonus for linked issues)
   * Hard Exclusion: -1.0 if created > 2 hours after DQ run
4. Augment via graph traversal (BFS depth=2, max 50 total)
5. Re-rank all candidates, apply exclusions, cap to 20
6. Pass top 5 to LLM with evidence context

Confidence Assessment
---------------------
- High: Top JEJQ score >= 25.0 AND created <= 10 days before run
- Medium: Top JEJQ score >= 15.0 OR multiple candidates (>= 3) with score > 10
- Low: Top score < 15.0 OR no suitable candidates found

LLM-Assisted Reasoning
----------------------
- Provides context: DQ symptoms, top JEJQ candidates with scores, ticket links
- Generates suspected_root_cause reasoning
- Produces expected_impact statement
- Recommends next_actions for ticket resolution

DQ Signal Extraction
--------------------
From JGPV description markup:
- *Dataset:* dataset_name
- *Table Name:* schema.table_name
- *RunId (runDate):* ISO8601 timestamp
- *Business Unit:* BU name (e.g., "ANGen MAF")
- *Row Count Direction:* "dropped" / "increased" / pattern
- Break keys from markdown table

Development Field Parsing
------------------------
- Extracts Collibra Development (customfield_14100) field
- Parses JSON structure for merged PR timestamps
- Uses merge date as causality signal (prefer over created date)
- Example: merged 2026-07-16 01:36:04 UTC (exact match to run_dt = strong signal)

Configuration Parameters
------------------------
- S8_MAX_SEED_ISSUES = 15: Max candidates per search strategy
- TIMEOUT = 30s: Jira API request timeout
- LLM confidence threshold: 25.0 for High, 15.0 for Medium

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)
- S8_MAX_SEED_ISSUES         : Max seed issues for search (default: 15)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- jira_url / jira_api_token / jira_ca_bundle
- jnj_gateway_url / jnj_gateway_key  (Claude gateway for LLM)
- uc_catalog / uc_schema

Signal Extraction Patterns
---------------------------
- RunId regex: Matches ISO8601 with *RunId (runDate):* marker
- Row count regex: Detects "-91.", "-9", percentage indicators
- Break line regex: Matches "**Column** – **Type**" format

TO-DO
-----
- Investigate JQL multi-term OR query behavior (single-term finds more results)
- Boost temporal signal when dev merge date = DQ run date (±1 day)
- Increase BU title match weight (12 pts vs current 4 pts per token)
- Add fallback single-term search when multi-term query returns incomplete results

2) Pull JGPV description and extract DQ signals (dataset, table, run_dt, row_count_direction).
3) Query JEJQ evidence via search + graph augmentation.
4) Score and rank JEJQ candidates deterministically.
5) Use JNJ model to summarize top-ranked root causes (only if high confidence).
6) Enrich JGPV ticket description and add comment (high confidence only).
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import urllib3
from pydantic import BaseModel, Field

import config
from jnj_strands_model import JNJClaudeGatewayModel
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

if WRITE_MODE not in ("csv", "uc", "both"):
    raise ValueError("PIPELINE_WRITE_MODE must be one of: csv, uc, both")


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

raw_jira_url = _load_secret_or_default("jira_url", config.JIRA_URL)
jira_token = _load_secret_or_default("jira_api_token", config.JIRA_API_TOKEN)
jira_verify_ssl_raw = _load_secret_or_default("jira_verify_ssl", config.JIRA_VERIFY_SSL)
jira_ca_bundle = _load_secret_or_default("jira_ca_bundle", config.JIRA_CA_BUNDLE)

JIRA_VERIFY_SSL = parse_bool(jira_verify_ssl_raw, default=False)
JIRA_CA_BUNDLE = jira_ca_bundle
if not JIRA_VERIFY_SSL:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

jira_match = re.match(r"(https?://[^/]+)", str(raw_jira_url or "").strip())
JIRA_BASE = jira_match.group(1) if jira_match else ""
if not JIRA_BASE or not jira_token:
    raise ValueError("JIRA_URL and JIRA_API_TOKEN are required for S8 diagnosis")


# Optional MCP endpoint + headers
MCP_URL = _load_secret_or_default("mcp_atlassian_url", os.getenv("MCP_ATLASSIAN_URL"))
MCP_HEADERS = {
    "X-Atlassian-Jira-Personal-Token": _load_secret_or_default(
        "x_atlassian_jira_personal_token", os.getenv("X_ATLASSIAN_JIRA_PERSONAL_TOKEN")
    ),
    "X-Atlassian-Jira-Url": _load_secret_or_default("x_atlassian_jira_url", os.getenv("X_ATLASSIAN_JIRA_URL", JIRA_BASE)),
    "X-Atlassian-Username": _load_secret_or_default("x_atlassian_username", os.getenv("X_ATLASSIAN_USERNAME")),
    "X-Atlassian-Read-Only-Mode": _load_secret_or_default("x_atlassian_read_only_mode", os.getenv("X_ATLASSIAN_READ_ONLY_MODE", "false")),
    "X-Atlassian-Enable-Xray": _load_secret_or_default("x_atlassian_enable_xray", os.getenv("X_ATLASSIAN_ENABLE_XRAY", "false")),
}
MCP_HEADERS = {k: v for k, v in MCP_HEADERS.items() if v}
MCP_SESSION_ID: Optional[str] = None

S8_JEJQ_SEARCH_WINDOW_DAYS = int(os.getenv("S8_JEJQ_SEARCH_WINDOW_DAYS", "45"))
S8_POST_RUN_GRACE_HOURS = float(os.getenv("S8_POST_RUN_GRACE_HOURS", "24"))
S8_MAX_SEED_ISSUES = int(os.getenv("S8_MAX_SEED_ISSUES", "15"))
S8_MAX_EVIDENCE_ISSUES = int(os.getenv("S8_MAX_EVIDENCE_ISSUES", "20"))

_DEV_FIELD_ID_CACHE: Optional[str] = None


def _jira_verify_value() -> Any:
    if JIRA_VERIFY_SSL:
        return JIRA_CA_BUNDLE or True
    return False


SESSION = requests.Session()
SESSION.headers.update(
    {
        "Authorization": f"Bearer {jira_token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
)


class DiagnosisResult(BaseModel):
    suspected_root_cause: str = Field(description="Most likely root cause summary")
    confidence: str = Field(description="High, Medium, or Low")
    evidence_summary: List[str] = Field(description="Evidence bullets from upstream tickets")
    suspected_upstream_tickets: List[str] = Field(description="Likely JEJQ ticket keys")
    next_actions: List[str] = Field(description="Recommended next actions")


def jira_get_issue(issue_key: str, fields: str = "summary,description,status,updated") -> Dict[str, Any]:
    response = SESSION.get(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}",
        params={"fields": fields},
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()
    return response.json()


def jira_update_description(issue_key: str, description: str) -> None:
    response = SESSION.put(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}",
        data=json.dumps({"fields": {"description": description}}),
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()


def jira_add_comment(issue_key: str, body: str) -> None:
    response = SESSION.post(
        f"{JIRA_BASE}/rest/api/2/issue/{issue_key}/comment",
        data=json.dumps({"body": body}),
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()


def jira_search(jql: str, max_results: int = 20) -> List[Dict[str, Any]]:
    fields = [
        "summary",
        "description",
        "status",
        "updated",
        "created",
        "project",
        "labels",
        "issuetype",
        "parent",
        "issuelinks",
        "subtasks",
        "comment",
    ]
    dev_field_id = get_development_field_id()
    if dev_field_id:
        fields.append(dev_field_id)

    response = SESSION.post(
        f"{JIRA_BASE}/rest/api/2/search",
        data=json.dumps(
            {
                "jql": jql,
                "maxResults": max_results,
                "fields": fields,
            }
        ),
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()
    return response.json().get("issues", [])


def _parse_mcp_text_result(payload: Dict[str, Any]) -> Any:
    result = payload.get("result")
    if isinstance(result, dict):
        content = result.get("content")
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict) and "text" in item:
                    text = item["text"]
                    try:
                        return json.loads(text)
                    except Exception:
                        return text
        if "text" in result:
            try:
                return json.loads(result["text"])
            except Exception:
                return result["text"]
    return result


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    try:
        dt = pd.to_datetime(value, utc=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.to_pydatetime()
    except Exception:
        return None


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _dataset_to_table_candidates(dataset: Optional[str], table_name: Optional[str]) -> List[str]:
    candidates: List[str] = []
    if table_name:
        candidates.append(table_name)

    if dataset:
        ds = str(dataset).strip()
        if ds.startswith("ds_"):
            tail = ds[3:]
            if "_pixonomy_" in tail:
                after = tail.split("_pixonomy_", 1)[1]
                candidates.append(f"pixonomy.{after}")
            if "_dbo." in tail:
                after = tail.split("_", 1)[1]
                candidates.append(after)
            candidates.append(ds)

    out: List[str] = []
    seen = set()
    for item in candidates:
        token = str(item).strip()
        if token and token not in seen:
            seen.add(token)
            out.append(token)
    return out


def _rowcount_direction_from_text(text: str) -> Optional[str]:
    t = _normalize_text(text)
    # Check for drop patterns - include percentage patterns like -91.
    if any(x in t for x in ["row count dropped", "row count has dropped", "row count: row count dropped", "decrease", "(-", "-91.", "-9"]):
        return "dropped"
    # Check for increase patterns
    if any(x in t for x in ["row count increased", "row count has increased", "increase", "(+", "+"]):
        return "increased"
    return None

def _extract_comment_text(fields: Dict[str, Any]) -> str:
    comments = (fields.get("comment") or {}).get("comments") if isinstance(fields.get("comment"), dict) else []
    if not isinstance(comments, list):
        return ""
    parts: List[str] = []
    for c in comments[:20]:
        body = c.get("body") if isinstance(c, dict) else None
        if isinstance(body, str):
            parts.append(body)
        elif body is not None:
            parts.append(json.dumps(body))
    return "\n".join(parts)

def _extract_comment_text_with_sanitization(fields: Dict[str, Any]) -> str:
    """Return concatenated Jira comment bodies with PII-like mention cleanup.

    Keeps full body text for every available comment, but redacts user handles
    and email addresses before downstream scoring/prompt usage.
    """
    def _sanitize_comment_body(text: str) -> str:
        # Jira user mention formats like [~user], [~user.name], @[~user].
        text = re.sub(r"@?\[~[^\]]+\]", "[USER]", text)
        # Email addresses.
        text = re.sub(r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b", "[EMAIL]", text)
        return text

    comments = (fields.get("comment") or {}).get("comments") if isinstance(fields.get("comment"), dict) else []
    if not isinstance(comments, list):
        return ""
    parts: List[str] = []
    for c in comments:
        body = c.get("body") if isinstance(c, dict) else None
        if isinstance(body, str):
            parts.append(_sanitize_comment_body(body))
        elif body is not None:
            parts.append(_sanitize_comment_body(json.dumps(body)))
    return "\n".join(parts)


def _extract_linked_keys(issue: Dict[str, Any]) -> List[str]:
    keys: List[str] = []
    fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else issue

    parent = fields.get("parent") if isinstance(fields, dict) else None
    if isinstance(parent, dict) and parent.get("key"):
        keys.append(str(parent.get("key")))

    for st in fields.get("subtasks", []) if isinstance(fields.get("subtasks"), list) else []:
        if isinstance(st, dict) and st.get("key"):
            keys.append(str(st.get("key")))

    links = fields.get("issuelinks", []) if isinstance(fields.get("issuelinks"), list) else []
    for link in links:
        if not isinstance(link, dict):
            continue
        inward = link.get("inwardIssue")
        outward = link.get("outwardIssue")
        if isinstance(inward, dict) and inward.get("key"):
            keys.append(str(inward.get("key")))
        if isinstance(outward, dict) and outward.get("key"):
            keys.append(str(outward.get("key")))

    unique: List[str] = []
    seen = set()
    for k in keys:
        if k and k not in seen:
            seen.add(k)
            unique.append(k)
    return unique


def get_development_field_id() -> Optional[str]:
    global _DEV_FIELD_ID_CACHE
    if _DEV_FIELD_ID_CACHE is not None:
        return _DEV_FIELD_ID_CACHE or None

    try:
        response = SESSION.get(
            f"{JIRA_BASE}/rest/api/2/field",
            timeout=(10, 60),
            verify=_jira_verify_value(),
        )
        response.raise_for_status()
        fields = response.json()
        for field in fields:
            name = str(field.get("name") or "").strip().lower()
            fid = str(field.get("id") or "").strip()
            if fid and (name == "development" or "development" in name):
                _DEV_FIELD_ID_CACHE = fid
                logger.info("Detected Jira Development field id: %s", fid)
                return fid
    except Exception as exc:
        logger.warning("Could not discover Jira Development field id: %s", exc)

    _DEV_FIELD_ID_CACHE = ""
    return None


def _extract_dev_latest_dt(issue: Dict[str, Any]) -> Optional[datetime]:
    dev_field_id = get_development_field_id()
    if not dev_field_id:
        return None
    fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else issue
    if not isinstance(fields, dict) or dev_field_id not in fields:
        return None

    value = fields.get(dev_field_id)
    text = value if isinstance(value, str) else json.dumps(value)

    matches = re.findall(r"\d{4}-\d{2}-\d{2}(?:[T\s]\d{2}:\d{2}:\d{2}(?:\.\d+)?)?", text)
    dts = [_parse_dt(m) for m in matches]
    dts = [d for d in dts if d is not None]
    return max(dts) if dts else None


def _score_issue(issue: Dict[str, Any], signals: Dict[str, Any]) -> Tuple[float, Optional[str]]:
    """Score a JEJQ candidate against extracted DQ signals.

    Scoring is additive across multiple signals and returns ``(score, reason)``.
    A non-``None`` reason means the issue is hard-excluded from ranking.

    Signal sources used for text matching:
    - summary
    - description
    - up to first 20 comments (via ``_extract_comment_text``)
    All three are concatenated into a normalized ``text_blob``. This means clues
    in comments can increase score, including table/db names and row-count
    symptom language.

    Components:
    - Table/data object match:
        - +45 exact full table token in summary
        - +35 exact full table token in text_blob (includes comments)
        - +15 short table token (last segment after ``.``) in summary
        - +8 short table token in text_blob (includes comments)
        Note: scoring loops through all table candidates and sums matches.
    - Row count symptom alignment:
        - +15 when DQ signal is ``dropped`` and text_blob contains
          ``dropped`` or ``decrease``
        - +15 when DQ signal is ``increased`` and text_blob contains
          ``increase``
    - Business unit token overlap:
        - Up to first 4 alphanumeric BU tokens (len >= 3)
        - +4 for each token found in text_blob
    - Temporal proximity (run vs. event time):
        - event time is ``dev merge dt`` (preferred), else created, else updated
        - hard exclusion with ``(-1.0, "post_run_event")`` if
          ``event_dt > run_dt + S8_POST_RUN_GRACE_HOURS``
        - otherwise +max(0, 18 - min(delta_days*3, 18))
    - Issue type:
        - +6 for Story
        - +3 for Task
    - Graph depth bonus (if present):
        - +max(0, 10 - depth*3)

    Args:
        issue: Jira issue payload with ``fields`` and optional graph metadata.
        signals: Extracted DQ signals (dataset/table_name/run_dt/business_unit/
            row_count_direction).

    Returns:
        Tuple of ``(score, exclusion_reason)`` where ``exclusion_reason`` is
        ``None`` for valid candidates.
    """
    fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else issue
    summary = str(fields.get("summary") or "")
    description = str(fields.get("description") or "")
    comments = _extract_comment_text(fields)
    text_blob = _normalize_text("\n".join([summary, description, comments]))
    summary_norm = _normalize_text(summary)

    table_candidates = _dataset_to_table_candidates(signals.get("dataset"), signals.get("table_name"))
    table_hit = False
    score = 0.0

    for table in table_candidates:
        table_norm = _normalize_text(table)
        short_table = table_norm.split(".")[-1]
        if table_norm and table_norm in summary_norm:
            score += 45
            table_hit = True
        elif table_norm and table_norm in text_blob:
            score += 35
            table_hit = True
        if short_table and short_table in summary_norm:
            score += 15
            table_hit = True
        elif short_table and short_table in text_blob:
            score += 8
            table_hit = True

    direction = signals.get("row_count_direction")
    if direction == "dropped" and ("dropped" in text_blob or "decrease" in text_blob):
        score += 15
    if direction == "increased" and "increase" in text_blob:
        score += 15

    bu = _normalize_text(signals.get("business_unit"))
    for token in [t for t in re.split(r"[^a-z0-9]+", bu) if len(t) >= 3][:4]:
        if token in text_blob:
            score += 4

    run_dt = signals.get("run_dt")
    created_dt = _parse_dt(fields.get("created"))
    updated_dt = _parse_dt(fields.get("updated"))
    dev_dt = _extract_dev_latest_dt(issue)
    event_dt = dev_dt or created_dt or updated_dt
    if dev_dt: logger.info(f"[SCORE {issue.get('key')}] DEV_MERGE_TIME: {dev_dt}")

    if run_dt and event_dt:
        threshold = run_dt + timedelta(hours=S8_POST_RUN_GRACE_HOURS)
        if event_dt > threshold:
            logger.info(f"[SCORE {issue.get('key')}] HARD EXCLUDE: post_run_event (event_dt={event_dt} > threshold={threshold})")
            return -1.0, "post_run_event"
        delta_days = max((run_dt - event_dt).total_seconds() / 86400.0, 0.0)
        score += max(0.0, 18.0 - min(delta_days * 3.0, 18.0))

    issue_type = _normalize_text((fields.get("issuetype") or {}).get("name") if isinstance(fields.get("issuetype"), dict) else fields.get("issuetype"))
    if issue_type == "story":
        score += 6
    elif issue_type == "task":
        score += 3

    if issue.get("_graph_depth") is not None:
        try:
            depth = int(issue.get("_graph_depth"))
            score += max(0.0, 10.0 - (depth * 3.0))
        except Exception:
            pass

    # logger.info(f"[SCORE {issue.get('key')}] === FINAL SCORE: {score:.1f}")
    return score, None


def _augment_with_linked_issues(seed_issues: List[Dict[str, Any]], signals: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not seed_issues:
        return []

    issue_map: Dict[str, Dict[str, Any]] = {}
    for issue in seed_issues:
        key = str(issue.get("key") or "").strip()
        if key:
            issue_map[key] = issue

    frontier = [(str(issue.get("key") or ""), 0) for issue in seed_issues[:3] if str(issue.get("key") or "").startswith("JEJQ-")]
    seen = set(issue_map.keys())
    max_depth = 2

    while frontier and len(issue_map) < 50:
        current_key, depth = frontier.pop(0)
        if depth >= max_depth or not current_key:
            continue

        current_issue = issue_map.get(current_key)
        if not current_issue:
            continue

        for related_key in _extract_linked_keys(current_issue):
            if not related_key.startswith("JEJQ-") or related_key in seen:
                continue
            seen.add(related_key)
            try:
                rel_issue = jira_get_issue(
                    related_key,
                    fields="summary,description,status,updated,created,project,labels,issuetype,parent,issuelinks,subtasks,comment",
                )
                rel_issue["_graph_depth"] = depth + 1
                rel_issue["_graph_from"] = current_key

                score, reason = _score_issue(rel_issue, signals)
                if reason is None:
                    issue_map[related_key] = rel_issue
                    frontier.append((related_key, depth + 1))
            except Exception as exc:
                logger.warning("Could not fetch linked JEJQ issue %s: %s", related_key, exc)

    return list(issue_map.values())


def _rank_evidence_issues(candidates: List[Dict[str, Any]], signals: Dict[str, Any]) -> List[Dict[str, Any]]:
    scored: List[Tuple[float, Dict[str, Any]]] = []
    for issue in candidates:
        score, reason = _score_issue(issue, signals)
        if reason is not None:
            continue
        issue["_s8_relevance_score"] = round(score, 3)
        scored.append((score, issue))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [item[1] for item in scored[:S8_MAX_EVIDENCE_ISSUES]]


def _mcp_http_headers(include_session: bool = True) -> Dict[str, str]:
    headers = {
        "Content-Type": "application/json",
        # Some MCP gateways require explicit Accept negotiation for JSON-RPC-over-HTTP.
        "Accept": "application/json, text/event-stream",
        **MCP_HEADERS,
    }
    if include_session and MCP_SESSION_ID:
        headers["Mcp-Session-Id"] = MCP_SESSION_ID
    return headers


def _parse_mcp_response_body(response: requests.Response) -> Dict[str, Any]:
    """Parse MCP response payload that may be JSON or SSE event-stream text."""
    content_type = str(response.headers.get("Content-Type") or "").lower()
    body = response.text or ""

    # Fast path for normal JSON responses.
    if "application/json" in content_type:
        return response.json()

    # Try direct JSON parse first even when content-type is missing/incorrect.
    text = body.strip()
    if text:
        try:
            return json.loads(text)
        except Exception:
            pass

    # Parse SSE stream by reading "data:" lines and decoding JSON payload chunks.
    # Keep the last decodable JSON object as the final response.
    last_json: Optional[Dict[str, Any]] = None
    for line in body.splitlines():
        line = line.strip()
        if not line.startswith("data:"):
            continue
        payload_text = line[5:].strip()
        if not payload_text:
            continue
        if payload_text == "[DONE]":
            break
        try:
            maybe_json = json.loads(payload_text)
            if isinstance(maybe_json, dict):
                last_json = maybe_json
        except Exception:
            continue

    if last_json is not None:
        return last_json

    snippet = body[:300].replace("\n", "\\n")
    raise ValueError(f"MCP response is not valid JSON/SSE payload. body={snippet}")


def _mcp_post(payload: Dict[str, Any], include_session: bool = True) -> Dict[str, Any]:
    response = requests.post(
        MCP_URL,
        headers=_mcp_http_headers(include_session=include_session),
        data=json.dumps(payload),
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()
    return _parse_mcp_response_body(response)


def _mcp_initialize_if_needed() -> None:
    global MCP_SESSION_ID
    if not MCP_URL or MCP_SESSION_ID:
        return

    init_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "dq-automation-s8", "version": "1.0"},
        },
    }

    response = requests.post(
        MCP_URL,
        headers=_mcp_http_headers(include_session=False),
        data=json.dumps(init_payload),
        timeout=(10, 60),
        verify=_jira_verify_value(),
    )
    response.raise_for_status()

    # MCP servers may return session id via response header.
    MCP_SESSION_ID = response.headers.get("Mcp-Session-Id") or response.headers.get("mcp-session-id")

    # Best-effort initialized notification; ignore if not supported by server.
    notify_payload = {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}
    try:
        _mcp_post(notify_payload, include_session=True)
    except Exception:
        pass


def mcp_call_tool(name: str, arguments: Dict[str, Any]) -> Any:
    if not MCP_URL:
        raise RuntimeError("MCP URL not configured")

    _mcp_initialize_if_needed()

    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    data = _mcp_post(payload, include_session=True)
    if "error" in data:
        raise RuntimeError(f"MCP tool call failed: {data['error']}")
    return _parse_mcp_text_result(data)


def search_upstream_evidence(query_text: str, signals: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Search JEJQ with multi-strategy approach: table+BU, BU+temporal, then rank+augment."""
    all_candidates = {}
    dataset = signals.get("dataset")
    run_id = signals.get("run_id")
    table_name = signals.get("table_name")
    business_unit = signals.get("business_unit")
    run_dt = signals.get("run_dt")
    project = signals.get("project")

    # Strategy 1: Primary search - table components + business unit + temporal window
    terms = []
    table_tokens = _dataset_to_table_candidates(dataset, table_name)
    for table_token in table_tokens:
        terms.append(table_token)
        # Also add components of table name (e.g., jpubs from jpubsdata)
        # change to use table_token.split(".") to handle schema.table_name format
        # for each part after split, further split by "_" and add to terms, no need to worry about len
        for part in table_token.split("."):
            for comp in part.split("_"):
                terms.append(comp)

    if project:
        terms.append(str(project))

    # Deduplicate and limit terms
    unique_terms = []
    seen = set()
    for t in terms:
        t = str(t).strip()
        if t and t not in seen and len(t) >= 1:
            seen.add(t)
            unique_terms.append(t)

    logger.info(f"[JEJQ Search] Strategy 1 terms: {unique_terms}")
    term_clause = " OR ".join([f'text ~ "{t.replace(chr(34), "")}"' for t in unique_terms])
    window_days = S8_JEJQ_SEARCH_WINDOW_DAYS
    run_dt = datetime.strptime(
        run_id,
        "%Y-%m-%dT%H:%M:%S.%f%z"
    )

    start_dt = run_dt - timedelta(days=window_days)

    jql1 = (
        f'project = JEJQ '
        f'AND updated >= "{start_dt.strftime("%Y-%m-%d %H:%M")}" '
        # f'AND updated <= "{run_dt.strftime("%Y-%m-%d %H:%M")}"'
    )

    if term_clause:
        jql1 += f" AND ({term_clause})"
    jql1 += " ORDER BY updated DESC"

    logger.info(f"[JEJQ JQL1] {jql1}")
    result1 = jira_search(jql1, max_results=500)
    # Find top 10 candidate issues with closest updated date to run_dt
    updated_dts = {}
    for issue in result1:
        updated_str = issue.get("fields", {}).get("updated") # e.g. '2026-07-02T05:24:13.000-0400'
        dev_dt = _extract_dev_latest_dt(issue)
        if dev_dt:
            updated_dts[issue.get("key")] = dev_dt # development updated time is more accurate than the issue updated time
        elif updated_str:
            try:
                updated_dt = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%S.%f%z")
                updated_dts[issue.get("key")] = updated_dt
            except Exception:
                updated_dts[issue.get("key")] = None
    # Sort issues by absolute time difference to run_dt
    sorted_issues = sorted(result1, key=lambda issue: abs((updated_dts.get(issue.get("key")) - run_dt).total_seconds()) if updated_dts.get(issue.get("key")) else float('inf'))
    result1 = sorted_issues[:S8_MAX_SEED_ISSUES]
    for issue in result1:
        all_candidates[issue.get("key")] = issue
    logger.info(f"[JEJQ Search] Strategy 1 found: {len(result1)} issues")

    # Combine all candidates
    seed = list(all_candidates.values())
    logger.info(f"[JEJQ Search] Total unique candidates after merge: {len(seed)}")
    logger.info(f"[JEJQ Search] All candidates: {[c.get('key') for c in seed]}")
    
    if not seed:
        logger.warning("No JEJQ candidates found from search strategies")
        return []
    
    # Rank and augment
    ranked_seed = _rank_evidence_issues(seed, signals)
    logger.info(f"[JEJQ Rank1] After ranking: {len(ranked_seed)} candidates")
    
    augmented = _augment_with_linked_issues(ranked_seed, signals)
    logger.info(f"[JEJQ Augment] After graph expansion: {len(augmented)} total candidates")
    
    final_ranked = _rank_evidence_issues(augmented, signals)
    logger.info(f"[JEJQ Rank2] Final ranking: {len(final_ranked)} top candidates")
    
    return final_ranked


def extract_dq_signals(description: str) -> Dict[str, Any]:
    desc = description or ""

    def _clean_capture(value: Optional[str]) -> Optional[str]:
        """Strip whitespace and Jira markup asterisks from captured values."""
        if not value:
            return None
        return value.strip().strip("*").strip() or None

    dataset = None
    m_dataset = re.search(r"\*?Dataset\*?\s*:\s*([^\n\r]+)", desc, flags=re.IGNORECASE)
    if m_dataset:
        dataset = _clean_capture(m_dataset.group(1))

    table_name = None
    m_table = re.search(r"\*?(DataTable|Table\s*Name)\*?\s*:\s*([^\n\r]+)", desc, flags=re.IGNORECASE)
    if m_table:
        table_name = _clean_capture(m_table.group(2))

    run_id = None
    # Try RunId (runDate): format first (JIRA actual format)
    m_run = re.search(r"\*?RunId\s*\([^)]*\)\*?\s*:\s*([^\n\r]+)", desc, flags=re.IGNORECASE)
    if m_run:
        run_id = _clean_capture(m_run.group(1))
    if not run_id:
        # Fallback to other patterns
        m_run = re.search(r"\*?(Run\s*ID|RunId|Run\s*Date|runDate)\*?\s*[:(]\s*([^\n\r)]+)", desc, flags=re.IGNORECASE)
        if m_run:
            run_id = _clean_capture(m_run.group(2))

    business_unit = None
    m_bu = re.search(r"\*?Project\*?\s*:\s*([^\n\r]+)", desc, flags=re.IGNORECASE)
    if m_bu:
        business_unit = _clean_capture(m_bu.group(1))

    break_keys = re.findall(r"([A-Za-z0-9_]+__([A-Za-z0-9_\- ]+))", desc)
    flat_breaks = [bk[0] for bk in break_keys]

    run_dt = _parse_dt(run_id)
    row_count_direction = _rowcount_direction_from_text(desc)
    return {
        "dataset": dataset,
        "table_name": table_name,
        "run_id": run_id,
        "run_dt": run_dt,
        "business_unit": business_unit,
        "row_count_direction": row_count_direction,
        "break_keys": flat_breaks,
    }


def issue_to_text(issue: Dict[str, Any], include_comments: bool = True) -> str:
    """Render an evidence issue into prompt text for LLM diagnosis.

    Includes full description and full extracted comment bodies so the model can
    use all available narrative context from Jira evidence.
    """
    if not isinstance(issue, dict):
        return str(issue)

    key = issue.get("key") or issue.get("issue_key") or "UNKNOWN"
    fields = issue.get("fields") if isinstance(issue.get("fields"), dict) else issue
    summary = fields.get("summary") or ""
    status = (fields.get("status") or {}).get("name") if isinstance(fields.get("status"), dict) else fields.get("status")
    updated = fields.get("updated") or ""
    created = fields.get("created") or ""
    issue_type = (fields.get("issuetype") or {}).get("name") if isinstance(fields.get("issuetype"), dict) else fields.get("issuetype")
    description = fields.get("description") or ""
    comments = _extract_comment_text_with_sanitization(fields) if include_comments else ""
    dev_dt = _extract_dev_latest_dt(issue)
    base_text = (
        f"Key: {key}\n"
        f"Type: {issue_type}\n"
        f"Summary: {summary}\n"
        f"Status: {status}\n"
        f"Created: {created}\n"
        f"Updated: {updated}\n"
        f"Development Latest: {dev_dt.isoformat() if dev_dt else ''}\n"
        f"S8 Relevance Score: {issue.get('_s8_relevance_score', '')}\n"
        f"Description: {str(description)}"
    )
    if include_comments:
        return f"{base_text}\nComments:\n{comments}"
    return base_text


def diagnose_with_agent(
    jgpv_key: str,
    jgpv_summary: str,
    jgpv_description: str,
    signals: Dict[str, Any],
    evidence_issues: List[Dict[str, Any]],
) -> DiagnosisResult:
    """Generate structured root-cause diagnosis from ranked JEJQ evidence.

    This function converts top-ranked upstream evidence issues into a compact
    textual context, builds an instruction prompt, and asks the JNJ Claude
    gateway model to return a validated ``DiagnosisResult``.

    Prompt behavior enforced here:
    - Uses only the first 12 evidence issues to keep context bounded.
    - Requests a confidence label (High/Medium/Low), concise evidence bullets,
      likely upstream ticket keys, and concrete next actions.
    - Constrains ``suspected_upstream_tickets`` to keys present in the provided
      evidence context.
    - Adds a CN/APAC routing constraint: unless the affected table is in
      pixonomy, CN-region tickets should not be selected.

    Args:
        jgpv_key: Target adaptive-rule incident key being diagnosed.
        jgpv_summary: JGPV issue summary text.
        jgpv_description: Full JGPV issue description markup/text.
        signals: Parsed DQ signals (dataset, run_id, break keys, etc.) used to
            ground the prompt.
        evidence_issues: Ranked JEJQ candidate issues from deterministic search
            and scoring.

    Returns:
        A ``DiagnosisResult`` produced via structured output validation,
        containing suspected root cause, confidence, evidence bullets,
        suspected upstream ticket keys, and next actions.
    """
    evidence_text = "\n\n".join(
        issue_to_text(item, include_comments=(idx < 4))
        for idx, item in enumerate(evidence_issues[:12])
    )
    prompt = f"""
You are a data quality analyst diagnosing pipeline incidents.

Target incident ticket: {jgpv_key}
Summary: {jgpv_summary}
Description:\n{jgpv_description}

Extracted DQ signals:
- dataset: {signals.get('dataset')}
- run_id: {signals.get('run_id')}
- break_keys: {signals.get('break_keys')}

Candidate upstream evidence tickets:
{evidence_text}

Task:
1) Infer suspected root cause(s) linking the DQ issue with upstream or system changes.
2) Provide confidence level (High/Medium/Low).
3) Provide concise evidence bullets referencing upstream ticket keys.
4) Provide actionable next steps.
5) suspected_upstream_tickets must contain only upstream ticket keys found in the evidence above.

Note that CN region is separated from APAC, so unless the affected table is in pixonomy db, do not include CN region tickets in suspected_upstream_tickets. 
""".strip()

    model = JNJClaudeGatewayModel(
        api_key=_load_secret_or_default("JNJ_GENAI_API_KEY", config.JNJ_GENAI_API_KEY),
        temperature=0.1,
        max_tokens=1500,
    )
    return model.structured_output(DiagnosisResult, prompt)


def build_enrichment_block(diagnosis: DiagnosisResult, generated_at: str) -> str:
    evidence_lines = "\n".join([f"- {line}" for line in diagnosis.evidence_summary]) or "- None"
    upstream_lines = "\n".join([f"- {key}" for key in diagnosis.suspected_upstream_tickets]) or "- None"
    action_lines = "\n".join([f"- {line}" for line in diagnosis.next_actions]) or "- None"

    return (
        "h3. Investigation Summary (AI-assisted)\n\n"
        f"*Generated At (UTC):* {generated_at}\n"
        f"*Confidence:* {diagnosis.confidence}\n\n"
        "h4. Suspected Root Cause\n\n"
        f"{diagnosis.suspected_root_cause}\n\n"
        "h4. Supporting Upstream JEJQ Evidence\n\n"
        f"{upstream_lines}\n\n"
        "h4. Evidence Details\n\n"
        f"{evidence_lines}\n\n"
        "h4. Proposed Next Actions\n\n"
        f"{action_lines}\n"
    )


def merge_description_with_investigation(original_description: str, enrichment_block: str) -> str:
    description = original_description or ""
    marker = "h3. Investigation Summary (AI-assisted)"

    if marker in description:
        description = description.split(marker, 1)[0].rstrip()

    if description and not description.endswith("\n"):
        description += "\n\n"

    return description + enrichment_block


def _looks_adaptive_ticket(row: pd.Series) -> bool:
    ticket_source = str(row.get("ticket_source") or "").strip().lower()
    if ticket_source == "adaptive":
        return True

    labels = str(row.get("suggested_labels") or "").lower()
    return "adaptive" in labels


def run() -> None:
    logger.info("Starting S8 diagnosis stage")

    df_new = read_input("new_jira_ticket_list")
    df_bu = read_input("business_unit_mapping")
    if df_new.empty:
        logger.info("No new Jira tickets found. Writing empty diagnosis output.")
        write_output(pd.DataFrame(), "issue_diagnosis_results")
        return

    df_adaptive = df_new[df_new.apply(_looks_adaptive_ticket, axis=1)].copy()
    logger.info(
        "S8 adaptive-only mode: total tickets=%s, adaptive tickets=%s",
        len(df_new),
        len(df_adaptive),
    )
    if df_adaptive.empty:
        logger.info("No adaptive Jira tickets found. Writing empty diagnosis output.")
        write_output(pd.DataFrame(), "issue_diagnosis_results")
        return

    issue_keys = []
    if "SUBTASK" in df_adaptive.columns:
        issue_keys = df_adaptive["SUBTASK"].dropna().astype(str).str.strip().unique().tolist()
    if not issue_keys and "issue_key" in df_new.columns:
        issue_keys = df_adaptive["issue_key"].dropna().astype(str).str.strip().unique().tolist()

    if not issue_keys:
        logger.info("No issue keys found in new_jira_ticket_list.")
        write_output(pd.DataFrame(), "issue_diagnosis_results")
        return

    results: List[Dict[str, Any]] = []
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    for issue_key in issue_keys:
        try:
            issue = jira_get_issue(issue_key, fields="summary,description,status,updated")
            fields = issue.get("fields") or {}
            summary = str(fields.get("summary") or "")
            description = str(fields.get("description") or "")

            signals = extract_dq_signals(description)
            # add dataset project mapping into signals
            signals["project"] = df_bu[
                df_bu["dataset"] == signals.get("dataset")
                ]["Project"].values[0] if not df_bu[df_bu["dataset"] == signals.get("dataset")].empty else None
            evidence = search_upstream_evidence(description, signals)
            logger.info(f"[DEBUG] search_upstream_evidence returned {len(evidence)} results")
            logger.info(f"[DEBUG] signals keys: {list(signals.keys())}")
            logger.info(f"[DEBUG] Ready to call diagnose_with_agent with issue_key={issue_key}")
            logger.info("Diagnosing issue key: %s", issue_key)
            logger.info(
                "Extracted signals: dataset=%s, table_name=%s, run_id=%s, row_count_direction=%s, break_keys=%s, evidence_count=%s",
                signals.get("dataset"),
                signals.get("table_name"),
                signals.get("run_id"),
                signals.get("row_count_direction"),
                signals.get("break_keys"),
                len(evidence),
            )
            logger.info(
                "Evidence keys (top ranked): %s",
                ", ".join([f"{str(e.get('key') or '')}:{e.get('_s8_relevance_score', '')}" for e in evidence[:10]]),
            )
            try:
                diagnosis = diagnose_with_agent(issue_key, summary, description, signals, evidence)
                logger.info(f"[LLM_REASONING] {issue_key}: {diagnosis.suspected_root_cause[:300]}")
            except Exception as e:
                import traceback
                logger.error(f"LLM diagnose failed: {type(e).__name__}: {e}")
                logger.error(f"Traceback:\n{traceback.format_exc()}")
                raise

            confidence = str(diagnosis.confidence or "").strip().lower()
            posted_to_jira = confidence == "high"
            # posted_to_jira = False # Disable auto-update for now; set to True to enable Jira updates
            if posted_to_jira:
                enrichment = build_enrichment_block(diagnosis, now_utc)
                new_description = merge_description_with_investigation(description, enrichment)
                jira_update_description(issue_key, new_description)

                jira_add_comment(
                    issue_key,
                    "[AUTO_DIAGNOSE] Investigation completed (High confidence). Ticket description updated with root-cause analysis.",
                )
            else:
                logger.info(
                    "Skipping Jira update for %s due to non-high confidence: %s",
                    issue_key,
                    diagnosis.confidence,
                )

            results.append(
                {
                    "issue_key": issue_key,
                    "dataset": signals.get("dataset"),
                    "run_id": signals.get("run_id"),
                    "confidence": diagnosis.confidence,
                    "suspected_root_cause": diagnosis.suspected_root_cause,
                    "suspected_upstream_tickets": ", ".join(diagnosis.suspected_upstream_tickets),
                    "evidence_count": len(diagnosis.evidence_summary),
                    "next_actions": " | ".join(diagnosis.next_actions),
                    "status": "updated" if posted_to_jira else "skipped_non_high_confidence",
                }
            )
            if posted_to_jira:
                logger.info("Diagnosed and updated %s", issue_key)
            else:
                logger.info("Diagnosed only (no Jira update) for %s", issue_key)
        except Exception as exc:
            logger.warning("Failed to diagnose %s: %s", issue_key, exc)
            results.append(
                {
                    "issue_key": issue_key,
                    "dataset": None,
                    "run_id": None,
                    "confidence": None,
                    "suspected_root_cause": None,
                    "suspected_upstream_tickets": None,
                    "evidence_count": 0,
                    "next_actions": None,
                    "status": f"error: {exc}",
                }
            )

    write_output(pd.DataFrame(results), "issue_diagnosis_results")
    logger.info("S8 diagnosis completed. Output rows: %s", len(results))


if __name__ == "__main__":
    run()
