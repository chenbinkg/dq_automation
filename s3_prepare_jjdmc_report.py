# -*- coding: utf-8 -*-
"""
S3: Prepare Quarterly JJDMC Report Artifacts (Optional)
========================================================

Generates quarterly JJDMC (Janssen Joint Data Management Committee) reports by
aggregating DQ metrics across business units and data domains. Loads enriched
dataset metadata from S2, queries historical DQ runs (past 3 months), and produces
domain-specific consolidated outputs for reporting and governance.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name             | Produced by | Description                                    |
|------------------------|-------------|------------------------------------------------|
| business_unit_mapping  | S2          | Dataset → business unit / market / project     |
| dataset_custom_rules   | S2          | Custom rule definitions with run-level metrics |

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name              | Description                                              |
|-------------------------|----------------------------------------------------------|
| epic_list_prepared      | Aggregated metrics by Epic / business unit / data domain |
| IM_APAC_Customer        | Domain-specific report: Customer data (APAC region)      |
| IM_APAC_Employee        | Domain-specific report: Employee data (APAC region)      |
| IM_APAC_HCP             | Domain-specific report: HCP data (APAC region)           |
| IM_APAC_Material        | Domain-specific report: Material data (APAC region)      |
| mwaa_dq_trigger_datasets| Reference list of datasets that trigger MWAA workflows   |

Processing Logic
----------------
1. Load enriched business_unit_mapping with db_nm/table_nm from S2
2. For each dataset:
   - Query all runIds from past 3 months (PostgreSQL history)
   - Extract latest score for each custom rule
   - Compute aggregated metrics (average, count) per rule
3. Generate domain-specific outputs by filtering on data domain tags
4. Include "Number of Records" column for each aggregated combination
5. Write outputs to configured store (CSV, Unity Catalog, or both)

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- db_host / db_port / db_name / db_user / db_password / db_table  (PostgreSQL)
- uc_catalog / uc_schema  (required when PIPELINE_WRITE_MODE != csv)

External Dependencies
---------------------
- PostgreSQL: Reads historical DQ run data
- Unity Catalog: Writes outputs when configured
"""

import os
import logging
import pandas as pd
import requests
import urllib3
import numpy as np
import config
from datetime import datetime, timedelta
from requests.utils import quote
from typing import Optional, Dict, Any, List, Tuple, Set
from pipeline_io import PipelineIO
from token_manager import CollibraTokenManager
from mwaa_dataset_reference import MWAADatasetReference
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------
# Logging
# ---------------------------------------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------
# Output Mode Configuration
# ---------------------------------------------------
WRITE_MODE = os.getenv("PIPELINE_WRITE_MODE", "csv").strip().lower()
LOCAL_OUTPUT_DIR = os.getenv("PIPELINE_LOCAL_OUTPUT_DIR", "./outputs")
SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")
DAYS_LOOKBACK = int(os.getenv("PIPELINE_QUARTERLY_DAYS", "90"))
DEFAULT_SYSTEM_MAPPING = "iDiscover DWH (Redshift + Synapse DB)"

logger.info(f"Pipeline write mode: {WRITE_MODE}")
logger.info(f"Looking back {DAYS_LOOKBACK} days for historical data")


# ---------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------
try:
    from databricks.sdk import WorkspaceClient
    dbutils = WorkspaceClient().dbutils
except Exception:
    print("Warning: dbutils not available. Running outside Databricks.")
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


# ---------------------------------------------------
# Token Managers
# ---------------------------------------------------
token_mgr_apac = CollibraTokenManager(
    base_url=cdq_url_apac,
    username=username_apac,
    password=password_apac,
    region="apac",
)

token_mgr_cn = CollibraTokenManager(
    base_url=cdq_url_cn,
    username=username_cn,
    password=password_cn,
    region="cn",
)


# ---------------------------------------------------
# Input/Output Helpers
# ---------------------------------------------------
pipeline_io = PipelineIO(
    write_mode=WRITE_MODE,
    local_output_dir=LOCAL_OUTPUT_DIR,
    dbutils=dbutils,
    spark=globals().get("spark"),
    secret_scope=SECRET_SCOPE,
    uc_catalog=uc_catalog,
    uc_schema=uc_schema,
    logger=logger,
    sanitize_uc_table_names=True,
)

read_input = pipeline_io.read_input
write_output = pipeline_io.write_output


# ---------------------------------------------------
# API Helpers
# ---------------------------------------------------
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


def _deep_get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    current: Any = d
    for part in path.split("."):
        if isinstance(current, dict) and part in current:
            current = current[part]
        else:
            return default
    return current


def get_dataset_runs_in_period(
    token_mgr: CollibraTokenManager,
    dataset: str,
    days_back: int = 90
) -> List[str]:
    """
    Query all runIds for a dataset from the past N days using getRunIdDetailsByDataset.
    
    Returns a filtered list of run IDs (ISO 8601 datetime strings) from the past N days.
    """
    # Calculate date cutoff
    cutoff_date = (datetime.now() - timedelta(days=days_back)).date()
    
    # Build URL with date parameters to filter on server side
    url = (
        f"{token_mgr.base_url}/v2/getRunIdDetailsByDataset"
        f"?dataset={quote(dataset, safe='')}"
        f"&startDate={cutoff_date.isoformat()}"
        f"&endDate={datetime.now().date().isoformat()}"
        f"&orderBy=ASC"
    )
    
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch run details for {dataset} ({token_mgr.region}): status={status}, err={err}")
        return []
    
    # Extract runIds array from response
    if isinstance(payload, dict) and "runIds" in payload:
        run_ids = payload.get("runIds", [])
        if isinstance(run_ids, list):
            logger.info(f"Found {len(run_ids)} runs for {dataset} in past {days_back} days")
            return run_ids
    
    logger.debug(f"No runIds found for {dataset} in response")
    return []


def get_rules_for_dataset_run(
    token_mgr: CollibraTokenManager,
    dataset: str,
    run_id: str
) -> List[Dict[str, Any]]:
    """Fetch per-run rule scores/metrics from v2/getDatasetReport for a specific dataset run."""
    url = f"{token_mgr.base_url}/v2/getDatasetReport?dataset={quote(dataset, safe='')}&runId={quote(str(run_id), safe='')}"
    
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch report for {dataset}/{run_id}: status={status}, err={err}")
        return []
    
    # Normalize payload - the report is a flat list of rule rows
    rules = []
    if isinstance(payload, list):
        rules = [x for x in payload if isinstance(x, dict)]
    elif isinstance(payload, dict):
        for key in ("rules", "data", "items", "results", "content"):
            if key in payload and isinstance(payload[key], list):
                rules = [x for x in payload[key] if isinstance(x, dict)]
                break
    
    return rules


def get_all_rules(token_mgr: CollibraTokenManager) -> List[Dict[str, Any]]:
    """Fetch all rule definitions from v3/rules (includes CUSTOM rule metadata)."""
    url = f"{token_mgr.base_url}/v3/rules"
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch all rules ({token_mgr.region}): status={status}, err={err}")
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("rules", "data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list):
                return [x for x in payload[key] if isinstance(x, dict)]
    return []


def get_template_rules(token_mgr: CollibraTokenManager) -> List[Dict[str, Any]]:
    """Fetch all template rules from v2/templateRules."""
    url = f"{token_mgr.base_url}/v2/templateRules"
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"Failed to fetch template rules ({token_mgr.region}): status={status}, err={err}")
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("templates", "data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list):
                return [x for x in payload[key] if isinstance(x, dict)]
    return []


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


def apply_template_enrichment(row: pd.Series, cn_datasets_set: set, lookup_apac: Dict, lookup_cn: Dict) -> pd.Series:
    """Enrich CUSTOM rules from template rules."""
    dataset = str(row.get("dataset") or "")
    is_cn = dataset in cn_datasets_set
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
                resolved = tmpl_value.replace("$colNm", str(col_name)) if col_name else tmpl_value
                if not row.get("ruleValue"):
                    row["ruleValue"] = resolved

            if tmpl_desc and not row.get("businessDesc"):
                row["businessDesc"] = tmpl_desc

    return row


def get_dataset_report_first_row(token_mgr: CollibraTokenManager, dataset: str, run_id: str) -> Dict[str, Any]:
    """Use v2/getDatasetReport with dataset + runId and return db_nm/table_nm from first row."""
    url = f"{token_mgr.base_url}/v2/getDatasetReport?dataset={quote(dataset, safe='')}&runId={quote(str(run_id), safe='')}"
    payload, status, err = safe_get(url, token_mgr)
    if status != 200:
        logger.warning(f"getDatasetReport failed for {dataset}/{run_id}: status={status}, err={err}")
        return {"db_nm": None, "table_nm": None}

    first = None
    if isinstance(payload, list) and len(payload) > 0 and isinstance(payload[0], dict):
        first = payload[0]
    elif isinstance(payload, dict):
        for key in ("data", "items", "content", "results"):
            if key in payload and isinstance(payload[key], list) and payload[key]:
                if isinstance(payload[key][0], dict):
                    first = payload[key][0]
                    break

    if not first:
        return {"db_nm": None, "table_nm": None}

    return {"db_nm": first.get("db_nm"), "table_nm": first.get("table_nm")}


# ---------------------------------------------------
# Domain Mappings
# ---------------------------------------------------
domain_mapping = {
    'ACT': '',
    'EMP': 'Employee',
    'FIN': '',
    'GEO': '',
    'HCO': 'Customer/Account',
    'HCP': 'HCP',
    'ORG': '',
    'OTH': '',
    'PAT': '',
    'PRO': 'Material/Product',
    'SAL': ''
}

system_mapping = {
    'Conn_Prod_DBx': 'Databricks',
    'Conn_Prod_Dbx_CN': 'Databricks',
    'Conn_Prod_ODS': 'iDiscover MDM (Pincore / ODS)',
    'Conn_Prod_Redshift_JP_Local': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_JP_Local_PD': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_Region': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_Region_PD': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_Region_TW': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_VI': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Redshift_VN': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Prod_Reshift_JP_Local_latest': 'iDiscover DWH (Redshift + Synapse DB)',
    'CAE_Outputs_S3': 'iDiscover Data Lake (S3 + Blob)',
    'conn_s3_angen_maf_files': 'iDiscover Data Lake (S3 + Blob)',
    'conn_s3_angen_unload': 'iDiscover Data Lake (S3 + Blob)',
    'conn_s3_anz_enrichment_refined': 'iDiscover Data Lake (S3 + Blob)',
    'Conn_S3_DQ_DE_Exfactory_SKU_Indication_Price': 'iDiscover Data Lake (S3 + Blob)',
    'Conn_S3_DQ_DE_Product_Attribute_Mapping': 'iDiscover Data Lake (S3 + Blob)',
    'conn_s3_sys_marketdef_indsplt_automation': 'iDiscover Data Lake (S3 + Blob)',
    's3_conn_angen_maf_files': 'iDiscover Data Lake (S3 + Blob)',
    's3_conn_anz_enrichment': 'iDiscover Data Lake (S3 + Blob)',
    's3_conn_kr_raw': 'iDiscover Data Lake (S3 + Blob)',
    'Conn_Synapse_Prod': 'iDiscover DWH (Redshift + Synapse DB)',
    'Conn_Synapse_Prod_PD': 'iDiscover DWH (Redshift + Synapse DB)'

}


def map_domain_values(sub_domain_value: Any) -> str:
    if pd.isna(sub_domain_value):
        return ""

    parts = [part.strip() for part in str(sub_domain_value).split(",") if part.strip()]
    mapped = []
    for part in parts:
        mapped_value = domain_mapping.get(part, part)
        if isinstance(mapped_value, str) and mapped_value.strip():
            mapped.extend([item.strip() for item in mapped_value.split(",") if item.strip()])
    return ",".join(mapped)

# ---------------------------------------------------
# Main
# ---------------------------------------------------
logger.info("=== S3: Prepare Quarterly JJDMC Report ===")

# Load inputs from S2 outputs
df_rule_details = read_input("dataset_rule_details")
df_bu = read_input("business_unit_mapping")
df_runid = read_input("dataset_runid")
df_ds_cn = read_input("dataset_cn")
logger.info(
    f"Loaded inputs: rule_details={len(df_rule_details)}, bu={len(df_bu)}, "
    f"runid={len(df_runid)}, cn={len(df_ds_cn)}"
)

cn_datasets: Set[str] = set(df_ds_cn["dataset"].dropna().astype(str).tolist()) if "dataset" in df_ds_cn.columns else set()


def _region_token_manager(dataset_name: str) -> CollibraTokenManager:
    return token_mgr_cn if dataset_name in cn_datasets else token_mgr_apac


# Fetch all rules + template rules once per region
rules_apac = get_all_rules(token_mgr_apac)
rules_cn = get_all_rules(token_mgr_cn)
templates_apac = get_template_rules(token_mgr_apac)
templates_cn = get_template_rules(token_mgr_cn)
lookup_apac = build_template_lookup(templates_apac)
lookup_cn = build_template_lookup(templates_cn)
logger.info(f"Fetched {len(rules_apac)} APAC rules, {len(rules_cn)} CN rules")

# Keep only datasets that appear in rule_details
wanted_datasets = set(df_rule_details["dataset"].dropna().astype(str).tolist()) if "dataset" in df_rule_details.columns else set()

all_rules = []
for rec in rules_apac + rules_cn:
    ds = rec.get("dataset")
    if isinstance(ds, str) and ds in wanted_datasets:
        all_rules.append(rec)

if not all_rules:
    logger.warning("No matching rules found from v3/rules for datasets in dataset_rule_details")

df_rules = pd.DataFrame(all_rules)

# Ensure required fields exist
for col in ["dataset", "ruleNm", "ruleType", "ruleValue", "ruleRepo", "columnName",
            "businessDesc", "dimId", "dimName", "points", "isActive", "suppressed"]:
    if col not in df_rules.columns:
        df_rules[col] = None

# Enrich CUSTOM rules from templateRules
if not df_rules.empty:
    df_rules = df_rules.apply(
        lambda row: apply_template_enrichment(row, cn_datasets, lookup_apac, lookup_cn), axis=1
    )

# Drop score/perc/exception from v3/rules — use dataset_rule_details values only
for metric_col in ["perc", "exception", "score"]:
    if metric_col in df_rules.columns:
        df_rules = df_rules.drop(columns=[metric_col])

# Merge with dataset_rule_details to bring run-level metrics (latest run per dataset+ruleNm)
join_cols = ["dataset", "ruleNm"]
available_rule_detail_cols = [c for c in ["dataset", "ruleNm", "runId", "score", "perc", "exception"] if c in df_rule_details.columns]

df_rule_metrics = df_rule_details[available_rule_detail_cols].copy() if available_rule_detail_cols else pd.DataFrame()

if not df_rule_metrics.empty:
    if "runId" in df_rule_metrics.columns:
        df_rule_metrics = df_rule_metrics.sort_values(by=["runId"]).drop_duplicates(subset=join_cols, keep="last")
    else:
        df_rule_metrics = df_rule_metrics.drop_duplicates(subset=join_cols, keep="last")
    df_custom_rules = pd.merge(df_rules, df_rule_metrics, how="left", on=join_cols)
else:
    df_custom_rules = df_rules.copy()

# Attach runId from dataset_runid if missing after merge
if "runId" not in df_custom_rules.columns:
    df_custom_rules["runId"] = None

if "dataset" in df_runid.columns and "run_date" in df_runid.columns and not df_custom_rules.empty:
    df_custom_rules = pd.merge(
        df_custom_rules,
        df_runid[["dataset", "run_date"]].rename(columns={"run_date": "runId_from_runid"}),
        how="left",
        on=["dataset"],
    )
    df_custom_rules["runId"] = df_custom_rules["runId"].combine_first(df_custom_rules["runId_from_runid"])
    df_custom_rules = df_custom_rules.drop(columns=["runId_from_runid"])

# Populate db_nm/table_nm from v2/getDatasetReport first row
report_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}

if not df_custom_rules.empty:
    db_vals = []
    table_vals = []

    for _, row in df_custom_rules.iterrows():
        dataset = row.get("dataset")
        run_id = row.get("runId")

        if not isinstance(dataset, str) or not dataset or not isinstance(run_id, str) or not run_id:
            db_vals.append(None)
            table_vals.append(None)
            continue

        cache_key = (dataset, run_id)
        if cache_key not in report_cache:
            mgr = _region_token_manager(dataset)
            report_cache[cache_key] = get_dataset_report_first_row(mgr, dataset, run_id)

        db_vals.append(report_cache[cache_key].get("db_nm"))
        table_vals.append(report_cache[cache_key].get("table_nm"))

    df_custom_rules["db_nm"] = db_vals
    df_custom_rules["table_nm"] = table_vals
else:
    df_custom_rules["db_nm"] = None
    df_custom_rules["table_nm"] = None

# Merge all BU mapping columns from S2 output by dataset
if not df_custom_rules.empty and not df_bu.empty and "dataset" in df_bu.columns:
    bu_cols = [c for c in df_bu.columns if c != "dataset" and c not in df_custom_rules.columns]
    df_custom_rules = pd.merge(
        df_custom_rules,
        df_bu[["dataset"] + bu_cols].drop_duplicates(subset=["dataset"]),
        how="left",
        on=["dataset"],
    )

# Keep a clean output order
preferred_order = [
    "dataset", "runId", "db_nm", "table_nm",
    "ruleNm", "ruleType", "ruleRepo", "columnName", "ruleValue", "businessDesc",
    "dimId", "dimName", "score", "perc", "exception",
    "business_unit", "Market", "Project", "CDE", "jobSchedule", "Data Domain", "subDomain", "connectionName",
]
remaining_cols = [c for c in df_custom_rules.columns if c not in preferred_order]
final_cols = [c for c in preferred_order if c in df_custom_rules.columns] + remaining_cols
df_dataset_custom_rules = df_custom_rules[final_cols] if not df_custom_rules.empty else pd.DataFrame(columns=final_cols)

# Count number of runs in past N days per dataset and attach as "Number of Records"
logger.info(f"Counting runs per dataset for past {DAYS_LOOKBACK} days...")
unique_datasets = df_dataset_custom_rules["dataset"].dropna().astype(str).unique() if not df_dataset_custom_rules.empty else []
dataset_run_counts: Dict[str, int] = {}

for dataset in unique_datasets:
    mgr = _region_token_manager(dataset)
    run_ids = get_dataset_runs_in_period(mgr, dataset, DAYS_LOOKBACK)
    dataset_run_counts[dataset] = len(run_ids)

df_dataset_custom_rules["Number of Records"] = (
    df_dataset_custom_rules["dataset"].map(dataset_run_counts).fillna(0).astype(int)
    if not df_dataset_custom_rules.empty else 0
)
logger.info(f"Built dataset_custom_rules with {len(df_dataset_custom_rules)} rows")


# ---------------------------------------------------
# Domain explosion to JJDMC outputs
# ---------------------------------------------------
mwaa_reference = MWAADatasetReference.default()
trigger_datasets = set(mwaa_reference.datasets)


def explode_domain_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    transformed = df.copy()
    transformed["Domain"] = transformed["subDomain"].apply(map_domain_values) if "subDomain" in transformed.columns else ""
    transformed["System or Platform"] = transformed["connectionName"].map(system_mapping).fillna(DEFAULT_SYSTEM_MAPPING) if "connectionName" in transformed.columns else DEFAULT_SYSTEM_MAPPING
    transformed["Applicable Critical Data Element"] = transformed["columnName"] if "columnName" in transformed.columns else None
    transformed["ID"] = transformed.apply(
        lambda row: f"{row['db_nm']}.{row['table_nm']}" if pd.notna(row.get("db_nm")) and pd.notna(row.get("table_nm")) else None,
        axis=1,
    )
    transformed["Name"] = transformed["ruleNm"] if "ruleNm" in transformed.columns else None
    transformed["Rule"] = transformed["businessDesc"] if "businessDesc" in transformed.columns else None
    transformed["DQ Dimension"] = transformed["dimName"] if "dimName" in transformed.columns else None
    transformed["Data Quality Score"] = pd.to_numeric(transformed.get("score"), errors="coerce")
    transformed["Data Quality Score"] = (100 - transformed["Data Quality Score"]) / 100
    transformed["Number of Records"] = pd.to_numeric(transformed.get("Number of Records"), errors="coerce").fillna(0).astype(int)
    transformed["MWAA DQ Trigger"] = transformed["dataset"].apply(lambda x: "Yes" if str(x) in trigger_datasets else "No") if "dataset" in transformed.columns else "No"

    keep_cols = [
        "Domain", "System or Platform", "Applicable Critical Data Element", "ID",
        "Name", "Rule", "DQ Dimension", "Data Quality Score", "Number of Records",
        "MWAA DQ Trigger", "Market", "Project", "jobSchedule", "Data Domain",
        "dataset"
    ]
    keep_cols = [c for c in keep_cols if c in transformed.columns]
    transformed = transformed[keep_cols].copy()

    transformed["Domain"] = transformed["Domain"].fillna("").astype(str)
    transformed["Domain"] = transformed["Domain"].apply(
        lambda v: [item.strip() for item in v.split(",") if item.strip()] if v.strip() else [""]
    )
    transformed = transformed.explode("Domain")
    transformed = transformed[transformed["Domain"].astype(str).str.strip() != ""]
    transformed["Domain"] = transformed["Domain"].astype(str).str.strip()
    transformed["System or Platform"] = transformed["System or Platform"].replace("", pd.NA)

    return transformed


jjdmc_df = explode_domain_rows(df_dataset_custom_rules)

jjdmc_domain_groups = {
    "IM_APAC_Customer": "Customer/Account",
    "IM_APAC_Material": "Material/Product",
    "IM_APAC_HCP": "HCP",
    "IM_APAC_Employee": "Employee",
}

jjdmc_outputs: Dict[str, pd.DataFrame] = {}
for output_name, domain_value in jjdmc_domain_groups.items():
    domain_df = jjdmc_df[jjdmc_df["Domain"] == domain_value].copy() if not jjdmc_df.empty and "Domain" in jjdmc_df.columns else pd.DataFrame()
    jjdmc_outputs[output_name] = domain_df

# Write outputs
write_output(df_dataset_custom_rules, "dataset_custom_rules")
logger.info(f"Wrote dataset_custom_rules: {len(df_dataset_custom_rules)} rows")

for output_name, output_df in jjdmc_outputs.items():
    write_output(output_df, output_name)
    logger.info(f"Wrote {output_name}: {len(output_df)} rows")

logger.info("S3 quarterly JJDMC report generation completed successfully")
