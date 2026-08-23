# -*- coding: utf-8 -*-
"""
S1: Query Datasets and Business Units from Collibra CDQ
========================================================

Entry point of the DQ automation pipeline.  Calls the Collibra CDQ REST API
for both the APAC and CN regions to retrieve the full list of monitored datasets,
map each dataset to its business unit, and resolve the latest DQ run ID for every
dataset.  The results are written to the configured output store and consumed by
subsequent stages (S2 onwards).

Inputs
------
No pipeline inputs — this is the first stage.  All data is fetched live from the
Collibra CDQ API.

External APIs Called
--------------------
- Collibra CDQ (APAC region): cdq_base_url_apac
    - GET /v2/getlistdatasets           – Full list of active datasets
    - GET /v2/business-unit             – All business unit definitions
    - GET /v2/business-unit-to-dataset  – Dataset → business unit ID mappings
    - GET /v2/getRunIdDetailsByDataset  – Latest run ID (run_date) per dataset
- Collibra CDQ (CN region): cdq_base_url_cn  – Same four endpoints for CN datasets

Outputs (written via PipelineIO)
---------------------------------
| Table name             | Columns                                      | Description                                    |
|------------------------|----------------------------------------------|------------------------------------------------|
| dataset_apac           | dataset, run_date                            | APAC datasets with their latest DQ run date    |
| dataset_cn             | dataset, run_date                            | CN datasets with their latest DQ run date      |
| business_unit_mapping  | dataset, business_unit, Market, Project, CDE | Dataset → BU name and parsed market/project    |
| dataset_runid          | dataset, run_date                            | Combined APAC + CN datasets with latest run ID |

Notes
-----
- Datasets not starting with ``ds_`` are filtered out (removes test/system entries).
- ``business_unit`` follows the pattern ``"<Market> - <Project>"``; the script parses
  this into separate ``Market`` and ``Project`` columns and sets ``CDE = "Yes"`` when
  the project token equals ``"CDE"``.
- Token refresh is handled automatically: a 401 response triggers one re-authentication
  attempt before raising an exception.

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- cdq_base_url_apac / cdq_base_url_cn
- username_apac / password_apac / username_cn / password_cn
- uc_catalog / uc_schema  (required when PIPELINE_WRITE_MODE != csv)
"""

import requests
import urllib3
import pandas as pd
import config
from typing import Any
from token_manager import CollibraTokenManager
from pipeline_io import PipelineIO
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------
# Output Mode Configuration
# ---------------------------------------------------
WRITE_MODE = os.getenv("PIPELINE_WRITE_MODE", "csv").strip().lower()
LOCAL_OUTPUT_DIR = os.getenv("PIPELINE_LOCAL_OUTPUT_DIR", "./outputs")
SECRET_SCOPE = os.getenv("DATABRICKS_SECRET_SCOPE", "collibra")

# ---------------------------------------------------
# Configuration & Credentials
# ---------------------------------------------------
# For Databricks: Use Databricks Secrets (dbutils.secrets.get)
# For Local Dev: Use config.py variables
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

# ---------------------------------------------------
# Initialize Token Managers
# ---------------------------------------------------
token_mgr_apac = CollibraTokenManager(
    base_url=cdq_url_apac,
    username=username_apac,
    password=password_apac,
    region="apac"
)

token_mgr_cn = CollibraTokenManager(
    base_url=cdq_url_cn,
    username=username_cn,
    password=password_cn,
    region="cn"
)

region_configs = [
    {
        "mgr": token_mgr_apac,
        "base_url": cdq_url_apac,
        "region": "apac",
        "table_name": "dataset_apac",
    },
    {
        "mgr": token_mgr_cn,
        "base_url": cdq_url_cn,
        "region": "cn",
        "table_name": "dataset_cn",
    },
]

pipeline_io = PipelineIO(
    write_mode=WRITE_MODE,
    local_output_dir=LOCAL_OUTPUT_DIR,
    dbutils=dbutils,
    spark=globals().get("spark"),
    secret_scope=SECRET_SCOPE,
    uc_catalog=uc_catalog,
    uc_schema=uc_schema,
)

read_input = pipeline_io.read_input
write_output = pipeline_io.write_output


def extract_business_unit_fields(bu_name):
    """Extract Market, Project, and CDE flag from business unit name."""
    if pd.isna(bu_name) or not isinstance(bu_name, str):
        return pd.Series({"Market": None, "Project": None, "CDE": "No"})

    normalized = bu_name.strip()
    if " - " in normalized:
        parts = [p.strip() for p in normalized.split(" - ", 1)]
        market = parts[0] if len(parts) > 0 else None
        project = parts[1] if len(parts) > 1 else None
    else:
        market = normalized if normalized else None
        project = None

    cde = "Yes" if isinstance(project, str) and project.upper() == "CDE" else "No"

    return pd.Series({"Market": market, "Project": project, "CDE": cde})

df_lst = [] # to store business unit mappings for all regions
df_ds_lst = [] # to store dataset run_id mappings for all regions

for config_item in region_configs:
    token_mgr = config_item["mgr"]
    cdq_base = config_item["base_url"]
    region = config_item["region"]
    dataset_table_name = config_item["table_name"]
    
    # ---------------------------------------------------
    # API Endpoints
    # ---------------------------------------------------
    url_bu = f"{cdq_base}/v2/business-unit"
    url_bu_to_ds = f"{cdq_base}/v2/business-unit-to-dataset"
    url_ds = f"{cdq_base}/v2/getlistdatasets"
    url_runid = f'{cdq_base}/v2/getRunIdDetailsByDataset'

    # ---------------------------------------------------
    # Helper: Safe GET with automatic token refresh on 401
    # ---------------------------------------------------
    def safe_get(url, max_retries=1):
        """
        Safely GET from API with automatic token refresh on 401.
        
        Args:
            url: API endpoint URL
            max_retries: Number of retry attempts (default: 1 for one refresh attempt)
            
        Returns:
            Parsed JSON response
            
        Raises:
            Exception: If request fails after retries
        """
        for attempt in range(max_retries + 1):
            headers = token_mgr.get_auth_header()
            
            try:
                r = requests.get(url, headers=headers, verify=False, timeout=30)
                
                if r.status_code == 200:
                    return r.json()
                
                elif r.status_code == 401:
                    print(f"[{region.upper()}] Token expired or invalid. Refreshing...")
                    if attempt < max_retries:
                        token_mgr.get_token(force_refresh=True)
                        continue
                    else:
                        raise Exception(
                            f"[{region.upper()}] Call failed after token refresh: "
                            f"{url} → HTTP {r.status_code}"
                        )
                
                else:
                    raise Exception(
                        f"[{region.upper()}] Call failed: {url} → HTTP {r.status_code} "
                        f"{r.text[:200]}"
                    )
            
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    print(f"[{region.upper()}] Request error: {e}. Retrying...")
                    token_mgr.get_token(force_refresh=True)
                    continue
                else:
                    raise Exception(f"[{region.upper()}] Request failed: {e}")
        
        raise Exception(f"[{region.upper()}] Unknown error occurred")
    
    # ---------------------------------------------------
    # 0) Fetch all Datasets
    # ---------------------------------------------------
    print(f"[{region.upper()}] Fetching datasets...")
    ds_raw = safe_get(url_ds)
    
    # ---------------------------------------------------
    # 1) Fetch all Business Units
    # ---------------------------------------------------
    print(f"[{region.upper()}] Fetching business units...")
    biz_units_raw = safe_get(url_bu)

    # business_unit_id → business_unit_name
    business_units = {}
    for item in biz_units_raw.get("result", []):
        bid = item["id"]
        business_units[bid] = item["name"]

    # ---------------------------------------------------
    # 2) Fetch all Business Unit → Dataset mappings
    # ---------------------------------------------------
    print(f"[{region.upper()}] Fetching business unit to dataset mappings...")
    biz_map_raw = safe_get(url_bu_to_ds)

    # dataset → business_unit_id
    dataset_to_buid = {}
    for item in biz_map_raw.get("result", []):
        dataset = item["dataset"]
        bu_id = item["businessUnitId"]
        dataset_to_buid[dataset] = bu_id

    # ---------------------------------------------------
    # 3) Build dataset → business_unit_name mapping
    # ---------------------------------------------------
    business_unit_by_dataset = {}
    for ds, buid in dataset_to_buid.items():
        bu_name = business_units.get(buid)
        # Assign name only if the ID exists
        if bu_name:
            business_unit_by_dataset[ds] = bu_name
        else:
            business_unit_by_dataset[ds] = None  # or "UNKNOWN"

    # ---------------------------------------------------
    # For debugging / validation
    # ---------------------------------------------------
    print(f"[{region.upper()}] Total datasets: {len(ds_raw)}")
    print(f"[{region.upper()}] Total business units: {len(business_units)}")
    print(f"[{region.upper()}] Total dataset mappings: {len(dataset_to_buid)}")
    print(f"[{region.upper()}] Sample mappings: {list(business_unit_by_dataset.items())[:5]}")

    # Build output dataframe
    df_output = (
        pd.DataFrame.from_dict(
            business_unit_by_dataset, orient="index", columns=["business_unit"]
        )
        .reset_index()
        .rename(columns={"index": "dataset"})
    )
    df_output[["Market", "Project", "CDE"]] = df_output["business_unit"].apply(extract_business_unit_fields)
    df_lst.append(df_output)
    
    # Export dataset list
    df_ds = pd.DataFrame(ds_raw, columns=['dataset'])
    df_ds = df_ds[df_ds['dataset'].str.startswith('ds_')]  # remove testing datasets

    # ---------------------------------------------------
    # 4) Fetch run_id for each dataset
    # ---------------------------------------------------
    for index, row in df_ds.iterrows():
        dataset = row['dataset']
        url_request = f'{url_runid}?dataset={dataset}'
        data = safe_get(url_request)
        runid = data.get('latestRun')
        df_ds.loc[index, 'run_date'] = runid

    df_ds_lst.append(df_ds)
    
    # ---------------------------------------------------
    # Export dataset list using configured mode
    # ---------------------------------------------------
    write_output(df_ds, dataset_table_name)

# ---------------------------------------------------
# Export Business Unit mapping
# ---------------------------------------------------
df_o = pd.concat(df_lst, axis=0)
print(df_o.shape)
print(df_o.head(5))
write_output(df_o, "business_unit_mapping")

# ---------------------------------------------------
# Export dataset run_id mapping
# ---------------------------------------------------
df_runid = pd.concat(df_ds_lst, axis=0)
print(df_runid.shape)
print(df_runid.head(5))
write_output(df_runid, "dataset_runid")

print("\n" + "="*60)
print("Pipeline completed successfully!")
print("="*60)
