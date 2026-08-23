# -*- coding: utf-8 -*-
"""
S4: Consolidate DQM Output and Publish to Tableau Cloud
=======================================================

Reads consolidated DQ metrics from S2, deduplicates against PostgreSQL historical
data, inserts new rows, and publishes monthly consolidated output to Tableau Cloud
as a Hyper extract for end-user dashboards and reporting.

Inputs (read via PipelineIO.read_input)
----------------------------------------
| Table name                    | Produced by | Description                              |
|-------------------------------|-------------|------------------------------------------|
| dqm_dashboard_by_data_domain  | S2          | Aggregated DQ scores joined with BU info |

External Data Sources
---------------------
- PostgreSQL: dqm_dashboard_history_apac table (historical DQ run data)

Outputs (written via PipelineIO.write_output)
----------------------------------------------
| Table name               | Description                                        |
|--------------------------|----------------------------------------------------|
| dqm_dashboard_consolidated| Monthly consolidated DQ metrics for Tableau export |

Publishing Flow
---------------
1. Read S2 dqm_dashboard_by_data_domain output (monthly DQ scores)
2. Query PostgreSQL dqm_dashboard_history_apac for existing records
3. Deduplicate: Filter new rows by (dataset, runId) keys
4. Insert only new rows into PostgreSQL (ON CONFLICT DO NOTHING)
5. Produce consolidated monthly output
6. Publish to Tableau Cloud as Hyper extract (if enabled)

Environment Variables
---------------------
- PIPELINE_WRITE_MODE        : csv | uc | both  (default: csv)
- PIPELINE_LOCAL_OUTPUT_DIR  : path for CSV outputs  (default: ./outputs)

Secrets (Databricks secret scope "collibra", or config.py fallback)
--------------------------------------------------------------------
- db_host / db_port / db_name / db_user / db_password / db_table  (PostgreSQL)
- uc_catalog / uc_schema  (required when PIPELINE_WRITE_MODE != csv)
- tableau_server / tableau_site / tableau_username / tableau_password  (Tableau Cloud)

External Integrations
----------------------
- PostgreSQL: Stores historical DQ records
- Tableau Cloud: Publishes Hyper extracts for dashboards

TO-DO
-----
- Activate _publish_to_tableau() before production deployment
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import pandas as pd

import config
from pipeline_io import PipelineIO
from postgres_io import (
    build_settings,
    filter_new_rows_by_keys,
    insert_on_conflict_do_nothing,
    read_table,
)
from tableau_publisher import publish_dataframe_to_tableau_cloud

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
OUTPUT_NAME = "merged_historical_data"

if WRITE_MODE not in ("csv", "uc", "both"):
    raise ValueError("PIPELINE_WRITE_MODE must be one of: csv, uc, both")

logger.info("Pipeline write mode: %s", WRITE_MODE)


# ---------------------------------------------------
# Input/Output Helpers
# ---------------------------------------------------
def _load_uc_target() -> tuple[Optional[str], Optional[str]]:
    if dbutils is not None:
        try:
            return (
                dbutils.secrets.get(scope=SECRET_SCOPE, key="uc_catalog"),
                dbutils.secrets.get(scope=SECRET_SCOPE, key="uc_schema"),
            )
        except Exception as exc:
            logger.info("Falling back to config.py UC values: %s", exc)

    return getattr(config, "UC_CATALOG", None), getattr(config, "UC_SCHEMA", None)


UC_CATALOG, UC_SCHEMA = _load_uc_target()
pipeline_io = PipelineIO(
    write_mode=WRITE_MODE,
    local_output_dir=LOCAL_OUTPUT_DIR,
    dbutils=dbutils,
    spark=globals().get("spark"),
    config_module=config,
    secret_scope=SECRET_SCOPE,
    uc_catalog=UC_CATALOG,
    uc_schema=UC_SCHEMA,
    logger=logger,
)

read_input = pipeline_io.read_input
write_output = pipeline_io.write_output

# DB insert order used by S2 and DB table schema.
DB_COLS = [
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
    "Comment",
    "Data Domain",
    "CDE",
    "Project",
    "Market",
    "report_time",
]


MARKET_MAP = {
    "CN": "CN",
    "China": "CN",
    "TW": "TW",
    "Taiwan": "TW",
    "JP": "JP",
    "Japan": "JP",
    "KR": "KR",
    "Korea": "KR",
    "HK": "HK",
    "Hong Kong": "HK",
    "ANZ": "ANZ",
    "Australia & New Zealand": "ANZ",
    "VN": "VN",
    "VIETNAM": "VN",
    "Region": "REGION",
}


def _to_bool(v):
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


def normalize_market(value: str):
    if pd.isna(value):
        return None
    cleaned = str(value).strip().replace("&AMP;", "&")
    return MARKET_MAP.get(cleaned, cleaned)


def _load_input_df() -> pd.DataFrame:
    """Read S2 output and normalize fields used by S4 consolidation logic."""
    df = read_input("dqm_dashboard_by_data_domain")
    if "runId" in df.columns:
        df["runId"] = df["runId"].astype(str).str[0:10]
    if "Market" in df.columns:
        df["Market"] = df["Market"].map(normalize_market)

    logger.info("Loaded S4 input rows: %s", len(df))
    return df


def _load_db_settings() -> Optional[object]:
    """Load PostgreSQL connection settings from Databricks secrets or config.py."""
    if dbutils is not None:
        try:
            return build_settings(
                host=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_host"),
                port=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_port"),
                dbname=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_name"),
                user=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_user"),
                password=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_password"),
                table=dbutils.secrets.get(scope=SECRET_SCOPE, key="db_table"),
            )
        except Exception as exc:
            logger.info("Falling back to config.py DB values: %s", exc)

    return build_settings(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        table=config.DB_TABLE,
    )


def _read_history(settings) -> pd.DataFrame:
    """Read historical S4 rows from PostgreSQL, returning an empty frame on failure."""
    try:
        hist = read_table(settings)
        logger.info("Loaded historical rows from PostgreSQL: %s", len(hist))
        if "runId" in hist.columns:
            hist["runId"] = hist["runId"].astype(str).str[0:10]
        return hist
    except Exception as exc:
        logger.warning("Could not read historical table, starting with empty history: %s", exc)
        return pd.DataFrame(columns=["dataset", "runId"])


def _prepare_for_insert(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column values and enforce DB column order before PostgreSQL insert."""
    insert_df = df.copy()
    if "runDate" in insert_df.columns:
        insert_df["runDate"] = (
            insert_df["runDate"]
            .astype(str)
            .str.replace(r"\+0000$", "+00:00", regex=True)
            .replace("NaT", None)
        )
    if "jobSchedule" in insert_df.columns:
        insert_df["jobSchedule"] = insert_df["jobSchedule"].map(_to_bool)

    for col in DB_COLS:
        if col not in insert_df.columns:
            insert_df[col] = None

    return insert_df[DB_COLS]


def _build_monthly_report(df_merged: pd.DataFrame, df_hist_old: pd.DataFrame) -> pd.DataFrame:
    """Build the consolidated monthly report, preferring fuller historical data for the latest period."""
    monthly = df_merged.drop(columns=["passFail", "passFailLimit", "avgRows"], errors="ignore")
    monthly = monthly.drop_duplicates(["dataset", "report_time"], keep="last")

    if df_hist_old.empty:
        return monthly

    monthly_from_hist = df_hist_old.drop(columns=["passFail", "passFailLimit", "avgRows"], errors="ignore")
    monthly_from_hist = monthly_from_hist.sort_values("runDate") if "runDate" in monthly_from_hist.columns else monthly_from_hist
    monthly_from_hist = monthly_from_hist.drop_duplicates(["dataset", "report_time"], keep="last")

    if "report_time" not in monthly.columns or monthly_from_hist.empty:
        return monthly

    latest_report_time = monthly_from_hist["report_time"].max()
    monthly_latest = monthly[monthly["report_time"] == latest_report_time]
    hist_latest = monthly_from_hist[monthly_from_hist["report_time"] == latest_report_time]

    return monthly_from_hist if len(hist_latest) > len(monthly_latest) else monthly
# ---------------------------------------------------
# Tableau Publish
# ---------------------------------------------------


def _publish_to_tableau(df: pd.DataFrame) -> None:
    cfg = {
        "url": config.TABLEAU_CLOUD_PROD_URL,
        "token_name": config.TABLEAU_CLOUD_PROD_TOKEN_NAME,
        "token_value": config.TABLEAU_CLOUD_PROD_TOKEN_VALUE,
        "site_id": config.TABLEAU_CLOUD_SITE_ID,
        "project_path": config.TABLEAU_CLOUD_PROJECT_PATH,
        "datasource_name": config.TABLEAU_CLOUD_DATASOURCE_NAME,
        "publish_mode": config.TABLEAU_CLOUD_PUBLISH_MODE,
    }

    if dbutils is not None:
        try:
            cfg = {
                "url": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_prod_url"),
                "token_name": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_prod_token_name"),
                "token_value": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_prod_token_value"),
                "site_id": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_site_id"),
                "project_path": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_project_path"),
                "datasource_name": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_datasource_name"),
                "publish_mode": dbutils.secrets.get(scope=SECRET_SCOPE, key="tableau_cloud_publish_mode"),
            }
        except Exception as exc:
            logger.info("Falling back to config.py Tableau values: %s", exc)

    tableau_cfg = [
        cfg["url"],
        cfg["token_name"],
        cfg["token_value"],
        cfg["site_id"],
        cfg["project_path"],
        cfg["datasource_name"],
    ]
    if not all(tableau_cfg):
        logger.warning("Tableau Cloud configuration is incomplete in environment/config.py - skipping publish")
        return

    logger.info(tableau_cfg)
    hyper_path = str(Path(LOCAL_OUTPUT_DIR) / f"{cfg['datasource_name']}.hyper")
    verify_ssl = config.TABLEAU_VERIFY_SSL.lower() == "true" if hasattr(config, "TABLEAU_VERIFY_SSL") else False
    try:
        datasource_id = publish_dataframe_to_tableau_cloud(
            df=df,
            datasource_name=cfg["datasource_name"],
            server_url=cfg["url"],
            token_name=cfg["token_name"],
            token_value=cfg["token_value"],
            site_id=cfg["site_id"],
            project_path=cfg["project_path"],
            hyper_file_path=hyper_path,
            publish_mode=cfg["publish_mode"] or "Overwrite",
            ssl_verify=verify_ssl,
        )
        logger.info("Published Tableau datasource id: %s", datasource_id)
    except Exception as exc:
        logger.warning("Failed to publish to Tableau Cloud: %s", exc)


def main() -> None:
    logger.info("=== S4: Consolidate output and publish to Tableau Cloud ===")

    df_s2 = _load_input_df()
    settings = _load_db_settings()

    df_hist_old = pd.DataFrame(columns=["dataset", "runId"])
    if settings:
        df_hist_old = _read_history(settings)

    df_merged = pd.concat([df_hist_old, df_s2], axis=0).drop_duplicates(
        subset=["dataset", "runId"],
        keep="last",
    )

    if settings:
        df_new = filter_new_rows_by_keys(df=df_merged, existing_df=df_hist_old, key_columns=["dataset", "runId"])
        if df_new.empty:
            logger.info("No new rows to write to PostgreSQL")
        else:
            inserted = insert_on_conflict_do_nothing(
                df=_prepare_for_insert(df_new),
                settings=settings,
                insert_columns=DB_COLS,
                conflict_columns=["dataset", "runId"],
            )
            logger.info("Attempted insert rows to PostgreSQL: %s", inserted)
    else:
        logger.warning("PostgreSQL configuration not available - skipping DB write")

    df_monthly_report = _build_monthly_report(df_merged=df_merged, df_hist_old=df_hist_old)
    write_output(df_monthly_report, OUTPUT_NAME)
    _publish_to_tableau(df_monthly_report)


if __name__ == "__main__":
    main()
