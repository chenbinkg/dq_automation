"""
Reusable PostgreSQL read/write helpers for pipeline scripts.
run python postgres_io.py to ensure all managed tables exist.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence
import json
import logging
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Managed PostgreSQL tables
# ---------------------------------------------------------------------------

DQM_DASHBOARD_HISTORY_APAC_TABLE = "public.dqm_dashboard_history_apac"
DQM_DASHBOARD_GLOBAL_POSITION_TABLE = "public.dqm_dashboard_global_position"
DQM_BUSINESS_UNIT_MAPPING_TABLE = "public.dqm_business_unit_mapping"
DQM_DATASET_DEFINITIONS_TABLE = "public.dqm_dataset_definitions"
DQM_DATASET_CUSTOM_RULE_DETAILS_TABLE = (
    "public.dqm_dataset_custom_rule_details"
)


CREATE_DQM_DASHBOARD_HISTORY_APAC_SQL = """
CREATE TABLE IF NOT EXISTS public.dqm_dashboard_history_apac (
    "dataset" text NOT NULL,
    "runId" timestamp NOT NULL,
    "runDate" timestamptz,
    "rows" bigint,
    "passFail" integer,
    "passFailLimit" integer,
    "peak" integer,
    "dayOfWeek" text,
    "timeZone" text,
    "avgRows" bigint,
    "cols" integer,
    "activeRules" integer,
    "activeAlerts" integer,
    "runTime" text,
    "shapeScore" integer,
    "dupeScore" integer,
    "patternScore" integer,
    "outlierScore" integer,
    "schemaScore" integer,
    "recordScore" integer,
    "ruleScore" integer,
    "sourceScore" integer,
    "behaviorScore" integer,
    "DQScore" integer,
    "jobSchedule" boolean,
    "business_unit" text,
    "Comment" text,
    "Data Domain" text,
    "CDE" text,
    "Project" text,
    "Market" text,
    "report_time" text,

    CONSTRAINT dqm_dashboard_history_apac_pk
        PRIMARY KEY ("dataset", "runId")
)
"""


CREATE_DQM_DASHBOARD_GLOBAL_POSITION_SQL = """
CREATE TABLE IF NOT EXISTS public.dqm_dashboard_global_position (
    "sector" varchar(100) PRIMARY KEY,
    "dq_capability_material_product" decimal(10,2),
    "dq_rules_score_dashboard_material_product" decimal(10,2),
    "dq_capability_hcp" decimal(10,2),
    "dq_rules_score_dashboard_hcp" decimal(10,2),
    "dq_capability_customer_account" decimal(10,2),
    "dq_rules_score_dashboard_customer_account" decimal(10,2),
    "dq_capability" decimal(10,2),
    "dq_rules_score_dashboard" decimal(10,2)
)
"""


CREATE_DQM_BUSINESS_UNIT_MAPPING_SQL = """
CREATE TABLE IF NOT EXISTS public.dqm_business_unit_mapping (
    "dataset" varchar(255) PRIMARY KEY,
    "business_unit" varchar(100),
    "Market" varchar(20),
    "Project" varchar(100),
    "CDE" varchar(10),
    "jobSchedule" boolean,
    "Data Domain" varchar(100),
    "subDomain" varchar(255),
    "connectionName" varchar(100),
    "db_nm" varchar(50),
    "table_nm" varchar(255),
    "scheduleTime" time,
    "timeZone" varchar(100)
)
"""


CREATE_DQM_DATASET_DEFINITIONS_SQL = """
CREATE TABLE IF NOT EXISTS public.dqm_dataset_definitions (
    "dataset" varchar(255) NOT NULL,
    "Run Id" timestamptz,
    "Link Id" text,
    "Date Filter" boolean,
    "Date Filter Key" varchar(255),
    "Scheduler" boolean,
    "Scheduled Freq" varchar(50),
    "Scheduled Time" time,
    "col_name" varchar(255) NOT NULL,
    "Data Type" varchar(100),
    "Row Count" boolean,
    "Execution Time" boolean,
    "Data Type Check" boolean,
    "Schema Change" boolean,
    "Dupes" boolean,
    "Custom Rules" boolean,
    "Null Values" boolean,
    "Empty Fields" boolean,
    "Uniqueness" boolean,
    "Min" boolean,
    "Max" boolean,
    "Mean" boolean,
    "Outliers" boolean,
    "Shapes" boolean,
    "Patterns" text,
    "Current Null Pct" numeric(18,6),
    "db_nm" varchar(100),
    "table_nm" varchar(255),
    "business_unit" varchar(255),
    "Market" varchar(50),
    "Project" varchar(255),
    "CDE" boolean,

    CONSTRAINT dqm_dataset_definitions_pk
        PRIMARY KEY ("dataset", "col_name")
)
"""


CREATE_DQM_DATASET_CUSTOM_RULE_DETAILS_SQL = """
CREATE TABLE IF NOT EXISTS public.dqm_dataset_custom_rule_details (
    "dataset" varchar(255) NOT NULL,
    "runId" timestamptz NOT NULL,
    "ruleNm" varchar(500) NOT NULL,

    "ruleType" varchar(50),
    "score" numeric(18,6),
    "perc" numeric(18,6),

    "exception" text,
    "owlId" bigint,
    "breakMsg" varchar(100),
    "assignmentId" jsonb,

    "dimId" numeric(18,6),
    "dimName" varchar(100),

    "stale" boolean,

    "business_unit" varchar(255),
    "Market" varchar(50),
    "Project" varchar(255),
    "CDE" boolean,

    "jobSchedule" boolean,
    "Data Domain" varchar(255),
    "subDomain" varchar(255),
    "connectionName" varchar(255),

    "db_nm" varchar(100),
    "table_nm" varchar(255),

    "scheduleTime" time,
    "timeZone" varchar(100),

    "created_ts" timestamp DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT dqm_dataset_custom_rule_details_pk
        PRIMARY KEY ("dataset", "runId", "ruleNm")
)
"""


CREATE_TABLE_STATEMENTS = (
    CREATE_DQM_DASHBOARD_HISTORY_APAC_SQL,
    CREATE_DQM_DASHBOARD_GLOBAL_POSITION_SQL,
    CREATE_DQM_BUSINESS_UNIT_MAPPING_SQL,
    CREATE_DQM_DATASET_DEFINITIONS_SQL,
    CREATE_DQM_DATASET_CUSTOM_RULE_DETAILS_SQL,
)


GLOBAL_POSITION_COLUMNS = (
    "sector",
    "dq_capability_material_product",
    "dq_rules_score_dashboard_material_product",
    "dq_capability_hcp",
    "dq_rules_score_dashboard_hcp",
    "dq_capability_customer_account",
    "dq_rules_score_dashboard_customer_account",
    "dq_capability",
    "dq_rules_score_dashboard",
)


GLOBAL_POSITION_ROWS = (
    ("MT Comm. APAC", 8, 8, 10, 15, 8, 8, 8.66666667, 10.33333333),
    ("MT Comm. Vision", 32, 5, None, None, 32, 5, 32, 5),
    ("MT Comm. LATAM", None, None, None, None, 43, 5, 43, 5),
    ("MT R&D Global", 47, 16, None, None, None, None, 47, 16),
    ("MT Comm. EMEA", 60, 18, 65, 15, 67, 17, 64, 16.66666667),
    ("MT Comm. NA", 68, 58, 72, 65, 68, 62, 69.33333333, 61.66666667),
    ("MT SC", 37, 50, None, None, 25, 15, 31, 32.5),
    ("IM Comm. LATAM", None, None, None, None, 55, 17, 55, 17),
    ("IM Comm. NA", 70, 50, 75, 50, 70, 50, 71.66666667, 50),
    ("IM Comm. EMEA", 55, 50, 52, 32, 55, 50, 54, 44),
    ("IM Comm. APAC", 70, 100, 65, 100, 57, 100, 64, 100),
    ("GSCS", 80, 100, None, None, 82, 100, 81, 100),
)


def settings_from_environment(
    table: str = DQM_DASHBOARD_HISTORY_APAC_TABLE,
) -> PostgresSettings:
    """
    Build PostgreSQL settings from environment variables.

    Required:
        DB_HOST
        DB_NAME
        DB_USER
        DB_PASSWORD

    Optional:
        DB_PORT, defaults to 5432
        DB_SSLMODE, defaults to require
    """
    settings = build_settings(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        table=table,
        sslmode=os.getenv("DB_SSLMODE", "require"),
    )

    if settings is None:
        required_variables = (
            "DB_HOST",
            "DB_NAME",
            "DB_USER",
            "DB_PASSWORD",
        )
        missing_variables = [
            variable
            for variable in required_variables
            if not os.getenv(variable)
        ]

        raise ValueError(
            "PostgreSQL settings are incomplete. Missing environment "
            f"variables: {', '.join(missing_variables)}"
        )

    return settings


def _seed_global_position(cur) -> None:
    """
    Insert or refresh the predefined global-position records.

    Existing records are updated using sector as the conflict key.
    """
    quoted_columns = ", ".join(
        f'"{column}"' for column in GLOBAL_POSITION_COLUMNS
    )

    update_columns = GLOBAL_POSITION_COLUMNS[1:]
    update_clause = ", ".join(
        f'"{column}" = EXCLUDED."{column}"'
        for column in update_columns
    )

    sql = f"""
        INSERT INTO {DQM_DASHBOARD_GLOBAL_POSITION_TABLE}
            ({quoted_columns})
        VALUES %s
        ON CONFLICT ("sector") DO UPDATE SET
            {update_clause}
    """

    extras.execute_values(
        cur,
        sql,
        GLOBAL_POSITION_ROWS,
        page_size=len(GLOBAL_POSITION_ROWS),
    )


def ensure_tables(
    settings: PostgresSettings | None = None,
    seed_global_position: bool = True,
) -> None:
    """
    Create all managed DQM tables if they do not already exist.

    Optionally insert or refresh the predefined global-position rows.
    All operations are performed in one transaction.
    """
    if settings is None:
        settings = settings_from_environment()

    logger.info(
        "Ensuring %d DQM PostgreSQL tables exist.",
        len(CREATE_TABLE_STATEMENTS),
    )

    with connect(settings) as conn:
        with conn.cursor() as cur:
            for create_statement in CREATE_TABLE_STATEMENTS:
                cur.execute(create_statement)

            if seed_global_position:
                _seed_global_position(cur)

        # psycopg2's connection context normally commits automatically,
        # but keeping this explicit makes the intended behaviour clear.
        conn.commit()

    logger.info(
        "Ensured the following tables exist: %s",
        ", ".join(
            [
                DQM_DASHBOARD_HISTORY_APAC_TABLE,
                DQM_DASHBOARD_GLOBAL_POSITION_TABLE,
                DQM_BUSINESS_UNIT_MAPPING_TABLE,
                DQM_DATASET_DEFINITIONS_TABLE,
                DQM_DATASET_CUSTOM_RULE_DETAILS_TABLE,
            ]
        ),
    )


def _clean_value(v: Any) -> Any:
    """
    Normalize a cell value for psycopg2: JSON-encode list/dict/tuple values
    (e.g. nested CDQ type definitions), and convert NaN/NaT to None. pd.isna()
    raises on array-like values, so those are handled before calling it.
    """
    if isinstance(v, (list, dict, tuple)):
        return json.dumps(v)
    try:
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return v


@dataclass(frozen=True)
class PostgresSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    table: str
    sslmode: str = "require"


def build_settings(
    host: str | None,
    port: int | str | None,
    dbname: str | None,
    user: str | None,
    password: str | None,
    table: str | None,
    sslmode: str = "require",
) -> PostgresSettings | None:
    """Return settings when all required DB credentials are present."""
    if not all([host, dbname, user, password, table]):
        return None
    return PostgresSettings(
        host=str(host),
        port=int(port or 5432),
        dbname=str(dbname),
        user=str(user),
        password=str(password),
        table=str(table),
        sslmode=sslmode,
    )


def load_db_credentials(dbutils, secret_scope: str, config_module) -> dict:
    """
    Load host/port/dbname/user/password for PostgreSQL from Databricks secrets
    (scope=secret_scope), falling back to config_module.DB_* attributes.
    """
    def _get(key: str, default):
        if dbutils is None:
            return default
        try:
            return dbutils.secrets.get(scope=secret_scope, key=key)
        except Exception:
            return default

    return {
        "host": _get("db_host", getattr(config_module, "DB_HOST", None)),
        "port": _get("db_port", getattr(config_module, "DB_PORT", None)),
        "dbname": _get("db_name", getattr(config_module, "DB_NAME", None)),
        "user": _get("db_user", getattr(config_module, "DB_USER", None)),
        "password": _get("db_password", getattr(config_module, "DB_PASSWORD", None)),
    }


def settings_for_table(
    db_credentials: dict,
    table: str | None,
    sslmode: str = "require",
) -> PostgresSettings | None:
    """Build PostgresSettings for a specific table using shared connection credentials."""
    return build_settings(
        host=db_credentials.get("host"),
        port=db_credentials.get("port"),
        dbname=db_credentials.get("dbname"),
        user=db_credentials.get("user"),
        password=db_credentials.get("password"),
        table=table,
        sslmode=sslmode,
    )


def connect(settings: PostgresSettings):
    """Create a psycopg2 connection using shared settings."""
    return psycopg2.connect(
        host=settings.host,
        port=settings.port,
        dbname=settings.dbname,
        user=settings.user,
        password=settings.password,
        sslmode=settings.sslmode,
    )


def read_sql(query: str, settings: PostgresSettings) -> pd.DataFrame:
    """Read query results into a DataFrame."""
    with connect(settings) as conn:
        return pd.read_sql(query, conn)


def read_table(settings: PostgresSettings, columns: Sequence[str] | None = None) -> pd.DataFrame:
    """Read a whole table (or selected columns) from PostgreSQL."""
    if columns:
        quoted_cols = ", ".join([f'"{c}"' for c in columns])
    else:
        quoted_cols = "*"
    query = f"SELECT {quoted_cols} FROM {settings.table}"
    return read_sql(query, settings)


def insert_on_conflict_do_nothing(
    df: pd.DataFrame,
    settings: PostgresSettings,
    insert_columns: Sequence[str],
    conflict_columns: Sequence[str],
    page_size: int = 500,
) -> int:
    """
    Bulk insert rows using execute_values and ON CONFLICT DO NOTHING.
    Returns inserted row count attempted.
    """
    if df.empty:
        return 0

    for col in insert_columns:
        if col not in df.columns:
            df[col] = None

    df_to_insert = df[list(insert_columns)]
    rows = [tuple(_clean_value(v) for v in r) for r in df_to_insert.to_numpy()]

    quoted_insert_cols = ",".join([f'"{c}"' for c in insert_columns])
    quoted_conflict_cols = ",".join([f'"{c}"' for c in conflict_columns])

    sql = f"""
        INSERT INTO {settings.table} ({quoted_insert_cols})
        VALUES %s
        ON CONFLICT ({quoted_conflict_cols}) DO NOTHING
    """

    with connect(settings) as conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, sql, rows, page_size=page_size)

    return len(rows)


def filter_new_rows_by_keys(
    df: pd.DataFrame,
    existing_df: pd.DataFrame,
    key_columns: Iterable[str],
) -> pd.DataFrame:
    """Return rows from df whose key tuple does not exist in existing_df."""
    key_columns = list(key_columns)
    if df.empty:
        return df.copy()
    if existing_df.empty:
        return df.copy()

    df_idx = df.set_index(key_columns).index
    existing_idx = existing_df.set_index(key_columns).index
    return df[~df_idx.isin(existing_idx)].copy()


def upsert_dataframe(
    df: pd.DataFrame,
    settings: PostgresSettings,
    key_columns: Sequence[str],
    all_columns: Sequence[str],
    change_detect_columns: Sequence[str] | None = None,
    page_size: int = 500,
) -> int:
    """
    Insert new rows and update existing rows (ON CONFLICT DO UPDATE) keyed on
    key_columns. If change_detect_columns is given, the UPDATE only fires when
    at least one of those columns differs from the stored row (columns not in
    change_detect_columns, e.g. a "latest run id", are still always refreshed
    whenever an update fires).
    """
    if df.empty:
        return 0

    df = df.copy()
    for col in all_columns:
        if col not in df.columns:
            df[col] = None
    df_to_write = df[list(all_columns)]
    rows = [tuple(_clean_value(v) for v in r) for r in df_to_write.to_numpy()]

    quoted_all = ",".join(f'"{c}"' for c in all_columns)
    quoted_keys = ",".join(f'"{c}"' for c in key_columns)
    update_cols = [c for c in all_columns if c not in key_columns]

    if not update_cols:
        return insert_on_conflict_do_nothing(df, settings, all_columns, key_columns, page_size)

    set_clause = ",".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

    where_sql = ""
    if change_detect_columns:
        conditions = [
            f'{settings.table}."{c}" IS DISTINCT FROM EXCLUDED."{c}"'
            for c in change_detect_columns
        ]
        where_sql = f"WHERE {' OR '.join(conditions)}"

    sql = f"""
        INSERT INTO {settings.table} ({quoted_all})
        VALUES %s
        ON CONFLICT ({quoted_keys}) DO UPDATE SET {set_clause}
        {where_sql}
    """

    with connect(settings) as conn:
        with conn.cursor() as cur:
            extras.execute_values(cur, sql, rows, page_size=page_size)

    return len(rows)


def delete_rows_not_in_keys(
    df: pd.DataFrame,
    settings: PostgresSettings,
    group_columns: Sequence[str],
    key_columns: Sequence[str],
) -> int:
    """
    For each distinct group (e.g. dataset) present in df, delete DB rows in
    that group whose full key tuple is no longer present in df. Used to prune
    rows (e.g. columns) that were removed from a dataset's latest definition.
    """
    if df.empty:
        return 0

    non_group_keys = [c for c in key_columns if c not in group_columns]
    if not non_group_keys:
        return 0

    group_where = " AND ".join(f'"{c}" = %s' for c in group_columns)
    deleted = 0

    with connect(settings) as conn:
        with conn.cursor() as cur:
            for group_values, grp_df in df.groupby(list(group_columns)):
                if not isinstance(group_values, tuple):
                    group_values = (group_values,)

                if len(non_group_keys) == 1:
                    col = non_group_keys[0]
                    flat_keys = tuple(grp_df[col].dropna().tolist())
                    if not flat_keys:
                        continue
                    sql = f'DELETE FROM {settings.table} WHERE {group_where} AND "{col}" NOT IN %s'
                    cur.execute(sql, (*group_values, flat_keys))
                else:
                    key_tuples = tuple(tuple(r) for r in grp_df[non_group_keys].to_numpy())
                    if not key_tuples:
                        continue
                    key_cols_sql = ",".join(f'"{c}"' for c in non_group_keys)
                    sql = (
                        f"DELETE FROM {settings.table} WHERE {group_where} "
                        f"AND ({key_cols_sql}) NOT IN %s"
                    )
                    cur.execute(sql, (*group_values, key_tuples))
                deleted += cur.rowcount

    return deleted

if __name__ == "__main__":
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    ensure_tables()
