"""
Reusable PostgreSQL read/write helpers for pipeline scripts.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence
import logging

import pandas as pd
import psycopg2
import psycopg2.extras as extras


logger = logging.getLogger(__name__)


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

    df_to_insert = df[list(insert_columns)].copy()
    rows = [tuple(r) for r in df_to_insert.to_numpy()]

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
