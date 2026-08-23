from __future__ import annotations

import platform
import sys
from pathlib import Path
import logging
from typing import Optional

import pandas as pd


logger = logging.getLogger(__name__)


def _sanitize_for_hyper(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame dtypes so PyArrow/pantab can serialize every column cleanly."""
    df = df.copy()
    for col in df.columns:
        col_dtype = df[col].dtype
        if hasattr(col_dtype, "tz") and col_dtype.tz is not None:
            # Strip timezone so pantab maps to a plain Tableau DATETIME
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
        elif col_dtype == object:
            # Cast mixed-type object columns to uniform strings; keep NaN as None
            df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    return df


def dataframe_to_hyper(
    df: pd.DataFrame,
    hyper_file_path: str,
    table_name: str = "Extract",
) -> Path:
    """Write a DataFrame to a Hyper extract file with pantab."""
    try:
        import pantab
    except ImportError as e:
        raise RuntimeError(
            f"pantab is not installed or not compatible with this environment: {e}. "
            "Cannot write Hyper extract files without pantab."
        ) from e

    path = Path(hyper_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pantab.frame_to_hyper(_sanitize_for_hyper(df), str(path), table=table_name)
    return path


def dataframe_to_hyper_arm64(
    df: pd.DataFrame,
    hyper_file_path: str,
    table_name: str = "Extract",
) -> Path:
    """Write a DataFrame to a Hyper extract file using tableauhyperapi (Linux ARM64 / aarch64)."""
    try:
        from tableauhyperapi import (
            HyperProcess, Telemetry, Connection, CreateMode,
            TableDefinition, SqlType, TableName, Inserter,
        )
    except ImportError as e:
        raise RuntimeError(
            f"tableauhyperapi is not installed: {e}. "
            "Install it with: pip install tableauhyperapi"
        ) from e

    def _pandas_dtype_to_sql_type(dtype) -> SqlType:
        kind = dtype.kind  # 'i'=int, 'u'=uint, 'f'=float, 'b'=bool, 'M'=datetime, 'U'/'O'=object
        if kind in ("i", "u"):
            return SqlType.big_int()
        if kind == "f":
            return SqlType.double()
        if kind == "b":
            return SqlType.bool()
        if kind == "M":
            return SqlType.timestamp()
        return SqlType.text()

    df = _sanitize_for_hyper(df)
    path = Path(hyper_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        TableDefinition.Column(col, _pandas_dtype_to_sql_type(df[col].dtype))
        for col in df.columns
    ]
    table_def = TableDefinition(TableName("Extract", table_name), columns)

    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(hyper.endpoint, str(path), CreateMode.CREATE_AND_REPLACE) as conn:
            conn.catalog.create_schema_if_not_exists("Extract")
            conn.catalog.create_table_if_not_exists(table_def)
            with Inserter(conn, table_def) as inserter:
                # Convert row values: NaN → None, everything else to native Python
                for row in df.itertuples(index=False, name=None):
                    inserter.add_row(
                        [None if (v != v) else v for v in row]  # NaN check via v != v
                    )
                inserter.execute()
    return path


def _dataframe_to_hyper_auto(
    df: pd.DataFrame,
    hyper_file_path: str,
    table_name: str = "Extract",
) -> Path:
    """Auto-dispatch: uses pantab on macOS/x86, tableauhyperapi on Linux ARM64."""
    is_linux_arm = sys.platform == "linux" and platform.machine() in ("aarch64", "arm64")
    if is_linux_arm:
        logger.debug("Detected Linux ARM64 — using tableauhyperapi backend")
        return dataframe_to_hyper_arm64(df, hyper_file_path, table_name)
    return dataframe_to_hyper(df, hyper_file_path, table_name)


def get_project_id_by_path(server, project_path: str) -> str:
    """Resolve a nested Tableau project path (for example Parent/Child/Subchild) to a project id."""
    import tableauserverclient as TSC

    parts = [p.strip() for p in project_path.split("/") if p.strip()]
    if not parts:
        raise ValueError("TABLEAU project path is empty")

    all_projects = list(TSC.Pager(server.projects))
    current_parent_id = None

    for part in parts:
        matched = next(
            (p for p in all_projects if p.name == part and p.parent_id == current_parent_id),
            None,
        )
        if matched is None:
            raise ValueError(f"Could not resolve project path '{project_path}' at segment '{part}'")
        current_parent_id = matched.id

    return current_parent_id


def publish_hyper_to_tableau_cloud(
    hyper_file_path: str,
    datasource_name: str,
    server_url: str,
    token_name: str,
    token_value: str,
    site_id: str,
    project_path: str,
    publish_mode: str = "Overwrite",
    ssl_verify: bool | str = False,
) -> str:
    """Publish a Hyper extract file to Tableau Cloud under a nested project path."""
    import tableauserverclient as TSC

    mode_attr = publish_mode.capitalize()
    if not hasattr(TSC.Server.PublishMode, mode_attr):
        raise ValueError("publish_mode must be CreateNew, Overwrite, or Append")

    auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site_id)
    server = TSC.Server(server_url, use_server_version=True)
    server.add_http_options({"verify": ssl_verify})

    with server.auth.sign_in(auth):
        project_id = get_project_id_by_path(server, project_path)
        datasource = TSC.DatasourceItem(project_id=project_id, name=datasource_name)
        published = server.datasources.publish(
            datasource,
            Path(hyper_file_path),
            getattr(TSC.Server.PublishMode, mode_attr),
        )
        logger.info("Published datasource '%s' to project '%s'", datasource_name, project_path)
        return published.id


def publish_dataframe_to_tableau_cloud(
    df: pd.DataFrame,
    datasource_name: str,
    server_url: str,
    token_name: str,
    token_value: str,
    site_id: str,
    project_path: str,
    hyper_file_path: str,
    publish_mode: str = "Overwrite",
    table_name: str = "Extract",
    ssl_verify: bool | str = False,
) -> str:
    """Create a Hyper extract from a DataFrame and publish it to Tableau Cloud."""
    _dataframe_to_hyper_auto(df=df, hyper_file_path=hyper_file_path, table_name=table_name)
    return publish_hyper_to_tableau_cloud(
        hyper_file_path=hyper_file_path,
        datasource_name=datasource_name,
        server_url=server_url,
        token_name=token_name,
        token_value=token_value,
        ssl_verify=ssl_verify,
        site_id=site_id,
        project_path=project_path,
        publish_mode=publish_mode,
    )
