"""
Tests for alpha_find_v2.data_ingest.schemas

Verification goals (per task 1 spec):
- All 18 dataset ids present in RAW_TABLE_DDL
- Each DDL parses cleanly in an in-memory DuckDB
- Every primary-key column exists in its table DDL
- DATASET_PRIMARY_KEYS and DATASET_INCREMENTAL_AXIS cover all 18 ids
- Every raw table has ingested_at and source_table columns
- META_DDL parses cleanly
"""
from __future__ import annotations

import duckdb
import pytest

from alpha_find_v2.data_ingest.schemas import (
    DATASET_INCREMENTAL_AXIS,
    DATASET_PRIMARY_KEYS,
    META_DDL,
    RAW_TABLE_DDL,
)

EXPECTED_DATASETS = frozenset({
    "stock_basic",
    "trade_cal",
    "namechange",
    "daily",
    "daily_basic",
    "adj_factor",
    "daily_qfq",
    "suspend_d",
    "stk_limit",
    "index_daily",
    "index_weight",
    "index_member_all",
    "fina_indicator",
    "income",
    "balancesheet",
    "cashflow",
    "forecast",
    "express",
})


def _all_table_columns(conn: duckdb.DuckDBPyConnection) -> dict[str, set[str]]:
    """Return {table_name: {column_name, ...}} for all tables in conn."""
    rows = conn.execute(
        "SELECT table_name, column_name FROM information_schema.columns"
    ).fetchall()
    result: dict[str, set[str]] = {}
    for table_name, col in rows:
        result.setdefault(table_name, set()).add(col)
    return result


def _table_name_from_ddl(ddl: str) -> str:
    """Extract the table name from a CREATE TABLE IF NOT EXISTS ... DDL."""
    # Pattern: ... EXISTS <table_name> (
    after_exists = ddl.split("EXISTS")[1]
    return after_exists.split("(")[0].strip()


# ---------------------------------------------------------------------------
# Coverage checks
# ---------------------------------------------------------------------------

def test_all_18_dataset_ids_present() -> None:
    assert set(RAW_TABLE_DDL.keys()) == EXPECTED_DATASETS


def test_primary_keys_covers_all_datasets() -> None:
    assert set(DATASET_PRIMARY_KEYS.keys()) == EXPECTED_DATASETS


def test_incremental_axis_covers_all_datasets() -> None:
    assert set(DATASET_INCREMENTAL_AXIS.keys()) == EXPECTED_DATASETS


def test_incremental_axis_values_are_valid() -> None:
    valid = {"trade_date", "period_end", "static"}
    for dataset_id, axis in DATASET_INCREMENTAL_AXIS.items():
        assert axis in valid, f"dataset {dataset_id!r} has invalid axis {axis!r}"


# ---------------------------------------------------------------------------
# DDL parse checks
# ---------------------------------------------------------------------------

def test_all_raw_table_ddl_parse_cleanly() -> None:
    """Every DDL in RAW_TABLE_DDL executes without error in an in-memory DuckDB."""
    conn = duckdb.connect()
    for dataset_id, ddl in RAW_TABLE_DDL.items():
        try:
            conn.execute(ddl)
        except Exception as exc:
            pytest.fail(f"DDL for {dataset_id!r} failed to parse: {exc}")
    conn.close()


def test_meta_ddl_parses_cleanly() -> None:
    """META_DDL schema and table DDLs execute without error."""
    conn = duckdb.connect()
    for key, ddl in META_DDL.items():
        try:
            conn.execute(ddl)
        except Exception as exc:
            pytest.fail(f"META_DDL[{key!r}] failed to parse: {exc}")
    conn.close()


# ---------------------------------------------------------------------------
# Structural checks
# ---------------------------------------------------------------------------

def test_every_primary_key_column_exists_in_its_table_ddl() -> None:
    """Exhaustive: for every dataset, every PK column must exist in the DDL."""
    conn = duckdb.connect()
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)

    table_columns = _all_table_columns(conn)
    conn.close()

    missing: list[str] = []
    for dataset_id, pk_cols in DATASET_PRIMARY_KEYS.items():
        ddl = RAW_TABLE_DDL[dataset_id]
        table_name = _table_name_from_ddl(ddl)
        cols = table_columns.get(table_name, set())
        for pk_col in pk_cols:
            if pk_col not in cols:
                missing.append(
                    f"dataset={dataset_id!r} table={table_name!r} missing PK column {pk_col!r}"
                )

    assert not missing, "Primary key columns absent from DDL:\n" + "\n".join(missing)


def test_all_raw_tables_have_ingested_at_and_source_table() -> None:
    """Every table in RAW_TABLE_DDL must have ingested_at TIMESTAMP and source_table VARCHAR."""
    conn = duckdb.connect()
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)

    table_columns = _all_table_columns(conn)
    conn.close()

    missing_ingested_at: list[str] = []
    missing_source_table: list[str] = []

    for dataset_id, ddl in RAW_TABLE_DDL.items():
        table_name = _table_name_from_ddl(ddl)
        cols = table_columns.get(table_name, set())
        if "ingested_at" not in cols:
            missing_ingested_at.append(f"{dataset_id!r} ({table_name!r})")
        if "source_table" not in cols:
            missing_source_table.append(f"{dataset_id!r} ({table_name!r})")

    errors: list[str] = []
    if missing_ingested_at:
        errors.append("Missing ingested_at: " + ", ".join(missing_ingested_at))
    if missing_source_table:
        errors.append("Missing source_table: " + ", ".join(missing_source_table))

    assert not errors, "\n".join(errors)


def test_meta_dataset_sync_state_has_required_columns() -> None:
    """meta.dataset_sync_state must have all columns from design §2.4."""
    required = {
        "dataset_id", "adapter", "last_trade_date", "last_period_end",
        "last_run_at", "last_status", "last_row_count", "error_message",
        "schema_version",
    }
    conn = duckdb.connect()
    for ddl in META_DDL.values():
        conn.execute(ddl)

    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'dataset_sync_state'"
    ).fetchall()
    conn.close()

    actual = {r[0] for r in rows}
    assert required <= actual, f"meta.dataset_sync_state missing columns: {required - actual}"
