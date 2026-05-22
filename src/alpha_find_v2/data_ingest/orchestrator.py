"""
Sync orchestrator for the data-ingest pipeline.

Responsibilities:
- Maintain meta.dataset_sync_state in raw.duckdb
- Dispatch fetch calls to adapters in priority order (with fallback)
- Retry transient errors with exponential backoff
- Write rows in transactional DELETE+INSERT batches
- Support incremental (since/until) and full (--reset) modes
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

from .adapters.base import (
    AdapterPermissionError,
    AdapterRateLimitError,
    AdapterUnavailable,
)
from .config_models import DataSourcesConfig
from .schemas import (
    ALL_DATASET_IDS,
    DATASET_INCREMENTAL_AXIS,
    DATASET_PRIMARY_KEYS,
    DATASET_TABLE_NAME,
    META_DDL,
    RAW_TABLE_DDL,
)

logger = logging.getLogger(__name__)

_DEFAULT_MAX_RETRIES = 3
_DEFAULT_RETRY_BASE_DELAY = 2.0

SyncStatus = Literal[
    "success", "partial", "failed", "permission_denied", "deferred", "skipped"
]


# ---------------------------------------------------------------------------
# Public result types
# ---------------------------------------------------------------------------


@dataclass
class DatasetSyncState:
    dataset_id: str
    adapter: str
    last_trade_date: str | None
    last_period_end: str | None
    last_run_at: datetime
    last_status: str   # 'success'|'partial'|'failed'|'permission_denied'|'deferred'|'skipped'
    last_row_count: int
    error_message: str | None
    schema_version: int = 1


@dataclass
class DatasetSyncResult:
    dataset_id: str
    adapter: str
    rows_added: int
    duration_seconds: float
    status: SyncStatus
    error_message: str | None


@dataclass
class SyncReport:
    raw_db_path: str
    started_at: str
    finished_at: str
    results: list[DatasetSyncResult]

    def success_count(self) -> int:
        return sum(1 for r in self.results if r.status == "success")

    def failed_count(self) -> int:
        return sum(1 for r in self.results if r.status in ("failed", "partial"))


# ---------------------------------------------------------------------------
# Sync state I/O  (Task 9)
# ---------------------------------------------------------------------------


def _ensure_meta_schema(conn: Any) -> None:
    """Create meta schema and dataset_sync_state table if absent."""
    conn.execute(META_DDL["meta_schema"])
    conn.execute(META_DDL["dataset_sync_state"])


def _load_state(conn: Any) -> dict[str, DatasetSyncState]:
    """Load all rows from meta.dataset_sync_state into a dict keyed by dataset_id."""
    rows = conn.execute(
        """
        SELECT dataset_id, adapter, last_trade_date, last_period_end,
               last_run_at, last_status, last_row_count, error_message, schema_version
        FROM meta.dataset_sync_state
        """
    ).fetchall()
    result: dict[str, DatasetSyncState] = {}
    for row in rows:
        (
            dataset_id, adapter, last_trade_date, last_period_end,
            last_run_at, last_status, last_row_count, error_message, schema_version,
        ) = row
        result[dataset_id] = DatasetSyncState(
            dataset_id=dataset_id,
            adapter=adapter or "",
            last_trade_date=last_trade_date,
            last_period_end=last_period_end,
            last_run_at=last_run_at if isinstance(last_run_at, datetime) else datetime.now(UTC),
            last_status=last_status or "",
            last_row_count=last_row_count or 0,
            error_message=error_message,
            schema_version=schema_version or 1,
        )
    return result


def _record_state(conn: Any, state: DatasetSyncState) -> None:
    """Upsert a single DatasetSyncState into meta.dataset_sync_state."""
    conn.execute("DELETE FROM meta.dataset_sync_state WHERE dataset_id = ?", [state.dataset_id])
    conn.execute(
        """
        INSERT INTO meta.dataset_sync_state
            (dataset_id, adapter, last_trade_date, last_period_end,
             last_run_at, last_status, last_row_count, error_message, schema_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            state.dataset_id, state.adapter, state.last_trade_date, state.last_period_end,
            state.last_run_at, state.last_status, state.last_row_count,
            state.error_message, state.schema_version,
        ],
    )


# ---------------------------------------------------------------------------
# Retry helper  (Task 10)
# ---------------------------------------------------------------------------


def _with_retries(
    call: Any,
    *,
    max_attempts: int = _DEFAULT_MAX_RETRIES,
    base_delay: float = _DEFAULT_RETRY_BASE_DELAY,
    _sleep: Any = None,
) -> Any:
    """
    Execute call(), retrying on AdapterRateLimitError and OSError only.
    AdapterPermissionError is NEVER retried.
    _sleep is injectable for tests; defaults to time.sleep.
    """
    sleep_fn = _sleep if _sleep is not None else time.sleep
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return call()
        except AdapterPermissionError:
            raise
        except (AdapterRateLimitError, OSError) as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(
                "Transient error attempt %d/%d: %s — retrying in %.1fs",
                attempt + 1, max_attempts, exc, delay,
            )
            sleep_fn(delay)
        except Exception:
            raise
    raise last_exc  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Transactional batch writer  (Task 10)
# ---------------------------------------------------------------------------


def _write_dataset(conn: Any, dataset_id: str, rows: list[dict[str, Any]]) -> int:
    """
    Write rows to the target table using transactional DELETE+INSERT per design §3.2.
    Clears existing rows whose primary key tuple matches any row in the batch,
    then inserts all rows. Returns count of inserted rows.
    """
    if not rows:
        return 0

    table_name = DATASET_TABLE_NAME[dataset_id]
    pks = DATASET_PRIMARY_KEYS.get(dataset_id, ())
    columns = list(rows[0].keys())
    staging_table = f"_staging_{table_name}"

    conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
    # Create temp staging table matching target schema
    conn.execute(f"CREATE TEMP TABLE {staging_table} AS SELECT * FROM {table_name} WHERE 1=0")

    placeholders = ", ".join(["?" for _ in columns])
    col_list = ", ".join(columns)
    rows_values = [[row.get(col) for col in columns] for row in rows]
    conn.executemany(
        f"INSERT INTO {staging_table} ({col_list}) VALUES ({placeholders})",
        rows_values,
    )

    # DELETE rows in the target whose PK matches anything in the staging table
    if pks:
        pk_conditions = " AND ".join([f"t.{pk} = s.{pk}" for pk in pks])
        conn.execute(
            f"DELETE FROM {table_name} AS t "
            f"WHERE EXISTS (SELECT 1 FROM {staging_table} AS s WHERE {pk_conditions})"
        )

    conn.execute(f"INSERT INTO {table_name} SELECT * FROM {staging_table}")
    conn.execute(f"DROP TABLE IF EXISTS {staging_table}")
    return len(rows)


# ---------------------------------------------------------------------------
# Incremental helpers  (Task 11)
# ---------------------------------------------------------------------------


def _next_day(date_str: str) -> str:
    """Increment an 8-char YYYYMMDD date string by one day."""
    dt = datetime.strptime(date_str, "%Y%m%d").date()
    return (dt + timedelta(days=1)).strftime("%Y%m%d")


def _effective_since(
    dataset_id: str,
    state: DatasetSyncState | None,
    cli_since: str | None,
) -> str | None:
    """Return the effective start date for an incremental fetch."""
    axis = DATASET_INCREMENTAL_AXIS.get(dataset_id, "static")
    if axis == "static":
        return None
    if state is None:
        return cli_since

    last: str | None = (
        state.last_trade_date if axis == "trade_date" else state.last_period_end
    )
    if last is None:
        return cli_since

    incremental = _next_day(last)
    if cli_since is None:
        return incremental
    # Use the more recent of the two
    return max(incremental, cli_since)


def _update_last_date(
    state: DatasetSyncState,
    dataset_id: str,
    rows: list[dict[str, Any]],
) -> None:
    """Update last_trade_date / last_period_end from fetched rows."""
    axis = DATASET_INCREMENTAL_AXIS.get(dataset_id, "static")
    if axis == "static" or not rows:
        return
    if axis == "trade_date":
        dates = [r.get("trade_date") for r in rows if r.get("trade_date")]
        if dates:
            state.last_trade_date = max(dates)
    else:
        dates = [r.get("end_date") for r in rows if r.get("end_date")]
        if dates:
            state.last_period_end = max(dates)


# ---------------------------------------------------------------------------
# Fallback chain  (Task 12)
# ---------------------------------------------------------------------------


def _pick_adapter(
    adapters: list[Any],
    dataset_id: str,
    config: DataSourcesConfig,
) -> tuple[Any, str] | None:
    """
    Walk config.priority(dataset_id) and return the first adapter whose name
    matches and whose supports(dataset_id) is True.
    Returns (adapter, adapter_name) or None if none found.
    """
    priority = config.priority(dataset_id)
    adapter_by_name: dict[str, Any] = {}
    for a in adapters:
        name = getattr(a, "name", None)
        if name and name not in adapter_by_name:
            adapter_by_name[name] = a

    for name in priority:
        adapter = adapter_by_name.get(name)
        if adapter is None:
            continue
        try:
            if adapter.supports(dataset_id):
                return (adapter, name)
        except AdapterUnavailable:
            logger.debug("Adapter '%s' unavailable for '%s'.", name, dataset_id)
    return None


# ---------------------------------------------------------------------------
# Raw tables bootstrap
# ---------------------------------------------------------------------------


def _ensure_raw_tables(conn: Any) -> None:
    for ddl in RAW_TABLE_DDL.values():
        conn.execute(ddl)


class _NamedAdapterProxy:
    """Wraps an adapter with a different name for priority-list matching."""

    def __init__(self, adapter: Any, name: str) -> None:
        self._adapter = adapter
        self.name = name

    def supports(self, dataset_id: str) -> bool:
        return self._adapter.supports(dataset_id)

    def fetch(self, dataset_id: str, **kwargs: Any) -> Any:
        return self._adapter.fetch(dataset_id, **kwargs)


# ---------------------------------------------------------------------------
# Main sync entry point  (Tasks 10, 11, 12)
# ---------------------------------------------------------------------------


def sync(
    *,
    raw_db_path: Path,
    config: DataSourcesConfig,
    adapters: list[Any] | None = None,
    adapter_map: dict[str, Any] | None = None,
    only: set[str] | None = None,
    reset: set[str] | None = None,
    since: str | None = None,
    until: str | None = None,
    dry_run: bool = False,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_base_delay: float = _DEFAULT_RETRY_BASE_DELAY,
    _sleep: Any = None,
) -> SyncReport:
    """
    Sync all enabled datasets in canonical dependency order.

    Args:
        raw_db_path: Path to output/raw.duckdb (use ":memory:" for tests).
        config: Parsed DataSourcesConfig.
        adapters: List of DataSourceAdapter instances (preferred).
        adapter_map: Dict of adapter_name -> adapter (legacy; merged with adapters).
        only: If set, restrict to these dataset_ids.
        reset: If set, clear sync state and raw table for these dataset_ids first.
        since: Override start date (YYYYMMDD).
        until: End date (YYYYMMDD). None = today.
        dry_run: If True, plan without making any API calls.
        max_retries: Max retry attempts for transient errors.
        retry_base_delay: Base delay in seconds for retries.
        _sleep: Injectable sleep function for retry tests.
    """
    # Normalise adapter inputs: merge adapter_map into adapters list
    # If adapter_map provides a key different from adapter.name, wrap it.
    merged_adapters: list[Any] = list(adapters or [])
    if adapter_map:
        for key, a in adapter_map.items():
            adapter_name = getattr(a, "name", None)
            if adapter_name != key:
                # Wrap so priority-list lookup by key finds it
                merged_adapters.append(_NamedAdapterProxy(a, key))
            elif a not in merged_adapters:
                merged_adapters.append(a)
    import duckdb

    started_at = datetime.now(UTC).isoformat()
    results: list[DatasetSyncResult] = []

    db_str = str(raw_db_path)
    if db_str != ":memory:":
        raw_db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_str)
    _ensure_meta_schema(conn)
    _ensure_raw_tables(conn)

    state_map = _load_state(conn)

    # Handle --reset: clear state row and truncate raw table
    if reset:
        for ds_id in reset:
            table = DATASET_TABLE_NAME.get(ds_id)
            if table:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                    conn.execute(RAW_TABLE_DDL[ds_id])
                except Exception as exc:
                    logger.warning("Reset '%s': could not recreate table: %s", ds_id, exc)
            state_map.pop(ds_id, None)
            conn.execute(
                "DELETE FROM meta.dataset_sync_state WHERE dataset_id = ?", [ds_id]
            )
            logger.info("Reset dataset '%s': cleared state and raw table.", ds_id)

    # Determine which datasets to process
    datasets_to_run = [
        ds_id for ds_id in ALL_DATASET_IDS
        if (only is None or ds_id in only)
        and config.datasets.get(ds_id) is not None
        and config.datasets[ds_id].enabled
    ]

    if dry_run:
        conn.close()
        for ds_id in datasets_to_run:
            results.append(DatasetSyncResult(
                dataset_id=ds_id, adapter="(dry_run)", rows_added=0,
                duration_seconds=0.0, status="skipped", error_message=None,
            ))
        return SyncReport(
            raw_db_path=db_str,
            started_at=started_at,
            finished_at=datetime.now(UTC).isoformat(),
            results=results,
        )

    for ds_id in datasets_to_run:
        t0 = time.monotonic()
        state = state_map.get(ds_id)
        eff_since = _effective_since(ds_id, state, since)
        axis = DATASET_INCREMENTAL_AXIS.get(ds_id, "static")
        is_incremental = state is not None and eff_since is not None and axis != "static"

        logger.info(
            "Syncing '%s' | mode=%s | since=%s | until=%s",
            ds_id,
            "incremental" if is_incremental else "full",
            eff_since or "beginning",
            until or "now",
        )

        final_status, total_rows, error_msg, used_adapter = _sync_one_dataset(
            ds_id=ds_id,
            conn=conn,
            config=config,
            adapters=merged_adapters,
            state=state,
            eff_since=eff_since,
            until=until,
            is_incremental=is_incremental,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
            _sleep=_sleep,
        )

        duration = time.monotonic() - t0

        new_state = DatasetSyncState(
            dataset_id=ds_id,
            adapter=used_adapter,
            last_trade_date=state.last_trade_date if state else None,
            last_period_end=state.last_period_end if state else None,
            last_run_at=datetime.now(UTC),
            last_status=final_status,
            last_row_count=total_rows,
            error_message=error_msg,
        )
        # Update last dates from what was actually written
        if final_status == "success" and total_rows > 0:
            # Re-query to get actual max dates written
            _update_last_date_from_db(conn, new_state, ds_id)
        _record_state(conn, new_state)

        logger.info(
            "Finished '%s': status=%s rows=%d duration=%.1fs",
            ds_id, final_status, total_rows, duration,
        )

        results.append(DatasetSyncResult(
            dataset_id=ds_id,
            adapter=used_adapter,
            rows_added=total_rows,
            duration_seconds=duration,
            status=final_status,
            error_message=error_msg,
        ))

    conn.close()
    return SyncReport(
        raw_db_path=db_str,
        started_at=started_at,
        finished_at=datetime.now(UTC).isoformat(),
        results=results,
    )


# ---------------------------------------------------------------------------
# Per-dataset sync with adapter fallback chain
# ---------------------------------------------------------------------------


def _sync_one_dataset(
    *,
    ds_id: str,
    conn: Any,
    config: DataSourcesConfig,
    adapters: list[Any],
    state: DatasetSyncState | None,
    eff_since: str | None,
    until: str | None,
    is_incremental: bool,
    max_retries: int = _DEFAULT_MAX_RETRIES,
    retry_base_delay: float = _DEFAULT_RETRY_BASE_DELAY,
    _sleep: Any = None,
) -> tuple[SyncStatus, int, str | None, str]:
    """
    Try each adapter in priority order.
    On AdapterPermissionError mid-list, log and try next.
    Returns (status, rows_added, error_msg, adapter_name).
    """
    priority = list(config.priority(ds_id))
    adapter_by_name: dict[str, Any] = {}
    for a in adapters:
        name = getattr(a, "name", None)
        if name and name not in adapter_by_name:
            adapter_by_name[name] = a

    perm_deny_attempts: list[str] = []

    for adapter_name in priority:
        adapter = adapter_by_name.get(adapter_name)
        if adapter is None:
            continue
        try:
            if not adapter.supports(ds_id):
                continue
        except AdapterUnavailable:
            logger.debug("Adapter '%s' unavailable for '%s'.", adapter_name, ds_id)
            continue

        try:
            def _do_fetch(_a: Any = adapter) -> list[dict[str, Any]]:
                return list(_a.fetch(ds_id, since=eff_since, until=until, full=(not is_incremental)))

            all_rows: list[dict[str, Any]] = _with_retries(
                _do_fetch,
                max_attempts=max_retries,
                base_delay=retry_base_delay,
                _sleep=_sleep,
            )

            conn.execute("BEGIN")
            try:
                total_rows = _write_dataset(conn, ds_id, all_rows)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

            return "success", total_rows, None, adapter.name

        except AdapterPermissionError as exc:
            perm_deny_attempts.append(f"{adapter.name}: {exc}")
            logger.warning(
                "Permission denied from '%s' for '%s', trying next adapter.",
                adapter.name, ds_id,
            )
            continue

        except Exception as exc:
            return "failed", 0, str(exc), adapter.name

    # All adapters exhausted
    if perm_deny_attempts:
        msg = "All adapters denied access for '{}'. Attempts: {}".format(
            ds_id, " | ".join(perm_deny_attempts)
        )
        return "permission_denied", 0, msg, "none"

    if priority:
        msg = f"no adapter available for '{ds_id}' in priority list"
        return "failed", 0, msg, "none"

    msg = f"no adapter available"
    return "failed", 0, msg, "none"


def _update_last_date_from_db(conn: Any, state: DatasetSyncState, dataset_id: str) -> None:
    """Query the raw table to find max trade_date / end_date and update state."""
    axis = DATASET_INCREMENTAL_AXIS.get(dataset_id, "static")
    if axis == "static":
        return
    table = DATASET_TABLE_NAME.get(dataset_id)
    if not table:
        return
    try:
        if axis == "trade_date":
            row = conn.execute(f"SELECT MAX(trade_date) FROM {table}").fetchone()
            if row and row[0]:
                state.last_trade_date = row[0]
        else:
            row = conn.execute(f"SELECT MAX(end_date) FROM {table}").fetchone()
            if row and row[0]:
                state.last_period_end = row[0]
    except Exception as exc:
        logger.debug("Could not update last_date from db for '%s': %s", dataset_id, exc)
