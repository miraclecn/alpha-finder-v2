from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import os
from pathlib import Path
from typing import Any, Iterable


DEFAULT_MEMBER_PAGE_SIZE = 3000
DEFAULT_WEIGHT_PAGE_SIZE = 2000
DEFAULT_INDEX_WEIGHT_WINDOW_MONTHS = 1
DEFAULT_LEGACY_ENV_PATH = Path.home() / ".openclaw" / "workspace-data-collector" / ".env"
INDUSTRY_LEVEL_DEFINITIONS = {
    "L1": ("sw2021_l1", "l1_code"),
    "L2": ("sw2021_l2", "l2_code"),
    "L3": ("sw2021_l3", "l3_code"),
}


@dataclass(slots=True)
class BenchmarkReferenceDefinition:
    benchmark_id: str
    index_code: str


def build_tushare_reference_db(
    *,
    target_db: str | Path,
    benchmarks: list[BenchmarkReferenceDefinition],
    start_date: str,
    end_date: str,
    token: str | None = None,
    client: Any | None = None,
    industry_levels: tuple[str, ...] = ("L1", "L2", "L3"),
    member_page_size: int = DEFAULT_MEMBER_PAGE_SIZE,
    weight_page_size: int = DEFAULT_WEIGHT_PAGE_SIZE,
    index_weight_window_months: int = DEFAULT_INDEX_WEIGHT_WINDOW_MONTHS,
) -> dict[str, Any]:
    if not benchmarks:
        raise ValueError("Reference staging requires at least one benchmark definition.")
    if not start_date or not end_date:
        raise ValueError("Reference staging requires explicit start_date and end_date.")
    if start_date > end_date:
        raise ValueError("Reference staging start_date cannot be after end_date.")
    invalid_levels = sorted(level for level in industry_levels if level not in INDUSTRY_LEVEL_DEFINITIONS)
    if invalid_levels:
        raise ValueError(
            "Unsupported industry levels for reference staging: "
            + ", ".join(invalid_levels)
        )
    if member_page_size <= 0:
        raise ValueError("Reference staging member_page_size must be positive.")
    if weight_page_size <= 0:
        raise ValueError("Reference staging weight_page_size must be positive.")
    if index_weight_window_months <= 0:
        raise ValueError("Reference staging index_weight_window_months must be positive.")

    if client is None:
        client = _build_tushare_client(load_tushare_token(token))

    member_records = _fetch_index_member_all_records(
        client=client,
        page_size=member_page_size,
    )
    industry_rows = _build_industry_rows(
        member_records=member_records,
        industry_levels=industry_levels,
    )
    weight_rows = _fetch_benchmark_weight_rows(
        client=client,
        benchmarks=benchmarks,
        start_date=start_date,
        end_date=end_date,
        page_size=weight_page_size,
        window_months=index_weight_window_months,
    )
    if not weight_rows:
        raise ValueError("Reference staging produced no benchmark weight rows.")
    membership_rows = _derive_membership_intervals(weight_rows)

    target_path = Path(target_db).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    _write_reference_db(
        target_path=target_path,
        industry_rows=industry_rows,
        membership_rows=membership_rows,
        weight_rows=weight_rows,
    )
    return {
        "target_db": str(target_path),
        "benchmarks": [definition.benchmark_id for definition in benchmarks],
        "industry_rows": len(industry_rows),
        "membership_rows": len(membership_rows),
        "weight_rows": len(weight_rows),
        "start_date": start_date,
        "end_date": end_date,
    }


def load_tushare_token(explicit_token: str | None = None) -> str:
    if explicit_token:
        return explicit_token

    token = os.getenv("TUSHARE_TOKEN")
    if token:
        return token

    if DEFAULT_LEGACY_ENV_PATH.exists():
        for raw_line in DEFAULT_LEGACY_ENV_PATH.read_text(encoding="utf-8").splitlines():
            if not raw_line or raw_line.lstrip().startswith("#") or "=" not in raw_line:
                continue
            key, value = raw_line.split("=", 1)
            if key.strip() == "TUSHARE_TOKEN" and value.strip():
                return value.strip()

    raise RuntimeError("TUSHARE_TOKEN is not set")


def _build_tushare_client(token: str) -> Any:
    import tushare as ts

    return ts.pro_api(token)


def _fetch_index_member_all_records(
    *,
    client: Any,
    page_size: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    offset = 0
    while True:
        frame = client.index_member_all(offset=offset, limit=page_size)
        page_rows = _dataframe_rows(frame)
        if not page_rows:
            break
        records.extend(page_rows)
        offset += len(page_rows)
        if len(page_rows) < page_size:
            break
    if not records:
        raise ValueError("Reference staging produced no index_member_all rows.")
    return records


def _build_industry_rows(
    *,
    member_records: list[dict[str, Any]],
    industry_levels: tuple[str, ...],
) -> list[tuple[str, str, str, str, str | None]]:
    unique_rows: set[tuple[str, str, str, str, str | None]] = set()
    for row in member_records:
        security_id = str(row["ts_code"])
        effective_at = _clean_date(row.get("in_date"))
        if not effective_at:
            continue
        removed_at = _clean_date(row.get("out_date"))
        for level in industry_levels:
            industry_schema, code_field = INDUSTRY_LEVEL_DEFINITIONS[level]
            industry_code = _clean_text(row.get(code_field))
            if not industry_code:
                continue
            unique_rows.add(
                (
                    security_id,
                    industry_schema,
                    industry_code,
                    effective_at,
                    removed_at,
                )
            )
    if not unique_rows:
        raise ValueError("Reference staging produced no PIT industry classification rows.")
    return _normalize_industry_intervals(unique_rows)


def _fetch_benchmark_weight_rows(
    *,
    client: Any,
    benchmarks: list[BenchmarkReferenceDefinition],
    start_date: str,
    end_date: str,
    page_size: int,
    window_months: int,
) -> list[tuple[str, str, str, float]]:
    rows: set[tuple[str, str, str, float]] = set()
    for benchmark in benchmarks:
        for window_start, window_end in _date_windows(start_date, end_date, window_months):
            offset = 0
            while True:
                frame = client.index_weight(
                    index_code=benchmark.index_code,
                    start_date=window_start,
                    end_date=window_end,
                    offset=offset,
                    limit=page_size,
                )
                page_rows = _dataframe_rows(frame)
                if not page_rows:
                    break
                for row in page_rows:
                    trade_date = _clean_date(row.get("trade_date"))
                    security_id = _clean_text(row.get("con_code"))
                    weight = row.get("weight")
                    if not trade_date or not security_id or weight is None:
                        continue
                    rows.add(
                        (
                            benchmark.benchmark_id,
                            security_id,
                            trade_date,
                            float(weight),
                        )
                    )
                offset += len(page_rows)
                if len(page_rows) < page_size:
                    break
    return sorted(rows, key=lambda item: (item[0], item[2], item[1]))


def _derive_membership_intervals(
    weight_rows: list[tuple[str, str, str, float]],
) -> list[tuple[str, str, str, str | None]]:
    snapshot_map: dict[str, dict[str, set[str]]] = {}
    for benchmark_id, security_id, trade_date, _ in weight_rows:
        snapshot_map.setdefault(benchmark_id, {}).setdefault(trade_date, set()).add(security_id)

    intervals: list[tuple[str, str, str, str | None]] = []
    for benchmark_id, snapshots in snapshot_map.items():
        active_since: dict[str, str] = {}
        for trade_date in sorted(snapshots):
            current_members = snapshots[trade_date]
            previous_members = set(active_since)

            for security_id in sorted(current_members - previous_members):
                active_since[security_id] = trade_date
            for security_id in sorted(previous_members - current_members):
                intervals.append(
                    (
                        benchmark_id,
                        security_id,
                        active_since.pop(security_id),
                        trade_date,
                    )
                )

        for security_id in sorted(active_since):
            intervals.append(
                (
                    benchmark_id,
                    security_id,
                    active_since[security_id],
                    None,
                )
            )
    return sorted(intervals, key=lambda item: (item[0], item[1], item[2]))


def _write_reference_db(
    *,
    target_path: Path,
    industry_rows: list[tuple[str, str, str, str, str | None]],
    membership_rows: list[tuple[str, str, str, str | None]],
    weight_rows: list[tuple[str, str, str, float]],
) -> None:
    import duckdb

    conn = duckdb.connect(str(target_path))
    try:
        conn.execute(
            """
            CREATE OR REPLACE TABLE industry_classification_pit (
                security_id VARCHAR,
                industry_schema VARCHAR,
                industry_code VARCHAR,
                effective_at VARCHAR,
                removed_at VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE TABLE benchmark_membership_pit (
                benchmark_id VARCHAR,
                security_id VARCHAR,
                effective_at VARCHAR,
                removed_at VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE OR REPLACE TABLE benchmark_weight_snapshot_pit (
                benchmark_id VARCHAR,
                security_id VARCHAR,
                trade_date VARCHAR,
                weight DOUBLE
            )
            """
        )
        conn.execute("DELETE FROM industry_classification_pit")
        conn.execute("DELETE FROM benchmark_membership_pit")
        conn.execute("DELETE FROM benchmark_weight_snapshot_pit")
        conn.executemany(
            "INSERT INTO industry_classification_pit VALUES (?, ?, ?, ?, ?)",
            industry_rows,
        )
        conn.executemany(
            "INSERT INTO benchmark_membership_pit VALUES (?, ?, ?, ?)",
            membership_rows,
        )
        conn.executemany(
            "INSERT INTO benchmark_weight_snapshot_pit VALUES (?, ?, ?, ?)",
            weight_rows,
        )
    finally:
        conn.close()


def _dataframe_rows(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    try:
        if frame.empty:
            return []
    except AttributeError:
        pass
    return list(frame.to_dict("records"))


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.lower() == "none" else text


def _clean_date(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _normalize_industry_intervals(
    rows: set[tuple[str, str, str, str, str | None]],
) -> list[tuple[str, str, str, str, str | None]]:
    grouped: dict[tuple[str, str], list[tuple[str, str, str, str, str | None]]] = {}
    for row in rows:
        grouped.setdefault((row[0], row[1]), []).append(row)

    normalized: list[tuple[str, str, str, str, str | None]] = []
    for key in sorted(grouped):
        ordered = sorted(grouped[key], key=lambda item: (item[3], item[2]))
        for index, row in enumerate(ordered):
            next_effective_at = ordered[index + 1][3] if index + 1 < len(ordered) else None
            removed_at = row[4]
            if next_effective_at and (removed_at is None or next_effective_at < removed_at):
                removed_at = next_effective_at
            normalized.append((row[0], row[1], row[2], row[3], removed_at))
    return sorted(normalized, key=lambda item: (item[0], item[1], item[3], item[2]))


def _date_windows(
    start_date: str,
    end_date: str,
    window_months: int,
) -> Iterable[tuple[str, str]]:
    current = datetime.strptime(start_date, "%Y%m%d").date()
    limit = datetime.strptime(end_date, "%Y%m%d").date()
    while current <= limit:
        window_end = min(_window_end(current, window_months), limit)
        yield current.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        current = window_end + timedelta(days=1)


def _window_end(start: date, window_months: int) -> date:
    month_index = start.month - 1 + window_months
    year = start.year + month_index // 12
    month = month_index % 12 + 1
    first_next_month = date(year, month, 1)
    return first_next_month - timedelta(days=1)
