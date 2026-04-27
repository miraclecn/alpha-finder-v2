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
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OFFICIAL_SW_STOCK_FILE = PROJECT_ROOT / "docs" / "data" / "StockClassifyUse_stock.xls"
DEFAULT_OFFICIAL_SW_CROSSWALK_FILE = PROJECT_ROOT / "docs" / "data" / "2014to2021.xlsx"
DEFAULT_OFFICIAL_SW_CODE_FILE = PROJECT_ROOT / "docs" / "data" / "SwClassCode_2021.xls"
DEFAULT_OFFICIAL_SW_SNAPSHOT_FILE = PROJECT_ROOT / "docs" / "data" / "最新个股申万行业分类(完整版-截至7月末).xlsx"
OFFICIAL_SW_MIN_EFFECTIVE_DATE = "20140221"
OFFICIAL_SW_CUTOVER_DATE = "20210730"
INDUSTRY_LEVEL_DEFINITIONS = {
    "L1": ("sw2021_l1", "l1_code"),
    "L2": ("sw2021_l2", "l2_code"),
    "L3": ("sw2021_l3", "l3_code"),
}


@dataclass(slots=True)
class BenchmarkReferenceDefinition:
    benchmark_id: str
    index_code: str


@dataclass(slots=True, frozen=True)
class _OfficialSwStockRecord:
    security_id: str
    effective_at: str
    raw_industry_code: str
    updated_at: str


@dataclass(slots=True, frozen=True)
class _ManualIndustryAdjudicationRecord:
    security_id: str
    start_date: str
    end_date: str
    industry_schema: str
    industry_level: str
    industry_code: str
    source_type: str
    evidence_url: str
    evidence_date: str
    available_at: str
    confidence: str
    adjudication_note: str


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


def refresh_official_sw_industry_reference_db(
    *,
    target_db: str | Path,
    stock_file: str | Path = DEFAULT_OFFICIAL_SW_STOCK_FILE,
    crosswalk_file: str | Path = DEFAULT_OFFICIAL_SW_CROSSWALK_FILE,
    code_file: str | Path = DEFAULT_OFFICIAL_SW_CODE_FILE,
    snapshot_file: str | Path = DEFAULT_OFFICIAL_SW_SNAPSHOT_FILE,
    industry_levels: tuple[str, ...] = ("L1",),
    min_effective_date: str = OFFICIAL_SW_MIN_EFFECTIVE_DATE,
) -> dict[str, Any]:
    invalid_levels = sorted(level for level in industry_levels if level not in INDUSTRY_LEVEL_DEFINITIONS)
    if invalid_levels:
        raise ValueError(
            "Unsupported industry levels for official SW import: "
            + ", ".join(invalid_levels)
        )
    if not min_effective_date or len(min_effective_date) != 8 or not min_effective_date.isdigit():
        raise ValueError("Official SW import requires min_effective_date in YYYYMMDD format.")

    stock_path = Path(stock_file).expanduser().resolve()
    crosswalk_path = Path(crosswalk_file).expanduser().resolve()
    code_path = Path(code_file).expanduser().resolve()
    snapshot_path = Path(snapshot_file).expanduser().resolve()

    stock_records = _load_official_sw_stock_records(stock_path)
    hierarchy_by_code = _load_official_sw_2021_hierarchy(code_path)
    crosswalk_level_maps = _load_official_sw_crosswalk_level_maps(
        crosswalk_path,
        hierarchy_by_code=hierarchy_by_code,
    )
    snapshot_codes = _load_official_sw_cutover_snapshot(snapshot_path)
    snapshot_anchor_records = _build_official_sw_snapshot_anchor_records(
        snapshot_codes=snapshot_codes,
        stock_records=stock_records,
    )
    empirical_level_maps = _build_empirical_official_sw_level_maps(
        stock_records=[*stock_records, *snapshot_anchor_records],
        hierarchy_by_code=hierarchy_by_code,
    )
    official_industry_rows, build_summary = _build_official_sw_industry_rows(
        stock_records=[*stock_records, *snapshot_anchor_records],
        raw_stock_records=stock_records,
        hierarchy_by_code=hierarchy_by_code,
        crosswalk_level_maps=crosswalk_level_maps,
        empirical_level_maps=empirical_level_maps,
        snapshot_codes=snapshot_codes,
        industry_levels=industry_levels,
        min_effective_date=min_effective_date,
    )
    manual_adjudications = _build_default_manual_industry_adjudications(
        stock_path=stock_path,
        official_industry_rows=official_industry_rows,
    )
    industry_rows, applied_manual_adjudications = _apply_manual_industry_adjudications(
        official_industry_rows=official_industry_rows,
        adjudication_records=manual_adjudications,
    )

    target_path = Path(target_db).expanduser().resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    _write_official_sw_industry_table(
        target_path=target_path,
        official_industry_rows=official_industry_rows,
        industry_rows=industry_rows,
        manual_adjudications=manual_adjudications,
    )
    return {
        "target_db": str(target_path),
        "stock_file": str(stock_path),
        "crosswalk_file": str(crosswalk_path),
        "code_file": str(code_path),
        "snapshot_file": str(snapshot_path),
        "industry_levels": list(industry_levels),
        "industry_rows": len(industry_rows),
        "official_industry_rows": len(official_industry_rows),
        "manual_adjudication_rows": len(manual_adjudications),
        "effective_manual_adjudication_rows": len(applied_manual_adjudications),
        "source_rows": len(stock_records),
        "snapshot_anchor_rows": len(snapshot_anchor_records),
        **build_summary,
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
        _write_reference_dataset_registry(conn)
    finally:
        conn.close()


def _write_official_sw_industry_table(
    *,
    target_path: Path,
    official_industry_rows: list[tuple[str, str, str, str, str | None]],
    industry_rows: list[tuple[str, str, str, str, str | None]],
    manual_adjudications: list[_ManualIndustryAdjudicationRecord],
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
            CREATE OR REPLACE TABLE industry_classification_pit_official_raw (
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
            CREATE OR REPLACE TABLE industry_classification_pit_manual_adjudication (
                security_id VARCHAR,
                start_date VARCHAR,
                end_date VARCHAR,
                industry_schema VARCHAR,
                industry_level VARCHAR,
                industry_code VARCHAR,
                source_type VARCHAR,
                evidence_url VARCHAR,
                evidence_date VARCHAR,
                available_at VARCHAR,
                confidence VARCHAR,
                adjudication_note VARCHAR
            )
            """
        )
        conn.execute("DELETE FROM industry_classification_pit")
        conn.execute("DELETE FROM industry_classification_pit_official_raw")
        conn.execute("DELETE FROM industry_classification_pit_manual_adjudication")
        conn.executemany(
            "INSERT INTO industry_classification_pit VALUES (?, ?, ?, ?, ?)",
            industry_rows,
        )
        conn.executemany(
            "INSERT INTO industry_classification_pit_official_raw VALUES (?, ?, ?, ?, ?)",
            official_industry_rows,
        )
        if manual_adjudications:
            conn.executemany(
                """
                INSERT INTO industry_classification_pit_manual_adjudication
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        record.security_id,
                        record.start_date,
                        record.end_date,
                        record.industry_schema,
                        record.industry_level,
                        record.industry_code,
                        record.source_type,
                        record.evidence_url,
                        record.evidence_date,
                        record.available_at,
                        record.confidence,
                        record.adjudication_note,
                    )
                    for record in manual_adjudications
                ],
            )
        _write_reference_dataset_registry(
            conn,
            industry_source_provider="official_shenwan_packet",
            industry_note=_official_sw_industry_note(len(manual_adjudications)),
        )
    finally:
        conn.close()


def _write_reference_dataset_registry(
    conn: Any,
    *,
    industry_source_provider: str = "tushare",
    industry_note: str = "staged SW2021 PIT industry truth for the V2 reference chain",
) -> None:
    conn.execute(
        """
        CREATE OR REPLACE TABLE reference_dataset_registry (
            dataset_id VARCHAR,
            source_provider VARCHAR,
            boundary_role VARCHAR,
            status VARCHAR,
            row_count BIGINT,
            earliest_date VARCHAR,
            latest_date VARCHAR,
            note VARCHAR
        )
        """
    )
    conn.execute("DELETE FROM reference_dataset_registry")
    if _table_exists_current_db(conn, "industry_classification_pit"):
        conn.execute(
            """
            INSERT INTO reference_dataset_registry
            SELECT
                'industry_classification_pit',
                ?,
                'pit_reference_staging',
                'green',
                COUNT(*),
                MIN(effective_at),
                MAX(COALESCE(removed_at, effective_at)),
                ?
            FROM industry_classification_pit
            """,
            [industry_source_provider, industry_note],
        )
    if _table_exists_current_db(conn, "benchmark_membership_pit"):
        conn.execute(
            """
            INSERT INTO reference_dataset_registry
            SELECT
                'benchmark_membership_pit',
                'tushare',
                'pit_reference_staging',
                'green',
                COUNT(*),
                MIN(effective_at),
                MAX(COALESCE(removed_at, effective_at)),
                'staged benchmark membership truth for the V2 reference chain'
            FROM benchmark_membership_pit
            """
        )
    if _table_exists_current_db(conn, "benchmark_weight_snapshot_pit"):
        conn.execute(
            """
            INSERT INTO reference_dataset_registry
            SELECT
                'benchmark_weight_snapshot_pit',
                'tushare',
                'pit_reference_staging',
                'green',
                COUNT(*),
                MIN(trade_date),
                MAX(trade_date),
                'staged provider benchmark weights for the V2 reference chain'
            FROM benchmark_weight_snapshot_pit
            """
        )


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
    return "" if text.lower() in {"none", "nan", "nat"} else text


def _clean_date(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _load_official_sw_stock_records(path: Path) -> list[_OfficialSwStockRecord]:
    import pandas as pd

    frame = pd.read_excel(path)
    required_columns = {"股票代码", "计入日期", "行业代码", "更新日期"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "Official SW stock ledger is missing required columns: "
            + ", ".join(missing_columns)
        )

    grouped: dict[str, dict[str, _OfficialSwStockRecord]] = {}
    for row in frame.to_dict("records"):
        security_id = _normalize_official_sw_security_id(row.get("股票代码"))
        effective_at = _normalize_official_sw_timestamp(row.get("计入日期"))
        raw_industry_code = _normalize_official_sw_code(row.get("行业代码"))
        updated_at = _normalize_official_sw_timestamp(row.get("更新日期")) or ""
        if not security_id or not effective_at or not raw_industry_code:
            continue
        existing = grouped.setdefault(security_id, {}).get(effective_at)
        candidate = _OfficialSwStockRecord(
            security_id=security_id,
            effective_at=effective_at,
            raw_industry_code=raw_industry_code,
            updated_at=updated_at,
        )
        if existing is None or candidate.updated_at >= existing.updated_at:
            grouped[security_id][effective_at] = candidate

    records: list[_OfficialSwStockRecord] = []
    for security_id in sorted(grouped):
        records.extend(grouped[security_id][effective_at] for effective_at in sorted(grouped[security_id]))
    if not records:
        raise ValueError("Official SW stock ledger produced no usable rows.")
    return records


def _load_official_sw_2021_hierarchy(path: Path) -> dict[str, dict[str, str]]:
    import pandas as pd

    frame = pd.read_excel(path)
    required_columns = {"行业代码", "一级行业名称", "二级行业名称", "三级行业名称"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "Official SW 2021 code table is missing required columns: "
            + ", ".join(missing_columns)
        )

    normalized_rows: list[tuple[str, str, str, str]] = []
    l1_key_to_code: dict[str, str] = {}
    l2_key_to_code: dict[tuple[str, str], str] = {}
    for row in frame.to_dict("records"):
        code = _normalize_official_sw_code(row.get("行业代码"))
        l1_name = _clean_text(row.get("一级行业名称"))
        l2_name = _clean_text(row.get("二级行业名称"))
        l3_name = _clean_text(row.get("三级行业名称"))
        normalized_rows.append((code, l1_name, l2_name, l3_name))
        if code and l1_name and not l2_name and not l3_name:
            l1_key_to_code[l1_name] = code
        if code and l1_name and l2_name and not l3_name:
            l2_key_to_code[(l1_name, l2_name)] = code

    hierarchy_by_code: dict[str, dict[str, str]] = {}
    for code, l1_name, l2_name, l3_name in normalized_rows:
        if not code or not l1_name:
            continue
        level_codes: dict[str, str] = {}
        l1_code = l1_key_to_code.get(l1_name)
        if l1_code:
            level_codes["L1"] = l1_code
        if l2_name:
            l2_code = l2_key_to_code.get((l1_name, l2_name))
            if l2_code:
                level_codes["L2"] = l2_code
        if l3_name:
            level_codes["L3"] = code
        hierarchy_by_code[code] = level_codes

    if not hierarchy_by_code:
        raise ValueError("Official SW 2021 code table produced no usable hierarchy rows.")
    return hierarchy_by_code


def _load_official_sw_cutover_snapshot(path: Path) -> dict[str, str]:
    import pandas as pd

    frame = pd.read_excel(path)
    required_columns = {"交易所", "行业代码", "股票代码"}
    missing_columns = sorted(required_columns - set(frame.columns))
    if missing_columns:
        raise ValueError(
            "Official SW cutover snapshot is missing required columns: "
            + ", ".join(missing_columns)
        )

    snapshot_codes: dict[str, str] = {}
    for row in frame.to_dict("records"):
        if _clean_text(row.get("交易所")) != "A股":
            continue
        security_id = _normalize_official_sw_security_id(row.get("股票代码"))
        industry_code = _normalize_official_sw_code(row.get("行业代码"))
        if not security_id or not industry_code:
            continue
        snapshot_codes[security_id] = industry_code
    if not snapshot_codes:
        raise ValueError("Official SW cutover snapshot produced no A-share rows.")
    return snapshot_codes


def _load_official_sw_crosswalk_level_maps(
    path: Path,
    *,
    hierarchy_by_code: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    import pandas as pd

    frame = pd.read_excel(path, sheet_name="新旧对比版本2", header=None)
    if frame.shape[1] < 8:
        raise ValueError("Official SW crosswalk sheet 新旧对比版本2 requires at least 8 columns.")

    old_code_to_old_l1: dict[str, str] = {}
    old_l1_to_new_l1: dict[str, str] = {}
    current_old_l1 = ""
    grouped: dict[str, set[str]] = {}
    for row in frame.iloc[:, :8].itertuples(index=False):
        old_code = _normalize_official_sw_code(row[3])
        new_code = _normalize_official_sw_code(row[7])
        if old_code.endswith("0000") and old_code:
            current_old_l1 = old_code
            if new_code.endswith("0000") and new_code in hierarchy_by_code:
                old_l1_to_new_l1[old_code] = hierarchy_by_code[new_code].get("L1", "")
        if old_code and current_old_l1:
            old_code_to_old_l1[old_code] = current_old_l1
        if not old_code or not new_code or new_code not in hierarchy_by_code:
            continue
        grouped.setdefault(old_code, set()).add(new_code)

    level_maps = {level: {} for level in INDUSTRY_LEVEL_DEFINITIONS}
    for old_code, new_codes in grouped.items():
        if len(new_codes) != 1:
            continue
        mapped_levels = hierarchy_by_code[next(iter(new_codes))]
        for level, level_code in mapped_levels.items():
            level_maps[level][old_code] = level_code
    for old_code, old_l1_code in old_code_to_old_l1.items():
        new_l1_code = old_l1_to_new_l1.get(old_l1_code, "")
        if new_l1_code:
            level_maps["L1"].setdefault(old_code, new_l1_code)
    return level_maps


def _build_official_sw_snapshot_anchor_records(
    *,
    snapshot_codes: dict[str, str],
    stock_records: list[_OfficialSwStockRecord],
) -> list[_OfficialSwStockRecord]:
    earliest_post_cutover_by_security: dict[str, str] = {}
    for record in stock_records:
        if _timestamp_date_key(record.effective_at) < OFFICIAL_SW_CUTOVER_DATE:
            continue
        existing = earliest_post_cutover_by_security.get(record.security_id)
        if existing is None or record.effective_at < existing:
            earliest_post_cutover_by_security[record.security_id] = record.effective_at

    anchor_records: list[_OfficialSwStockRecord] = []
    for security_id, industry_code in sorted(snapshot_codes.items()):
        earliest_post_cutover = earliest_post_cutover_by_security.get(security_id)
        if earliest_post_cutover is not None and _timestamp_date_key(earliest_post_cutover) <= OFFICIAL_SW_CUTOVER_DATE:
            continue
        anchor_records.append(
            _OfficialSwStockRecord(
                security_id=security_id,
                effective_at="2021-07-30 00:00:00",
                raw_industry_code=industry_code,
                updated_at="2021-07-30 00:00:00",
            )
        )
    return anchor_records


def _build_empirical_official_sw_level_maps(
    *,
    stock_records: list[_OfficialSwStockRecord],
    hierarchy_by_code: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    by_security: dict[str, list[_OfficialSwStockRecord]] = {}
    for record in stock_records:
        by_security.setdefault(record.security_id, []).append(record)

    candidate_levels: dict[str, dict[str, set[str]]] = {}
    for security_id in sorted(by_security):
        records = sorted(by_security[security_id], key=lambda item: item.effective_at)
        left_record: _OfficialSwStockRecord | None = None
        right_record: _OfficialSwStockRecord | None = None
        for record in records:
            if _timestamp_date_key(record.effective_at) < OFFICIAL_SW_CUTOVER_DATE:
                left_record = record
                continue
            right_record = record
            break
        if left_record is None or right_record is None:
            continue
        right_levels = hierarchy_by_code.get(right_record.raw_industry_code)
        if not right_levels:
            continue
        level_sets = candidate_levels.setdefault(
            left_record.raw_industry_code,
            {level: set() for level in INDUSTRY_LEVEL_DEFINITIONS},
        )
        for level, level_code in right_levels.items():
            level_sets[level].add(level_code)

    resolved = {level: {} for level in INDUSTRY_LEVEL_DEFINITIONS}
    for raw_code, level_sets in candidate_levels.items():
        for level, values in level_sets.items():
            if len(values) == 1:
                resolved[level][raw_code] = next(iter(values))
    return resolved


def _build_official_sw_industry_rows(
    *,
    stock_records: list[_OfficialSwStockRecord],
    raw_stock_records: list[_OfficialSwStockRecord],
    hierarchy_by_code: dict[str, dict[str, str]],
    crosswalk_level_maps: dict[str, dict[str, str]],
    empirical_level_maps: dict[str, dict[str, str]],
    snapshot_codes: dict[str, str],
    industry_levels: tuple[str, ...],
    min_effective_date: str,
) -> tuple[list[tuple[str, str, str, str, str | None]], dict[str, Any]]:
    unique_rows: set[tuple[str, str, str, str, str | None]] = set()
    unresolved_rows = {level: 0 for level in industry_levels}
    placeholder_rows = 0
    pre_2014_rows = 0
    carry_forward_rows = {level: 0 for level in industry_levels}
    bridge_fill_rows = {level: 0 for level in industry_levels}
    min_effective_timestamp = (
        f"{min_effective_date[:4]}-{min_effective_date[4:6]}-{min_effective_date[6:8]} 00:00:00"
    )
    latest_pre_window_record_by_security: dict[str, _OfficialSwStockRecord] = {}

    for record in stock_records:
        effective_date = _timestamp_date_key(record.effective_at)
        if effective_date == "19900101":
            placeholder_rows += 1
            continue
        if effective_date < min_effective_date:
            pre_2014_rows += 1
            existing = latest_pre_window_record_by_security.get(record.security_id)
            if existing is None or record.effective_at > existing.effective_at:
                latest_pre_window_record_by_security[record.security_id] = record
            continue

        for level in industry_levels:
            level_code = _resolve_official_sw_level_code(
                raw_industry_code=record.raw_industry_code,
                level=level,
                hierarchy_by_code=hierarchy_by_code,
                crosswalk_level_maps=crosswalk_level_maps,
                empirical_level_maps=empirical_level_maps,
            )
            if not level_code:
                unresolved_rows[level] += 1
                continue
            industry_schema, _ = INDUSTRY_LEVEL_DEFINITIONS[level]
            unique_rows.add(
                (
                    record.security_id,
                    industry_schema,
                    level_code,
                    record.effective_at,
                    None,
                )
            )

    min_effective_keys = {
        (security_id, industry_schema)
        for security_id, industry_schema, _, effective_at, _ in unique_rows
        if effective_at == min_effective_timestamp
    }
    for record in latest_pre_window_record_by_security.values():
        for level in industry_levels:
            industry_schema, _ = INDUSTRY_LEVEL_DEFINITIONS[level]
            if (record.security_id, industry_schema) in min_effective_keys:
                continue
            level_code = _resolve_official_sw_level_code(
                raw_industry_code=record.raw_industry_code,
                level=level,
                hierarchy_by_code=hierarchy_by_code,
                crosswalk_level_maps=crosswalk_level_maps,
                empirical_level_maps=empirical_level_maps,
            )
            if not level_code:
                continue
            unique_rows.add(
                (
                    record.security_id,
                    industry_schema,
                    level_code,
                    min_effective_timestamp,
                    None,
                )
            )
            min_effective_keys.add((record.security_id, industry_schema))
            carry_forward_rows[level] += 1

    pre_cutover_resolved_keys = {
        (security_id, industry_schema)
        for security_id, industry_schema, _, effective_at, _ in unique_rows
        if _timestamp_date_key(effective_at) < OFFICIAL_SW_CUTOVER_DATE
    }
    pre_cutover_raw_by_security = {
        record.security_id
        for record in raw_stock_records
        if min_effective_date <= _timestamp_date_key(record.effective_at) < OFFICIAL_SW_CUTOVER_DATE
    }
    for security_id, snapshot_raw_code in snapshot_codes.items():
        if security_id in pre_cutover_raw_by_security:
            continue
        for level in industry_levels:
            industry_schema, _ = INDUSTRY_LEVEL_DEFINITIONS[level]
            if (security_id, industry_schema) in pre_cutover_resolved_keys:
                continue
            level_code = _resolve_official_sw_level_code(
                raw_industry_code=snapshot_raw_code,
                level=level,
                hierarchy_by_code=hierarchy_by_code,
                crosswalk_level_maps=crosswalk_level_maps,
                empirical_level_maps=empirical_level_maps,
            )
            if not level_code:
                continue
            unique_rows.add(
                (
                    security_id,
                    industry_schema,
                    level_code,
                    min_effective_timestamp,
                    None,
                )
            )
            bridge_fill_rows[level] += 1

    if not unique_rows:
        raise ValueError("Official SW import produced no PIT industry classification rows.")
    return (
        _normalize_industry_intervals(unique_rows),
        {
            "quarantined_placeholder_rows": placeholder_rows,
            "quarantined_pre_2014_rows": pre_2014_rows,
            "quarantined_unresolved_rows": unresolved_rows,
            "window_carry_forward_rows": carry_forward_rows,
            "snapshot_backfill_rows": bridge_fill_rows,
        },
    )


def _build_default_manual_industry_adjudications(
    *,
    stock_path: Path,
    official_industry_rows: list[tuple[str, str, str, str, str | None]],
) -> list[_ManualIndustryAdjudicationRecord]:
    def provider_gap_confirmation(
        *,
        security_id: str,
        start_date: str,
        end_date: str,
        industry_code: str,
        evidence_date: str,
        available_at: str | None = None,
    ) -> _ManualIndustryAdjudicationRecord:
        return _ManualIndustryAdjudicationRecord(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code=industry_code,
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date=evidence_date,
            available_at=available_at or start_date,
            confidence="medium",
            adjudication_note=(
                f"Confirm the official {start_date} to {end_date} provider-gap "
                f"run for {security_id} from the Shenwan change-node ledger; "
                f"the supported official interval maps to sw2021_l1 {industry_code}."
            ),
        )

    def external_left_edge_backfill(
        *,
        security_id: str,
        start_date: str,
        end_date: str,
        industry_code: str,
        evidence_url: str,
        evidence_date: str,
        available_at: str,
        adjudication_note: str,
    ) -> _ManualIndustryAdjudicationRecord:
        return _ManualIndustryAdjudicationRecord(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code=industry_code,
            source_type="external_left_edge_backfill",
            evidence_url=evidence_url,
            evidence_date=evidence_date,
            available_at=available_at,
            confidence="medium",
            adjudication_note=adjudication_note,
        )

    def security_code_alias_backfill(
        *,
        security_id: str,
        legacy_security_id: str,
        start_date: str,
        end_date: str,
        industry_code: str,
        evidence_date: str,
    ) -> _ManualIndustryAdjudicationRecord:
        return _ManualIndustryAdjudicationRecord(
            security_id=security_id,
            start_date=start_date,
            end_date=end_date,
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code=industry_code,
            source_type="security_code_alias_backfill",
            evidence_url=str(stock_path),
            evidence_date=evidence_date,
            available_at=evidence_date,
            confidence="medium",
            adjudication_note=(
                f"Backfill {security_id} from legacy code {legacy_security_id}: "
                f"the market-data spine normalizes this security's historical bars "
                f"to the current code, while the official Shenwan ledger carries "
                f"the {start_date} to {end_date} interval under the legacy code."
            ),
        )

    candidate_records = [
        _ManualIndustryAdjudicationRecord(
            security_id="001979.SZ",
            start_date="2015-12-31 00:00:00",
            end_date="2016-01-07 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="430000",
            source_type="listing_lag_backfill",
            evidence_url=str(stock_path),
            evidence_date="2016-01-07 00:00:00",
            available_at="2015-12-30 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Backfill the listing-week left-edge gap before the first official "
                "Shenwan node for 001979.SZ using the matching 2016-01-07 "
                "official node and the live Tushare continuity interval."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="000623.SZ",
            start_date="2014-07-01 00:00:00",
            end_date="2019-07-24 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2014-07-01 00:00:00",
            available_at="2014-07-01 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2014-07-01 to 2019-07-24 provider-gap run "
                "for 000623.SZ from the Shenwan change-node ledger: the official "
                "raw code changes into 370201 on 2014-07-01 and next changes to "
                "370102 on 2019-07-24, both mapping to sw2021_l1 370000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="000408.SZ",
            start_date="2018-07-13 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="220000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2018-07-13 00:00:00",
            available_at="2018-07-13 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2018-07-13 to 2021-07-30 provider-gap "
                "run for 000408.SZ from the Shenwan change-node ledger: the "
                "official raw code changes into 220306 on 2018-07-13 and next "
                "changes to 220804 on 2021-07-30, both mapping to sw2021_l1 "
                "220000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="000919.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2015-11-02 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2014-01-01 00:00:00",
            available_at="2014-02-21 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2014-02-21 to 2015-11-02 provider-gap "
                "run for 000919.SZ from the Shenwan change-node ledger: the "
                "2014 import-window carry-forward is anchored by the official "
                "2014-01-01 raw code 370601, and the next raw code 370201 on "
                "2015-11-02 also maps to sw2021_l1 370000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="000919.SZ",
            start_date="2015-11-02 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2015-11-02 00:00:00",
            available_at="2015-11-02 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2015-11-02 to 2021-07-30 provider-gap "
                "run for 000919.SZ from the Shenwan change-node ledger: raw "
                "code 370201 remains at sw2021_l1 370000 until the 2021-07-30 "
                "cutover anchor."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="000975.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2014-07-01 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="240000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2014-02-21 00:00:00",
            available_at="2014-02-21 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2014-02-21 to 2014-07-01 provider-gap "
                "run for 000975.SZ from the Shenwan change-node ledger: the "
                "official raw code changes into 240504 on 2014-02-21 and next "
                "changes to 240303 on 2014-07-01, both mapping to sw2021_l1 "
                "240000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="002332.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2011-01-10 00:00:00",
            available_at="2014-02-21 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2014-02-21 to 2021-07-30 provider-gap "
                "run for 002332.SZ from the Shenwan change-node ledger: the "
                "2014 import-window carry-forward is anchored by official raw "
                "code 370102 from 2011-01-10, mapping to sw2021_l1 370000 "
                "until the 2021-07-30 cutover anchor."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="002411.SZ",
            start_date="2016-04-14 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2016-04-14 00:00:00",
            available_at="2016-04-14 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2016-04-14 to 2021-07-30 provider-gap "
                "run for 002411.SZ from the Shenwan change-node ledger: the "
                "official raw code changes into 370102 on 2016-04-14 and next "
                "changes to 370402 on 2021-07-30, both mapping to sw2021_l1 "
                "370000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="600061.SH",
            start_date="2018-06-19 16:12:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="490000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2018-06-19 16:12:00",
            available_at="2018-06-19 16:12:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2018-06-19 16:12:00 to 2021-07-30 "
                "provider-gap run for 600061.SH from the Shenwan change-node "
                "ledger: the official raw code changes into 490101 on "
                "2018-06-19 16:12:00 and next changes to 490302 on 2021-07-30, "
                "both mapping to sw2021_l1 490000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="600575.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2017-05-20 01:33:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="420000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2003-03-28 00:00:00",
            available_at="2014-02-21 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2014-02-21 to 2017-05-20 01:33:00 "
                "provider-gap run for 600575.SH from the Shenwan change-node "
                "ledger: the 2014 import-window carry-forward is anchored by "
                "official raw code 420101 from 2003-03-28, and the next raw "
                "code 420801 on 2017-05-20 01:33:00 also maps to sw2021_l1 "
                "420000."
            ),
        ),
        external_left_edge_backfill(
            security_id="600651.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="270000",
            evidence_url=(
                "https://epaper.stcn.com/paper/zqsb/html/2014-09/27/content_615668.htm; "
                "https://static.cninfo.com.cn/finalpage/2016-09-28/1202732400.PDF"
            ),
            evidence_date="2014-09-27 00:00:00",
            available_at="2014-09-27 00:00:00",
            adjudication_note=(
                "Backfill the 600651.SH left-edge gap after online review: "
                "Securities Times listed 600651.SH under other electronics on "
                "2014-09-27, and a 2016 CNINFO disclosure using Shenwan "
                "classification included 600651.SH in the other-electronics peer "
                "set. The quarantined official 1990 placeholder raw code 270401 "
                "and the next official 2017-06-29 raw code 270302 both map to "
                "sw2021_l1 270000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="600575.SH",
            start_date="2017-05-20 01:33:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="420000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2017-05-20 01:33:00",
            available_at="2017-05-20 01:33:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2017-05-20 01:33:00 to 2021-07-30 "
                "provider-gap run for 600575.SH from the Shenwan change-node "
                "ledger: the official raw code changes into 420801 on "
                "2017-05-20 01:33:00 and next changes to 420903 on "
                "2021-07-30, both mapping to sw2021_l1 420000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="603456.SH",
            start_date="2021-07-30 00:00:00",
            end_date="2022-07-29 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2021-07-30 00:00:00",
            available_at="2021-07-30 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2021-07-30 to 2022-07-29 provider-gap "
                "run for 603456.SH from the Shenwan change-node ledger: the "
                "official raw code changes into 370603 on 2021-07-30 and next "
                "changes to 370101 on 2022-07-29, both mapping to sw2021_l1 "
                "370000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="603456.SH",
            start_date="2022-07-29 00:00:00",
            end_date="2023-07-04 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="370000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2022-07-29 00:00:00",
            available_at="2022-07-29 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2022-07-29 to 2023-07-04 provider-gap "
                "run for 603456.SH from the Shenwan change-node ledger: the "
                "official raw code changes into 370101 on 2022-07-29 and next "
                "changes to 370603 on 2023-07-04, both mapping to sw2021_l1 "
                "370000."
            ),
        ),
        _ManualIndustryAdjudicationRecord(
            security_id="603650.SH",
            start_date="2019-07-24 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_schema="sw2021_l1",
            industry_level="L1",
            industry_code="220000",
            source_type="provider_gap_confirmation",
            evidence_url=str(stock_path),
            evidence_date="2019-07-24 00:00:00",
            available_at="2019-07-24 00:00:00",
            confidence="medium",
            adjudication_note=(
                "Confirm the official 2019-07-24 to 2021-07-30 provider-gap "
                "run for 603650.SH from the Shenwan change-node ledger: the "
                "official raw code changes into 220309 on 2019-07-24 and next "
                "changes to 220604 on 2021-07-30, both mapping to "
                "sw2021_l1 220000."
            ),
        ),
        provider_gap_confirmation(
            security_id="000088.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="420000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="000417.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="450000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="000422.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="000541.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="270000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="002310.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2016-05-25 00:00:00",
            industry_code="620000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="002648.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="300285.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        security_code_alias_backfill(
            security_id="302132.SZ",
            legacy_security_id="300114.SZ",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="640000",
            evidence_date="2025-02-17 00:00:00",
        ),
        security_code_alias_backfill(
            security_id="302132.SZ",
            legacy_security_id="300114.SZ",
            start_date="2021-07-30 00:00:00",
            end_date="2025-02-17 00:00:00",
            industry_code="650000",
            evidence_date="2025-02-17 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="300114.SZ",
            start_date="2021-07-30 00:00:00",
            end_date="2025-02-28 00:00:00",
            industry_code="650000",
            evidence_date="2021-07-30 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="300450.SZ",
            start_date="2015-01-06 00:05:00",
            end_date="2019-07-24 00:00:00",
            industry_code="640000",
            evidence_date="2015-01-06 00:05:00",
        ),
        provider_gap_confirmation(
            security_id="600251.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2015-07-01 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600261.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2017-05-20 01:33:00",
            industry_code="270000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600409.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600426.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600488.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2019-07-24 00:00:00",
            industry_code="370000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600589.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600596.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2019-07-24 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600673.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2019-07-24 00:00:00",
            industry_code="240000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600803.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="220000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="600841.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="640000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="601010.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2019-07-24 00:00:00",
            industry_code="450000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="601020.SH",
            start_date="2016-03-23 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="240000",
            evidence_date="2016-03-23 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="601168.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2017-06-29 00:00:00",
            industry_code="240000",
            evidence_date="2014-02-21 00:00:00",
        ),
        provider_gap_confirmation(
            security_id="601212.SH",
            start_date="2017-01-05 21:39:00",
            end_date="2021-07-30 00:00:00",
            industry_code="240000",
            evidence_date="2017-01-05 21:39:00",
        ),
        provider_gap_confirmation(
            security_id="603993.SH",
            start_date="2014-02-21 00:00:00",
            end_date="2021-07-30 00:00:00",
            industry_code="240000",
            evidence_date="2014-02-21 00:00:00",
        ),
    ]
    return [
        record
        for record in candidate_records
        if _manual_adjudication_is_supported(
            record=record,
            official_industry_rows=official_industry_rows,
        )
    ]


def _manual_adjudication_is_supported(
    *,
    record: _ManualIndustryAdjudicationRecord,
    official_industry_rows: list[tuple[str, str, str, str, str | None]],
) -> bool:
    matching_rows = [
        row
        for row in official_industry_rows
        if row[0] == record.security_id
        and row[1] == record.industry_schema
        and row[2] == record.industry_code
    ]
    if record.source_type == "provider_gap_confirmation":
        if not matching_rows:
            return False
        return any(
            row[3] == record.start_date
            and (
                row[4] == record.end_date
                or (row[4] is None and record.end_date > record.start_date)
            )
            for row in matching_rows
        )
    if record.source_type == "external_left_edge_backfill":
        if not matching_rows:
            return False
        return record.evidence_url.startswith(("http://", "https://")) and any(
            row[3] == record.end_date for row in matching_rows
        )
    if record.source_type == "listing_lag_backfill":
        if not matching_rows:
            return False
        return any(row[3] == record.end_date for row in matching_rows)
    if record.source_type == "security_code_alias_backfill":
        if record.security_id != "302132.SZ":
            return False
        new_identity_rows = [
            row
            for row in official_industry_rows
            if row[0] == record.security_id
            and row[1] == record.industry_schema
        ]
        legacy_rows = [
            row
            for row in official_industry_rows
            if row[0] == "300114.SZ"
            and row[1] == record.industry_schema
            and row[2] == record.industry_code
        ]
        return bool(new_identity_rows) and any(
            row[3] <= record.start_date
            and (row[4] is None or row[4] >= record.end_date)
            for row in legacy_rows
        )
    return False


def _manual_adjudication_extends_effective_pit(record: _ManualIndustryAdjudicationRecord) -> bool:
    return record.source_type in {
        "external_left_edge_backfill",
        "listing_lag_backfill",
        "security_code_alias_backfill",
    }


def _apply_manual_industry_adjudications(
    *,
    official_industry_rows: list[tuple[str, str, str, str, str | None]],
    adjudication_records: list[_ManualIndustryAdjudicationRecord],
) -> tuple[list[tuple[str, str, str, str, str | None]], list[_ManualIndustryAdjudicationRecord]]:
    effective_rows = set(official_industry_rows)
    grouped: dict[tuple[str, str], list[tuple[str, str, str, str, str | None]]] = {}
    for row in official_industry_rows:
        grouped.setdefault((row[0], row[1]), []).append(row)

    applied_records: list[_ManualIndustryAdjudicationRecord] = []
    for record in adjudication_records:
        if not _manual_adjudication_extends_effective_pit(record):
            continue
        key = (record.security_id, record.industry_schema)
        candidate_rows = grouped.get(key, [])
        if not candidate_rows:
            continue
        if any(
            _industry_intervals_overlap(
                left_start=row[3],
                left_end=row[4],
                right_start=record.start_date,
                right_end=record.end_date,
            )
            for row in candidate_rows
        ):
            continue
        if record.source_type != "security_code_alias_backfill":
            if not any(
                row[2] == record.industry_code and row[3] == record.end_date
                for row in candidate_rows
            ):
                continue
        effective_rows.add(
            (
                record.security_id,
                record.industry_schema,
                record.industry_code,
                record.start_date,
                record.end_date,
            )
        )
        applied_records.append(record)

    return _normalize_industry_intervals(effective_rows), applied_records


def _industry_intervals_overlap(
    *,
    left_start: str,
    left_end: str | None,
    right_start: str,
    right_end: str,
) -> bool:
    normalized_left_end = left_end or "9999-12-31 23:59:59"
    return left_start < right_end and right_start < normalized_left_end


def _official_sw_industry_note(manual_adjudication_count: int) -> str:
    base_note = "conservative derived SW2021 PIT industry layer from the official Shenwan packet"
    if manual_adjudication_count <= 0:
        return base_note
    return (
        f"{base_note}; includes {manual_adjudication_count} explicit "
        "manual adjudication row(s)"
    )


def _resolve_official_sw_level_code(
    *,
    raw_industry_code: str,
    level: str,
    hierarchy_by_code: dict[str, dict[str, str]],
    crosswalk_level_maps: dict[str, dict[str, str]],
    empirical_level_maps: dict[str, dict[str, str]],
) -> str:
    direct_levels = hierarchy_by_code.get(raw_industry_code)
    if direct_levels:
        return direct_levels.get(level, "")
    crosswalk_levels = crosswalk_level_maps.get(level, {})
    if raw_industry_code in crosswalk_levels:
        return crosswalk_levels[raw_industry_code]
    empirical_levels = empirical_level_maps.get(level, {})
    return empirical_levels.get(raw_industry_code, "")


def _normalize_official_sw_security_id(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    digits = "".join(character for character in text if character.isdigit())
    if not digits:
        return ""
    symbol = digits.zfill(6)
    if symbol.startswith("6"):
        exchange = "SH"
    elif symbol.startswith(("4", "8")):
        exchange = "BJ"
    else:
        exchange = "SZ"
    return f"{symbol}.{exchange}"


def _normalize_official_sw_code(value: Any) -> str:
    text = _clean_text(value)
    if not text:
        return ""
    digits = "".join(character for character in text if character.isdigit())
    return digits.zfill(6) if digits else ""


def _normalize_official_sw_timestamp(value: Any) -> str | None:
    import pandas as pd

    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except TypeError:
        pass

    timestamp = pd.Timestamp(value)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def _timestamp_date_key(value: str) -> str:
    return value[:10].replace("-", "")


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


def _table_exists_current_db(conn: Any, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE database_name = current_database()
          AND table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


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
