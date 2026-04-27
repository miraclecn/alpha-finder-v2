from __future__ import annotations

from dataclasses import asdict, dataclass
import json
from pathlib import Path
import tomllib
from typing import Any

from .config_loader import PROJECT_ROOT
from .live_state import BenchmarkConstituent, BenchmarkStateArtifact, BenchmarkStateStep


JsonMap = dict[str, Any]
SUPPORTED_WEIGHTING_METHODS = {"float_mcap_proxy", "provider_weight"}


@dataclass(slots=True)
class BenchmarkStateBuildCaseDefinition:
    case_id: str
    description: str
    source_db_path: str
    output_path: str
    benchmark_id: str
    industry_schema: str
    start_date: str = ""
    end_date: str = ""
    weighting_method: str = "float_mcap_proxy"
    effective_time: str = "15:00:00+08:00"
    available_time: str = "15:30:00+08:00"

    @classmethod
    def from_toml(cls, data: JsonMap) -> "BenchmarkStateBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported benchmark state build case schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "benchmark_state_build_case":
            raise ValueError(
                f"Unsupported benchmark state build case type: {artifact_type}"
            )

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            source_db_path=str(data["source_db_path"]),
            output_path=str(data["output_path"]),
            benchmark_id=str(data["benchmark_id"]),
            industry_schema=str(data["industry_schema"]),
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            weighting_method=str(data.get("weighting_method", "float_mcap_proxy")),
            effective_time=str(data.get("effective_time", "15:00:00+08:00")),
            available_time=str(data.get("available_time", "15:30:00+08:00")),
        )


@dataclass(slots=True)
class LoadedBenchmarkStateBuildCase:
    definition: BenchmarkStateBuildCaseDefinition
    source_db_path: Path


@dataclass(slots=True)
class _BenchmarkMemberRow:
    trade_date: str
    security_id: str
    weight_value: float | None
    industry_code: str = ""


def load_benchmark_state_build_case(path: Path | str) -> LoadedBenchmarkStateBuildCase:
    definition = BenchmarkStateBuildCaseDefinition.from_toml(_read_toml(path))
    if definition.weighting_method not in SUPPORTED_WEIGHTING_METHODS:
        raise ValueError(
            "Benchmark state builder currently supports only "
            "weighting_method='float_mcap_proxy'."
        )
    if not definition.benchmark_id:
        raise ValueError("Benchmark state build case must define benchmark_id.")
    if not definition.industry_schema:
        raise ValueError("Benchmark state build case must define industry_schema.")
    if definition.start_date and definition.end_date and definition.start_date > definition.end_date:
        raise ValueError("Benchmark state build case start_date cannot be after end_date.")
    return LoadedBenchmarkStateBuildCase(
        definition=definition,
        source_db_path=_resolve_project_path(definition.source_db_path),
    )


def build_benchmark_state_artifact(
    loaded_case: LoadedBenchmarkStateBuildCase,
) -> BenchmarkStateArtifact:
    import duckdb

    conn = duckdb.connect(str(loaded_case.source_db_path), read_only=True)
    try:
        _validate_required_tables(conn, loaded_case.definition.weighting_method)
        trade_dates = _load_trade_dates(
            conn,
            start_date=loaded_case.definition.start_date,
            end_date=loaded_case.definition.end_date,
        )
        rows = _load_member_rows(
            conn,
            weighting_method=loaded_case.definition.weighting_method,
            benchmark_id=loaded_case.definition.benchmark_id,
            industry_schema=loaded_case.definition.industry_schema,
            start_date=loaded_case.definition.start_date,
            end_date=loaded_case.definition.end_date,
        )
    finally:
        conn.close()

    rows_by_date: dict[str, list[_BenchmarkMemberRow]] = {}
    for row in rows:
        rows_by_date.setdefault(row.trade_date, []).append(row)

    earliest_continuous_full_coverage_date = _earliest_continuous_full_industry_coverage_date(
        trade_dates=trade_dates,
        rows_by_date=rows_by_date,
    )
    steps: list[BenchmarkStateStep] = []
    for trade_date in trade_dates:
        date_rows = rows_by_date.get(trade_date, [])
        if not date_rows:
            raise ValueError(
                f"Missing benchmark membership coverage for {loaded_case.definition.benchmark_id} on {trade_date}"
            )
        missing_industry_row = next(
            (row for row in date_rows if not row.industry_code),
            None,
        )
        if missing_industry_row is not None:
            message = (
                "Missing PIT industry classification for benchmark member: "
                f"{missing_industry_row.security_id} on {trade_date}"
            )
            if (
                earliest_continuous_full_coverage_date
                and earliest_continuous_full_coverage_date > trade_date
            ):
                message += (
                    "; earliest continuous full PIT industry coverage date is "
                    f"{earliest_continuous_full_coverage_date}"
                )
            raise ValueError(message)
        steps.append(
            _build_step(
                trade_date=trade_date,
                rows=date_rows,
                weighting_method=loaded_case.definition.weighting_method,
                effective_time=loaded_case.definition.effective_time,
                available_time=loaded_case.definition.available_time,
            )
        )

    return BenchmarkStateArtifact(
        benchmark_id=loaded_case.definition.benchmark_id,
        classification=loaded_case.definition.industry_schema,
        weighting_method=loaded_case.definition.weighting_method,
        steps=steps,
    )


def write_benchmark_state_artifact(
    artifact: BenchmarkStateArtifact,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_ready(
        {
            "schema_version": 1,
            "artifact_type": "benchmark_state_history",
            "benchmark_id": artifact.benchmark_id,
            "classification": artifact.classification,
            "weighting_method": artifact.weighting_method,
            "steps": [asdict(step) for step in artifact.steps],
        }
    )
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _build_step(
    *,
    trade_date: str,
    rows: list[_BenchmarkMemberRow],
    weighting_method: str,
    effective_time: str,
    available_time: str,
) -> BenchmarkStateStep:
    seen_assets: set[str] = set()
    industry_weights: dict[str, float] = {}
    raw_weights: list[tuple[str, float, str]] = []
    total_raw_weight = 0.0
    for row in rows:
        if row.security_id in seen_assets:
            raise ValueError(
                f"Duplicate active benchmark membership for {row.security_id} on {trade_date}"
            )
        seen_assets.add(row.security_id)
        if not row.industry_code:
            raise ValueError(
                f"Missing PIT industry classification for benchmark member: {row.security_id} on {trade_date}"
            )
        if row.weight_value is None or row.weight_value <= 0.0:
            if weighting_method == "provider_weight":
                raise ValueError(
                    f"Missing positive provider weight for benchmark member: {row.security_id} on {trade_date}"
                )
            raise ValueError(
                f"Missing positive float market cap for benchmark member: {row.security_id} on {trade_date}"
            )
        total_raw_weight += row.weight_value
        raw_weights.append((row.security_id, row.weight_value, row.industry_code))

    if total_raw_weight <= 0.0:
        if weighting_method == "provider_weight":
            raise ValueError(f"Benchmark state requires positive provider weight sum on {trade_date}")
        raise ValueError(f"Benchmark state requires positive total float market cap on {trade_date}")

    constituents: list[BenchmarkConstituent] = []
    for security_id, raw_weight, industry_code in raw_weights:
        weight = raw_weight / total_raw_weight
        industry_weights[industry_code] = industry_weights.get(industry_code, 0.0) + weight
        constituents.append(
            BenchmarkConstituent(
                asset_id=security_id,
                weight=weight,
                industry=industry_code,
            )
        )

    constituents.sort(key=lambda item: (-item.weight, item.asset_id))
    industry_weights = {
        industry: industry_weights[industry]
        for industry in sorted(industry_weights)
    }
    return BenchmarkStateStep(
        trade_date=trade_date,
        effective_at=_trade_timestamp(trade_date, effective_time),
        available_at=_trade_timestamp(trade_date, available_time),
        industry_weights=industry_weights,
        constituents=constituents,
    )


def _earliest_continuous_full_industry_coverage_date(
    *,
    trade_dates: list[str],
    rows_by_date: dict[str, list[_BenchmarkMemberRow]],
) -> str:
    earliest_trade_date = ""
    for trade_date in reversed(trade_dates):
        date_rows = rows_by_date.get(trade_date, [])
        if not date_rows or any(not row.industry_code for row in date_rows):
            break
        earliest_trade_date = trade_date
    return earliest_trade_date


def _load_trade_dates(conn: Any, *, start_date: str, end_date: str) -> list[str]:
    filters: list[str] = []
    params: list[str] = []
    if start_date:
        filters.append("trade_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("trade_date <= ?")
        params.append(end_date)
    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)
    rows = conn.execute(
        f"""
        SELECT trade_date
        FROM market_trade_calendar
        {where_clause}
        ORDER BY trade_date
        """,
        params,
    ).fetchall()
    trade_dates = [str(trade_date) for (trade_date,) in rows]
    if not trade_dates:
        raise ValueError("Benchmark state build case calendar window produced no trade dates.")
    return trade_dates


def _load_member_rows(
    conn: Any,
    *,
    weighting_method: str,
    benchmark_id: str,
    industry_schema: str,
    start_date: str,
    end_date: str,
) -> list[_BenchmarkMemberRow]:
    if weighting_method == "provider_weight":
        return _load_provider_weight_rows(
            conn,
            benchmark_id=benchmark_id,
            industry_schema=industry_schema,
            start_date=start_date,
            end_date=end_date,
        )

    filters: list[str] = []
    params: list[str] = []
    if start_date:
        filters.append("c.trade_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("c.trade_date <= ?")
        params.append(end_date)

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    query = f"""
        WITH calendar AS (
            SELECT c.trade_date
            FROM market_trade_calendar AS c
            {where_clause}
        )
        SELECT
            c.trade_date,
            membership.security_id,
            bar.float_mcap_cny,
            industry.industry_code
        FROM calendar AS c
        INNER JOIN benchmark_membership_pit AS membership
            ON membership.benchmark_id = ?
           AND {_timestamp_sql('membership.effective_at')} <= strptime(c.trade_date, '%Y%m%d')
           AND (
                {_timestamp_sql('membership.removed_at')} IS NULL
                OR {_timestamp_sql('membership.removed_at')} > strptime(c.trade_date, '%Y%m%d')
           )
        LEFT JOIN daily_bar_pit AS bar
            ON bar.security_id = membership.security_id
           AND bar.trade_date = c.trade_date
        LEFT JOIN industry_classification_pit AS industry
            ON industry.security_id = membership.security_id
           AND industry.industry_schema = ?
           AND {_timestamp_sql('industry.effective_at')} <= strptime(c.trade_date, '%Y%m%d')
           AND (
                {_timestamp_sql('industry.removed_at')} IS NULL
                OR {_timestamp_sql('industry.removed_at')} > strptime(c.trade_date, '%Y%m%d')
           )
        ORDER BY c.trade_date, membership.security_id
    """
    rows = conn.execute(query, [*params, benchmark_id, industry_schema]).fetchall()
    return [
        _BenchmarkMemberRow(
            trade_date=str(trade_date),
            security_id=str(security_id),
            weight_value=float(float_mcap_cny) if float_mcap_cny is not None else None,
            industry_code=str(industry_code or ""),
        )
        for trade_date, security_id, float_mcap_cny, industry_code in rows
    ]


def _load_provider_weight_rows(
    conn: Any,
    *,
    benchmark_id: str,
    industry_schema: str,
    start_date: str,
    end_date: str,
) -> list[_BenchmarkMemberRow]:
    filters: list[str] = []
    params: list[str] = []
    if start_date:
        filters.append("c.trade_date >= ?")
        params.append(start_date)
    if end_date:
        filters.append("c.trade_date <= ?")
        params.append(end_date)

    where_clause = ""
    if filters:
        where_clause = "WHERE " + " AND ".join(filters)

    query = f"""
        WITH calendar AS (
            SELECT c.trade_date
            FROM market_trade_calendar AS c
            {where_clause}
        ),
        latest_snapshots AS (
            SELECT
                c.trade_date,
                MAX(w.trade_date) AS snapshot_date
            FROM calendar AS c
            LEFT JOIN benchmark_weight_snapshot_pit AS w
                ON w.benchmark_id = ?
               AND w.trade_date <= c.trade_date
            GROUP BY c.trade_date
        )
        SELECT
            snapshots.trade_date,
            weights.security_id,
            weights.weight,
            industry.industry_code
        FROM latest_snapshots AS snapshots
        INNER JOIN benchmark_weight_snapshot_pit AS weights
            ON weights.benchmark_id = ?
           AND weights.trade_date = snapshots.snapshot_date
        LEFT JOIN industry_classification_pit AS industry
            ON industry.security_id = weights.security_id
           AND industry.industry_schema = ?
           AND {_timestamp_sql('industry.effective_at')} <= strptime(snapshots.trade_date, '%Y%m%d')
           AND (
                {_timestamp_sql('industry.removed_at')} IS NULL
                OR {_timestamp_sql('industry.removed_at')} > strptime(snapshots.trade_date, '%Y%m%d')
           )
        ORDER BY snapshots.trade_date, weights.security_id
    """
    rows = conn.execute(query, [*params, benchmark_id, benchmark_id, industry_schema]).fetchall()
    return [
        _BenchmarkMemberRow(
            trade_date=str(trade_date),
            security_id=str(security_id),
            weight_value=float(weight_value) if weight_value is not None else None,
            industry_code=str(industry_code or ""),
        )
        for trade_date, security_id, weight_value, industry_code in rows
    ]


def _timestamp_sql(column_name: str) -> str:
    return f"""
        CASE
            WHEN {column_name} IS NULL OR trim(CAST({column_name} AS VARCHAR)) = '' THEN NULL
            WHEN length(trim(CAST({column_name} AS VARCHAR))) = 8
                THEN strptime(trim(CAST({column_name} AS VARCHAR)), '%Y%m%d')
            ELSE CAST({column_name} AS TIMESTAMP)
        END
    """


def _validate_required_tables(conn: Any, weighting_method: str) -> None:
    missing_tables = sorted(
        table_name
        for table_name in (
            "market_trade_calendar",
            "industry_classification_pit",
            *(
                ("daily_bar_pit", "benchmark_membership_pit")
                if weighting_method == "float_mcap_proxy"
                else ("benchmark_weight_snapshot_pit",)
            ),
        )
        if not _table_exists(conn, table_name)
    )
    if missing_tables:
        raise ValueError(
            "Benchmark state builder requires source_db tables: "
            + ", ".join(missing_tables)
        )

    if weighting_method == "float_mcap_proxy":
        _validate_required_columns(
            conn,
            "daily_bar_pit",
            {"security_id", "trade_date", "float_mcap_cny"},
        )
        _validate_required_columns(
            conn,
            "benchmark_membership_pit",
            {"benchmark_id", "security_id", "effective_at", "removed_at"},
        )
    else:
        _validate_required_columns(
            conn,
            "benchmark_weight_snapshot_pit",
            {"benchmark_id", "security_id", "trade_date", "weight"},
        )
    _validate_required_columns(
        conn,
        "industry_classification_pit",
        {"security_id", "industry_schema", "industry_code", "effective_at", "removed_at"},
    )
    _validate_required_columns(conn, "market_trade_calendar", {"trade_date"})


def _validate_required_columns(conn: Any, table_name: str, required_columns: set[str]) -> None:
    columns = _table_columns(conn, table_name)
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            f"Benchmark state builder requires {table_name} columns: {', '.join(missing_columns)}"
        )


def _table_exists(conn: Any, table_name: str) -> bool:
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


def _table_columns(conn: Any, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM duckdb_columns()
        WHERE database_name = current_database()
          AND table_name = ?
        """,
        [table_name],
    ).fetchall()
    return {str(column_name) for (column_name,) in rows}


def _trade_timestamp(trade_date: str, time_value: str) -> str:
    return f"{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:8]}T{time_value}"


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_toml(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("rb") as handle:
        return tomllib.load(handle)


def _json_ready(value: object) -> object:
    if isinstance(value, float):
        return round(value, 12)
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    return value
