from __future__ import annotations

import argparse
import csv
from dataclasses import asdict, dataclass, field
import json
from pathlib import Path
import time
from typing import Any, Iterable

from .reference_data_staging import INDUSTRY_LEVEL_DEFINITIONS, load_tushare_token


DEFAULT_REFERENCE_DB = "output/pit_reference_staging.duckdb"
DEFAULT_RESEARCH_DB = "output/research_source.duckdb"
DEFAULT_BENCHMARK_ID = "CSI 800"
DEFAULT_INDUSTRY_SCHEMA = "sw2021_l1"
DEFAULT_INDUSTRY_LEVEL = "L1"
DEFAULT_TUSHARE_CACHE = "output/audits/cache/tushare_index_member_all_l1_intervals.csv"
DEFAULT_AKSHARE_CACHE = "output/audits/cache/akshare_sw_hist_l1_intervals.csv"
DEFAULT_GAP_EXAMPLE_LIMIT = 50
SCHEMA_TRANSITION_DATES = ("20140221", "20210730")

IntervalRow = tuple[str, str, str, str | None]


@dataclass(slots=True, frozen=True)
class ConstituentDay:
    trade_date: str
    security_id: str


@dataclass(slots=True, frozen=True)
class CoverageGapRow:
    trade_date: str
    security_id: str
    classification: str
    staged_exclusive: bool
    staged_inclusive: bool
    live_tushare_exclusive: bool
    live_tushare_inclusive: bool
    akshare: bool
    staged_industry_code: str = ""
    live_tushare_industry_code: str = ""
    akshare_industry_code: str = ""


@dataclass(slots=True, frozen=True)
class BridgeFillRunDecision:
    security_id: str
    start_date: str
    end_date: str
    gap_trade_days: int
    decision: str
    imputed_industry_code: str = ""
    left_live_industry_code: str = ""
    left_live_removed_at: str = ""
    right_live_industry_code: str = ""
    right_live_effective_at: str = ""
    akshare_codes_in_gap: list[str] = field(default_factory=list)


@dataclass(slots=True)
class CoverageSummary:
    constituent_days_total: int
    staged_exclusive_covered: int
    staged_inclusive_covered: int
    live_tushare_exclusive_covered: int
    live_tushare_inclusive_covered: int
    akshare_covered: int
    gap_classification_counts: dict[str, int]
    gap_rows: list[CoverageGapRow]
    bridge_fill_run_counts: dict[str, int]
    bridge_fill_day_counts: dict[str, int]
    bridge_fill_runs: list[BridgeFillRunDecision]


def build_akshare_intervals(rows: Iterable[dict[str, Any]]) -> list[IntervalRow]:
    grouped: dict[str, dict[str, tuple[str, str]]] = {}
    for row in rows:
        security_id = _normalize_security_id(row.get("symbol"))
        effective_at = _normalize_date(row.get("start_date"))
        industry_code = _normalize_text(row.get("industry_code"))
        update_time = _normalize_date(row.get("update_time")) or ""
        if not security_id or not effective_at or not industry_code:
            continue
        existing = grouped.setdefault(security_id, {}).get(effective_at)
        if existing is None or update_time >= existing[0]:
            grouped[security_id][effective_at] = (update_time, industry_code)

    intervals: list[IntervalRow] = []
    for security_id in sorted(grouped):
        start_dates = sorted(grouped[security_id])
        for index, effective_at in enumerate(start_dates):
            removed_at = start_dates[index + 1] if index + 1 < len(start_dates) else None
            intervals.append(
                (
                    security_id,
                    grouped[security_id][effective_at][1],
                    effective_at,
                    removed_at,
                )
            )
    return intervals


def build_tushare_intervals(
    rows: Iterable[dict[str, Any]],
    *,
    industry_level: str,
) -> list[IntervalRow]:
    if industry_level not in INDUSTRY_LEVEL_DEFINITIONS:
        raise ValueError(f"Unsupported Tushare industry level: {industry_level}")
    _, code_field = INDUSTRY_LEVEL_DEFINITIONS[industry_level]

    intervals: set[IntervalRow] = set()
    for row in rows:
        security_id = _normalize_security_id(row.get("ts_code"))
        effective_at = _normalize_date(row.get("in_date"))
        industry_code = _normalize_text(row.get(code_field))
        if not security_id or not effective_at or not industry_code:
            continue
        intervals.add(
            (
                security_id,
                industry_code,
                effective_at,
                _normalize_date(row.get("out_date")),
            )
        )
    return sorted(intervals, key=lambda item: (item[0], item[2], item[1], item[3] or ""))


def audit_constituent_days(
    *,
    constituent_days: list[ConstituentDay],
    staged_intervals: list[IntervalRow],
    live_tushare_intervals: list[IntervalRow],
    akshare_intervals: list[IntervalRow],
    manual_adjudication_intervals: list[IntervalRow] | None = None,
) -> CoverageSummary:
    staged_lookup = _index_intervals(staged_intervals)
    live_tushare_lookup = _index_intervals(live_tushare_intervals)
    akshare_lookup = _index_intervals(akshare_intervals)
    manual_adjudication_lookup = _index_intervals(manual_adjudication_intervals or [])

    staged_exclusive_covered = 0
    staged_inclusive_covered = 0
    live_tushare_exclusive_covered = 0
    live_tushare_inclusive_covered = 0
    akshare_covered = 0
    gap_counts: dict[str, int] = {}
    gap_rows: list[CoverageGapRow] = []

    for row in constituent_days:
        staged_exclusive_code = _lookup_active_industry(
            staged_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=False,
        )
        staged_inclusive_code = _lookup_active_industry(
            staged_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=True,
        )
        live_tushare_exclusive_code = _lookup_active_industry(
            live_tushare_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=False,
        )
        live_tushare_inclusive_code = _lookup_active_industry(
            live_tushare_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=True,
        )
        akshare_code = _lookup_active_industry(
            akshare_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=False,
        )

        staged_exclusive_covered += int(bool(staged_exclusive_code))
        staged_inclusive_covered += int(bool(staged_inclusive_code))
        live_tushare_exclusive_covered += int(bool(live_tushare_exclusive_code))
        live_tushare_inclusive_covered += int(bool(live_tushare_inclusive_code))
        akshare_covered += int(bool(akshare_code))

        staged_industry_code = staged_exclusive_code or staged_inclusive_code
        manual_adjudicated_code = _lookup_active_industry(
            manual_adjudication_lookup,
            security_id=row.security_id,
            trade_date=row.trade_date,
            inclusive_end=False,
        )

        classification = _classify_gap(
            staged_exclusive=bool(staged_exclusive_code),
            staged_inclusive=bool(staged_inclusive_code),
            live_tushare_exclusive=bool(live_tushare_exclusive_code),
            live_tushare_inclusive=bool(live_tushare_inclusive_code),
            akshare=bool(akshare_code),
            staged_industry_code=staged_industry_code,
            manual_adjudicated_code=manual_adjudicated_code,
        )
        if classification:
            gap_counts[classification] = gap_counts.get(classification, 0) + 1
            gap_rows.append(
                CoverageGapRow(
                    trade_date=row.trade_date,
                    security_id=row.security_id,
                    classification=classification,
                    staged_exclusive=bool(staged_exclusive_code),
                    staged_inclusive=bool(staged_inclusive_code),
                    live_tushare_exclusive=bool(live_tushare_exclusive_code),
                    live_tushare_inclusive=bool(live_tushare_inclusive_code),
                    akshare=bool(akshare_code),
                    staged_industry_code=staged_industry_code,
                    live_tushare_industry_code=(
                        live_tushare_exclusive_code or live_tushare_inclusive_code
                    ),
                    akshare_industry_code=akshare_code,
                )
            )

    bridge_fill_runs = _derive_bridge_fill_runs(
        constituent_days=constituent_days,
        gap_rows=gap_rows,
        live_tushare_intervals=live_tushare_intervals,
        akshare_lookup=akshare_lookup,
    )
    bridge_fill_run_counts: dict[str, int] = {}
    bridge_fill_day_counts: dict[str, int] = {}
    for run in bridge_fill_runs:
        bridge_fill_run_counts[run.decision] = bridge_fill_run_counts.get(run.decision, 0) + 1
        bridge_fill_day_counts[run.decision] = (
            bridge_fill_day_counts.get(run.decision, 0) + run.gap_trade_days
        )

    return CoverageSummary(
        constituent_days_total=len(constituent_days),
        staged_exclusive_covered=staged_exclusive_covered,
        staged_inclusive_covered=staged_inclusive_covered,
        live_tushare_exclusive_covered=live_tushare_exclusive_covered,
        live_tushare_inclusive_covered=live_tushare_inclusive_covered,
        akshare_covered=akshare_covered,
        gap_classification_counts=gap_counts,
        gap_rows=gap_rows,
        bridge_fill_run_counts=bridge_fill_run_counts,
        bridge_fill_day_counts=bridge_fill_day_counts,
        bridge_fill_runs=bridge_fill_runs,
    )


def run_one_shot_sw_industry_pit_audit(
    *,
    reference_db: str | Path,
    research_db: str | Path,
    benchmark_id: str,
    industry_schema: str,
    industry_level: str,
    start_date: str,
    end_date: str,
    output_json: str | Path,
    output_gap_csv: str | Path,
    tushare_cache: str | Path = DEFAULT_TUSHARE_CACHE,
    akshare_cache: str | Path = DEFAULT_AKSHARE_CACHE,
    refresh_tushare: bool = False,
    refresh_akshare: bool = False,
    tushare_token: str | None = None,
    tushare_page_size: int = 3000,
    tushare_pause_seconds: float = 0.35,
    gap_example_limit: int = DEFAULT_GAP_EXAMPLE_LIMIT,
) -> dict[str, Any]:
    if not start_date or not end_date:
        raise ValueError("The one-shot audit requires explicit start_date and end_date.")
    if start_date > end_date:
        raise ValueError("Audit start_date cannot be after end_date.")

    reference_path = _resolve_path(reference_db)
    research_path = _resolve_path(research_db)
    output_json_path = _resolve_path(output_json)
    output_gap_csv_path = _resolve_path(output_gap_csv)
    tushare_cache_path = _resolve_path(tushare_cache)
    akshare_cache_path = _resolve_path(akshare_cache)

    staged_intervals = _load_staged_intervals(
        reference_db_path=reference_path,
        industry_schema=industry_schema,
    )
    constituent_days = _load_constituent_days(
        research_db_path=research_path,
        benchmark_id=benchmark_id,
        start_date=start_date,
        end_date=end_date,
    )
    live_tushare_intervals = _load_or_fetch_tushare_intervals(
        cache_path=tushare_cache_path,
        refresh=refresh_tushare,
        token=tushare_token,
        page_size=tushare_page_size,
        pause_seconds=tushare_pause_seconds,
        industry_level=industry_level,
    )
    akshare_intervals = _load_or_fetch_akshare_intervals(
        cache_path=akshare_cache_path,
        refresh=refresh_akshare,
    )

    summary = audit_constituent_days(
        constituent_days=constituent_days,
        staged_intervals=staged_intervals,
        live_tushare_intervals=live_tushare_intervals,
        akshare_intervals=akshare_intervals,
        manual_adjudication_intervals=_load_manual_adjudication_intervals(
            reference_db_path=reference_path,
            industry_schema=industry_schema,
        ),
    )

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_gap_csv_path.parent.mkdir(parents=True, exist_ok=True)
    _write_gap_rows_csv(output_gap_csv_path, summary.gap_rows)

    payload = {
        "schema_version": 2,
        "artifact_type": "sw_industry_pit_one_shot_audit",
        "benchmark_id": benchmark_id,
        "industry_schema": industry_schema,
        "industry_level": industry_level,
        "audit_window": {
            "start_date": start_date,
            "end_date": end_date,
            "constituent_days": summary.constituent_days_total,
            "benchmark_symbols": len({row.security_id for row in constituent_days}),
        },
        "source_rows": {
            "staged_intervals": len(staged_intervals),
            "live_tushare_intervals": len(live_tushare_intervals),
            "akshare_intervals": len(akshare_intervals),
        },
        "comparison_mode": {
            "coverage": "presence_only",
            "industry_code_equality": False,
            "note": (
                "AKShare and Tushare industry-code vocabularies are not treated as "
                "directly interchangeable here; AKShare is used only as PIT "
                "history-coverage evidence for the stock/date."
            ),
        },
        "coverage_summary": {
            "staged_exclusive_covered": summary.staged_exclusive_covered,
            "staged_inclusive_covered": summary.staged_inclusive_covered,
            "live_tushare_exclusive_covered": summary.live_tushare_exclusive_covered,
            "live_tushare_inclusive_covered": summary.live_tushare_inclusive_covered,
            "akshare_covered": summary.akshare_covered,
            "staged_exclusive_ratio": _safe_ratio(
                summary.staged_exclusive_covered,
                summary.constituent_days_total,
            ),
            "staged_inclusive_ratio": _safe_ratio(
                summary.staged_inclusive_covered,
                summary.constituent_days_total,
            ),
            "live_tushare_exclusive_ratio": _safe_ratio(
                summary.live_tushare_exclusive_covered,
                summary.constituent_days_total,
            ),
            "live_tushare_inclusive_ratio": _safe_ratio(
                summary.live_tushare_inclusive_covered,
                summary.constituent_days_total,
            ),
            "akshare_ratio": _safe_ratio(
                summary.akshare_covered,
                summary.constituent_days_total,
            ),
        },
        "gap_classification_counts": summary.gap_classification_counts,
        "gap_rows_count": len(summary.gap_rows),
        "gap_examples": [asdict(row) for row in summary.gap_rows[:gap_example_limit]],
        "derived_bridge_fill_summary": {
            "mode": "derived_only",
            "applies_to": "provider_gap_runs_with_live_tushare_inclusive_false",
            "transition_dates": list(SCHEMA_TRANSITION_DATES),
            "run_count": len(summary.bridge_fill_runs),
            "run_counts": summary.bridge_fill_run_counts,
            "day_counts": summary.bridge_fill_day_counts,
            "eligible_run_count": summary.bridge_fill_run_counts.get(
                "eligible_bridge_fill",
                0,
            ),
            "eligible_day_count": summary.bridge_fill_day_counts.get(
                "eligible_bridge_fill",
                0,
            ),
            "blocked_run_count": sum(
                count
                for decision, count in summary.bridge_fill_run_counts.items()
                if decision != "eligible_bridge_fill"
            ),
            "blocked_day_count": sum(
                count
                for decision, count in summary.bridge_fill_day_counts.items()
                if decision != "eligible_bridge_fill"
            ),
            "note": (
                "Bridge-fill decisions are a derived audit layer for residual "
                "provider gaps only. They do not replace raw PIT provider truth."
            ),
        },
        "derived_bridge_fill_examples": [
            asdict(run) for run in summary.bridge_fill_runs[:gap_example_limit]
        ],
        "output_gap_csv": str(output_gap_csv_path),
        "cache_paths": {
            "tushare_intervals": str(tushare_cache_path),
            "akshare_intervals": str(akshare_cache_path),
        },
        "audit_rule": {
            "akshare_role": "amber_audit_only",
            "bridge_fill_policy": (
                "If a residual provider gap is bracketed by the same live Tushare "
                "industry code on both sides, does not cross a schema-transition "
                "date, and AKShare does not show multiple codes inside the gap, "
                "the gap may be marked eligible_bridge_fill as derived evidence "
                "only."
            ),
            "operational_note": (
                "AKShare is fetched at most once per refresh and otherwise reused "
                "from cache; long audit runs should be checked no more frequently "
                "than every 30 minutes."
            ),
        },
    }
    output_json_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run a one-shot PIT SW industry audit against staged, Tushare, and AKShare data.",
    )
    parser.add_argument("--reference-db", default=DEFAULT_REFERENCE_DB)
    parser.add_argument("--research-db", default=DEFAULT_RESEARCH_DB)
    parser.add_argument("--benchmark-id", default=DEFAULT_BENCHMARK_ID)
    parser.add_argument("--industry-schema", default=DEFAULT_INDUSTRY_SCHEMA)
    parser.add_argument("--industry-level", default=DEFAULT_INDUSTRY_LEVEL)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-gap-csv", required=True)
    parser.add_argument("--tushare-cache", default=DEFAULT_TUSHARE_CACHE)
    parser.add_argument("--akshare-cache", default=DEFAULT_AKSHARE_CACHE)
    parser.add_argument("--refresh-tushare", action="store_true")
    parser.add_argument("--refresh-akshare", action="store_true")
    parser.add_argument("--tushare-page-size", type=int, default=3000)
    parser.add_argument("--tushare-pause-seconds", type=float, default=0.35)
    args = parser.parse_args(argv)

    payload = run_one_shot_sw_industry_pit_audit(
        reference_db=args.reference_db,
        research_db=args.research_db,
        benchmark_id=args.benchmark_id,
        industry_schema=args.industry_schema,
        industry_level=args.industry_level,
        start_date=args.start_date,
        end_date=args.end_date,
        output_json=args.output_json,
        output_gap_csv=args.output_gap_csv,
        tushare_cache=args.tushare_cache,
        akshare_cache=args.akshare_cache,
        refresh_tushare=args.refresh_tushare,
        refresh_akshare=args.refresh_akshare,
        tushare_page_size=args.tushare_page_size,
        tushare_pause_seconds=args.tushare_pause_seconds,
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


def _load_or_fetch_tushare_intervals(
    *,
    cache_path: Path,
    refresh: bool,
    token: str | None,
    page_size: int,
    pause_seconds: float,
    industry_level: str,
) -> list[IntervalRow]:
    if cache_path.exists() and not refresh:
        return _read_interval_rows_csv(cache_path)

    import tushare as ts

    client = ts.pro_api(load_tushare_token(token))
    rows: list[dict[str, Any]] = []
    for is_new in ("N", "Y"):
        offset = 0
        while True:
            frame = client.index_member_all(is_new=is_new, offset=offset, limit=page_size)
            page_rows = _dataframe_rows(frame)
            if not page_rows:
                break
            rows.extend(page_rows)
            offset += len(page_rows)
            if len(page_rows) < page_size:
                break
            if pause_seconds > 0:
                time.sleep(pause_seconds)

    intervals = build_tushare_intervals(rows, industry_level=industry_level)
    _write_interval_rows_csv(cache_path, intervals)
    return intervals


def _load_or_fetch_akshare_intervals(
    *,
    cache_path: Path,
    refresh: bool,
) -> list[IntervalRow]:
    if cache_path.exists() and not refresh:
        return _read_interval_rows_csv(cache_path)

    import akshare as ak

    frame = ak.stock_industry_clf_hist_sw()
    intervals = build_akshare_intervals(_dataframe_rows(frame))
    _write_interval_rows_csv(cache_path, intervals)
    return intervals


def _load_staged_intervals(
    *,
    reference_db_path: Path,
    industry_schema: str,
) -> list[IntervalRow]:
    import duckdb

    conn = duckdb.connect(str(reference_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT security_id, industry_code, effective_at, removed_at
            FROM industry_classification_pit
            WHERE industry_schema = ?
            ORDER BY security_id, effective_at, industry_code
            """,
            [industry_schema],
        ).fetchall()
    finally:
        conn.close()
    return [
        (
            _normalize_security_id(security_id),
            _normalize_text(industry_code),
            _normalize_date(effective_at) or "",
            _normalize_date(removed_at),
        )
        for security_id, industry_code, effective_at, removed_at in rows
        if _normalize_security_id(security_id) and _normalize_date(effective_at)
    ]


def _load_manual_adjudication_intervals(
    *,
    reference_db_path: Path,
    industry_schema: str,
) -> list[IntervalRow]:
    import duckdb

    conn = duckdb.connect(str(reference_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT security_id, industry_code, start_date, end_date
            FROM industry_classification_pit_manual_adjudication
            WHERE industry_schema = ?
            ORDER BY security_id, start_date, industry_code
            """,
            [industry_schema],
        ).fetchall()
    except duckdb.Error:
        rows = []
    finally:
        conn.close()
    return [
        (
            _normalize_security_id(security_id),
            _normalize_text(industry_code),
            _normalize_date(start_date) or "",
            _normalize_date(end_date),
        )
        for security_id, industry_code, start_date, end_date in rows
        if _normalize_security_id(security_id) and _normalize_date(start_date)
    ]


def _load_constituent_days(
    *,
    research_db_path: Path,
    benchmark_id: str,
    start_date: str,
    end_date: str,
) -> list[ConstituentDay]:
    import duckdb

    conn = duckdb.connect(str(research_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            WITH calendar AS (
                SELECT trade_date
                FROM market_trade_calendar
                WHERE trade_date >= ? AND trade_date <= ?
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
            SELECT snapshots.trade_date, weights.security_id
            FROM latest_snapshots AS snapshots
            INNER JOIN benchmark_weight_snapshot_pit AS weights
                ON weights.benchmark_id = ?
               AND weights.trade_date = snapshots.snapshot_date
            ORDER BY snapshots.trade_date, weights.security_id
            """,
            [start_date, end_date, benchmark_id, benchmark_id],
        ).fetchall()
    finally:
        conn.close()
    constituent_days = [
        ConstituentDay(
            trade_date=_normalize_date(trade_date) or "",
            security_id=_normalize_security_id(security_id),
        )
        for trade_date, security_id in rows
        if _normalize_date(trade_date) and _normalize_security_id(security_id)
    ]
    if not constituent_days:
        raise ValueError("Audit window produced no benchmark constituent days.")
    return constituent_days


def _index_intervals(intervals: list[IntervalRow]) -> dict[str, list[IntervalRow]]:
    grouped = _group_intervals_by_security(intervals)
    for security_id in grouped:
        grouped[security_id].sort(key=lambda item: (item[2], item[1]), reverse=True)
    return grouped


def _group_intervals_by_security(intervals: list[IntervalRow]) -> dict[str, list[IntervalRow]]:
    grouped: dict[str, list[IntervalRow]] = {}
    for interval in intervals:
        grouped.setdefault(interval[0], []).append(interval)
    return grouped


def _lookup_active_industry(
    lookup: dict[str, list[IntervalRow]],
    *,
    security_id: str,
    trade_date: str,
    inclusive_end: bool,
) -> str:
    for _, industry_code, effective_at, removed_at in lookup.get(security_id, []):
        if effective_at > trade_date:
            continue
        if removed_at is None:
            return industry_code
        if inclusive_end and trade_date <= removed_at:
            return industry_code
        if not inclusive_end and trade_date < removed_at:
            return industry_code
    return ""


def _classify_gap(
    *,
    staged_exclusive: bool,
    staged_inclusive: bool,
    live_tushare_exclusive: bool,
    live_tushare_inclusive: bool,
    akshare: bool,
    staged_industry_code: str,
    manual_adjudicated_code: str,
) -> str | None:
    if staged_exclusive and live_tushare_exclusive:
        return None
    if not staged_exclusive and staged_inclusive:
        return "staging_end_date_boundary"
    if not staged_inclusive and live_tushare_inclusive:
        return "staging_sync_gap"
    if not live_tushare_exclusive and live_tushare_inclusive:
        return "tushare_end_date_boundary"
    if not live_tushare_inclusive and akshare:
        if manual_adjudicated_code and manual_adjudicated_code == staged_industry_code:
            return "manual_adjudicated_provider_gap"
        return "tushare_provider_gap_akshare_covers"
    return "unresolved_gap"


def _derive_bridge_fill_runs(
    *,
    constituent_days: list[ConstituentDay],
    gap_rows: list[CoverageGapRow],
    live_tushare_intervals: list[IntervalRow],
    akshare_lookup: dict[str, list[IntervalRow]],
) -> list[BridgeFillRunDecision]:
    provider_gap_days = {
        (row.security_id, row.trade_date)
        for row in gap_rows
        if row.classification == "tushare_provider_gap_akshare_covers"
    }
    if not provider_gap_days:
        return []

    security_trade_dates: dict[str, list[str]] = {}
    for row in constituent_days:
        security_trade_dates.setdefault(row.security_id, []).append(row.trade_date)

    live_intervals_by_security = _group_intervals_by_security(live_tushare_intervals)
    decisions: list[BridgeFillRunDecision] = []
    for security_id, trade_dates in security_trade_dates.items():
        run_dates: list[str] = []
        for trade_date in trade_dates:
            if (security_id, trade_date) in provider_gap_days:
                run_dates.append(trade_date)
                continue
            if run_dates:
                decisions.append(
                    _classify_bridge_fill_run(
                        security_id=security_id,
                        run_dates=run_dates,
                        live_intervals=live_intervals_by_security.get(security_id, []),
                        akshare_lookup=akshare_lookup,
                    )
                )
                run_dates = []
        if run_dates:
            decisions.append(
                _classify_bridge_fill_run(
                    security_id=security_id,
                    run_dates=run_dates,
                    live_intervals=live_intervals_by_security.get(security_id, []),
                    akshare_lookup=akshare_lookup,
                )
            )
    return decisions


def _classify_bridge_fill_run(
    *,
    security_id: str,
    run_dates: list[str],
    live_intervals: list[IntervalRow],
    akshare_lookup: dict[str, list[IntervalRow]],
) -> BridgeFillRunDecision:
    start_date = run_dates[0]
    end_date = run_dates[-1]
    left_interval = _find_left_interval(live_intervals, start_date)
    right_interval = _find_right_interval(live_intervals, end_date)
    akshare_codes_in_gap = sorted(
        {
            code
            for trade_date in run_dates
            if (
                code := _lookup_active_industry(
                    akshare_lookup,
                    security_id=security_id,
                    trade_date=trade_date,
                    inclusive_end=False,
                )
            )
        }
    )

    decision = "eligible_bridge_fill"
    imputed_industry_code = ""
    if left_interval is None or right_interval is None:
        decision = "blocked_one_sided_gap"
    elif _crosses_schema_transition(left_interval, right_interval):
        decision = "blocked_schema_transition"
    elif left_interval[1] != right_interval[1]:
        decision = "blocked_endpoint_mismatch"
    elif len(akshare_codes_in_gap) > 1:
        decision = "blocked_counterevidence"
    else:
        imputed_industry_code = left_interval[1]

    return BridgeFillRunDecision(
        security_id=security_id,
        start_date=start_date,
        end_date=end_date,
        gap_trade_days=len(run_dates),
        decision=decision,
        imputed_industry_code=imputed_industry_code,
        left_live_industry_code=left_interval[1] if left_interval else "",
        left_live_removed_at=(left_interval[3] or "") if left_interval else "",
        right_live_industry_code=right_interval[1] if right_interval else "",
        right_live_effective_at=right_interval[2] if right_interval else "",
        akshare_codes_in_gap=akshare_codes_in_gap,
    )


def _find_left_interval(intervals: list[IntervalRow], gap_start_date: str) -> IntervalRow | None:
    left_interval: IntervalRow | None = None
    for interval in intervals:
        effective_at = interval[2]
        if effective_at > gap_start_date:
            continue
        if left_interval is None or effective_at >= left_interval[2]:
            left_interval = interval
    return left_interval


def _find_right_interval(intervals: list[IntervalRow], gap_end_date: str) -> IntervalRow | None:
    right_interval: IntervalRow | None = None
    for interval in intervals:
        effective_at = interval[2]
        if effective_at <= gap_end_date:
            continue
        if right_interval is None or effective_at < right_interval[2]:
            right_interval = interval
    return right_interval


def _crosses_schema_transition(
    left_interval: IntervalRow,
    right_interval: IntervalRow,
) -> bool:
    left_removed_at = left_interval[3] or left_interval[2]
    right_effective_at = right_interval[2]
    return any(
        left_removed_at < transition_date <= right_effective_at
        for transition_date in SCHEMA_TRANSITION_DATES
    )


def _write_interval_rows_csv(path: Path, intervals: list[IntervalRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["security_id", "industry_code", "effective_at", "removed_at"],
        )
        writer.writeheader()
        for security_id, industry_code, effective_at, removed_at in intervals:
            writer.writerow(
                {
                    "security_id": security_id,
                    "industry_code": industry_code,
                    "effective_at": effective_at,
                    "removed_at": removed_at or "",
                }
            )


def _read_interval_rows_csv(path: Path) -> list[IntervalRow]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            (
                _normalize_security_id(row.get("security_id")),
                _normalize_text(row.get("industry_code")),
                _normalize_date(row.get("effective_at")) or "",
                _normalize_date(row.get("removed_at")),
            )
            for row in reader
            if _normalize_security_id(row.get("security_id"))
            and _normalize_date(row.get("effective_at"))
        ]


def _write_gap_rows_csv(path: Path, rows: list[CoverageGapRow]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "trade_date",
                "security_id",
                "classification",
                "staged_exclusive",
                "staged_inclusive",
                "live_tushare_exclusive",
                "live_tushare_inclusive",
                "akshare",
                "staged_industry_code",
                "live_tushare_industry_code",
                "akshare_industry_code",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(asdict(row))


def _dataframe_rows(frame: Any) -> list[dict[str, Any]]:
    if frame is None:
        return []
    try:
        if frame.empty:
            return []
    except AttributeError:
        pass
    return list(frame.to_dict("records"))


def _normalize_security_id(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if "." in text:
        return text
    if text.startswith(("600", "601", "603", "605", "688", "689", "900")):
        return f"{text}.SH"
    if text.startswith(("000", "001", "002", "003", "300", "301", "200")):
        return f"{text}.SZ"
    if text.startswith(("430", "440", "830", "831", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "877", "878", "879", "920")):
        return f"{text}.BJ"
    return text


def _normalize_date(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    text = _normalize_text(value)
    if not text:
        return None
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return None


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "none", "nan", "nat"}:
        return ""
    return text


def _resolve_path(path: str | Path) -> Path:
    raw = Path(path).expanduser()
    if raw.is_absolute():
        return raw.resolve()
    return (Path.cwd() / raw).resolve()


def _safe_ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


if __name__ == "__main__":
    raise SystemExit(main())
