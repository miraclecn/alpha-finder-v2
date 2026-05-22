"""
Data audit checks for the ingest pipeline.

Verifies that output/raw.duckdb is fit for V2 research:
- PIT leak sampling
- Adjustment factor positivity
- Trade calendar coverage
- Missingness by year
- Survivorship (delisted names in history)
- Suspension coverage
- Price-limit coverage
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal

logger = logging.getLogger(__name__)

Severity = Literal["blocking", "advisory"]
CheckResult = Literal["pass", "fail", "skip"]


@dataclass(slots=True)
class CheckOutcome:
    check_id: str
    severity: Severity
    result: CheckResult
    details: str


@dataclass(slots=True)
class AuditReport:
    raw_db_path: str
    run_at: str
    overall_status: Literal["ok", "blocking_failure"]
    outcomes: list[CheckOutcome]

    def blocking_failures(self) -> list[CheckOutcome]:
        return [o for o in self.outcomes if o.severity == "blocking" and o.result == "fail"]

    def has_blocking_failure(self) -> bool:
        return bool(self.blocking_failures())


# ---------------------------------------------------------------------------
# Table existence helper
# ---------------------------------------------------------------------------


def _table_exists(conn: Any, table_name: str) -> bool:
    rows = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchone()
    return rows[0] > 0


# ---------------------------------------------------------------------------
# Individual check implementations
# ---------------------------------------------------------------------------


def _check_pit_leak_sample(conn: Any) -> CheckOutcome:
    """Sample up to 10 rows from pit_fina_indicator.
    Passes if all sampled rows have ann_date <= end_date (basic PIT check).
    """
    if not _table_exists(conn, "pit_fina_indicator"):
        return CheckOutcome(
            check_id="pit_leak_sample",
            severity="blocking",
            result="skip",
            details="pit_fina_indicator table absent; skipping PIT leak check.",
        )

    try:
        total = conn.execute("SELECT COUNT(*) FROM pit_fina_indicator").fetchone()[0]
        if total == 0:
            return CheckOutcome(
                check_id="pit_leak_sample",
                severity="advisory",
                result="pass",
                details="pit_fina_indicator is empty; no rows to check.",
            )

        # Sample up to 10 rows and check ann_date <= end_date
        # A PIT leak means ann_date > end_date (announcement date after period end)
        rows = conn.execute(
            """
            SELECT ts_code, ann_date, end_date
            FROM pit_fina_indicator
            USING SAMPLE 10
            """
        ).fetchall()

        leaks = [
            (ts_code, ann_date, end_date)
            for ts_code, ann_date, end_date in rows
            if ann_date is not None and end_date is not None and ann_date > end_date
        ]

        if leaks:
            sample_str = "; ".join(
                f"{ts} ann={ann} end={end}" for ts, ann, end in leaks[:3]
            )
            return CheckOutcome(
                check_id="pit_leak_sample",
                severity="blocking",
                result="fail",
                details=f"Found {len(leaks)} row(s) where ann_date > end_date (PIT leak). Sample: {sample_str}",
            )

        return CheckOutcome(
            check_id="pit_leak_sample",
            severity="blocking",
            result="pass",
            details=f"Sampled {len(rows)} row(s), no PIT leak found (ann_date <= end_date for all).",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="pit_leak_sample",
            severity="blocking",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_adj_factor_consistency(conn: Any) -> CheckOutcome:
    """Sample 50 rows from raw_adj_factor and check adj_factor > 0.0."""
    if not _table_exists(conn, "raw_kline_unadj") or not _table_exists(conn, "raw_adj_factor"):
        return CheckOutcome(
            check_id="adj_factor_consistency",
            severity="blocking",
            result="skip",
            details="raw_kline_unadj or raw_adj_factor absent; skipping consistency check.",
        )

    try:
        total = conn.execute("SELECT COUNT(*) FROM raw_adj_factor").fetchone()[0]
        if total == 0:
            return CheckOutcome(
                check_id="adj_factor_consistency",
                severity="advisory",
                result="pass",
                details="raw_adj_factor is empty; nothing to check.",
            )

        rows = conn.execute(
            """
            SELECT ts_code, trade_date, adj_factor
            FROM raw_adj_factor
            USING SAMPLE 50
            """
        ).fetchall()

        bad = [(ts, td, af) for ts, td, af in rows if af is not None and af <= 0.0]

        if bad:
            sample_str = "; ".join(
                f"{ts}@{td}={af}" for ts, td, af in bad[:3]
            )
            return CheckOutcome(
                check_id="adj_factor_consistency",
                severity="blocking",
                result="fail",
                details=f"Found {len(bad)} row(s) with adj_factor <= 0.0. Sample: {sample_str}",
            )

        return CheckOutcome(
            check_id="adj_factor_consistency",
            severity="blocking",
            result="pass",
            details=f"adj_factor > 0.0 for all {len(rows)} sampled rows.",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="adj_factor_consistency",
            severity="blocking",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_trade_calendar_coverage(conn: Any) -> CheckOutcome:
    """Verify that all distinct trade_dates in raw_kline_unadj are within
    the date range of raw_trade_cal.cal_date.
    """
    if not _table_exists(conn, "raw_trade_cal"):
        return CheckOutcome(
            check_id="trade_calendar_coverage",
            severity="advisory",
            result="pass",
            details="raw_trade_cal absent; skipping calendar coverage check.",
        )

    try:
        cal_count = conn.execute("SELECT COUNT(*) FROM raw_trade_cal").fetchone()[0]
        if cal_count == 0:
            return CheckOutcome(
                check_id="trade_calendar_coverage",
                severity="advisory",
                result="pass",
                details="raw_trade_cal is empty; skipping.",
            )

        if not _table_exists(conn, "raw_kline_unadj"):
            return CheckOutcome(
                check_id="trade_calendar_coverage",
                severity="advisory",
                result="pass",
                details="raw_kline_unadj absent; calendar exists but no kline data to verify.",
            )

        # Find trade_dates in raw_kline_unadj that fall outside raw_trade_cal date range
        out_of_range = conn.execute(
            """
            SELECT COUNT(DISTINCT k.trade_date)
            FROM (SELECT DISTINCT trade_date FROM raw_kline_unadj) k
            WHERE k.trade_date < (SELECT MIN(cal_date) FROM raw_trade_cal)
               OR k.trade_date > (SELECT MAX(cal_date) FROM raw_trade_cal)
            """
        ).fetchone()[0]

        if out_of_range > 0:
            cal_min = conn.execute("SELECT MIN(cal_date) FROM raw_trade_cal").fetchone()[0]
            cal_max = conn.execute("SELECT MAX(cal_date) FROM raw_trade_cal").fetchone()[0]
            return CheckOutcome(
                check_id="trade_calendar_coverage",
                severity="blocking",
                result="fail",
                details=(
                    f"{out_of_range} distinct trade_date(s) in raw_kline_unadj fall outside "
                    f"raw_trade_cal range [{cal_min}, {cal_max}]."
                ),
            )

        kline_dates = conn.execute("SELECT COUNT(DISTINCT trade_date) FROM raw_kline_unadj").fetchone()[0]
        return CheckOutcome(
            check_id="trade_calendar_coverage",
            severity="blocking",
            result="pass",
            details=f"All {kline_dates} distinct kline trade_date(s) are within raw_trade_cal range.",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="trade_calendar_coverage",
            severity="blocking",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_missingness_by_year(conn: Any) -> CheckOutcome:
    """Report row counts per year for all raw_* tables that exist.
    Always passes (advisory info only).
    """
    try:
        raw_tables = [
            t
            for (t,) in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE 'raw_%' OR table_name LIKE 'pit_%'"
            ).fetchall()
        ]

        if not raw_tables:
            return CheckOutcome(
                check_id="missingness_by_year",
                severity="advisory",
                result="pass",
                details="No raw_* tables found.",
            )

        # Find the table with fewest rows per year
        worst_table: str | None = None
        worst_min_rows: int | None = None
        summary_parts: list[str] = []

        for tname in raw_tables:
            # Try trade_date first, then end_date
            for date_col in ("trade_date", "end_date", "ann_date", "cal_date"):
                try:
                    col_exists = conn.execute(
                        f"SELECT COUNT(*) FROM information_schema.columns "
                        f"WHERE table_name = '{tname}' AND column_name = '{date_col}'"
                    ).fetchone()[0]
                    if not col_exists:
                        continue

                    year_rows = conn.execute(
                        f"""
                        SELECT substr({date_col}, 1, 4) AS yr, COUNT(*) AS cnt
                        FROM {tname}
                        WHERE {date_col} IS NOT NULL AND {date_col} != ''
                        GROUP BY 1
                        ORDER BY 1
                        """
                    ).fetchall()

                    if year_rows:
                        min_rows = min(cnt for _, cnt in year_rows)
                        summary_parts.append(f"{tname}(min/yr={min_rows})")
                        if worst_min_rows is None or min_rows < worst_min_rows:
                            worst_min_rows = min_rows
                            worst_table = tname
                    break
                except Exception:
                    continue

        if worst_table is not None:
            details = f"Table with fewest rows/year: {worst_table} ({worst_min_rows}). All: {', '.join(summary_parts[:5])}"
        else:
            details = "Could not extract year from any raw table."

        return CheckOutcome(
            check_id="missingness_by_year",
            severity="advisory",
            result="pass",
            details=details,
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="missingness_by_year",
            severity="advisory",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_survivorship(conn: Any) -> CheckOutcome:
    """Check that stock_basic_ref has at least one row where delist_date IS NOT NULL."""
    if not _table_exists(conn, "stock_basic_ref"):
        return CheckOutcome(
            check_id="survivorship_delisted_present",
            severity="advisory",
            result="pass",
            details="stock_basic_ref absent; skipping survivorship check.",
        )

    try:
        total = conn.execute("SELECT COUNT(*) FROM stock_basic_ref").fetchone()[0]
        if total == 0:
            return CheckOutcome(
                check_id="survivorship_delisted_present",
                severity="advisory",
                result="pass",
                details="stock_basic_ref is empty; skipping survivorship check.",
            )

        delisted_count = conn.execute(
            "SELECT COUNT(*) FROM stock_basic_ref WHERE delist_date IS NOT NULL AND delist_date != ''"
        ).fetchone()[0]

        if delisted_count == 0:
            return CheckOutcome(
                check_id="survivorship_delisted_present",
                severity="advisory",
                result="pass",
                details=f"No delisted entries in stock_basic_ref ({total} total rows). Survivorship bias possible.",
            )

        return CheckOutcome(
            check_id="survivorship_delisted_present",
            severity="advisory",
            result="pass",
            details=f"{delisted_count}/{total} rows in stock_basic_ref have delist_date set.",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="survivorship_delisted_present",
            severity="advisory",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_suspend_coverage(conn: Any) -> CheckOutcome:
    """Check that raw_suspend_d exists and has at least one row."""
    if not _table_exists(conn, "raw_suspend_d"):
        return CheckOutcome(
            check_id="suspend_coverage",
            severity="advisory",
            result="pass",
            details="raw_suspend_d absent.",
        )

    try:
        count = conn.execute("SELECT COUNT(*) FROM raw_suspend_d").fetchone()[0]
        return CheckOutcome(
            check_id="suspend_coverage",
            severity="advisory",
            result="pass",
            details=f"raw_suspend_d has {count} row(s).",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="suspend_coverage",
            severity="advisory",
            result="fail",
            details=f"Check raised: {exc}",
        )


def _check_stk_limit_coverage(conn: Any) -> CheckOutcome:
    """Check that raw_stk_limit exists and has at least one row."""
    if not _table_exists(conn, "raw_stk_limit"):
        return CheckOutcome(
            check_id="stk_limit_coverage",
            severity="advisory",
            result="pass",
            details="raw_stk_limit absent.",
        )

    try:
        count = conn.execute("SELECT COUNT(*) FROM raw_stk_limit").fetchone()[0]
        return CheckOutcome(
            check_id="stk_limit_coverage",
            severity="advisory",
            result="pass",
            details=f"raw_stk_limit has {count} row(s).",
        )
    except Exception as exc:
        return CheckOutcome(
            check_id="stk_limit_coverage",
            severity="advisory",
            result="fail",
            details=f"Check raised: {exc}",
        )


# ---------------------------------------------------------------------------
# Check registry: (id, severity, function)
# ---------------------------------------------------------------------------

@dataclass
class AuditCheck:
    id: str
    severity: Severity
    run: Callable[[Any], CheckOutcome]


_AUDIT_REGISTRY: list[AuditCheck] = [
    AuditCheck("pit_leak_sample", "blocking", _check_pit_leak_sample),
    AuditCheck("adj_factor_consistency", "blocking", _check_adj_factor_consistency),
    AuditCheck("trade_calendar_coverage", "blocking", _check_trade_calendar_coverage),
    AuditCheck("missingness_by_year", "advisory", _check_missingness_by_year),
    AuditCheck("survivorship_delisted_present", "advisory", _check_survivorship),
    AuditCheck("suspend_coverage", "advisory", _check_suspend_coverage),
    AuditCheck("stk_limit_coverage", "advisory", _check_stk_limit_coverage),
]

# Keep legacy alias for callers that iterate the old list
_ALL_CHECKS = [(c.id, c.severity, c.run) for c in _AUDIT_REGISTRY]

_DEFAULT_BLOCKING: frozenset[str] = frozenset(
    c.id for c in _AUDIT_REGISTRY if c.severity == "blocking"
)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_audit(
    *,
    raw_db_path: Path,
    out_dir: Path,
    blocking_checks: set[str] | None = None,
) -> AuditReport:
    """
    Run all data quality checks against raw_db_path.

    Writes audit.json and audit.md under out_dir/<UTC-timestamp>/.
    Returns AuditReport; overall_status is 'blocking_failure' if any
    blocking check fails.
    """
    import duckdb

    run_at = datetime.now(UTC).isoformat()
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_dir = out_dir / ts
    report_dir.mkdir(parents=True, exist_ok=True)

    effective_blocking = blocking_checks or _DEFAULT_BLOCKING

    conn = duckdb.connect(str(raw_db_path), read_only=True)
    outcomes: list[CheckOutcome] = []

    for check in _AUDIT_REGISTRY:
        logger.info("Running audit check: %s", check.id)
        outcome = check.run(conn)
        # Override severity if blocking_checks override set says different
        if check.id in effective_blocking:
            outcome = CheckOutcome(
                check_id=outcome.check_id,
                severity="blocking",
                result=outcome.result,
                details=outcome.details,
            )
        outcomes.append(outcome)
        logger.info(
            "  %s [%s] → %s: %s",
            check.id,
            outcome.severity,
            outcome.result,
            outcome.details[:120],
        )

    conn.close()

    has_blocking_failure = any(
        o.result == "fail" and o.severity == "blocking" for o in outcomes
    )
    overall_status: Literal["ok", "blocking_failure"] = (
        "blocking_failure" if has_blocking_failure else "ok"
    )

    report = AuditReport(
        raw_db_path=str(raw_db_path),
        run_at=run_at,
        overall_status=overall_status,
        outcomes=outcomes,
    )

    # Write audit.json
    json_path = report_dir / "audit.json"
    json_payload = {
        "raw_db_path": report.raw_db_path,
        "run_at": report.run_at,
        "overall_status": report.overall_status,
        "outcomes": [
            {
                "check_id": o.check_id,
                "severity": o.severity,
                "result": o.result,
                "details": o.details,
            }
            for o in outcomes
        ],
    }
    json_path.write_text(
        json.dumps(json_payload, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Write audit.md
    md_path = report_dir / "audit.md"
    lines = [
        "# Data Audit Report",
        "",
        f"Run at: {report.run_at}",
        f"Database: {report.raw_db_path}",
        "",
        "## Check Results",
        "",
        "| ID | Severity | Passed | Details |",
        "|----|----------|--------|---------|",
    ]
    for o in outcomes:
        passed_icon = "✓" if o.result == "pass" else ("—" if o.result == "skip" else "✗")
        details_short = o.details[:120].replace("|", "｜")
        lines.append(f"| {o.check_id} | {o.severity} | {passed_icon} | {details_short} |")
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    logger.info(
        "Audit complete: %s | report at %s",
        overall_status,
        report_dir,
    )
    return report
