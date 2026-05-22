"""
CLI handler functions for factor_evaluation commands.
Called from cli.py main() branches.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


def handle_compute_descriptor(args: Any) -> int:
    """Handle 'compute-descriptor' subcommand. Returns exit code."""
    import duckdb

    from .descriptor_compute import ComputeContext, get, list_registered
    from .exceptions import DescriptorNotImplemented, EvaluationError

    descriptor_id: str = args.id
    research_db = Path(getattr(args, "research_db", "output/research_source.duckdb"))
    start_date: str | None = getattr(args, "start", None)
    end_date: str | None = getattr(args, "end", None)
    out_path: str | None = getattr(args, "out", None)

    # Check registry
    try:
        spec = get(descriptor_id)
    except KeyError:
        print(
            json.dumps({"error": f"Unknown descriptor '{descriptor_id}'.",
                        "registered": list_registered()},
                       ensure_ascii=False, indent=2),
            file=sys.stdout,
        )
        return 2

    if not research_db.exists():
        print(
            json.dumps({"error": f"Research database not found: {research_db}."},
                       ensure_ascii=False, indent=2),
            file=sys.stdout,
        )
        return 4

    conn = duckdb.connect(str(research_db), read_only=True)
    try:
        # Resolve date range if not provided
        if not start_date or not end_date:
            row = conn.execute(
                "SELECT MIN(trade_date), MAX(trade_date) FROM market_trade_calendar"
            ).fetchone()
            start_date = start_date or row[0]
            end_date = end_date or row[1]

        ctx = ComputeContext(conn=conn, start_date=start_date, end_date=end_date)

        try:
            df = spec.fn(ctx)
        except DescriptorNotImplemented as exc:
            print(
                json.dumps({
                    "error": str(exc),
                    "descriptor_id": exc.descriptor_id,
                    "requires": list(exc.requires),
                }, ensure_ascii=False, indent=2),
                file=sys.stdout,
            )
            return 3

        # Summary statistics
        summary = {
            "descriptor_id": descriptor_id,
            "rows": int(len(df)),
            "security_count": int(df["security_id"].nunique()),
            "distinct_trade_dates": int(df["trade_date"].nunique()),
            "missing_rate": float(df["descriptor_value"].isna().mean()),
            "mean": float(df["descriptor_value"].mean()) if len(df) > 0 else None,
            "std": float(df["descriptor_value"].std()) if len(df) > 0 else None,
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2), file=sys.stdout)

        # Optionally write Parquet
        if out_path:
            df.to_parquet(out_path, index=False)
            print(f"Written to {out_path}", file=sys.stderr)

        return 0

    finally:
        conn.close()


def handle_evaluate_descriptor(args: Any) -> int:
    """Handle 'evaluate-descriptor' subcommand. Returns exit code."""
    from .descriptor_compute import get, list_registered
    from .descriptor_evaluator import evaluate_descriptor
    from .exceptions import (
        DescriptorNotImplemented,
        EvaluationError,
        UniverseEmpty,
    )
    from .report_writer import write_report

    descriptor_id: str = args.id
    research_db = Path(getattr(args, "research_db", "output/research_source.duckdb"))
    raw_db_path_str: str = getattr(args, "raw_db", "output/raw.duckdb")
    raw_db_path = Path(raw_db_path_str) if raw_db_path_str else None
    universe: str = getattr(args, "universe", "investable_a_share_core")
    start_date: str = getattr(args, "start", "")
    end_date: str = getattr(args, "end", "")
    horizons_str: str = getattr(args, "horizons", "5,20,60")
    primary_horizon: int = int(getattr(args, "primary_horizon", 20))
    correlation_str: str = getattr(args, "correlation_against", "")
    cost_model_str: str = getattr(args, "cost_model", "")
    weighting: str = getattr(args, "weighting", "equal")
    include_untradeable: bool = bool(getattr(args, "include_untradeable", False))
    out_dir = Path(getattr(args, "out_dir", "output/descriptor_evaluation"))

    horizons = tuple(int(h) for h in horizons_str.split(",") if h.strip())
    correlation_against = tuple(c.strip() for c in correlation_str.split(",") if c.strip())
    cost_model_path = Path(cost_model_str) if cost_model_str else None

    # Validate registry
    try:
        get(descriptor_id)
    except KeyError:
        print(json.dumps({"error": f"Unknown descriptor '{descriptor_id}'.",
                          "registered": list_registered()},
                         ensure_ascii=False, indent=2))
        return 2

    if not start_date or not end_date:
        import duckdb
        if not research_db.exists():
            print(json.dumps({"error": f"Research database not found: {research_db}."},
                             ensure_ascii=False, indent=2))
            return 4
        conn = duckdb.connect(str(research_db), read_only=True)
        row = conn.execute(
            "SELECT MIN(trade_date), MAX(trade_date) FROM market_trade_calendar"
        ).fetchone()
        conn.close()
        start_date = start_date or row[0]
        end_date = end_date or row[1]

    try:
        report = evaluate_descriptor(
            descriptor_id=descriptor_id,
            research_db=research_db,
            universe=universe,
            start_date=start_date,
            end_date=end_date,
            horizons=horizons,
            primary_horizon=primary_horizon,
            correlation_against=correlation_against,
            cost_model_path=cost_model_path,
            weighting=weighting,
            include_untradeable=include_untradeable,
            raw_db_path=raw_db_path,
        )
    except DescriptorNotImplemented as exc:
        print(json.dumps({"error": str(exc), "requires": list(exc.requires)},
                         ensure_ascii=False, indent=2))
        return 3
    except EvaluationError as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return exc.exit_code
    except UniverseEmpty as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False, indent=2))
        return 5

    report_dir = write_report(report, out_dir)

    # Print summary to stdout
    ph = report.meta.primary_horizon
    hm = report.horizon_metrics.get(ph)
    summary = {
        "descriptor_id": descriptor_id,
        "primary_horizon": ph,
        "ic_ir": hm.ic_pearson.ir if hm else None,
        "rank_stability_lag1": hm.rank_stability_lag1 if hm else None,
        "coverage_mean": report.coverage.rows_used_over_possible,
        "low_coverage_warning": report.coverage.low_coverage_warning,
        "warnings": report.diagnostics.warnings,
        "report_dir": str(report_dir),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


def handle_list_evaluation_reports(args: Any) -> int:
    """Handle 'list-evaluation-reports' subcommand. Returns exit code."""
    import json as _json

    filter_id: str | None = getattr(args, "id", None)
    out_dir = Path(getattr(args, "out_dir", "output/descriptor_evaluation"))

    rows: list[dict] = []

    if not out_dir.exists():
        print(_json.dumps([], ensure_ascii=False, indent=2))
        return 0

    for descriptor_dir in sorted(out_dir.iterdir()):
        if not descriptor_dir.is_dir():
            continue
        descriptor_id = descriptor_dir.name
        if filter_id and descriptor_id != filter_id:
            continue
        for run_dir in sorted(descriptor_dir.iterdir(), reverse=True):
            report_path = run_dir / "report.json"
            if not report_path.exists():
                continue
            try:
                payload = _json.loads(report_path.read_text(encoding="utf-8"))
                eval_meta = payload.get("evaluation", {})
                horizons_section = payload.get("horizons", {})
                ph = eval_meta.get("primary_horizon", None)
                ph_key = str(ph) if ph is not None else None
                ic_ir = None
                if ph_key and ph_key in horizons_section:
                    ic_ir = horizons_section[ph_key].get("ic_ir")
                coverage = payload.get("coverage", {})
                rows.append({
                    "descriptor_id": descriptor_id,
                    "run_at": eval_meta.get("run_at", run_dir.name),
                    "ic_ir_primary": ic_ir,
                    "coverage_mean": coverage.get("rows_used_over_possible"),
                    "status": "ok",
                })
            except Exception as exc:
                rows.append({
                    "descriptor_id": descriptor_id,
                    "run_at": run_dir.name,
                    "status": f"parse_error: {exc}",
                })

    print(_json.dumps(rows, ensure_ascii=False, indent=2))
    return 0
