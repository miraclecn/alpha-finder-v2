"""
Persist DescriptorEvaluationReport as JSON + Markdown.
"""
from __future__ import annotations

import json
import math
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from .descriptor_evaluator import DescriptorEvaluationReport


def _safe_float(v: float | None) -> object:
    """Replace NaN/inf with None for JSON serialization."""
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def _sanitize(obj: object) -> object:
    """Recursively replace NaN/inf floats with None in nested structures."""
    if isinstance(obj, float):
        return _safe_float(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


def write_report(report: DescriptorEvaluationReport, out_dir: Path) -> Path:
    """
    Write report.json and report.md to:
        out_dir/<descriptor_id>/<run_at>/

    Returns the directory path.
    """
    # Sanitize run_at for a filesystem-safe directory name
    run_at_safe = report.meta.run_at.replace(":", "-").replace("+", "p")
    report_dir = out_dir / report.meta.descriptor_id / run_at_safe
    report_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    raw = _to_dict(report)
    json_path = report_dir / "report.json"
    json_path.write_text(
        json.dumps(_sanitize(raw), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    # Markdown
    md_path = report_dir / "report.md"
    md_path.write_text(_to_markdown(report), encoding="utf-8")

    return report_dir


def _to_dict(report: DescriptorEvaluationReport) -> dict:
    m = report.meta
    d: dict = {
        "schema_version": 1,
        "artifact_type": "descriptor_evaluation_report",
        "descriptor_id": m.descriptor_id,
        "descriptor_version": m.descriptor_version,
        "evaluation": {
            "start_date": m.start_date,
            "end_date": m.end_date,
            "universe_definition": m.universe_definition,
            "horizons": m.horizons,
            "primary_horizon": m.primary_horizon,
            "cost_model_id": m.cost_model_id,
            "weighting": m.weighting,
            "run_at": m.run_at,
            "sample_size": m.sample_size,
        },
        "horizons": {},
        "ic_decay": {
            "horizons": report.ic_decay.horizons,
            "ic_means": report.ic_decay.ic_means,
            "half_life": report.ic_decay.half_life,
        },
        "slice_stability": {
            "by_industry": [
                {
                    "industry": r.slice_value,
                    "ic_pearson_mean": r.ic_pearson_mean,
                    "ic_spearman_mean": r.ic_spearman_mean,
                    "n": r.n,
                }
                for r in report.slice_stability.by_industry
            ],
            "by_size_tertile": [
                {
                    "tertile": r.slice_value,
                    "ic_pearson_mean": r.ic_pearson_mean,
                    "ic_spearman_mean": r.ic_spearman_mean,
                    "n": r.n,
                }
                for r in report.slice_stability.by_size_tertile
            ],
        },
        "cross_correlation": report.cross_correlation,
        "coverage": {
            "rows_used_over_possible": report.coverage.rows_used_over_possible,
            "tradeable_rate": report.coverage.tradeable_rate,
            "low_coverage_warning": report.coverage.low_coverage_warning,
        },
        "diagnostics": {
            "warnings": report.diagnostics.warnings,
            "compute_duration_ms": report.diagnostics.compute_duration_ms,
            "evaluation_duration_ms": report.diagnostics.evaluation_duration_ms,
        },
    }

    for H, hm in report.horizon_metrics.items():
        d["horizons"][str(H)] = {
            "ic_pearson": {
                "mean": hm.ic_pearson.mean,
                "std": hm.ic_pearson.std,
                "tstat": hm.ic_pearson.tstat,
                "n": hm.ic_pearson.n,
            },
            "ic_spearman": {
                "mean": hm.ic_spearman.mean,
                "std": hm.ic_spearman.std,
                "tstat": hm.ic_spearman.tstat,
                "n": hm.ic_spearman.n,
            },
            "ic_ir": hm.ic_pearson.ir,
            "rank_stability_lag1": hm.rank_stability_lag1,
            "turnover_per_period": hm.turnover_per_period,
            "decile_long_short": {
                "annualised_return_gross": hm.decile_ls.annualised_return_gross,
                "annualised_return_net": hm.decile_ls.annualised_return_net,
                "sharpe": hm.decile_ls.sharpe,
                "max_drawdown": hm.decile_ls.max_drawdown,
                "monotonicity_spearman": hm.decile_ls.monotonicity_spearman,
            },
            "decile_returns": hm.decile_ls.decile_returns,
        }

    return d


def _fmt(v: object, digits: int = 3) -> str:
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "—"
    if isinstance(v, float):
        return f"{v:.{digits}f}"
    return str(v)


def _to_markdown(report: DescriptorEvaluationReport) -> str:
    m = report.meta
    lines: list[str] = [
        f"# Descriptor Evaluation: {m.descriptor_id}",
        "",
        f"- **Universe**: {m.universe_definition.get('id', '?')}",
        f"- **Window**: {m.start_date} → {m.end_date}",
        f"- **Primary horizon**: {m.primary_horizon}d",
        f"- **Version**: `{m.descriptor_version}`",
        f"- **Run at**: {m.run_at}",
        "",
    ]

    # Per-horizon summary
    if report.horizon_metrics:
        lines += [
            "## IC Summary",
            "",
            "| Horizon | IC mean | IC std | IC t-stat | IC_IR | Rank IC mean | L-S gross | Monotonicity |",
            "|---------|---------|--------|-----------|-------|--------------|-----------|--------------|",
        ]
        for H in sorted(report.horizon_metrics):
            hm = report.horizon_metrics[H]
            lines.append(
                f"| {H}d | {_fmt(hm.ic_pearson.mean)} | {_fmt(hm.ic_pearson.std)} "
                f"| {_fmt(hm.ic_pearson.tstat)} | {_fmt(hm.ic_pearson.ir)} "
                f"| {_fmt(hm.ic_spearman.mean)} "
                f"| {_fmt(hm.decile_ls.annualised_return_gross, 3)} "
                f"| {_fmt(hm.decile_ls.monotonicity_spearman)} |"
            )
        lines.append("")

    # IC Decay
    lines += [
        "## IC Decay",
        "",
        f"Half-life: {report.ic_decay.half_life}d",
        "",
        "| Horizon | IC mean |",
        "|---------|---------|",
    ]
    for H, ic in zip(report.ic_decay.horizons, report.ic_decay.ic_means):
        lines.append(f"| {H}d | {_fmt(ic)} |")
    lines.append("")

    # Decile returns for primary horizon
    ph = m.primary_horizon
    if ph in report.horizon_metrics:
        hm = report.horizon_metrics[ph]
        lines += [
            f"## Decile Returns ({ph}d horizon)",
            "",
            "| Decile | Mean Return |",
            "|--------|-------------|",
        ]
        for dr in hm.decile_ls.decile_returns:
            lines.append(f"| {dr['decile']} | {_fmt(dr['mean_return'])} |")
        lines.append("")

    # Slice stability
    if report.slice_stability.by_industry:
        lines += [
            "## Slice Stability — By Industry",
            "",
            "| Industry | IC Pearson mean | IC Spearman mean | N |",
            "|----------|-----------------|------------------|---|",
        ]
        for r in report.slice_stability.by_industry:
            lines.append(
                f"| {r.slice_value} | {_fmt(r.ic_pearson_mean)} "
                f"| {_fmt(r.ic_spearman_mean)} | {r.n} |"
            )
        lines.append("")

    if report.slice_stability.by_size_tertile:
        lines += [
            "## Slice Stability — By Size Tertile",
            "",
            "| Tertile | IC Pearson mean | IC Spearman mean | N |",
            "|---------|-----------------|------------------|---|",
        ]
        for r in report.slice_stability.by_size_tertile:
            lines.append(
                f"| {r.slice_value} | {_fmt(r.ic_pearson_mean)} "
                f"| {_fmt(r.ic_spearman_mean)} | {r.n} |"
            )
        lines.append("")

    # Cross-correlation
    if report.cross_correlation:
        lines += [
            "## Cross-Descriptor Correlation",
            "",
            "| Descriptor | Pearson r |",
            "|------------|-----------|",
        ]
        for did, corr in sorted(report.cross_correlation.items()):
            lines.append(f"| {did} | {_fmt(corr)} |")
        lines.append("")

    # Coverage
    lines += [
        "## Coverage",
        "",
        f"- Rows used / possible: {_fmt(report.coverage.rows_used_over_possible)}",
        f"- Tradeable rate: {_fmt(report.coverage.tradeable_rate)}",
        f"- Low coverage warning: {report.coverage.low_coverage_warning}",
        "",
    ]

    # Diagnostics
    if report.diagnostics.warnings:
        lines += [
            "## Warnings",
            "",
        ]
        for w in report.diagnostics.warnings:
            lines.append(f"- {w}")
        lines.append("")

    return "\n".join(lines)
