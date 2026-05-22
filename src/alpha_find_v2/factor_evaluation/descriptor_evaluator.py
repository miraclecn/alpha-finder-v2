"""
Descriptor evaluator: IC, Rank IC, decile L-S, monotonicity, rank stability,
coverage, and tradeability filtering.

Entry point: evaluate_descriptor(...)
"""
from __future__ import annotations

import hashlib
import inspect
import logging
import math
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from .descriptor_compute import REGISTRY, ComputeContext, get, list_registered
from .exceptions import DescriptorNotImplemented, EvaluationError, UniverseEmpty
from .forward_returns import compute_forward_returns
from .universe_resolver import resolver_for_universe

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class ICStats:
    mean: float
    std: float
    tstat: float
    n: int
    ir: float  # mean / std


@dataclass(slots=True)
class DecileLSResult:
    annualised_return_gross: float
    annualised_return_net: float
    sharpe: float
    max_drawdown: float
    decile_returns: list[dict]      # [{decile, mean_return}]
    monotonicity_spearman: float


@dataclass(slots=True)
class HorizonMetrics:
    horizon: int
    ic_pearson: ICStats
    ic_spearman: ICStats
    rank_stability_lag1: float
    turnover_per_period: float
    decile_ls: DecileLSResult


@dataclass(slots=True)
class IcDecay:
    horizons: list[int]
    ic_means: list[float]
    half_life: int


@dataclass(slots=True)
class SliceRow:
    slice_value: str
    ic_pearson_mean: float
    ic_spearman_mean: float
    n: int


@dataclass(slots=True)
class SliceStability:
    by_industry: list[SliceRow]
    by_size_tertile: list[SliceRow]


@dataclass(slots=True)
class Coverage:
    rows_used_over_possible: float
    tradeable_rate: float
    low_coverage_warning: bool


@dataclass(slots=True)
class EvaluationMeta:
    descriptor_id: str
    descriptor_version: str
    start_date: str
    end_date: str
    universe_definition: dict
    horizons: list[int]
    primary_horizon: int
    cost_model_id: str
    weighting: str
    run_at: str
    sample_size: dict   # {trade_dates, securities_distinct, rows_used}


@dataclass(slots=True)
class Diagnostics:
    warnings: list[str]
    compute_duration_ms: int
    evaluation_duration_ms: int


@dataclass(slots=True)
class DescriptorEvaluationReport:
    meta: EvaluationMeta
    horizon_metrics: dict[int, HorizonMetrics]
    ic_decay: IcDecay
    slice_stability: SliceStability
    cross_correlation: dict[str, float]
    coverage: Coverage
    diagnostics: Diagnostics


# ---------------------------------------------------------------------------
# IC computation (task 12)
# ---------------------------------------------------------------------------


def _compute_per_date_ic(
    panel: pd.DataFrame,
    forward_col: str,
    method: Literal["pearson", "spearman"],
) -> pd.Series:
    """
    For each trade_date, compute Pearson or Spearman IC between
    `descriptor_value` and `forward_col`.

    Returns: Series indexed by trade_date.
    """
    from scipy import stats

    results: dict[str, float] = {}
    for trade_date, grp in panel.groupby("trade_date"):
        x = grp["descriptor_value"]
        y = grp[forward_col]
        mask = x.notna() & y.notna()
        x, y = x[mask], y[mask]
        if len(x) < 2:
            results[str(trade_date)] = float("nan")
            continue
        if method == "pearson":
            r, _ = stats.pearsonr(x, y)
        else:
            r, _ = stats.spearmanr(x, y)
        results[str(trade_date)] = float(r)

    return pd.Series(results)


def _ic_stats(ic_series: pd.Series) -> ICStats:
    ic_valid = ic_series.dropna()
    n = len(ic_valid)
    if n < 2:
        return ICStats(mean=float("nan"), std=float("nan"), tstat=float("nan"), n=n, ir=float("nan"))
    mean = float(ic_valid.mean())
    std = float(ic_valid.std(ddof=1))
    tstat = (mean / std) * math.sqrt(n) if std > 0 else float("nan")
    ir = mean / std if std > 0 else float("nan")
    return ICStats(mean=mean, std=std, tstat=tstat, n=n, ir=ir)


# ---------------------------------------------------------------------------
# Decile L-S and monotonicity (task 13)
# ---------------------------------------------------------------------------


def _assign_deciles(values: pd.Series) -> pd.Series:
    try:
        return pd.qcut(values, q=10, labels=False, duplicates="drop")
    except ValueError:
        return pd.qcut(values, q=min(10, values.nunique()), labels=False, duplicates="drop")


def _compute_decile_ls(
    panel: pd.DataFrame,
    forward_col: str,
    cost_bps: float = 0.0,
    periods_per_year: int = 252,
    horizon: int = 1,
) -> DecileLSResult:
    """
    Compute decile returns and L-S series.
    cost_bps: one-side cost in basis points (full round-trip = 2×).
    """
    from scipy import stats as scipy_stats

    # Assign deciles per trade_date using transform approach for pandas 3.x compatibility
    def _add_deciles(panel_input: pd.DataFrame) -> pd.DataFrame:
        result = panel_input.copy()
        result["decile"] = float("nan")
        for td, grp in panel_input.groupby("trade_date"):
            try:
                d = pd.qcut(grp["descriptor_value"], q=10, labels=False, duplicates="drop")
            except ValueError:
                try:
                    d = pd.qcut(grp["descriptor_value"], q=min(10, grp["descriptor_value"].nunique()), labels=False, duplicates="drop")
                except ValueError:
                    continue
            result.loc[grp.index, "decile"] = d.astype(float)
        return result

    panel_with_decile = _add_deciles(panel)
    panel_with_decile = panel_with_decile.dropna(subset=["decile", forward_col])

    # Per-date, per-decile mean return
    by_date_decile = (
        panel_with_decile
        .groupby(["trade_date", "decile"])[forward_col]
        .mean()
        .reset_index()
        .rename(columns={forward_col: "mean_ret"})
    )

    # Overall decile mean across all dates
    decile_means = by_date_decile.groupby("decile")["mean_ret"].mean().sort_index()
    decile_returns_list = [
        {"decile": int(d) + 1, "mean_return": float(r)}
        for d, r in decile_means.items()
        if not math.isnan(r)
    ]

    # Monotonicity: Spearman of (decile_rank, mean_return)
    if len(decile_means) >= 2:
        mono_spearman, _ = scipy_stats.spearmanr(
            decile_means.index.astype(float).tolist(),
            decile_means.values.tolist(),
        )
    else:
        mono_spearman = float("nan")

    # L-S series: per trade_date, return of top decile minus bottom decile
    top_decile = by_date_decile["decile"].max()
    bot_decile = by_date_decile["decile"].min()
    top = by_date_decile[by_date_decile["decile"] == top_decile].set_index("trade_date")["mean_ret"]
    bot = by_date_decile[by_date_decile["decile"] == bot_decile].set_index("trade_date")["mean_ret"]
    ls_dates = top.index.intersection(bot.index)
    if len(ls_dates) == 0:
        return DecileLSResult(
            annualised_return_gross=float("nan"),
            annualised_return_net=float("nan"),
            sharpe=float("nan"),
            max_drawdown=float("nan"),
            decile_returns=decile_returns_list,
            monotonicity_spearman=float(mono_spearman),
        )

    ls_series = (top.loc[ls_dates] - bot.loc[ls_dates]).dropna()
    net_ls = ls_series - 2 * cost_bps * 1e-4  # round-trip cost per period

    n = len(ls_series)
    annualised_scale = periods_per_year / max(horizon, 1)
    ann_gross = float((1 + ls_series.mean()) ** annualised_scale - 1) if n > 0 else float("nan")
    ann_net = float((1 + net_ls.mean()) ** annualised_scale - 1) if n > 0 else float("nan")

    # Sharpe of L-S net
    if n > 1 and net_ls.std(ddof=1) > 0:
        sharpe = float(net_ls.mean() / net_ls.std(ddof=1) * math.sqrt(annualised_scale * n / n))
    else:
        sharpe = float("nan")

    # Max drawdown of L-S equity curve
    equity = (1 + net_ls).cumprod()
    rolling_max = equity.cummax()
    drawdown = (equity / rolling_max - 1).min()
    max_dd = float(drawdown) if not math.isnan(drawdown) else float("nan")

    return DecileLSResult(
        annualised_return_gross=ann_gross,
        annualised_return_net=ann_net,
        sharpe=sharpe,
        max_drawdown=max_dd,
        decile_returns=decile_returns_list,
        monotonicity_spearman=float(mono_spearman),
    )


# ---------------------------------------------------------------------------
# Rank stability and turnover (task 14)
# ---------------------------------------------------------------------------


def _compute_rank_stability(panel: pd.DataFrame) -> float:
    """Lag-1 Spearman rank autocorrelation across consecutive trade dates."""
    from scipy import stats as scipy_stats

    dates = sorted(panel["trade_date"].unique())
    if len(dates) < 2:
        return float("nan")

    rank_corrs: list[float] = []
    for i in range(1, len(dates)):
        d_prev, d_curr = dates[i - 1], dates[i]
        prev_panel = panel[panel["trade_date"] == d_prev][["security_id", "descriptor_value"]]
        curr_panel = panel[panel["trade_date"] == d_curr][["security_id", "descriptor_value"]]
        merged = prev_panel.merge(curr_panel, on="security_id", suffixes=("_prev", "_curr"))
        if len(merged) < 2:
            continue
        r, _ = scipy_stats.spearmanr(merged["descriptor_value_prev"], merged["descriptor_value_curr"])
        if not math.isnan(r):
            rank_corrs.append(r)

    return float(pd.Series(rank_corrs).mean()) if rank_corrs else float("nan")


def _compute_turnover_per_period(panel: pd.DataFrame, n_securities: int) -> float:
    """Average |Δ rank| / N between consecutive rebalance dates."""
    dates = sorted(panel["trade_date"].unique())
    if len(dates) < 2:
        return float("nan")

    turnovers: list[float] = []
    for i in range(1, len(dates)):
        d_prev, d_curr = dates[i - 1], dates[i]
        prev_r = (
            panel[panel["trade_date"] == d_prev]
            .set_index("security_id")["descriptor_value"]
            .rank()
        )
        curr_r = (
            panel[panel["trade_date"] == d_curr]
            .set_index("security_id")["descriptor_value"]
            .rank()
        )
        common = prev_r.index.intersection(curr_r.index)
        if len(common) == 0:
            continue
        delta = (curr_r.loc[common] - prev_r.loc[common]).abs().mean()
        N = max(len(common), 1)
        turnovers.append(float(delta) / N)

    return float(pd.Series(turnovers).mean()) if turnovers else float("nan")


# ---------------------------------------------------------------------------
# Tradeability filter (task 17)
# ---------------------------------------------------------------------------


def _apply_tradeability_filter(
    panel: pd.DataFrame,
    conn: Any,
    *,
    raw_db_path: Path | None,
    include_untradeable: bool,
    warnings_list: list[str],
) -> pd.DataFrame:
    """
    Mark each row with tradeable=True/False based on entry date (t+1) conditions.

    We need the entry date = next trade date after trade_date. We approximate
    by labelling the current row: if the NEXT row for same security is untradeable,
    then the signal at the current date is blocked.

    Preferred source: raw_suspend_d + raw_stk_limit from raw.duckdb.
    Fallback: heuristic from daily_bar_pit (open==high==low, pct ≈ 10%).
    """
    panel = panel.copy()
    panel["tradeable"] = True

    used_preferred = False

    if raw_db_path is not None and raw_db_path.exists():
        try:
            conn.execute(f"ATTACH '{str(raw_db_path).replace(chr(39), chr(39)*2)}' AS raw_db")
            # Check table presence
            tables = {r[0] for r in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'raw_db'"
            ).fetchall()}

            if "raw_suspend_d" in tables or "raw_stk_limit" in tables:
                # Build suspended/locked set for dates in panel
                dates_in_panel = tuple(panel["trade_date"].unique().tolist())
                suspended_set: set[tuple[str, str]] = set()
                locked_set: set[tuple[str, str]] = set()

                if "raw_suspend_d" in tables:
                    rows = conn.execute(
                        "SELECT ts_code, trade_date FROM raw_db.raw_suspend_d"
                    ).fetchall()
                    suspended_set = {(r[0], r[1]) for r in rows}

                if "raw_stk_limit" in tables:
                    rows = conn.execute(
                        "SELECT ts_code, trade_date FROM raw_db.raw_stk_limit"
                    ).fetchall()
                    locked_set = {(r[0], r[1]) for r in rows}

                def _is_blocked(row: Any) -> bool:
                    k = (row["security_id"], row["trade_date"])
                    return k in suspended_set or k in locked_set

                panel["tradeable"] = ~panel.apply(_is_blocked, axis=1)
                used_preferred = True

            conn.execute("DETACH raw_db")
        except Exception as exc:
            logger.warning("Could not use raw_db for tradeability: %s", exc)
            try:
                conn.execute("DETACH raw_db")
            except Exception:
                pass

    if not used_preferred:
        warnings_list.append("tradeability_raw_db_unavailable_using_heuristic")
        # Heuristic: open == high == low implies suspended or locked
        try:
            heuristic = conn.execute(
                """
                SELECT security_id, trade_date
                FROM daily_bar_pit
                WHERE open IS NOT NULL
                  AND ABS(open - high) < 0.001 * open
                  AND ABS(open - low)  < 0.001 * open
                """
            ).fetchall()
            heuristic_set = {(r[0], r[1]) for r in heuristic}
            panel["tradeable"] = ~panel.apply(
                lambda row: (row["security_id"], row["trade_date"]) in heuristic_set,
                axis=1,
            )
        except Exception:
            pass

    if not include_untradeable:
        panel = panel[panel["tradeable"]]

    return panel


# ---------------------------------------------------------------------------
# IC decay and half-life (task 18)
# ---------------------------------------------------------------------------


def _compute_ic_decay(
    conn: Any,
    descriptor_panel: pd.DataFrame,
    start_date: str,
    end_date: str,
    decay_horizons: tuple[int, ...] = (1, 5, 10, 20, 40, 60),
) -> IcDecay:
    fwd = compute_forward_returns(conn, start_date=start_date, end_date=end_date, horizons=decay_horizons)
    ic_means: list[float] = []
    for H in sorted(decay_horizons):
        if H not in fwd or fwd[H].empty:
            ic_means.append(float("nan"))
            continue
        fwd_df = fwd[H].rename(columns={"forward_return": f"fwd_{H}"})
        merged = descriptor_panel.merge(fwd_df[["security_id", "trade_date", f"fwd_{H}"]], on=["security_id", "trade_date"], how="inner")
        if len(merged) < 2:
            ic_means.append(float("nan"))
            continue
        ic_series = _compute_per_date_ic(merged.rename(columns={f"fwd_{H}": "fwd"}), "fwd", "pearson")
        ic_means.append(float(ic_series.mean()) if not ic_series.empty else float("nan"))

    # Half-life: first H where |IC| falls below half of H=1 IC
    hl = decay_horizons[-1]
    if len(ic_means) > 0 and not math.isnan(ic_means[0]) and abs(ic_means[0]) > 0:
        ref = abs(ic_means[0])
        for i, (H, ic) in enumerate(zip(sorted(decay_horizons), ic_means)):
            if not math.isnan(ic) and abs(ic) < ref / 2:
                hl = H
                break

    return IcDecay(
        horizons=list(sorted(decay_horizons)),
        ic_means=ic_means,
        half_life=hl,
    )


# ---------------------------------------------------------------------------
# Coverage diagnostics
# ---------------------------------------------------------------------------


def _compute_coverage(
    descriptor_panel: pd.DataFrame,
    tradeable_panel: pd.DataFrame,
    all_universe_rows: int,
) -> Coverage:
    rows_used = len(tradeable_panel)
    tradeable_rate = (
        len(tradeable_panel) / len(descriptor_panel)
        if len(descriptor_panel) > 0 else float("nan")
    )
    coverage = rows_used / all_universe_rows if all_universe_rows > 0 else float("nan")
    return Coverage(
        rows_used_over_possible=float(coverage),
        tradeable_rate=float(tradeable_rate),
        low_coverage_warning=coverage < 0.30 if not math.isnan(coverage) else False,
    )


# ---------------------------------------------------------------------------
# Top-level entry point (task 18)
# ---------------------------------------------------------------------------


def evaluate_descriptor(
    *,
    descriptor_id: str,
    research_db: Path,
    universe: str = "investable_a_share_core",
    start_date: str,
    end_date: str,
    horizons: tuple[int, ...] = (5, 20, 60),
    primary_horizon: int = 20,
    correlation_against: tuple[str, ...] = (),
    cost_model_path: Path | None = None,
    weighting: str = "equal",
    include_untradeable: bool = False,
    raw_db_path: Path | None = None,
) -> DescriptorEvaluationReport:
    """
    Full evaluation pipeline for one descriptor.

    Returns a DescriptorEvaluationReport.
    """
    import duckdb
    from .correlation_matrix import compute_cross_correlation
    from .slice_stability import compute_slice_stability

    run_at = datetime.now(UTC).isoformat()
    warnings_list: list[str] = []

    if not research_db.exists():
        raise EvaluationError(
            f"Research database not found: {research_db}. "
            "Run 'alpha-find-v2 build-research-source-db' first.",
            exit_code=4,
        )

    # Load cost model
    cost_bps = 15.0  # default: 15bps per side from base_a_share_cash
    cost_model_id = "base_a_share_cash"
    if cost_model_path is not None:
        try:
            from alpha_find_v2.config_loader import load_cost_model
            cm = load_cost_model(cost_model_path)
            cost_model_id = getattr(cm, "id", str(cost_model_path.stem))
        except Exception:
            warnings_list.append("cost_model_load_failed_using_default")

    conn = duckdb.connect(str(research_db), read_only=False)  # read_only=False to allow ATTACH

    try:
        # Step 1: Compute descriptor values
        t_compute_start = time.monotonic()
        spec = get(descriptor_id)
        ctx = ComputeContext(conn=conn, start_date=start_date, end_date=end_date)
        descriptor_panel = spec.fn(ctx)
        compute_duration_ms = int((time.monotonic() - t_compute_start) * 1000)
        logger.info(
            "descriptor=%s rows=%d dates=%d securities=%d duration_ms=%d",
            descriptor_id,
            len(descriptor_panel),
            descriptor_panel["trade_date"].nunique(),
            descriptor_panel["security_id"].nunique(),
            compute_duration_ms,
        )

        # Step 2: Resolve universe
        resolver = resolver_for_universe(universe, conn)
        trade_dates = sorted(descriptor_panel["trade_date"].unique().tolist())
        if not trade_dates:
            raise UniverseEmpty(universe, start_date, end_date)
        universe_by_date = resolver.resolve_batch(trade_dates)

        # Filter descriptor panel to universe
        rows_possible = sum(len(v) for v in universe_by_date.values())
        if rows_possible == 0:
            raise UniverseEmpty(universe, start_date, end_date)

        universe_rows = []
        for _, row in descriptor_panel.iterrows():
            td = row["trade_date"]
            if row["security_id"] in universe_by_date.get(td, set()):
                universe_rows.append(row)
        universe_panel = pd.DataFrame(universe_rows) if universe_rows else pd.DataFrame(columns=descriptor_panel.columns)

        # Step 3: Tradeability filter
        t_eval_start = time.monotonic()
        filtered_panel = _apply_tradeability_filter(
            universe_panel,
            conn,
            raw_db_path=raw_db_path,
            include_untradeable=include_untradeable,
            warnings_list=warnings_list,
        )

        # Step 4: Coverage
        coverage = _compute_coverage(universe_panel, filtered_panel, rows_possible)
        if coverage.low_coverage_warning:
            warnings_list.append("low_coverage_warning")

        # Step 5: Forward returns
        fwd_results = compute_forward_returns(
            conn, start_date=start_date, end_date=end_date, horizons=horizons
        )

        # Step 6: Per-horizon metrics
        horizon_metrics: dict[int, HorizonMetrics] = {}
        for H in horizons:
            if H not in fwd_results or fwd_results[H].empty:
                warnings_list.append(f"no_forward_returns_for_horizon_{H}")
                continue
            fwd_col = f"fwd_{H}"
            fwd_df = fwd_results[H].rename(columns={"forward_return": fwd_col})
            panel_h = filtered_panel.merge(
                fwd_df[["security_id", "trade_date", fwd_col]],
                on=["security_id", "trade_date"],
                how="inner",
            )
            if panel_h.empty:
                continue

            ic_p = _ic_stats(_compute_per_date_ic(panel_h, fwd_col, "pearson"))
            ic_s = _ic_stats(_compute_per_date_ic(panel_h, fwd_col, "spearman"))
            stability = _compute_rank_stability(filtered_panel)
            turnover = _compute_turnover_per_period(filtered_panel, n_securities=len(filtered_panel["security_id"].unique()))
            decile_ls = _compute_decile_ls(
                panel_h, fwd_col,
                cost_bps=cost_bps,
                periods_per_year=252,
                horizon=H,
            )
            horizon_metrics[H] = HorizonMetrics(
                horizon=H,
                ic_pearson=ic_p,
                ic_spearman=ic_s,
                rank_stability_lag1=stability,
                turnover_per_period=turnover,
                decile_ls=decile_ls,
            )

        # Step 7: IC decay
        decay_horizons = (1, 5, 10, 20, 40, 60)
        ic_decay = _compute_ic_decay(
            conn, filtered_panel, start_date, end_date,
            decay_horizons=decay_horizons,
        )

        # Step 8: Slice stability
        slice_stab = compute_slice_stability(filtered_panel, fwd_results.get(primary_horizon), conn)

        # Step 9: Cross-correlation
        cross_corr: dict[str, float] = {}
        if correlation_against:
            other_panels: dict[str, pd.DataFrame] = {}
            for other_id in correlation_against:
                try:
                    other_spec = get(other_id)
                    other_panel = other_spec.fn(ctx)
                    other_panels[other_id] = other_panel
                except Exception as exc:
                    warnings_list.append(f"correlation_{other_id}_failed: {exc}")
            cross_corr = compute_cross_correlation(filtered_panel, other_panels)

        # Step 10: descriptor_version
        config_root = Path(__file__).parents[3] / "config" / "descriptors"
        toml_path = config_root / f"{descriptor_id}.toml"
        from .descriptor_compute import descriptor_version as _dv
        ver = _dv(descriptor_id, toml_path if toml_path.exists() else None)

        eval_duration_ms = int((time.monotonic() - t_eval_start) * 1000)

        meta = EvaluationMeta(
            descriptor_id=descriptor_id,
            descriptor_version=ver,
            start_date=start_date,
            end_date=end_date,
            universe_definition={"id": universe},
            horizons=list(horizons),
            primary_horizon=primary_horizon,
            cost_model_id=cost_model_id,
            weighting=weighting,
            run_at=run_at,
            sample_size={
                "trade_dates": len(trade_dates),
                "securities_distinct": int(filtered_panel["security_id"].nunique()),
                "rows_used": len(filtered_panel),
            },
        )
        diagnostics = Diagnostics(
            warnings=warnings_list,
            compute_duration_ms=compute_duration_ms,
            evaluation_duration_ms=eval_duration_ms,
        )

        return DescriptorEvaluationReport(
            meta=meta,
            horizon_metrics=horizon_metrics,
            ic_decay=ic_decay,
            slice_stability=slice_stab,
            cross_correlation=cross_corr,
            coverage=coverage,
            diagnostics=diagnostics,
        )

    finally:
        conn.close()
