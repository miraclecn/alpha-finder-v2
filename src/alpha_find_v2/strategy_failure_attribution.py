from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
from pathlib import Path
from typing import Any

from .config_loader import CONFIG_ROOT, PROJECT_ROOT, load_regime_overlay
from .regime_overlay import (
    RegimeOverlayEvaluator,
    load_regime_overlay_observation_artifact,
)


JsonMap = dict[str, Any]


def build_strategy_failure_attribution(
    *,
    backtest_path: Path | str,
    source_db_path: Path | str,
    overlay_observations_path: Path | str | None = None,
    overlay_config_path: Path | str | None = None,
    industry_schema: str = "sw2021_l1",
    top_n: int = 10,
) -> JsonMap:
    payload = _read_json(backtest_path)
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError("Strategy failure attribution requires schema_version=1.")
    if str(payload.get("artifact_type", "")) != "portfolio_backtest_result":
        raise ValueError("Strategy failure attribution requires portfolio_backtest_result.")

    artifact = dict(payload.get("artifact", {}))
    daily_curve = _sorted_by_date(artifact.get("daily_curve", []), "trade_date")
    daily_holdings = _sorted_by_date(artifact.get("daily_holdings", []), "trade_date")
    orders = _sorted_by_date(artifact.get("orders", []), "execution_date")
    fills = _sorted_by_date(artifact.get("fills", []), "execution_date")
    diagnostics = dict(artifact.get("diagnostics", {}))
    summary = dict(artifact.get("summary", {}))
    if not daily_curve:
        raise ValueError("Strategy failure attribution requires a non-empty daily_curve.")

    asset_ids = sorted(
        {
            str(item.get("asset_id", ""))
            for item in daily_holdings
            if str(item.get("asset_id", "")).strip()
        }
    )
    trade_dates = [_normalize_date_key(str(item["trade_date"])) for item in daily_curve]
    raw_returns = _load_raw_returns(
        source_db_path=source_db_path,
        asset_ids=asset_ids,
        start_date=trade_dates[0],
        end_date=trade_dates[-1],
    )
    industry_lookup = _load_industry_lookup(
        source_db_path=source_db_path,
        asset_ids=asset_ids,
        industry_schema=industry_schema,
    )

    contribution = _holding_and_industry_contribution(
        daily_curve=daily_curve,
        daily_holdings=daily_holdings,
        raw_returns=raw_returns,
        industry_lookup=industry_lookup,
        industry_schema=industry_schema,
        top_n=top_n,
    )
    overlay = _overlay_state_comparison(
        daily_curve=daily_curve,
        overlay_observations_path=overlay_observations_path,
        overlay_config_path=overlay_config_path,
    )
    market_state = _market_state_attribution(
        daily_curve=daily_curve,
        overlay_observations_path=overlay_observations_path,
    )

    report = {
        "schema_version": 1,
        "artifact_type": "strategy_failure_attribution_report",
        "backtest": {
            "case_id": str(payload.get("case_id", "")),
            "description": str(payload.get("description", "")),
            "path": str(_resolve_project_path(backtest_path)),
            "summary": summary,
        },
        "source_db_path": str(_resolve_project_path(source_db_path)),
        "return_buckets": {
            "yearly": _return_buckets(daily_curve, "year"),
            "monthly": _return_buckets(daily_curve, "month"),
        },
        "holding_contribution": contribution["holding_contribution"],
        "industry_contribution": contribution["industry_contribution"],
        "market_state_attribution": market_state,
        "turnover": _turnover_report(daily_curve=daily_curve, fills=fills, summary=summary),
        "trade_friction": _trade_friction_report(
            daily_curve=daily_curve,
            orders=orders,
            fills=fills,
            diagnostics=diagnostics,
            summary=summary,
        ),
        "overlay_state_comparison": overlay,
        "loss_concentration": _loss_concentration(
            holding_totals=contribution["holding_totals"],
            industry_totals=contribution["industry_totals"],
        ),
    }
    return report


def write_strategy_failure_attribution(report: JsonMap, path: Path | str) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    return target


def _holding_and_industry_contribution(
    *,
    daily_curve: list[JsonMap],
    daily_holdings: list[JsonMap],
    raw_returns: dict[tuple[str, str], float],
    industry_lookup: dict[str, list[tuple[str, str, str]]],
    industry_schema: str,
    top_n: int,
) -> JsonMap:
    holdings_by_date: dict[str, list[JsonMap]] = defaultdict(list)
    for holding in daily_holdings:
        holdings_by_date[_normalize_date_key(str(holding["trade_date"]))].append(holding)

    dates = [_normalize_date_key(str(item["trade_date"])) for item in daily_curve]
    holding_totals: dict[str, float] = defaultdict(float)
    holding_weight_sums: dict[str, float] = defaultdict(float)
    holding_observations: Counter[str] = Counter()
    industry_totals: dict[str, float] = defaultdict(float)
    industry_observations: Counter[str] = Counter()
    missing_return_count = 0
    missing_industry_count = 0

    for previous_date, trade_date in zip(dates, dates[1:]):
        for holding in holdings_by_date.get(previous_date, []):
            asset_id = str(holding.get("asset_id", "")).strip()
            if not asset_id:
                continue
            raw_return = raw_returns.get((asset_id, trade_date))
            if raw_return is None:
                missing_return_count += 1
                continue
            weight = float(holding.get("weight", 0.0) or 0.0)
            contribution = weight * raw_return
            holding_totals[asset_id] += contribution
            holding_weight_sums[asset_id] += weight
            holding_observations[asset_id] += 1

            industry_code = _industry_for_date(industry_lookup, asset_id, trade_date)
            if industry_code is None:
                industry_code = "unclassified"
                missing_industry_count += 1
            industry_totals[industry_code] += contribution
            industry_observations[industry_code] += 1

    holding_rows = [
        {
            "asset_id": asset_id,
            "contribution": contribution,
            "observation_count": holding_observations[asset_id],
            "average_weight": (
                holding_weight_sums[asset_id] / holding_observations[asset_id]
                if holding_observations[asset_id]
                else 0.0
            ),
        }
        for asset_id, contribution in holding_totals.items()
    ]
    industry_rows = [
        {
            "industry_schema": industry_schema,
            "industry_code": industry_code,
            "contribution": contribution,
            "observation_count": industry_observations[industry_code],
        }
        for industry_code, contribution in industry_totals.items()
    ]
    return {
        "holding_totals": dict(holding_totals),
        "industry_totals": dict(industry_totals),
        "holding_contribution": {
            "method": "previous_close_holding_weight_times_current_raw_daily_return",
            "top_losers": _top_losers(holding_rows, top_n),
            "top_winners": _top_winners(holding_rows, top_n),
            "coverage": {
                "asset_count": len(holding_totals),
                "missing_raw_return_observations": missing_return_count,
            },
        },
        "industry_contribution": {
            "industry_schema": industry_schema,
            "top_losers": _top_losers(industry_rows, top_n),
            "top_winners": _top_winners(industry_rows, top_n),
            "coverage": {
                "industry_count": len(industry_totals),
                "missing_industry_observations": missing_industry_count,
            },
        },
    }


def _return_buckets(daily_curve: list[JsonMap], bucket: str) -> JsonMap:
    grouped: dict[str, list[JsonMap]] = defaultdict(list)
    for state in daily_curve:
        trade_date = _normalize_date_key(str(state["trade_date"]))
        key = trade_date[:4] if bucket == "year" else f"{trade_date[:4]}-{trade_date[4:6]}"
        grouped[key].append(state)

    result: JsonMap = {}
    for key in sorted(grouped):
        states = grouped[key]
        first_equity = float(states[0].get("equity", 0.0) or 0.0)
        last_equity = float(states[-1].get("equity", 0.0) or 0.0)
        returns = [float(state.get("daily_return", 0.0) or 0.0) for state in states]
        result[key] = {
            "start_date": _normalize_date_key(str(states[0]["trade_date"])),
            "end_date": _normalize_date_key(str(states[-1]["trade_date"])),
            "trading_days": len(states),
            "return": (last_equity / first_equity) - 1.0 if first_equity > 0.0 else 0.0,
            "max_drawdown": _max_drawdown(
                [float(state.get("equity", 0.0) or 0.0) for state in states]
            ),
            "average_daily_return": sum(returns) / len(returns) if returns else 0.0,
        }
    return result


def _turnover_report(
    *,
    daily_curve: list[JsonMap],
    fills: list[JsonMap],
    summary: JsonMap,
) -> JsonMap:
    average_equity = _average_equity(daily_curve)
    buy_gross = sum(float(fill.get("gross_value", 0.0) or 0.0) for fill in fills if fill.get("side") == "buy")
    sell_gross = sum(float(fill.get("gross_value", 0.0) or 0.0) for fill in fills if fill.get("side") == "sell")
    return {
        "summary_turnover": float(summary.get("turnover", 0.0) or 0.0),
        "computed_turnover": _turnover_from_gross(buy_gross, sell_gross, average_equity),
        "buy_gross_value_cny": buy_gross,
        "sell_gross_value_cny": sell_gross,
        "by_year": _fill_value_buckets(daily_curve, fills, "year"),
        "by_month": _fill_value_buckets(daily_curve, fills, "month"),
    }


def _trade_friction_report(
    *,
    daily_curve: list[JsonMap],
    orders: list[JsonMap],
    fills: list[JsonMap],
    diagnostics: JsonMap,
    summary: JsonMap,
) -> JsonMap:
    blocked_orders = [dict(item) for item in diagnostics.get("blocked_orders", [])]
    partial_fills = [dict(item) for item in diagnostics.get("partial_fills", [])]
    total_cost = sum(float(fill.get("cost", 0.0) or 0.0) for fill in fills)
    initial_cash = float(summary.get("initial_cash_cny", 0.0) or 0.0)
    return {
        "orders": {
            "total": len(orders),
            "by_side": _count_by(orders, "side"),
            "by_reason": _count_by(orders, "reason"),
        },
        "fills": {
            "total": len(fills),
            "by_side": _count_by(fills, "side"),
        },
        "blocked_orders": {
            "total": len(blocked_orders),
            "share": len(blocked_orders) / len(orders) if orders else 0.0,
            "summary_share": float(summary.get("blocked_trade_share", 0.0) or 0.0),
            "by_reason": _count_by(blocked_orders, "reason"),
            "by_side": _count_by(blocked_orders, "side"),
            "by_year": _diagnostic_buckets(blocked_orders, "year"),
            "by_month": _diagnostic_buckets(blocked_orders, "month"),
        },
        "partial_fills": {
            "total": len(partial_fills),
            "share": len(partial_fills) / len(orders) if orders else 0.0,
            "summary_share": float(summary.get("partial_fill_share", 0.0) or 0.0),
            "by_reason": _count_by(partial_fills, "reason"),
            "by_side": _count_by(partial_fills, "side"),
            "by_year": _diagnostic_buckets(partial_fills, "year"),
            "by_month": _diagnostic_buckets(partial_fills, "month"),
        },
        "cost_drag": {
            "total_cost_cny": total_cost,
            "summary_total_cost_cny": float(summary.get("total_costs", total_cost) or 0.0),
            "return_drag_on_initial_cash": -(total_cost / initial_cash)
            if initial_cash > 0.0
            else 0.0,
            "by_year": _cost_buckets(daily_curve, fills, "year"),
            "by_month": _cost_buckets(daily_curve, fills, "month"),
        },
    }


def _overlay_state_comparison(
    *,
    daily_curve: list[JsonMap],
    overlay_observations_path: Path | str | None,
    overlay_config_path: Path | str | None,
) -> JsonMap:
    observation_path = _resolve_optional_existing_overlay_path(overlay_observations_path)
    if observation_path is None:
        return {
            "available": False,
            "reason": "overlay_observations_not_found",
            "state_counts": {},
            "by_state": {},
        }

    observations = load_regime_overlay_observation_artifact(observation_path)
    config_path = (
        _resolve_project_path(overlay_config_path)
        if overlay_config_path is not None
        else CONFIG_ROOT / "regime_overlays" / f"{observations.overlay_id}.toml"
    )
    if not config_path.exists():
        return {
            "available": False,
            "reason": "overlay_config_not_found",
            "overlay_observations_path": str(observation_path),
            "state_counts": {},
            "by_state": {},
        }

    curve_by_date = {
        _normalize_date_key(str(state["trade_date"])): state for state in daily_curve
    }
    trade_dates = [
        _normalize_date_key(step.trade_date)
        for step in observations.steps
        if _normalize_date_key(step.trade_date) in curve_by_date
    ]
    evidence = RegimeOverlayEvaluator(load_regime_overlay(config_path)).evaluate_history(
        trade_dates=trade_dates,
        observations=observations.steps,
    )
    grouped: dict[str, list[float]] = defaultdict(list)
    for decision in evidence.decisions:
        state = curve_by_date[decision.trade_date]
        grouped[decision.state].append(float(state.get("daily_return", 0.0) or 0.0))

    return {
        "available": True,
        "overlay_id": observations.overlay_id,
        "overlay_observations_path": str(observation_path),
        "state_counts": {state: len(returns) for state, returns in sorted(grouped.items())},
        "by_state": {
            state: _return_sample_summary(returns)
            for state, returns in sorted(grouped.items())
        },
    }


def _market_state_attribution(
    *,
    daily_curve: list[JsonMap],
    overlay_observations_path: Path | str | None,
) -> JsonMap:
    observation_path = _resolve_optional_existing_overlay_path(overlay_observations_path)
    if observation_path is None:
        return {
            "available": False,
            "reason": "overlay_observations_not_found",
            "by_risk_off_bucket": {},
            "by_input_state": {},
        }

    curve_by_date = {
        _normalize_date_key(str(state["trade_date"])): state for state in daily_curve
    }
    observations = load_regime_overlay_observation_artifact(observation_path)
    by_risk_off_bucket: dict[str, list[float]] = defaultdict(list)
    by_input_state: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for step in observations.steps:
        trade_date = _normalize_date_key(step.trade_date)
        state = curve_by_date.get(trade_date)
        if state is None:
            continue
        daily_return = float(state.get("daily_return", 0.0) or 0.0)
        risk_off_count = sum(1 for value in step.input_states.values() if value == "risk_off")
        by_risk_off_bucket[_risk_off_bucket(risk_off_count)].append(daily_return)
        for input_name, input_state in step.input_states.items():
            by_input_state[input_name][input_state].append(daily_return)

    return {
        "available": True,
        "overlay_observations_path": str(observation_path),
        "by_risk_off_bucket": {
            bucket: _return_sample_summary(returns)
            for bucket, returns in sorted(by_risk_off_bucket.items())
        },
        "by_input_state": {
            input_name: {
                input_state: _return_sample_summary(returns)
                for input_state, returns in sorted(state_returns.items())
            }
            for input_name, state_returns in sorted(by_input_state.items())
        },
    }


def _loss_concentration(
    *,
    holding_totals: dict[str, float],
    industry_totals: dict[str, float],
) -> JsonMap:
    holding_losses = sorted(
        (-value for value in holding_totals.values() if value < 0.0),
        reverse=True,
    )
    industry_losses = sorted(
        (-value for value in industry_totals.values() if value < 0.0),
        reverse=True,
    )
    holding_share = _top_loss_share(holding_losses, 5)
    industry_share = _top_loss_share(industry_losses, 3)
    if not holding_losses and not industry_losses:
        classification = "no_losses"
    elif holding_share >= 0.5 or industry_share >= 0.5:
        classification = "concentrated"
    else:
        classification = "broad_based"
    return {
        "classification": classification,
        "total_holding_loss_contribution": -sum(holding_losses),
        "total_industry_loss_contribution": -sum(industry_losses),
        "losing_holding_count": len(holding_losses),
        "losing_industry_count": len(industry_losses),
        "top_5_holding_loss_share": holding_share,
        "top_3_industry_loss_share": industry_share,
        "rule": "concentrated if top 5 holdings or top 3 industries explain at least 50% of losses",
    }


def _load_raw_returns(
    *,
    source_db_path: Path | str,
    asset_ids: list[str],
    start_date: str,
    end_date: str,
) -> dict[tuple[str, str], float]:
    if not asset_ids:
        return {}
    import duckdb

    conn = duckdb.connect(str(_resolve_project_path(source_db_path)), read_only=True)
    try:
        if not _table_exists(conn, "daily_bar_pit"):
            raise ValueError("source DB must contain daily_bar_pit.")
        placeholders = ", ".join("?" for _ in asset_ids)
        rows = conn.execute(
            f"""
            SELECT security_id, trade_date, pct_chg, close, pre_close
            FROM daily_bar_pit
            WHERE security_id IN ({placeholders})
              AND trade_date >= ?
              AND trade_date <= ?
            """,
            [*asset_ids, start_date, end_date],
        ).fetchall()
    finally:
        conn.close()

    returns: dict[tuple[str, str], float] = {}
    for asset_id, trade_date, pct_chg, close_price, pre_close in rows:
        normalized_date = _normalize_date_key(str(trade_date))
        raw_return = _raw_return(
            pct_chg=pct_chg,
            close_price=close_price,
            pre_close=pre_close,
        )
        if raw_return is not None:
            returns[(str(asset_id), normalized_date)] = raw_return
    return returns


def _load_industry_lookup(
    *,
    source_db_path: Path | str,
    asset_ids: list[str],
    industry_schema: str,
) -> dict[str, list[tuple[str, str, str]]]:
    if not asset_ids:
        return {}
    import duckdb

    conn = duckdb.connect(str(_resolve_project_path(source_db_path)), read_only=True)
    try:
        if not _table_exists(conn, "industry_classification_pit"):
            return {}
        placeholders = ", ".join("?" for _ in asset_ids)
        rows = conn.execute(
            f"""
            SELECT security_id, industry_code, effective_at, removed_at
            FROM industry_classification_pit
            WHERE security_id IN ({placeholders})
              AND industry_schema = ?
            ORDER BY security_id, effective_at
            """,
            [*asset_ids, industry_schema],
        ).fetchall()
    finally:
        conn.close()

    lookup: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    for asset_id, industry_code, effective_at, removed_at in rows:
        lookup[str(asset_id)].append(
            (
                _normalize_date_key(str(effective_at)[:10]),
                _normalize_date_key(str(removed_at)[:10]) if removed_at else "",
                str(industry_code),
            )
        )
    return dict(lookup)


def _industry_for_date(
    lookup: dict[str, list[tuple[str, str, str]]],
    asset_id: str,
    trade_date: str,
) -> str | None:
    for effective_at, removed_at, industry_code in lookup.get(asset_id, []):
        if effective_at <= trade_date and (not removed_at or trade_date < removed_at):
            return industry_code
    return None


def _fill_value_buckets(daily_curve: list[JsonMap], fills: list[JsonMap], bucket: str) -> JsonMap:
    equity_by_bucket = _equity_by_bucket(daily_curve, bucket)
    gross_by_bucket: dict[str, dict[str, float]] = defaultdict(lambda: {"buy": 0.0, "sell": 0.0})
    for fill in fills:
        key = _bucket_key(str(fill.get("execution_date", "")), bucket)
        side = str(fill.get("side", ""))
        if side in {"buy", "sell"}:
            gross_by_bucket[key][side] += float(fill.get("gross_value", 0.0) or 0.0)
    result: JsonMap = {}
    for key in sorted(set(equity_by_bucket) | set(gross_by_bucket)):
        buy_gross = gross_by_bucket[key]["buy"]
        sell_gross = gross_by_bucket[key]["sell"]
        average_equity = _mean(equity_by_bucket.get(key, []))
        result[key] = {
            "buy_gross_value_cny": buy_gross,
            "sell_gross_value_cny": sell_gross,
            "turnover": _turnover_from_gross(buy_gross, sell_gross, average_equity),
        }
    return result


def _cost_buckets(daily_curve: list[JsonMap], fills: list[JsonMap], bucket: str) -> JsonMap:
    equity_by_bucket = _equity_by_bucket(daily_curve, bucket)
    cost_by_bucket: Counter[str] = Counter()
    for fill in fills:
        cost_by_bucket[_bucket_key(str(fill.get("execution_date", "")), bucket)] += float(
            fill.get("cost", 0.0) or 0.0
        )
    result: JsonMap = {}
    for key in sorted(set(equity_by_bucket) | set(cost_by_bucket)):
        average_equity = _mean(equity_by_bucket.get(key, []))
        total_cost = float(cost_by_bucket.get(key, 0.0))
        result[key] = {
            "total_cost_cny": total_cost,
            "return_drag_on_average_equity": -(total_cost / average_equity)
            if average_equity > 0.0
            else 0.0,
        }
    return result


def _diagnostic_buckets(items: list[JsonMap], bucket: str) -> JsonMap:
    grouped: dict[str, list[JsonMap]] = defaultdict(list)
    for item in items:
        grouped[_bucket_key(str(item.get("execution_date", "")), bucket)].append(item)
    return {
        key: {
            "total": len(values),
            "by_reason": _count_by(values, "reason"),
            "by_side": _count_by(values, "side"),
        }
        for key, values in sorted(grouped.items())
    }


def _return_sample_summary(returns: list[float]) -> JsonMap:
    if not returns:
        return {
            "observation_count": 0,
            "average_daily_return": 0.0,
            "cumulative_return": 0.0,
            "positive_days": 0,
            "negative_days": 0,
        }
    cumulative = 1.0
    for value in returns:
        cumulative *= 1.0 + value
    return {
        "observation_count": len(returns),
        "average_daily_return": sum(returns) / len(returns),
        "cumulative_return": cumulative - 1.0,
        "positive_days": sum(1 for value in returns if value > 0.0),
        "negative_days": sum(1 for value in returns if value < 0.0),
    }


def _top_losers(rows: list[JsonMap], top_n: int) -> list[JsonMap]:
    return sorted(rows, key=lambda item: float(item["contribution"]))[:top_n]


def _top_winners(rows: list[JsonMap], top_n: int) -> list[JsonMap]:
    return sorted(rows, key=lambda item: float(item["contribution"]), reverse=True)[:top_n]


def _top_loss_share(losses: list[float], top_n: int) -> float:
    total = sum(losses)
    if total <= 0.0:
        return 0.0
    return sum(losses[:top_n]) / total


def _count_by(items: list[JsonMap], key: str) -> dict[str, int]:
    counts = Counter(str(item.get(key, "unknown") or "unknown") for item in items)
    return dict(sorted(counts.items()))


def _average_equity(daily_curve: list[JsonMap]) -> float:
    return _mean([float(state.get("equity", 0.0) or 0.0) for state in daily_curve])


def _equity_by_bucket(daily_curve: list[JsonMap], bucket: str) -> dict[str, list[float]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for state in daily_curve:
        grouped[_bucket_key(str(state["trade_date"]), bucket)].append(
            float(state.get("equity", 0.0) or 0.0)
        )
    return dict(grouped)


def _turnover_from_gross(buy_gross: float, sell_gross: float, average_equity: float) -> float:
    if average_equity <= 0.0:
        return 0.0
    return ((buy_gross + sell_gross) / 2.0) / average_equity


def _raw_return(*, pct_chg: object, close_price: object, pre_close: object) -> float | None:
    if pct_chg is not None:
        return float(pct_chg) / 100.0
    if close_price is None or pre_close is None:
        return None
    previous = float(pre_close)
    if previous <= 0.0:
        return None
    return (float(close_price) / previous) - 1.0


def _max_drawdown(equity: list[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    max_drawdown = 0.0
    for value in equity:
        peak = max(peak, value)
        if peak > 0.0:
            max_drawdown = min(max_drawdown, (value / peak) - 1.0)
    return max_drawdown


def _bucket_key(trade_date: str, bucket: str) -> str:
    normalized = _normalize_date_key(trade_date)
    if bucket == "year":
        return normalized[:4]
    if bucket == "month":
        return f"{normalized[:4]}-{normalized[4:6]}"
    raise ValueError(f"Unsupported bucket: {bucket}")


def _risk_off_bucket(risk_off_count: int) -> str:
    if risk_off_count <= 0:
        return "risk_off_0"
    if risk_off_count <= 2:
        return "risk_off_1_to_2"
    return "risk_off_3_plus"


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _sorted_by_date(items: object, key: str) -> list[JsonMap]:
    rows = [dict(item) for item in list(items or [])]
    return sorted(rows, key=lambda item: _normalize_date_key(str(item.get(key, ""))))


def _normalize_date_key(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    if len(text) == 8 and text.isdigit():
        return text
    return datetime.strptime(text[:10], "%Y-%m-%d").strftime("%Y%m%d")


def _table_exists(conn: object, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM duckdb_tables()
        WHERE table_name = ?
        LIMIT 1
        """,
        [table_name],
    ).fetchone()
    return row is not None


def _resolve_optional_existing_overlay_path(path: Path | str | None) -> Path | None:
    if path is not None:
        target = _resolve_project_path(path)
        return target if target.exists() else None
    default_path = PROJECT_ROOT / "output" / "trend_live_candidate_overlay_observations.json"
    return default_path if default_path.exists() else None


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_json(path: Path | str) -> JsonMap:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)
