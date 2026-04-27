from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from pathlib import Path
import statistics
import tomllib

from .config_loader import PROJECT_ROOT, load_regime_overlay
from .live_state import BenchmarkStateArtifact, load_benchmark_state_artifact
from .models import RegimeOverlay
from .regime_overlay import (
    RegimeOverlayObservationArtifact,
    RegimeOverlayObservationStep,
)
from .research_artifact_loader import SleeveResearchArtifact, load_sleeve_artifact
from .trend_research_input_builder import _is_cn_a_directional_open_lock


JsonMap = dict[str, object]
_SUPPORTED_LIMIT_LOCK_MODES = {"cn_a_directional_open_lock"}


@dataclass(slots=True)
class RegimeOverlayObservationBuildCaseDefinition:
    case_id: str
    description: str
    overlay_path: str
    source_db_path: str
    benchmark_state_path: str
    trade_dates_artifact_path: str
    output_path: str
    trend_short_lookback_days: int = 20
    trend_long_lookback_days: int = 60
    breadth_return_lookback_days: int = 20
    dispersion_return_lookback_days: int = 20
    realized_volatility_lookback_days: int = 20
    breadth_supportive_min: float = 0.60
    breadth_risk_off_max: float = 0.45
    dispersion_supportive_max: float = 0.10
    dispersion_risk_off_min: float = 0.20
    realized_volatility_supportive_max: float = 0.20
    realized_volatility_risk_off_min: float = 0.35
    price_limit_stress_supportive_max: float = 0.01
    price_limit_stress_risk_off_min: float = 0.03
    limit_lock_mode: str = "cn_a_directional_open_lock"

    @classmethod
    def from_toml(cls, data: JsonMap) -> "RegimeOverlayObservationBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                "Unsupported regime overlay observation build case schema version: "
                f"{schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "regime_overlay_observation_build_case":
            raise ValueError(
                "Unsupported regime overlay observation build case type: "
                f"{artifact_type}"
            )
        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            overlay_path=str(data["overlay_path"]),
            source_db_path=str(data["source_db_path"]),
            benchmark_state_path=str(data["benchmark_state_path"]),
            trade_dates_artifact_path=str(data["trade_dates_artifact_path"]),
            output_path=str(data["output_path"]),
            trend_short_lookback_days=int(data.get("trend_short_lookback_days", 20)),
            trend_long_lookback_days=int(data.get("trend_long_lookback_days", 60)),
            breadth_return_lookback_days=int(data.get("breadth_return_lookback_days", 20)),
            dispersion_return_lookback_days=int(
                data.get("dispersion_return_lookback_days", 20)
            ),
            realized_volatility_lookback_days=int(
                data.get("realized_volatility_lookback_days", 20)
            ),
            breadth_supportive_min=float(data.get("breadth_supportive_min", 0.60)),
            breadth_risk_off_max=float(data.get("breadth_risk_off_max", 0.45)),
            dispersion_supportive_max=float(
                data.get("dispersion_supportive_max", 0.10)
            ),
            dispersion_risk_off_min=float(data.get("dispersion_risk_off_min", 0.20)),
            realized_volatility_supportive_max=float(
                data.get("realized_volatility_supportive_max", 0.20)
            ),
            realized_volatility_risk_off_min=float(
                data.get("realized_volatility_risk_off_min", 0.35)
            ),
            price_limit_stress_supportive_max=float(
                data.get("price_limit_stress_supportive_max", 0.01)
            ),
            price_limit_stress_risk_off_min=float(
                data.get("price_limit_stress_risk_off_min", 0.03)
            ),
            limit_lock_mode=str(
                data.get("limit_lock_mode", "cn_a_directional_open_lock")
            ),
        )


@dataclass(slots=True)
class LoadedRegimeOverlayObservationBuildCase:
    definition: RegimeOverlayObservationBuildCaseDefinition
    overlay: RegimeOverlay
    benchmark_state_artifact: BenchmarkStateArtifact
    trade_dates_artifact: SleeveResearchArtifact
    source_db_path: Path


@dataclass(slots=True)
class RegimeOverlayObservationBuildResult:
    case_id: str
    description: str
    overlay_id: str
    benchmark_id: str
    trade_date_count: int
    artifact: RegimeOverlayObservationArtifact
    state_counts: dict[str, dict[str, int]] = field(default_factory=dict)


@dataclass(slots=True)
class _DailyBarSnapshot:
    board: str
    is_st: bool
    pre_close: float | None
    open_price: float | None
    high_price: float | None
    low_price: float | None
    close_adj: float | None


def load_regime_overlay_observation_build_case(
    path: Path | str,
) -> LoadedRegimeOverlayObservationBuildCase:
    definition = RegimeOverlayObservationBuildCaseDefinition.from_toml(_read_toml(path))
    _validate_definition(definition)
    overlay = load_regime_overlay(definition.overlay_path)
    benchmark_state_artifact = load_benchmark_state_artifact(
        definition.benchmark_state_path
    )
    trade_dates_artifact = load_sleeve_artifact(definition.trade_dates_artifact_path)
    if overlay.benchmark != benchmark_state_artifact.benchmark_id:
        raise ValueError(
            "Regime overlay observation build case benchmark_state_path must match "
            "the configured regime overlay benchmark."
        )
    return LoadedRegimeOverlayObservationBuildCase(
        definition=definition,
        overlay=overlay,
        benchmark_state_artifact=benchmark_state_artifact,
        trade_dates_artifact=trade_dates_artifact,
        source_db_path=_resolve_project_path(definition.source_db_path),
    )


def build_regime_overlay_observation_history(
    loaded_case: LoadedRegimeOverlayObservationBuildCase,
) -> RegimeOverlayObservationBuildResult:
    decision_trade_dates = loaded_case.trade_dates_artifact.trade_dates()
    if not decision_trade_dates:
        raise ValueError(
            "Regime overlay observation build case trade_dates_artifact_path must "
            "contain at least one trade date."
        )

    calendar = _load_trade_calendar(loaded_case.source_db_path)
    calendar_index = {trade_date: index for index, trade_date in enumerate(calendar)}
    max_lookback = max(
        loaded_case.definition.trend_long_lookback_days,
        loaded_case.definition.breadth_return_lookback_days,
        loaded_case.definition.dispersion_return_lookback_days,
        loaded_case.definition.realized_volatility_lookback_days,
    )

    first_trade_date = decision_trade_dates[0]
    last_trade_date = decision_trade_dates[-1]
    if first_trade_date not in calendar_index:
        raise ValueError(
            f"Trade-date artifact contains date outside source calendar: {first_trade_date}"
        )
    if last_trade_date not in calendar_index:
        raise ValueError(
            f"Trade-date artifact contains date outside source calendar: {last_trade_date}"
        )
    first_index = calendar_index[first_trade_date]
    lower_index = max(0, first_index - max_lookback)
    lower_bound = calendar[lower_index]
    upper_bound = last_trade_date

    required_security_ids = sorted(
        {
            constituent.asset_id
            for trade_date in decision_trade_dates
            for constituent in loaded_case.benchmark_state_artifact.step_for_date(
                trade_date
            ).constituents
        }
    )
    bar_history = _load_bar_history(
        source_db_path=loaded_case.source_db_path,
        security_ids=required_security_ids,
        start_date=lower_bound,
        end_date=upper_bound,
    )
    benchmark_daily_returns = _load_benchmark_daily_returns(
        source_db_path=loaded_case.source_db_path,
        benchmark_id=loaded_case.benchmark_state_artifact.benchmark_id,
        start_date=lower_bound,
        end_date=upper_bound,
    )

    steps: list[RegimeOverlayObservationStep] = []
    state_counts = {
        input_name: {"supportive": 0, "neutral": 0, "risk_off": 0, "missing": 0}
        for input_name in loaded_case.overlay.required_inputs
    }
    for trade_date in decision_trade_dates:
        step = loaded_case.benchmark_state_artifact.step_for_date(trade_date)
        metrics = _metrics_for_trade_date(
            definition=loaded_case.definition,
            step=step,
            trade_date=trade_date,
            calendar=calendar,
            calendar_index=calendar_index,
            benchmark_daily_returns=benchmark_daily_returns,
            bar_history=bar_history,
        )
        input_states = {
            "benchmark_trend": _benchmark_trend_state(metrics),
            "market_breadth": _low_is_bad_state(
                value=metrics.get("market_breadth_share"),
                supportive_min=loaded_case.definition.breadth_supportive_min,
                risk_off_max=loaded_case.definition.breadth_risk_off_max,
            ),
            "dispersion": _high_is_bad_state(
                value=metrics.get("dispersion_return_std"),
                supportive_max=loaded_case.definition.dispersion_supportive_max,
                risk_off_min=loaded_case.definition.dispersion_risk_off_min,
            ),
            "realized_volatility": _high_is_bad_state(
                value=metrics.get("realized_volatility_annualized"),
                supportive_max=loaded_case.definition.realized_volatility_supportive_max,
                risk_off_min=loaded_case.definition.realized_volatility_risk_off_min,
            ),
            "price_limit_stress": _high_is_bad_state(
                value=metrics.get("price_limit_stress_share"),
                supportive_max=loaded_case.definition.price_limit_stress_supportive_max,
                risk_off_min=loaded_case.definition.price_limit_stress_risk_off_min,
            ),
        }
        for input_name, state in input_states.items():
            state_counts[input_name].setdefault(state, 0)
            state_counts[input_name][state] += 1
        steps.append(
            RegimeOverlayObservationStep(
                trade_date=trade_date,
                input_states=input_states,
                metrics=metrics,
            )
        )

    return RegimeOverlayObservationBuildResult(
        case_id=loaded_case.definition.case_id,
        description=loaded_case.definition.description,
        overlay_id=loaded_case.overlay.id,
        benchmark_id=loaded_case.benchmark_state_artifact.benchmark_id,
        trade_date_count=len(steps),
        artifact=RegimeOverlayObservationArtifact(
            overlay_id=loaded_case.overlay.id,
            steps=steps,
        ),
        state_counts=state_counts,
    )


def write_regime_overlay_observation_history(
    result: RegimeOverlayObservationBuildResult,
    path: Path | str,
) -> Path:
    resolved_path = _resolve_project_path(path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "artifact_type": "regime_overlay_observation_history",
        "overlay_id": result.overlay_id,
        "summary": {
            "benchmark_id": result.benchmark_id,
            "trade_date_count": result.trade_date_count,
            "state_counts": result.state_counts,
        },
        "steps": [
            {
                "trade_date": step.trade_date,
                "input_states": dict(step.input_states),
                "metrics": dict(step.metrics),
            }
            for step in result.artifact.steps
        ],
    }
    resolved_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return resolved_path


def _validate_definition(definition: RegimeOverlayObservationBuildCaseDefinition) -> None:
    for name, value in (
        ("trend_short_lookback_days", definition.trend_short_lookback_days),
        ("trend_long_lookback_days", definition.trend_long_lookback_days),
        ("breadth_return_lookback_days", definition.breadth_return_lookback_days),
        ("dispersion_return_lookback_days", definition.dispersion_return_lookback_days),
        (
            "realized_volatility_lookback_days",
            definition.realized_volatility_lookback_days,
        ),
    ):
        if value <= 0:
            raise ValueError(
                f"Regime overlay observation build case {name} must be positive."
            )
    if definition.trend_short_lookback_days > definition.trend_long_lookback_days:
        raise ValueError(
            "Regime overlay observation build case trend_short_lookback_days "
            "cannot exceed trend_long_lookback_days."
        )
    if not 0.0 <= definition.breadth_risk_off_max <= definition.breadth_supportive_min <= 1.0:
        raise ValueError(
            "Regime overlay observation build case breadth thresholds must satisfy "
            "0 <= risk_off_max <= supportive_min <= 1."
        )
    if not 0.0 <= definition.price_limit_stress_supportive_max <= definition.price_limit_stress_risk_off_min <= 1.0:
        raise ValueError(
            "Regime overlay observation build case price_limit_stress thresholds "
            "must satisfy 0 <= supportive_max <= risk_off_min <= 1."
        )
    if definition.dispersion_supportive_max < 0.0 or (
        definition.dispersion_supportive_max > definition.dispersion_risk_off_min
    ):
        raise ValueError(
            "Regime overlay observation build case dispersion thresholds must satisfy "
            "0 <= supportive_max <= risk_off_min."
        )
    if definition.realized_volatility_supportive_max < 0.0 or (
        definition.realized_volatility_supportive_max
        > definition.realized_volatility_risk_off_min
    ):
        raise ValueError(
            "Regime overlay observation build case realized_volatility thresholds "
            "must satisfy 0 <= supportive_max <= risk_off_min."
        )
    if definition.limit_lock_mode not in _SUPPORTED_LIMIT_LOCK_MODES:
        raise ValueError(
            "Regime overlay observation build case currently supports only "
            "limit_lock_mode='cn_a_directional_open_lock'."
        )


def _metrics_for_trade_date(
    *,
    definition: RegimeOverlayObservationBuildCaseDefinition,
    step: object,
    trade_date: str,
    calendar: list[str],
    calendar_index: dict[str, int],
    benchmark_daily_returns: dict[str, float],
    bar_history: dict[tuple[str, str], _DailyBarSnapshot],
) -> dict[str, float]:
    trade_index = calendar_index[trade_date]
    metrics: dict[str, float] = {
        "benchmark_constituent_count": float(len(step.constituents)),
    }

    short_returns = _benchmark_window_returns(
        benchmark_daily_returns,
        calendar,
        trade_index,
        definition.trend_short_lookback_days,
    )
    long_returns = _benchmark_window_returns(
        benchmark_daily_returns,
        calendar,
        trade_index,
        definition.trend_long_lookback_days,
    )
    if short_returns is not None:
        metrics["benchmark_short_return"] = short_returns
    if long_returns is not None:
        metrics["benchmark_long_return"] = long_returns

    volatility = _benchmark_window_volatility(
        benchmark_daily_returns,
        calendar,
        trade_index,
        definition.realized_volatility_lookback_days,
    )
    if volatility is not None:
        metrics["realized_volatility_annualized"] = volatility

    breadth_returns = _constituent_returns(
        constituents=step.constituents,
        trade_date=trade_date,
        lookback_days=definition.breadth_return_lookback_days,
        calendar=calendar,
        calendar_index=calendar_index,
        bar_history=bar_history,
    )
    if breadth_returns:
        metrics["market_breadth_share"] = sum(
            1.0 for value in breadth_returns if value > 0.0
        ) / len(breadth_returns)
        metrics["market_breadth_valid_name_count"] = float(len(breadth_returns))

    dispersion_returns = _constituent_returns(
        constituents=step.constituents,
        trade_date=trade_date,
        lookback_days=definition.dispersion_return_lookback_days,
        calendar=calendar,
        calendar_index=calendar_index,
        bar_history=bar_history,
    )
    if len(dispersion_returns) >= 2:
        metrics["dispersion_return_std"] = statistics.pstdev(dispersion_returns)
        metrics["dispersion_valid_name_count"] = float(len(dispersion_returns))

    price_limit_stress = _price_limit_stress_share(
        constituents=step.constituents,
        trade_date=trade_date,
        bar_history=bar_history,
    )
    if price_limit_stress is not None:
        metrics["price_limit_stress_share"] = price_limit_stress

    return metrics


def _benchmark_window_returns(
    benchmark_daily_returns: dict[str, float],
    calendar: list[str],
    trade_index: int,
    lookback_days: int,
) -> float | None:
    start_index = trade_index - lookback_days + 1
    if start_index < 0:
        return None
    window = [
        benchmark_daily_returns.get(trade_date)
        for trade_date in calendar[start_index : trade_index + 1]
    ]
    if any(value is None for value in window):
        return None
    cumulative = 1.0
    for value in window:
        assert value is not None
        cumulative *= 1.0 + value
    return cumulative - 1.0


def _benchmark_window_volatility(
    benchmark_daily_returns: dict[str, float],
    calendar: list[str],
    trade_index: int,
    lookback_days: int,
) -> float | None:
    start_index = trade_index - lookback_days + 1
    if start_index < 0:
        return None
    window = [
        benchmark_daily_returns.get(trade_date)
        for trade_date in calendar[start_index : trade_index + 1]
    ]
    if len(window) < 2 or any(value is None for value in window):
        return None
    realized = statistics.pstdev(value for value in window if value is not None)
    return realized * math.sqrt(252.0)


def _constituent_returns(
    *,
    constituents: list[object],
    trade_date: str,
    lookback_days: int,
    calendar: list[str],
    calendar_index: dict[str, int],
    bar_history: dict[tuple[str, str], _DailyBarSnapshot],
) -> list[float]:
    trade_index = calendar_index[trade_date]
    start_index = trade_index - lookback_days
    if start_index < 0:
        return []
    lookback_trade_date = calendar[start_index]
    returns: list[float] = []
    for constituent in constituents:
        current_bar = bar_history.get((constituent.asset_id, trade_date))
        lookback_bar = bar_history.get((constituent.asset_id, lookback_trade_date))
        if current_bar is None or lookback_bar is None:
            continue
        if (
            current_bar.close_adj is None
            or lookback_bar.close_adj is None
            or lookback_bar.close_adj <= 0.0
        ):
            continue
        returns.append((current_bar.close_adj / lookback_bar.close_adj) - 1.0)
    return returns


def _price_limit_stress_share(
    *,
    constituents: list[object],
    trade_date: str,
    bar_history: dict[tuple[str, str], _DailyBarSnapshot],
) -> float | None:
    if not constituents:
        return None
    stressed = 0
    valid = 0
    for constituent in constituents:
        bar = bar_history.get((constituent.asset_id, trade_date))
        if bar is None:
            continue
        valid += 1
        if _is_cn_a_directional_open_lock(
            board=bar.board,
            is_st=bar.is_st,
            pre_close=bar.pre_close,
            open_price=bar.open_price,
            high_price=bar.high_price,
            low_price=bar.low_price,
            direction="entry",
        ) or _is_cn_a_directional_open_lock(
            board=bar.board,
            is_st=bar.is_st,
            pre_close=bar.pre_close,
            open_price=bar.open_price,
            high_price=bar.high_price,
            low_price=bar.low_price,
            direction="exit",
        ):
            stressed += 1
    if valid == 0:
        return None
    return stressed / valid


def _benchmark_trend_state(metrics: dict[str, float]) -> str:
    short_return = metrics.get("benchmark_short_return")
    long_return = metrics.get("benchmark_long_return")
    if short_return is None or long_return is None:
        return "missing"
    tolerance = 0.001
    if short_return > tolerance and long_return > tolerance:
        return "supportive"
    if short_return < -tolerance and long_return < -tolerance:
        return "risk_off"
    return "neutral"


def _low_is_bad_state(
    *,
    value: float | None,
    supportive_min: float,
    risk_off_max: float,
) -> str:
    if value is None:
        return "missing"
    if value >= supportive_min:
        return "supportive"
    if value <= risk_off_max:
        return "risk_off"
    return "neutral"


def _high_is_bad_state(
    *,
    value: float | None,
    supportive_max: float,
    risk_off_min: float,
) -> str:
    if value is None:
        return "missing"
    if value <= supportive_max:
        return "supportive"
    if value >= risk_off_min:
        return "risk_off"
    return "neutral"


def _load_trade_calendar(source_db_path: Path) -> list[str]:
    import duckdb

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT trade_date
            FROM market_trade_calendar
            ORDER BY trade_date
            """
        ).fetchall()
    finally:
        conn.close()
    calendar = [str(trade_date) for (trade_date,) in rows]
    if not calendar:
        raise ValueError(
            "Regime overlay observation builder requires a non-empty market_trade_calendar."
        )
    return calendar


def _load_bar_history(
    *,
    source_db_path: Path,
    security_ids: list[str],
    start_date: str,
    end_date: str,
) -> dict[tuple[str, str], _DailyBarSnapshot]:
    if not security_ids:
        return {}
    import duckdb

    placeholders = ", ".join("?" for _ in security_ids)
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            SELECT
                security_id,
                trade_date,
                board,
                is_st,
                pre_close,
                open,
                high,
                low,
                close_adj
            FROM daily_bar_pit
            WHERE security_id IN ({placeholders})
              AND trade_date BETWEEN ? AND ?
            ORDER BY security_id, trade_date
            """,
            [*security_ids, start_date, end_date],
        ).fetchall()
    finally:
        conn.close()
    return {
        (str(security_id), str(trade_date)): _DailyBarSnapshot(
            board=str(board),
            is_st=bool(is_st),
            pre_close=None if pre_close is None else float(pre_close),
            open_price=None if open_price is None else float(open_price),
            high_price=None if high_price is None else float(high_price),
            low_price=None if low_price is None else float(low_price),
            close_adj=None if close_adj is None else float(close_adj),
        )
        for (
            security_id,
            trade_date,
            board,
            is_st,
            pre_close,
            open_price,
            high_price,
            low_price,
            close_adj,
        ) in rows
    }


def _load_benchmark_daily_returns(
    *,
    source_db_path: Path,
    benchmark_id: str,
    start_date: str,
    end_date: str,
) -> dict[str, float]:
    import duckdb

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            WITH calendar AS (
                SELECT trade_date
                FROM market_trade_calendar
                WHERE trade_date BETWEEN ? AND ?
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
                SUM(weights.weight * ((bars.close_adj / bars.previous_close_adj) - 1.0))
                    / SUM(weights.weight) AS benchmark_return
            FROM latest_snapshots AS snapshots
            INNER JOIN benchmark_weight_snapshot_pit AS weights
                ON weights.benchmark_id = ?
               AND weights.trade_date = snapshots.snapshot_date
            INNER JOIN (
                SELECT
                    current.security_id,
                    current.trade_date,
                    current.close_adj,
                    (
                        SELECT prev.close_adj
                        FROM daily_bar_pit AS prev
                        WHERE prev.security_id = current.security_id
                          AND prev.trade_date < current.trade_date
                          AND prev.close_adj IS NOT NULL
                          AND prev.close_adj > 0.0
                        ORDER BY prev.trade_date DESC
                        LIMIT 1
                    ) AS previous_close_adj
                FROM daily_bar_pit AS current
                WHERE current.trade_date BETWEEN ? AND ?
                  AND current.close_adj IS NOT NULL
            ) AS bars
                ON bars.security_id = weights.security_id
               AND bars.trade_date = snapshots.trade_date
               AND bars.previous_close_adj IS NOT NULL
               AND bars.previous_close_adj > 0.0
            GROUP BY snapshots.trade_date
            ORDER BY snapshots.trade_date
            """,
            [start_date, end_date, benchmark_id, benchmark_id, start_date, end_date],
        ).fetchall()
    finally:
        conn.close()
    return {
        str(trade_date): float(benchmark_return)
        for trade_date, benchmark_return in rows
        if benchmark_return is not None
    }


def _resolve_project_path(path: Path | str) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return PROJECT_ROOT / candidate


def _read_toml(path: Path | str) -> JsonMap:
    resolved_path = _resolve_project_path(path)
    with resolved_path.open("rb") as handle:
        return tomllib.load(handle)
