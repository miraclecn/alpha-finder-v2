from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
from pathlib import Path

from .config_loader import CONFIG_ROOT, load_cost_model, load_descriptor_set, load_sleeve, load_target
from .research_artifact_loader import (
    SleeveResearchObservationInput,
    SleeveResearchObservationRecord,
    SleeveResearchObservationStep,
)
from .target_builder import TradeLegState
from .trend_research_input_builder import (
    SUPPORTED_LIMIT_LOCK_MODES,
    _calendar_bounds,
    _is_cn_a_directional_open_lock,
    _load_industry_labels,
    _load_trade_calendar,
    _load_trade_leg_snapshots,
    _read_toml,
    _resolve_project_path,
    _resolve_trade_leg_open,
    _trade_leg_fallback_price,
    write_trend_research_observation_input,
)


SUPPORTED_DESCRIPTOR_IDS = {
    "sector_relative_valuation",
    "profitability_quality",
    "accrual_quality",
    "leverage_conservatism",
}
SUPPORTED_INDUSTRY_LABEL_SOURCES = {
    "industry_classification_pit",
}


@dataclass(slots=True)
class FundamentalResearchInputBuildCaseDefinition:
    case_id: str
    description: str
    sleeve_path: str
    source_db_path: str
    output_path: str
    residual_component_snapshot_path: str = ""
    start_date: str = ""
    end_date: str = ""
    min_listing_days: int = 120
    lookback_days: int = 20
    turnover_window_days: int = 20
    rebalance_stride: int = 5
    industry_label_source: str = "industry_classification_pit"
    industry_schema: str = ""
    limit_lock_mode: str = "disabled"

    @classmethod
    def from_toml(cls, data: dict[str, object]) -> "FundamentalResearchInputBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                "Unsupported fundamental research input build case schema version: "
                f"{schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "fundamental_research_input_build_case":
            raise ValueError(
                f"Unsupported fundamental research input build case type: {artifact_type}"
            )

        return cls(
            case_id=str(data["case_id"]),
            description=str(data["description"]),
            sleeve_path=str(data["sleeve_path"]),
            source_db_path=str(data["source_db_path"]),
            output_path=str(data["output_path"]),
            residual_component_snapshot_path=str(
                data.get("residual_component_snapshot_path", "")
            ),
            start_date=str(data.get("start_date", "")),
            end_date=str(data.get("end_date", "")),
            min_listing_days=int(data.get("min_listing_days", 120)),
            lookback_days=int(data.get("lookback_days", 20)),
            turnover_window_days=int(data.get("turnover_window_days", 20)),
            rebalance_stride=int(data.get("rebalance_stride", 5)),
            industry_label_source=str(
                data.get("industry_label_source", "industry_classification_pit")
            ),
            industry_schema=str(data.get("industry_schema", "")),
            limit_lock_mode=str(data.get("limit_lock_mode", "disabled")),
        )


@dataclass(slots=True)
class LoadedFundamentalResearchInputBuildCase:
    definition: FundamentalResearchInputBuildCaseDefinition
    sleeve_id: str
    descriptor_set_id: str
    target_id: str
    source_db_path: Path
    output_path: str
    holding_count: int
    holding_horizon_days: int
    min_turnover_cny_mn: float
    descriptor_weights: dict[str, float]
    residual_components: list[str] = field(default_factory=list)
    residual_component_snapshot_path: Path | None = None


@dataclass(slots=True)
class FundamentalResearchObservationBuildResult:
    case_id: str
    description: str
    sleeve_id: str
    descriptor_set_id: str
    source_db_path: str
    observation_input: SleeveResearchObservationInput
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _FundamentalCandidateRow:
    security_id: str
    trade_date: str
    list_date: str
    board: str
    entry_open: float
    exit_open: float
    median_turnover_cny: float
    entry_suspended: bool
    exit_suspended: bool
    entry_liquidity_pass: bool
    exit_liquidity_pass: bool
    entry_limit_locked: bool
    exit_limit_locked: bool
    industry: str
    pb: float
    roe: float
    roa: float
    netprofit_margin: float
    debt_to_assets: float
    netprofit_yoy: float
    dt_netprofit_yoy: float


def load_fundamental_research_input_build_case(
    path: Path | str,
) -> LoadedFundamentalResearchInputBuildCase:
    definition = FundamentalResearchInputBuildCaseDefinition.from_toml(_read_toml(path))
    if definition.rebalance_stride <= 0:
        raise ValueError(
            "Fundamental research input build case rebalance_stride must be positive."
        )
    if definition.lookback_days < definition.turnover_window_days:
        raise ValueError(
            "Fundamental research input build case lookback_days must cover turnover_window_days."
        )
    if definition.industry_label_source not in SUPPORTED_INDUSTRY_LABEL_SOURCES:
        raise ValueError(
            "Fundamental research input builder currently supports only "
            "industry_label_source='industry_classification_pit'."
        )
    if not definition.industry_schema.strip():
        raise ValueError(
            "Fundamental research input build case must define industry_schema."
        )
    if definition.limit_lock_mode not in SUPPORTED_LIMIT_LOCK_MODES:
        raise ValueError(
            "Fundamental research input builder currently supports only "
            "limit_lock_mode in {'disabled', 'cn_a_directional_open_lock'}."
        )

    sleeve = load_sleeve(definition.sleeve_path)
    if not sleeve.target_id:
        raise ValueError("Fundamental research input build case sleeve must define a target.")

    descriptor_set = load_descriptor_set(
        CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml"
    )
    target = load_target(CONFIG_ROOT / "targets" / f"{sleeve.target_id}.toml")
    if target.residualization and not definition.residual_component_snapshot_path.strip():
        raise ValueError(
            "Fundamental research input build case requires residual_component_snapshot_path "
            "for residual targets."
        )

    default_cost_model = load_cost_model(
        CONFIG_ROOT / "cost_models" / f"{target.cost_model}.toml"
    )
    holding_count = int(sleeve.construction.get("holding_count", 0))
    min_turnover_cny_mn = max(
        float(sleeve.constraints.get("min_median_daily_turnover_cny_mn", 0.0)),
        default_cost_model.min_median_daily_turnover_cny_mn,
    )

    return LoadedFundamentalResearchInputBuildCase(
        definition=definition,
        sleeve_id=sleeve.id,
        descriptor_set_id=descriptor_set.id,
        target_id=target.id,
        source_db_path=_resolve_project_path(definition.source_db_path),
        output_path=definition.output_path,
        holding_count=holding_count,
        holding_horizon_days=target.horizon_days,
        min_turnover_cny_mn=min_turnover_cny_mn,
        descriptor_weights=_descriptor_weights(descriptor_set),
        residual_components=list(target.residualization),
        residual_component_snapshot_path=(
            _resolve_project_path(definition.residual_component_snapshot_path)
            if definition.residual_component_snapshot_path.strip()
            else None
        ),
    )


def build_fundamental_research_observation_input(
    loaded_case: LoadedFundamentalResearchInputBuildCase,
) -> FundamentalResearchObservationBuildResult:
    calendar = _load_trade_calendar(loaded_case.source_db_path)
    rebalance_dates, lower_bound, upper_bound = _calendar_bounds(
        calendar=calendar,
        start_date=loaded_case.definition.start_date,
        end_date=loaded_case.definition.end_date,
        lookback_days=loaded_case.definition.lookback_days,
        horizon_days=loaded_case.holding_horizon_days,
        rebalance_stride=loaded_case.definition.rebalance_stride,
    )

    candidates = _load_candidate_rows(
        source_db_path=loaded_case.source_db_path,
        calendar=calendar,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        lookback_days=loaded_case.definition.lookback_days,
        turnover_window_days=loaded_case.definition.turnover_window_days,
        horizon_days=loaded_case.holding_horizon_days,
        min_turnover_cny_mn=loaded_case.min_turnover_cny_mn,
        min_listing_days=loaded_case.definition.min_listing_days,
        rebalance_dates=set(rebalance_dates),
        limit_lock_mode=loaded_case.definition.limit_lock_mode,
        industry_schema=loaded_case.definition.industry_schema,
    )

    if not candidates:
        raise ValueError(
            f"Fundamental research input build case {loaded_case.definition.case_id} produced no eligible candidates."
        )

    selected_by_date: list[tuple[str, list[dict[str, object]]]] = []
    for trade_date in rebalance_dates:
        date_candidates = [candidate for candidate in candidates if candidate.trade_date == trade_date]
        if not date_candidates:
            continue
        scored = _score_candidates(
            candidates=date_candidates,
            descriptor_weights=loaded_case.descriptor_weights,
        )
        selected_count = loaded_case.holding_count or len(scored)
        selected = scored[:selected_count]
        if not selected:
            continue
        selected_by_date.append((trade_date, selected))

    if not selected_by_date:
        raise ValueError(
            f"Fundamental research input build case {loaded_case.definition.case_id} produced no eligible steps."
        )

    residual_components_by_observation: dict[tuple[str, str], dict[str, float]] = {}
    if loaded_case.residual_components:
        residual_components_by_observation = _load_residual_component_snapshot(
            path=loaded_case.residual_component_snapshot_path,
            target_id=loaded_case.target_id,
            requested_observations=[
                (trade_date, str(item["candidate"].security_id))
                for trade_date, selected in selected_by_date
                for item in selected
            ],
            required_components=loaded_case.residual_components,
        )

    steps: list[SleeveResearchObservationStep] = []
    for trade_date, selected in selected_by_date:
        target_weight = 1.0 / len(selected)
        records = [
            SleeveResearchObservationRecord(
                asset_id=item["candidate"].security_id,
                rank=rank,
                score=item["score"],
                target_weight=target_weight,
                entry_open=item["candidate"].entry_open,
                exit_open=item["candidate"].exit_open,
                industry=item["candidate"].industry,
                entry_state=TradeLegState(
                    suspended=item["candidate"].entry_suspended,
                    liquidity_pass=item["candidate"].entry_liquidity_pass,
                    limit_locked=item["candidate"].entry_limit_locked,
                ),
                exit_state=TradeLegState(
                    suspended=item["candidate"].exit_suspended,
                    liquidity_pass=item["candidate"].exit_liquidity_pass,
                    limit_locked=item["candidate"].exit_limit_locked,
                ),
                residual_components=residual_components_by_observation.get(
                    (trade_date, item["candidate"].security_id),
                    {},
                ),
            )
            for rank, item in enumerate(selected, start=1)
        ]
        steps.append(SleeveResearchObservationStep(trade_date=trade_date, records=records))

    return FundamentalResearchObservationBuildResult(
        case_id=loaded_case.definition.case_id,
        description=loaded_case.definition.description,
        sleeve_id=loaded_case.sleeve_id,
        descriptor_set_id=loaded_case.descriptor_set_id,
        source_db_path=str(loaded_case.source_db_path),
        observation_input=SleeveResearchObservationInput(steps=steps),
        warnings=_build_warnings(loaded_case.definition.limit_lock_mode),
    )


def write_fundamental_research_observation_input(
    result: FundamentalResearchObservationBuildResult,
    path: Path | str,
) -> Path:
    return write_trend_research_observation_input(result, path)


def _load_candidate_rows(
    *,
    source_db_path: Path,
    calendar: list[str],
    lower_bound: str,
    upper_bound: str,
    lookback_days: int,
    turnover_window_days: int,
    horizon_days: int,
    min_turnover_cny_mn: float,
    min_listing_days: int,
    rebalance_dates: set[str],
    limit_lock_mode: str,
    industry_schema: str,
) -> list[_FundamentalCandidateRow]:
    import duckdb

    calendar_index = {trade_date: index for index, trade_date in enumerate(calendar)}
    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            WITH history AS (
                SELECT
                    d.security_id,
                    d.trade_date,
                    s.list_date,
                    d.is_st,
                    d.board,
                    d.pb,
                    d.turnover_value_cny,
                    quantile_cont(d.turnover_value_cny, 0.5) OVER (
                        PARTITION BY d.security_id
                        ORDER BY d.trade_date
                        ROWS BETWEEN ? PRECEDING AND CURRENT ROW
                    ) AS median_turnover_cny
                FROM daily_bar_pit AS d
                INNER JOIN security_master_ref AS s
                    ON s.security_id = d.security_id
                WHERE d.trade_date BETWEEN ? AND ?
                  AND s.is_a_share
            ),
            featured AS (
                SELECT
                    *,
                    lead(median_turnover_cny, ?) OVER (
                        PARTITION BY security_id
                        ORDER BY trade_date
                    ) AS exit_median_turnover_cny
                FROM history
            ),
            fundamental_ranked AS (
                SELECT
                    featured.security_id,
                    featured.trade_date,
                    featured.list_date,
                    featured.is_st,
                    featured.board,
                    featured.pb,
                    featured.median_turnover_cny,
                    featured.exit_median_turnover_cny,
                    fundamentals.roe,
                    fundamentals.roa,
                    fundamentals.netprofit_margin,
                    fundamentals.debt_to_assets,
                    fundamentals.netprofit_yoy,
                    fundamentals.dt_netprofit_yoy,
                    row_number() OVER (
                        PARTITION BY featured.security_id, featured.trade_date
                        ORDER BY fundamentals.available_date DESC NULLS LAST,
                                 fundamentals.period_end DESC NULLS LAST
                    ) AS fundamental_row_number
                FROM featured
                LEFT JOIN fundamental_snapshot_pit AS fundamentals
                    ON fundamentals.security_id = featured.security_id
                   AND fundamentals.available_date <= featured.trade_date
            )
            SELECT
                ranked.security_id,
                ranked.trade_date,
                ranked.list_date,
                ranked.is_st,
                ranked.board,
                ranked.pb,
                ranked.median_turnover_cny,
                ranked.exit_median_turnover_cny,
                ranked.roe,
                ranked.roa,
                ranked.netprofit_margin,
                ranked.debt_to_assets,
                ranked.netprofit_yoy,
                ranked.dt_netprofit_yoy,
                industry.industry_code
            FROM fundamental_ranked AS ranked
            LEFT JOIN industry_classification_pit AS industry
                ON industry.security_id = ranked.security_id
               AND industry.industry_schema = ?
               AND substr(regexp_replace(COALESCE(industry.effective_at, ''), '[^0-9]', '', 'g'), 1, 8) <= ranked.trade_date
               AND (
                    industry.removed_at IS NULL
                    OR industry.removed_at = ''
                    OR substr(regexp_replace(COALESCE(industry.removed_at, ''), '[^0-9]', '', 'g'), 1, 8) > ranked.trade_date
               )
            WHERE ranked.fundamental_row_number = 1
            ORDER BY ranked.trade_date, ranked.security_id
            """,
            [
                turnover_window_days - 1,
                lower_bound,
                upper_bound,
                horizon_days,
                industry_schema,
            ],
        ).fetchall()
    finally:
        conn.close()

    min_turnover_cny = min_turnover_cny_mn * 1_000_000.0
    candidate_inputs: list[dict[str, object]] = []
    requested_trade_legs: set[tuple[str, str]] = set()
    for row in rows:
        (
            security_id,
            trade_date,
            list_date,
            is_st,
            board,
            pb,
            median_turnover_cny,
            exit_median_turnover_cny,
            roe,
            roa,
            netprofit_margin,
            debt_to_assets,
            netprofit_yoy,
            dt_netprofit_yoy,
            industry,
        ) = row

        trade_date = str(trade_date)
        if trade_date not in rebalance_dates:
            continue
        if bool(is_st):
            continue
        if (
            pb is None
            or float(pb) <= 0.0
            or median_turnover_cny is None
            or float(median_turnover_cny) < min_turnover_cny
            or not str(industry or "").strip()
        ):
            continue
        if _listing_age_days(str(list_date), trade_date) < min_listing_days:
            continue
        if any(
            value is None
            for value in (
                roe,
                roa,
                netprofit_margin,
                debt_to_assets,
                netprofit_yoy,
                dt_netprofit_yoy,
            )
        ):
            continue

        signal_index = calendar_index.get(trade_date)
        if signal_index is None or signal_index + horizon_days >= len(calendar):
            continue
        entry_trade_date = calendar[signal_index + 1]
        exit_trade_date = calendar[signal_index + horizon_days]
        security_id = str(security_id)
        candidate_inputs.append(
            {
                "security_id": security_id,
                "trade_date": trade_date,
                "list_date": str(list_date),
                "board": str(board),
                "industry": str(industry),
                "median_turnover_cny": float(median_turnover_cny),
                "exit_liquidity_pass": bool(
                    exit_median_turnover_cny is not None
                    and float(exit_median_turnover_cny) >= min_turnover_cny
                ),
                "pb": float(pb),
                "roe": float(roe),
                "roa": float(roa),
                "netprofit_margin": float(netprofit_margin),
                "debt_to_assets": float(debt_to_assets),
                "netprofit_yoy": float(netprofit_yoy),
                "dt_netprofit_yoy": float(dt_netprofit_yoy),
                "entry_trade_date": entry_trade_date,
                "exit_trade_date": exit_trade_date,
            }
        )
        requested_trade_legs.add((security_id, entry_trade_date))
        requested_trade_legs.add((security_id, exit_trade_date))

    trade_leg_snapshots = _load_trade_leg_snapshots(
        source_db_path=source_db_path,
        requested_trade_legs=requested_trade_legs,
    )

    candidates: list[_FundamentalCandidateRow] = []
    for candidate_input in candidate_inputs:
        security_id = str(candidate_input["security_id"])
        entry_trade_date = str(candidate_input["entry_trade_date"])
        exit_trade_date = str(candidate_input["exit_trade_date"])
        entry_snapshot = trade_leg_snapshots[(security_id, entry_trade_date)]
        exit_snapshot = trade_leg_snapshots[(security_id, exit_trade_date)]
        entry_effective_open, entry_suspended = _resolve_trade_leg_open(
            open_price=entry_snapshot.open_price,
            fallback_price=_trade_leg_fallback_price(entry_snapshot),
        )
        exit_effective_open, exit_suspended = _resolve_trade_leg_open(
            open_price=exit_snapshot.open_price,
            fallback_price=_trade_leg_fallback_price(exit_snapshot),
        )
        if entry_effective_open is None or exit_effective_open is None:
            continue

        entry_limit_locked = False
        exit_limit_locked = False
        if limit_lock_mode == "cn_a_directional_open_lock":
            if not entry_suspended:
                entry_limit_locked = _is_cn_a_directional_open_lock(
                    board=str(candidate_input["board"]),
                    is_st=bool(entry_snapshot.is_st),
                    pre_close=entry_snapshot.pre_close,
                    open_price=entry_snapshot.open_price,
                    high_price=entry_snapshot.high_price,
                    low_price=entry_snapshot.low_price,
                    direction="entry",
                )
            if not exit_suspended:
                exit_limit_locked = _is_cn_a_directional_open_lock(
                    board=str(candidate_input["board"]),
                    is_st=bool(exit_snapshot.is_st),
                    pre_close=exit_snapshot.pre_close,
                    open_price=exit_snapshot.open_price,
                    high_price=exit_snapshot.high_price,
                    low_price=exit_snapshot.low_price,
                    direction="exit",
                )

        candidates.append(
            _FundamentalCandidateRow(
                security_id=security_id,
                trade_date=str(candidate_input["trade_date"]),
                list_date=str(candidate_input["list_date"]),
                board=str(candidate_input["board"]),
                entry_open=entry_effective_open,
                exit_open=exit_effective_open,
                median_turnover_cny=float(candidate_input["median_turnover_cny"]),
                entry_suspended=entry_suspended,
                exit_suspended=exit_suspended,
                entry_liquidity_pass=True,
                exit_liquidity_pass=bool(candidate_input["exit_liquidity_pass"]),
                entry_limit_locked=entry_limit_locked,
                exit_limit_locked=exit_limit_locked,
                industry=str(candidate_input["industry"]),
                pb=float(candidate_input["pb"]),
                roe=float(candidate_input["roe"]),
                roa=float(candidate_input["roa"]),
                netprofit_margin=float(candidate_input["netprofit_margin"]),
                debt_to_assets=float(candidate_input["debt_to_assets"]),
                netprofit_yoy=float(candidate_input["netprofit_yoy"]),
                dt_netprofit_yoy=float(candidate_input["dt_netprofit_yoy"]),
            )
        )
    return candidates


def _score_candidates(
    *,
    candidates: list[_FundamentalCandidateRow],
    descriptor_weights: dict[str, float],
) -> list[dict[str, object]]:
    raw_metrics = {
        candidate.security_id: {
            "sector_relative_valuation": -math.log(max(candidate.pb, 0.05)),
            "profitability_quality": (
                (0.45 * candidate.roe)
                + (0.35 * candidate.roa)
                + (0.20 * candidate.netprofit_margin)
            ),
            "accrual_quality": -abs(candidate.netprofit_yoy - candidate.dt_netprofit_yoy),
            "leverage_conservatism": -candidate.debt_to_assets,
        }
        for candidate in candidates
    }

    zscores_by_descriptor = {
        descriptor_id: _industry_neutral_zscore_map(
            candidates=candidates,
            values_by_security={
                candidate.security_id: float(raw_metrics[candidate.security_id][descriptor_id])
                for candidate in candidates
            },
        )
        for descriptor_id in descriptor_weights
    }

    scored = []
    for candidate in candidates:
        score = sum(
            descriptor_weights[descriptor_id]
            * zscores_by_descriptor[descriptor_id][candidate.security_id]
            for descriptor_id in descriptor_weights
        )
        scored.append({"candidate": candidate, "score": score})

    return sorted(
        scored,
        key=lambda item: (-float(item["score"]), str(item["candidate"].security_id)),
    )


def _industry_neutral_zscore_map(
    *,
    candidates: list[_FundamentalCandidateRow],
    values_by_security: dict[str, float],
) -> dict[str, float]:
    grouped: dict[str, dict[str, float]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.industry, {})[candidate.security_id] = values_by_security[
            candidate.security_id
        ]

    zscores: dict[str, float] = {}
    for group in grouped.values():
        zscores.update(_zscore_map(group))
    return zscores


def _descriptor_weights(descriptor_set) -> dict[str, float]:
    unsupported = [
        component.descriptor_id
        for component in descriptor_set.components
        if component.descriptor_id not in SUPPORTED_DESCRIPTOR_IDS
    ]
    if unsupported:
        joined = ", ".join(sorted(unsupported))
        raise ValueError(
            "Fundamental research input builder only supports audited descriptor branches. "
            f"Unsupported descriptor ids: {joined}"
        )

    weights = {
        component.descriptor_id: component.weight for component in descriptor_set.components
    }
    if not weights:
        raise ValueError(
            "Fundamental research input builder requires at least one descriptor component."
        )
    return weights


def _load_residual_component_snapshot(
    *,
    path: Path | None,
    target_id: str,
    requested_observations: list[tuple[str, str]],
    required_components: list[str],
) -> dict[tuple[str, str], dict[str, float]]:
    if path is None:
        raise ValueError(
            "Fundamental research input build case requires residual_component_snapshot_path "
            "for residual targets."
        )

    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(
            "Unsupported residual component snapshot schema version: "
            f"{schema_version}"
        )
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "residual_component_snapshot":
        raise ValueError(
            f"Unsupported residual component snapshot type: {artifact_type}"
        )
    snapshot_target_id = str(payload.get("target_id", ""))
    if snapshot_target_id and snapshot_target_id != target_id:
        raise ValueError(
            "Residual component snapshot target_id does not match build case target_id."
        )

    snapshot_by_observation: dict[tuple[str, str], dict[str, float]] = {}
    for step in payload.get("steps", []):
        trade_date = str(step["trade_date"])
        for record in step.get("records", []):
            snapshot_by_observation[(trade_date, str(record["asset_id"]))] = {
                str(key): float(value)
                for key, value in dict(record.get("residual_components", {})).items()
            }

    selected: dict[tuple[str, str], dict[str, float]] = {}
    for trade_date, asset_id in requested_observations:
        observation_key = (trade_date, asset_id)
        components = snapshot_by_observation.get(observation_key)
        if components is None:
            raise ValueError(
                f"Missing residual components for selected observation: {asset_id} on {trade_date}"
            )
        missing_components = [
            component for component in required_components if component not in components
        ]
        if missing_components:
            joined = ", ".join(sorted(missing_components))
            raise ValueError(
                "Missing residual components for selected observation: "
                f"{asset_id} on {trade_date}: {joined}"
            )
        selected[observation_key] = {
            component: float(components[component]) for component in required_components
        }
    return selected


def _build_warnings(limit_lock_mode: str) -> list[str]:
    warnings = ["fundamental_snapshot_pit_amber_anchor_only"]
    if limit_lock_mode == "disabled":
        warnings.append("limit_lock_detection_disabled")
    return warnings


def _zscore_map(values: dict[str, float]) -> dict[str, float]:
    series = list(values.values())
    if len(series) < 2:
        return {key: 0.0 for key in values}

    mean = sum(series) / len(series)
    variance = sum((value - mean) ** 2 for value in series) / (len(series) - 1)
    if variance <= 0.0:
        return {key: 0.0 for key in values}
    stdev = math.sqrt(variance)
    return {key: (value - mean) / stdev for key, value in values.items()}


def _listing_age_days(list_date: str, trade_date: str) -> int:
    listed = int(list_date[:4]), int(list_date[4:6]), int(list_date[6:8])
    traded = int(trade_date[:4]), int(trade_date[4:6]), int(trade_date[6:8])
    from datetime import date

    return (date(*traded) - date(*listed)).days


def _read_json(path: Path | str) -> dict[str, object]:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)
