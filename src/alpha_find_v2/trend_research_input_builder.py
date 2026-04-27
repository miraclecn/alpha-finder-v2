from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
import json
import math
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
import tomllib

from .config_loader import CONFIG_ROOT, PROJECT_ROOT, load_cost_model, load_descriptor_set, load_sleeve, load_target
from .research_artifact_loader import (
    SleeveResearchObservationInput,
    SleeveResearchObservationRecord,
    SleeveResearchObservationStep,
)
from .target_builder import TradeLegState


SUPPORTED_DESCRIPTOR_IDS = {
    "medium_term_relative_strength",
    "trend_stability",
    "turnover_confirmation",
}
SUPPORTED_INDUSTRY_LABEL_SOURCES = {
    "omit",
    "industry_classification_pit",
}
SUPPORTED_LIMIT_LOCK_MODES = {
    "disabled",
    "cn_a_directional_open_lock",
}
SUPPORTED_RESIDUALIZATION_MODES = {
    "non_residual_target",
    "audited_residual_components",
}


@dataclass(slots=True)
class TrendResearchInputBuildCaseDefinition:
    case_id: str
    description: str
    sleeve_path: str
    source_db_path: str
    output_path: str
    residual_component_snapshot_path: str = ""
    start_date: str = ""
    end_date: str = ""
    min_listing_days: int = 120
    lookback_days: int = 60
    short_window_days: int = 20
    turnover_window_days: int = 20
    rebalance_stride: int = 5
    industry_label_source: str = "omit"
    industry_schema: str = ""
    limit_lock_mode: str = "disabled"
    residualization_mode: str = "non_residual_target"
    exclude_boards: list[str] = field(default_factory=list)

    @classmethod
    def from_toml(cls, data: dict[str, object]) -> "TrendResearchInputBuildCaseDefinition":
        schema_version = int(data.get("schema_version", 0))
        if schema_version != 1:
            raise ValueError(
                f"Unsupported trend research input build case schema version: {schema_version}"
            )
        artifact_type = str(data.get("artifact_type", ""))
        if artifact_type != "trend_research_input_build_case":
            raise ValueError(
                f"Unsupported trend research input build case type: {artifact_type}"
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
            lookback_days=int(data.get("lookback_days", 60)),
            short_window_days=int(data.get("short_window_days", 20)),
            turnover_window_days=int(data.get("turnover_window_days", 20)),
            rebalance_stride=int(data.get("rebalance_stride", 5)),
            industry_label_source=str(data.get("industry_label_source", "omit")),
            industry_schema=str(data.get("industry_schema", "")),
            limit_lock_mode=str(data.get("limit_lock_mode", "disabled")),
            residualization_mode=str(data.get("residualization_mode", "non_residual_target")),
            exclude_boards=[
                str(board).strip()
                for board in data.get("exclude_boards", [])
                if str(board).strip()
            ],
        )


@dataclass(slots=True)
class LoadedTrendResearchInputBuildCase:
    definition: TrendResearchInputBuildCaseDefinition
    sleeve_id: str
    descriptor_set_id: str
    target_id: str
    risk_model_id: str
    source_db_path: Path
    output_path: str
    holding_count: int
    holding_horizon_days: int
    min_turnover_cny_mn: float
    descriptor_weights: dict[str, float]
    residual_components: list[str] = field(default_factory=list)
    residual_component_snapshot_path: Path | None = None


@dataclass(slots=True)
class TrendResearchObservationBuildResult:
    case_id: str
    description: str
    sleeve_id: str
    descriptor_set_id: str
    source_db_path: str
    observation_input: SleeveResearchObservationInput
    warnings: list[str] = field(default_factory=list)


@dataclass(slots=True)
class _CandidateRow:
    security_id: str
    trade_date: str
    list_date: str
    entry_open: float
    exit_open: float
    median_turnover_cny: float
    entry_suspended: bool
    exit_suspended: bool
    entry_liquidity_pass: bool
    exit_liquidity_pass: bool
    entry_limit_locked: bool
    exit_limit_locked: bool
    ret_short: float
    ret_long: float
    short_return_vol: float | None


@dataclass(slots=True)
class _TradeLegSnapshot:
    open_price: float | None
    high_price: float | None
    low_price: float | None
    pre_close: float | None
    previous_close_adj: float | None
    is_st: bool | None


def load_trend_research_input_build_case(
    path: Path | str,
) -> LoadedTrendResearchInputBuildCase:
    definition = TrendResearchInputBuildCaseDefinition.from_toml(_read_toml(path))
    if definition.rebalance_stride <= 0:
        raise ValueError("Trend research input build case rebalance_stride must be positive.")
    if definition.lookback_days < definition.short_window_days:
        raise ValueError("Trend research input build case lookback_days must cover short_window_days.")
    if definition.industry_label_source not in SUPPORTED_INDUSTRY_LABEL_SOURCES:
        raise ValueError(
            "Trend research input builder currently supports only "
            "industry_label_source in {'omit', 'industry_classification_pit'}."
        )
    if (
        definition.industry_label_source == "industry_classification_pit"
        and not definition.industry_schema.strip()
    ):
        raise ValueError(
            "Trend research input build case must define industry_schema when "
            "industry_label_source='industry_classification_pit'."
        )
    if definition.limit_lock_mode not in SUPPORTED_LIMIT_LOCK_MODES:
        raise ValueError(
            "Trend research input builder currently supports only "
            "limit_lock_mode in {'disabled', 'cn_a_directional_open_lock'}."
        )
    if definition.residualization_mode not in SUPPORTED_RESIDUALIZATION_MODES:
        supported_modes = "', '".join(sorted(SUPPORTED_RESIDUALIZATION_MODES))
        raise ValueError(
            "Trend research input builder currently supports only "
            f"residualization_mode in {{'{supported_modes}'}}."
        )

    sleeve = load_sleeve(definition.sleeve_path)
    if not sleeve.target_id:
        raise ValueError("Trend research input build case sleeve must define a target.")
    descriptor_set = load_descriptor_set(
        CONFIG_ROOT / "descriptor_sets" / f"{sleeve.descriptor_set_id}.toml"
    )
    target = load_target(CONFIG_ROOT / "targets" / f"{sleeve.target_id}.toml")
    if definition.residualization_mode == "non_residual_target" and (
        target.label_kind != "net_return" or target.residualization or target.risk_model_id
    ):
        raise ValueError(
            "Trend research input builder requires an explicit non-residual target; wire audited residualization before binding a residual target."
        )
    if definition.residualization_mode == "audited_residual_components":
        if not target.residualization:
            raise ValueError(
                "Trend research input build case residualization_mode='audited_residual_components' requires a residual target."
            )
        if not definition.residual_component_snapshot_path.strip():
            raise ValueError(
                "Trend research input build case requires residual_component_snapshot_path "
                "for residual targets."
            )
    default_cost_model = load_cost_model(CONFIG_ROOT / "cost_models" / f"{target.cost_model}.toml")

    holding_count = int(sleeve.construction.get("holding_count", 0))
    min_turnover_cny_mn = max(
        float(sleeve.constraints.get("min_median_daily_turnover_cny_mn", 0.0)),
        default_cost_model.min_median_daily_turnover_cny_mn,
    )
    descriptor_weights = _descriptor_weights(descriptor_set)

    return LoadedTrendResearchInputBuildCase(
        definition=definition,
        sleeve_id=sleeve.id,
        descriptor_set_id=descriptor_set.id,
        target_id=target.id,
        risk_model_id=target.risk_model_id,
        source_db_path=_resolve_project_path(definition.source_db_path),
        output_path=definition.output_path,
        holding_count=holding_count,
        holding_horizon_days=target.horizon_days,
        min_turnover_cny_mn=min_turnover_cny_mn,
        descriptor_weights=descriptor_weights,
        residual_components=list(target.residualization),
        residual_component_snapshot_path=(
            _resolve_project_path(definition.residual_component_snapshot_path)
            if definition.residual_component_snapshot_path.strip()
            else None
        ),
    )


def build_trend_research_observation_input(
    loaded_case: LoadedTrendResearchInputBuildCase,
) -> TrendResearchObservationBuildResult:
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
        short_window_days=loaded_case.definition.short_window_days,
        lookback_days=loaded_case.definition.lookback_days,
        turnover_window_days=loaded_case.definition.turnover_window_days,
        horizon_days=loaded_case.holding_horizon_days,
        min_turnover_cny_mn=loaded_case.min_turnover_cny_mn,
        min_listing_days=loaded_case.definition.min_listing_days,
        rebalance_dates=set(rebalance_dates),
        limit_lock_mode=loaded_case.definition.limit_lock_mode,
        exclude_boards=set(loaded_case.definition.exclude_boards),
    )

    steps: list[SleeveResearchObservationStep] = []
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
            f"Trend research input build case {loaded_case.definition.case_id} produced no eligible steps."
        )

    residual_components_by_observation: dict[tuple[str, str], dict[str, float]] = {}
    if loaded_case.residual_components:
        # Reuse the same audited residual snapshot contract as the slower fundamental lane.
        from .fundamental_research_input_builder import _load_residual_component_snapshot

        residual_components_by_observation = _load_residual_component_snapshot(
            path=loaded_case.residual_component_snapshot_path,
            target_id=loaded_case.target_id,
            risk_model_id=loaded_case.risk_model_id,
            requested_observations=[
                (trade_date, str(item["candidate"].security_id))
                for trade_date, selected in selected_by_date
                for item in selected
            ],
            required_components=loaded_case.residual_components,
        )

    industry_by_observation = _load_industry_labels(
        source_db_path=loaded_case.source_db_path,
        industry_label_source=loaded_case.definition.industry_label_source,
        industry_schema=loaded_case.definition.industry_schema,
        requested_observations=[
            (trade_date, str(item["candidate"].security_id))
            for trade_date, selected in selected_by_date
            for item in selected
        ],
    )

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
                industry=industry_by_observation.get(
                    (trade_date, item["candidate"].security_id),
                    "",
                ),
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

    return TrendResearchObservationBuildResult(
        case_id=loaded_case.definition.case_id,
        description=loaded_case.definition.description,
        sleeve_id=loaded_case.sleeve_id,
        descriptor_set_id=loaded_case.descriptor_set_id,
        source_db_path=str(loaded_case.source_db_path),
        observation_input=SleeveResearchObservationInput(steps=steps),
        warnings=_build_warnings(
            loaded_case.definition.industry_label_source,
            loaded_case.definition.limit_lock_mode,
        ),
    )


def write_trend_research_observation_input(
    result: TrendResearchObservationBuildResult,
    path: Path | str,
) -> Path:
    target = _resolve_project_path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = _json_ready(
        {
            "schema_version": 1,
            "artifact_type": "sleeve_research_observation_input",
            "case_id": result.case_id,
            "sleeve_id": result.sleeve_id,
            "descriptor_set_id": result.descriptor_set_id,
            "source_db_path": result.source_db_path,
            "warnings": list(result.warnings),
            "steps": [asdict(step) for step in result.observation_input.steps],
        }
    )
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return target


def _load_trade_calendar(source_db_path: Path) -> list[str]:
    import duckdb

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            "SELECT trade_date FROM market_trade_calendar ORDER BY trade_date"
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]) for row in rows]


def _load_industry_labels(
    *,
    source_db_path: Path,
    industry_label_source: str,
    industry_schema: str,
    requested_observations: list[tuple[str, str]],
) -> dict[tuple[str, str], str]:
    if industry_label_source == "omit" or not requested_observations:
        return {}
    if industry_label_source != "industry_classification_pit":
        raise ValueError(
            f"Unsupported trend research input industry label source: {industry_label_source}"
        )

    columns = _load_table_columns(source_db_path, "industry_classification_pit")
    required_columns = {
        "security_id",
        "industry_schema",
        "industry_code",
        "effective_at",
        "removed_at",
    }
    missing_columns = sorted(required_columns - columns)
    if missing_columns:
        raise ValueError(
            "Trend research input builder requires industry_classification_pit columns: "
            f"{', '.join(missing_columns)}"
        )

    unique_observations = sorted(set(requested_observations))
    placeholders = ", ".join("(?, ?)" for _ in unique_observations)
    parameters = [value for observation in unique_observations for value in observation]
    parameters.append(industry_schema)

    import duckdb

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            WITH requested(trade_date, security_id) AS (
                VALUES {placeholders}
            ),
            matched AS (
                SELECT
                    requested.trade_date,
                    requested.security_id,
                    pit.industry_code,
                    row_number() OVER (
                        PARTITION BY requested.trade_date, requested.security_id
                        ORDER BY {_timestamp_sql('pit.effective_at')} DESC NULLS LAST
                    ) AS row_number
                FROM requested
                LEFT JOIN industry_classification_pit AS pit
                    ON pit.security_id = requested.security_id
                   AND pit.industry_schema = ?
                   AND {_timestamp_sql('pit.effective_at')} <= strptime(requested.trade_date, '%Y%m%d')
                   AND (
                       {_timestamp_sql('pit.removed_at')} IS NULL
                       OR {_timestamp_sql('pit.removed_at')} > strptime(requested.trade_date, '%Y%m%d')
                   )
            )
            SELECT trade_date, security_id, industry_code
            FROM matched
            WHERE row_number = 1
            """,
            parameters,
        ).fetchall()
    finally:
        conn.close()

    industry_by_observation = {
        (str(trade_date), str(security_id)): (
            "" if industry_code is None else str(industry_code)
        )
        for trade_date, security_id, industry_code in rows
    }

    missing = [
        (trade_date, security_id)
        for trade_date, security_id in unique_observations
        if not industry_by_observation.get((trade_date, security_id), "").strip()
    ]
    if missing:
        missing_date, missing_security = missing[0]
        raise ValueError(
            "Missing PIT industry label for trend research observation: "
            f"{missing_security} on {missing_date}"
        )
    return industry_by_observation


def _load_table_columns(source_db_path: Path, table_name: str) -> set[str]:
    import duckdb

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        try:
            rows = conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        except duckdb.Error as exc:
            raise ValueError(
                f"Trend research input builder requires source table {table_name}."
            ) from exc
    finally:
        conn.close()
    return {str(row[1]) for row in rows}


def _timestamp_sql(expression: str) -> str:
    return f"""
        CASE
            WHEN {expression} IS NULL OR trim(CAST({expression} AS VARCHAR)) = '' THEN NULL
            WHEN length(trim(CAST({expression} AS VARCHAR))) = 8
                THEN strptime(trim(CAST({expression} AS VARCHAR)), '%Y%m%d')
            ELSE CAST({expression} AS TIMESTAMP)
        END
    """


def _build_warnings(industry_label_source: str, limit_lock_mode: str) -> list[str]:
    warnings = ["industry_relative_branch_blocked"]
    if industry_label_source == "omit":
        warnings.append("industry_labels_omitted")
    if limit_lock_mode == "disabled":
        warnings.append("limit_lock_detection_disabled")
    return warnings


def _calendar_bounds(
    *,
    calendar: list[str],
    start_date: str,
    end_date: str,
    lookback_days: int,
    horizon_days: int,
    rebalance_stride: int,
) -> tuple[list[str], str, str]:
    if not calendar:
        raise ValueError("Trend research input builder requires a non-empty market_trade_calendar.")

    start = start_date or calendar[0]
    end = end_date or calendar[-1]
    try:
        start_index = calendar.index(start)
    except ValueError as exc:
        raise ValueError(f"Trend research input start_date not found in trade calendar: {start}") from exc
    try:
        end_index = calendar.index(end)
    except ValueError as exc:
        raise ValueError(f"Trend research input end_date not found in trade calendar: {end}") from exc
    if start_index > end_index:
        raise ValueError("Trend research input build case start_date must be <= end_date.")

    rebalance_dates = calendar[start_index : end_index + 1 : rebalance_stride]
    if not rebalance_dates:
        raise ValueError("Trend research input builder found no rebalance dates in the requested range.")

    lower_index = max(0, start_index - lookback_days)
    upper_index = min(len(calendar) - 1, end_index + horizon_days)
    return rebalance_dates, calendar[lower_index], calendar[upper_index]


def _load_candidate_rows(
    *,
    source_db_path: Path,
    calendar: list[str],
    lower_bound: str,
    upper_bound: str,
    short_window_days: int,
    lookback_days: int,
    turnover_window_days: int,
    horizon_days: int,
    min_turnover_cny_mn: float,
    min_listing_days: int,
    rebalance_dates: set[str],
    limit_lock_mode: str,
    exclude_boards: set[str],
) -> list[_CandidateRow]:
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
                    d.close_adj,
                    d.turnover_value_cny,
                    lag(d.close_adj, 1) OVER w AS prev_close_adj,
                    lag(d.close_adj, ?) OVER w AS short_close_lag,
                    lag(d.close_adj, ?) OVER w AS long_close_lag,
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
                WINDOW w AS (PARTITION BY d.security_id ORDER BY d.trade_date)
            ),
            with_returns AS (
                SELECT
                    *,
                    CASE
                        WHEN prev_close_adj > 0 THEN (close_adj / prev_close_adj) - 1.0
                        ELSE NULL
                    END AS daily_return
                FROM history
            ),
            featured AS (
                SELECT
                    *,
                    CASE
                        WHEN short_close_lag > 0 THEN (close_adj / short_close_lag) - 1.0
                        ELSE NULL
                    END AS ret_short,
                    CASE
                        WHEN long_close_lag > 0 THEN (close_adj / long_close_lag) - 1.0
                        ELSE NULL
                    END AS ret_long,
                    stddev_samp(daily_return) OVER (
                        PARTITION BY security_id
                        ORDER BY trade_date
                        ROWS BETWEEN ? PRECEDING AND CURRENT ROW
                    ) AS short_return_vol,
                    lead(median_turnover_cny, ?) OVER (
                        PARTITION BY security_id
                        ORDER BY trade_date
                    ) AS exit_median_turnover_cny
                FROM with_returns
            )
            SELECT
                security_id,
                trade_date,
                list_date,
                is_st,
                board,
                median_turnover_cny,
                exit_median_turnover_cny,
                ret_short,
                ret_long,
                short_return_vol
            FROM featured
            ORDER BY trade_date, security_id
            """,
            [
                short_window_days,
                lookback_days,
                turnover_window_days - 1,
                lower_bound,
                upper_bound,
                short_window_days - 1,
                horizon_days,
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
            median_turnover_cny,
            exit_median_turnover_cny,
            ret_short,
            ret_long,
            short_return_vol,
        ) = row
        trade_date = str(trade_date)
        if trade_date not in rebalance_dates:
            continue
        if str(board) in exclude_boards:
            continue
        if bool(is_st):
            continue
        if ret_short is None or ret_long is None:
            continue
        if median_turnover_cny is None or float(median_turnover_cny) < min_turnover_cny:
            continue
        if _listing_age_days(str(list_date), trade_date) < min_listing_days:
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
                "median_turnover_cny": float(median_turnover_cny),
                "exit_liquidity_pass": bool(
                    exit_median_turnover_cny is not None
                    and float(exit_median_turnover_cny) >= min_turnover_cny
                ),
                "ret_short": float(ret_short),
                "ret_long": float(ret_long),
                "short_return_vol": (
                    None if short_return_vol is None else float(short_return_vol)
                ),
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

    candidates: list[_CandidateRow] = []
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
            _CandidateRow(
                security_id=security_id,
                trade_date=str(candidate_input["trade_date"]),
                list_date=str(candidate_input["list_date"]),
                entry_open=entry_effective_open,
                exit_open=exit_effective_open,
                median_turnover_cny=float(candidate_input["median_turnover_cny"]),
                entry_suspended=entry_suspended,
                exit_suspended=exit_suspended,
                entry_liquidity_pass=True,
                exit_liquidity_pass=bool(candidate_input["exit_liquidity_pass"]),
                entry_limit_locked=entry_limit_locked,
                exit_limit_locked=exit_limit_locked,
                ret_short=float(candidate_input["ret_short"]),
                ret_long=float(candidate_input["ret_long"]),
                short_return_vol=(
                    None
                    if candidate_input["short_return_vol"] is None
                    else float(candidate_input["short_return_vol"])
                ),
            )
        )
    return candidates


def _load_trade_leg_snapshots(
    *,
    source_db_path: Path,
    requested_trade_legs: set[tuple[str, str]],
) -> dict[tuple[str, str], _TradeLegSnapshot]:
    if not requested_trade_legs:
        return {}

    import duckdb

    ordered_requests = sorted(requested_trade_legs)
    placeholders = ", ".join("(?, ?)" for _ in ordered_requests)
    parameters = [value for request in ordered_requests for value in request]

    conn = duckdb.connect(str(source_db_path), read_only=True)
    try:
        rows = conn.execute(
            f"""
            WITH requested(security_id, trade_date) AS (
                VALUES {placeholders}
            )
            SELECT
                requested.security_id,
                requested.trade_date,
                d.open,
                d.high,
                d.low,
                d.pre_close,
                (
                    SELECT prev.close_adj
                    FROM daily_bar_pit AS prev
                    WHERE prev.security_id = requested.security_id
                      AND prev.trade_date < requested.trade_date
                      AND prev.close_adj IS NOT NULL
                      AND prev.close_adj > 0.0
                    ORDER BY prev.trade_date DESC
                    LIMIT 1
                ) AS previous_close_adj,
                d.is_st
            FROM requested
            LEFT JOIN daily_bar_pit AS d
                ON d.security_id = requested.security_id
               AND d.trade_date = requested.trade_date
            """,
            parameters,
        ).fetchall()
    finally:
        conn.close()

    return {
        (str(security_id), str(trade_date)): _TradeLegSnapshot(
            open_price=None if open_price is None else float(open_price),
            high_price=None if high_price is None else float(high_price),
            low_price=None if low_price is None else float(low_price),
            pre_close=None if pre_close is None else float(pre_close),
            previous_close_adj=(
                None if previous_close_adj is None else float(previous_close_adj)
            ),
            is_st=None if is_st is None else bool(is_st),
        )
        for (
            security_id,
            trade_date,
            open_price,
            high_price,
            low_price,
            pre_close,
            previous_close_adj,
            is_st,
        ) in rows
    }


def _trade_leg_fallback_price(snapshot: _TradeLegSnapshot) -> float | None:
    if snapshot.pre_close is not None and snapshot.pre_close > 0.0:
        return snapshot.pre_close
    if snapshot.previous_close_adj is not None and snapshot.previous_close_adj > 0.0:
        return snapshot.previous_close_adj
    return None


def _resolve_trade_leg_open(
    *,
    open_price: float | None,
    fallback_price: float | None,
) -> tuple[float | None, bool]:
    suspended = open_price is None or float(open_price) <= 0.0
    if not suspended:
        return float(open_price), False
    if fallback_price is None or float(fallback_price) <= 0.0:
        return None, True
    return float(fallback_price), True


def _is_cn_a_directional_open_lock(
    *,
    board: str,
    is_st: bool,
    pre_close: float | None,
    open_price: float | None,
    high_price: float | None,
    low_price: float | None,
    direction: str,
) -> bool:
    if (
        pre_close is None
        or open_price is None
        or high_price is None
        or low_price is None
        or pre_close <= 0.0
    ):
        return False
    if direction not in {"entry", "exit"}:
        raise ValueError(f"Unsupported directional lock check: {direction}")

    limit_ratio = _cn_a_limit_ratio(board=board, is_st=is_st)
    if direction == "entry":
        upper_limit = _round_cn_price(float(pre_close) * (1.0 + limit_ratio))
        return open_price >= upper_limit - 1e-6 and low_price >= upper_limit - 1e-6

    lower_limit = _round_cn_price(float(pre_close) * (1.0 - limit_ratio))
    return open_price <= lower_limit + 1e-6 and high_price <= lower_limit + 1e-6


def _cn_a_limit_ratio(*, board: str, is_st: bool) -> float:
    if is_st:
        return 0.05
    if board == "beijing":
        return 0.30
    if board in {"chinext", "star"}:
        return 0.20
    return 0.10


def _round_cn_price(value: float) -> float:
    return float(
        Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    )


def _score_candidates(
    *,
    candidates: list[_CandidateRow],
    descriptor_weights: dict[str, float],
) -> list[dict[str, object]]:
    raw_metrics = {
        candidate.security_id: {
            "medium_term_relative_strength": 0.5 * candidate.ret_short + 0.5 * candidate.ret_long,
            "trend_stability": (
                candidate.ret_long / candidate.short_return_vol
                if candidate.short_return_vol and candidate.short_return_vol > 0.0
                else 0.0
            ),
            "turnover_confirmation": math.log(
                max(candidate.median_turnover_cny / 1_000_000.0, 1.0)
            ),
        }
        for candidate in candidates
    }

    zscores_by_descriptor = {
        descriptor_id: _zscore_map(
            {
                candidate.security_id: float(raw_metrics[candidate.security_id][descriptor_id])
                for candidate in candidates
            }
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


def _descriptor_weights(descriptor_set) -> dict[str, float]:
    unsupported = [
        component.descriptor_id
        for component in descriptor_set.components
        if component.descriptor_id not in SUPPORTED_DESCRIPTOR_IDS
    ]
    if unsupported:
        joined = ", ".join(sorted(unsupported))
        raise ValueError(
            "Trend research input builder only supports audited descriptor branches. "
            f"Unsupported descriptor ids: {joined}"
        )

    weights = {
        component.descriptor_id: component.weight for component in descriptor_set.components
    }
    if not weights:
        raise ValueError("Trend research input builder requires at least one descriptor component.")
    return weights


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
    listed = datetime.strptime(list_date, "%Y%m%d").date()
    traded = datetime.strptime(trade_date, "%Y%m%d").date()
    return (traded - listed).days


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_toml(path: Path | str) -> dict[str, object]:
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
