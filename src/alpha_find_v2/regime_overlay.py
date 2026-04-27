from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path

from .config_loader import PROJECT_ROOT
from .models import RegimeOverlay


ALLOWED_GREEN_INPUTS = {
    "benchmark_trend",
    "market_breadth",
    "dispersion",
    "realized_volatility",
    "price_limit_stress",
}
ALLOWED_INPUT_STATES = {
    "supportive",
    "neutral",
    "risk_off",
    "missing",
    "invalid",
}
ALLOWED_OVERLAY_STATES = {"normal", "de_risk", "cash_heavier"}


@dataclass(slots=True)
class RegimeOverlayObservationStep:
    trade_date: str
    input_states: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class RegimeOverlayObservationArtifact:
    overlay_id: str
    steps: list[RegimeOverlayObservationStep] = field(default_factory=list)


@dataclass(slots=True)
class RegimeOverlayDecision:
    trade_date: str
    status: str
    state: str
    risk_off_inputs: list[str] = field(default_factory=list)
    missing_inputs: list[str] = field(default_factory=list)
    invalid_inputs: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RegimeOverlaySummary:
    overlay_id: str
    period_count: int = 0
    active_periods: int = 0
    downgraded_periods: int = 0
    blocked_periods: int = 0
    normal_periods: int = 0
    de_risk_periods: int = 0
    cash_heavier_periods: int = 0
    missing_inputs: list[str] = field(default_factory=list)
    invalid_inputs: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RegimeOverlayEvidence:
    decisions: list[RegimeOverlayDecision] = field(default_factory=list)
    summary: RegimeOverlaySummary = field(
        default_factory=lambda: RegimeOverlaySummary(overlay_id="")
    )


def load_regime_overlay_observation_artifact(
    path: Path | str,
) -> RegimeOverlayObservationArtifact:
    payload = _read_json(path)
    schema_version = int(payload.get("schema_version", 0))
    if schema_version != 1:
        raise ValueError(
            f"Unsupported regime overlay observation schema version: {schema_version}"
        )
    artifact_type = str(payload.get("artifact_type", ""))
    if artifact_type != "regime_overlay_observation_history":
        raise ValueError(f"Unsupported regime overlay observation type: {artifact_type}")

    steps = [
        RegimeOverlayObservationStep(
            trade_date=str(item["trade_date"]),
            input_states={
                str(key): str(value)
                for key, value in dict(item.get("input_states", {})).items()
            },
            metrics={
                str(key): float(value)
                for key, value in dict(item.get("metrics", {})).items()
            },
        )
        for item in payload.get("steps", [])
    ]
    _validate_observation_steps(steps)
    return RegimeOverlayObservationArtifact(
        overlay_id=str(payload["overlay_id"]),
        steps=steps,
    )


class RegimeOverlayEvaluator:
    def __init__(self, overlay: RegimeOverlay) -> None:
        self.overlay = overlay
        self._validate_overlay()

    def evaluate_history(
        self,
        *,
        trade_dates: list[str],
        observations: list[RegimeOverlayObservationStep],
    ) -> RegimeOverlayEvidence:
        observations_by_date = {step.trade_date: step for step in observations}
        decisions = [
            self.evaluate_step(
                trade_date=trade_date,
                observation=observations_by_date.get(trade_date),
            )
            for trade_date in trade_dates
        ]
        return RegimeOverlayEvidence(
            decisions=decisions,
            summary=self._summarize(decisions),
        )

    def evaluate_step(
        self,
        *,
        trade_date: str,
        observation: RegimeOverlayObservationStep | None,
    ) -> RegimeOverlayDecision:
        input_states = dict(observation.input_states) if observation is not None else {}
        unexpected_inputs = sorted(set(input_states) - set(self.overlay.required_inputs))
        if unexpected_inputs:
            raise ValueError(
                "Regime overlay observations contain unsupported inputs: "
                + ", ".join(unexpected_inputs)
            )

        risk_off_inputs: list[str] = []
        missing_inputs: list[str] = []
        invalid_inputs: list[str] = []
        for input_name in self.overlay.required_inputs:
            raw_state = input_states.get(input_name, "missing")
            state = str(raw_state)
            if state not in ALLOWED_INPUT_STATES:
                invalid_inputs.append(input_name)
                continue
            if state == "risk_off":
                risk_off_inputs.append(input_name)
            elif state == "missing":
                missing_inputs.append(input_name)
            elif state == "invalid":
                invalid_inputs.append(input_name)

        stop_inputs = set(self.overlay.stop_inputs)
        if any(input_name in stop_inputs for input_name in [*missing_inputs, *invalid_inputs]):
            return RegimeOverlayDecision(
                trade_date=trade_date,
                status="blocked",
                state="cash_heavier",
                risk_off_inputs=risk_off_inputs,
                missing_inputs=sorted(missing_inputs),
                invalid_inputs=sorted(invalid_inputs),
            )

        if missing_inputs or invalid_inputs:
            return RegimeOverlayDecision(
                trade_date=trade_date,
                status="downgraded",
                state="de_risk",
                risk_off_inputs=risk_off_inputs,
                missing_inputs=sorted(missing_inputs),
                invalid_inputs=sorted(invalid_inputs),
            )

        state = "normal"
        if len(risk_off_inputs) >= self.overlay.cash_heavier_min_risk_off:
            state = "cash_heavier"
        elif len(risk_off_inputs) >= self.overlay.de_risk_min_risk_off:
            state = "de_risk"

        return RegimeOverlayDecision(
            trade_date=trade_date,
            status="active",
            state=state,
            risk_off_inputs=risk_off_inputs,
        )

    def _summarize(self, decisions: list[RegimeOverlayDecision]) -> RegimeOverlaySummary:
        summary = RegimeOverlaySummary(
            overlay_id=self.overlay.id,
            period_count=len(decisions),
        )
        missing_inputs = {
            input_name
            for decision in decisions
            for input_name in decision.missing_inputs
        }
        invalid_inputs = {
            input_name
            for decision in decisions
            for input_name in decision.invalid_inputs
        }
        summary.missing_inputs = sorted(missing_inputs)
        summary.invalid_inputs = sorted(invalid_inputs)

        for decision in decisions:
            if decision.status == "active":
                summary.active_periods += 1
            elif decision.status == "downgraded":
                summary.downgraded_periods += 1
            elif decision.status == "blocked":
                summary.blocked_periods += 1

            if decision.state == "normal":
                summary.normal_periods += 1
            elif decision.state == "de_risk":
                summary.de_risk_periods += 1
            elif decision.state == "cash_heavier":
                summary.cash_heavier_periods += 1

        return summary

    def _validate_overlay(self) -> None:
        unexpected_inputs = sorted(set(self.overlay.required_inputs) - ALLOWED_GREEN_INPUTS)
        if unexpected_inputs:
            raise ValueError(
                "Regime overlay required_inputs must stay inside the green input family: "
                + ", ".join(unexpected_inputs)
            )
        unknown_stop_inputs = sorted(set(self.overlay.stop_inputs) - set(self.overlay.required_inputs))
        if unknown_stop_inputs:
            raise ValueError(
                "Regime overlay stop_inputs must be a subset of required_inputs: "
                + ", ".join(unknown_stop_inputs)
            )
        allowed_states = set(self.overlay.allowed_states)
        if allowed_states != ALLOWED_OVERLAY_STATES:
            raise ValueError(
                "Regime overlay allowed_states must be exactly: normal, de_risk, cash_heavier"
            )
        if self.overlay.de_risk_min_risk_off < 1:
            raise ValueError("Regime overlay de_risk_min_risk_off must be positive.")
        if self.overlay.cash_heavier_min_risk_off < self.overlay.de_risk_min_risk_off:
            raise ValueError(
                "Regime overlay cash_heavier_min_risk_off cannot be below de_risk_min_risk_off."
            )
        if self.overlay.normal_gross_exposure <= 0.0 or self.overlay.normal_gross_exposure > 1.0:
            raise ValueError("Regime overlay normal_gross_exposure must be inside (0, 1].")
        if self.overlay.de_risk_gross_exposure <= 0.0:
            raise ValueError("Regime overlay de_risk_gross_exposure must be positive.")
        if self.overlay.cash_heavier_gross_exposure <= 0.0:
            raise ValueError("Regime overlay cash_heavier_gross_exposure must be positive.")
        if self.overlay.de_risk_gross_exposure > self.overlay.normal_gross_exposure:
            raise ValueError(
                "Regime overlay de_risk_gross_exposure cannot exceed normal_gross_exposure."
            )
        if self.overlay.cash_heavier_gross_exposure > self.overlay.de_risk_gross_exposure:
            raise ValueError(
                "Regime overlay cash_heavier_gross_exposure cannot exceed de_risk_gross_exposure."
            )


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if target.is_absolute():
        return target
    return PROJECT_ROOT / target


def _read_json(path: Path | str) -> dict[str, object]:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _validate_observation_steps(steps: list[RegimeOverlayObservationStep]) -> None:
    seen_dates: set[str] = set()
    for step in steps:
        if step.trade_date in seen_dates:
            raise ValueError(
                f"Duplicate regime overlay observation trade date: {step.trade_date}"
            )
        seen_dates.add(step.trade_date)
