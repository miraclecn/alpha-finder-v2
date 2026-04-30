from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Any, Callable, TypeVar
import tomllib

from .config_loader import (
    PROJECT_ROOT,
    load_cost_model,
    load_descriptor_set,
    load_mandate,
    load_portfolio,
    load_sleeve,
    load_target,
    load_thesis,
)


JsonMap = dict[str, Any]
T = TypeVar("T")

REJECTED_OBJECTIVES = {
    "gross_return_only",
    "ignore_costs",
    "ignore_tradeability",
}


@dataclass(slots=True)
class GeneratedStrategyManifest:
    schema_version: int
    artifact_type: str
    strategy_id: str
    objectives: list[str] = field(default_factory=list)
    promotion_review_requested: bool = False
    mandate_path: str = ""
    thesis_path: str = ""
    descriptor_set_path: str = ""
    sleeve_path: str = ""
    target_path: str = ""
    portfolio_path: str = ""
    cost_model_path: str = ""
    data_quality_audit_path: str = ""
    daily_backtest_path: str = ""
    promotion_replay_path: str = ""

    @classmethod
    def from_json(cls, data: JsonMap) -> "GeneratedStrategyManifest":
        objectives = [str(item) for item in data.get("objectives", [])]
        if not objectives and data.get("objective"):
            objectives = [str(data["objective"])]
        return cls(
            schema_version=int(data.get("schema_version", 0)),
            artifact_type=str(data.get("artifact_type", "")),
            strategy_id=str(data.get("strategy_id", "")),
            objectives=objectives,
            promotion_review_requested=bool(data.get("promotion_review_requested", False)),
            mandate_path=str(data.get("mandate_path", "")),
            thesis_path=str(data.get("thesis_path", "")),
            descriptor_set_path=str(data.get("descriptor_set_path", "")),
            sleeve_path=str(data.get("sleeve_path", "")),
            target_path=str(data.get("target_path", "")),
            portfolio_path=str(data.get("portfolio_path", "")),
            cost_model_path=str(data.get("cost_model_path", "")),
            data_quality_audit_path=str(data.get("data_quality_audit_path", "")),
            daily_backtest_path=str(data.get("daily_backtest_path", "")),
            promotion_replay_path=str(data.get("promotion_replay_path", "")),
        )


@dataclass(slots=True)
class StrategyGenerationGuardrailResult:
    manifest: GeneratedStrategyManifest
    valid: bool
    promotion_review_allowed: bool
    blockers: list[str] = field(default_factory=list)
    rejected_objectives: list[str] = field(default_factory=list)
    bound_ids: dict[str, str] = field(default_factory=dict)
    evidence_paths: dict[str, str] = field(default_factory=dict)


def load_generated_strategy_manifest(path: Path | str) -> GeneratedStrategyManifest:
    target = _resolve_project_path(path)
    with target.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    manifest = GeneratedStrategyManifest.from_json(payload)
    if manifest.schema_version != 1:
        raise ValueError("Generated strategy manifest requires schema_version=1.")
    if manifest.artifact_type != "generated_strategy_manifest":
        raise ValueError("Generated strategy manifest requires generated_strategy_manifest.")
    if not manifest.strategy_id.strip():
        raise ValueError("Generated strategy manifest requires strategy_id.")
    return manifest


def evaluate_generated_strategy_manifest(
    path: Path | str,
) -> StrategyGenerationGuardrailResult:
    manifest = load_generated_strategy_manifest(path)
    blockers: list[str] = []
    bound_ids: dict[str, str] = {}
    evidence_paths: dict[str, str] = {}

    rejected_objectives = [
        objective
        for objective in manifest.objectives
        if objective in REJECTED_OBJECTIVES
    ]
    if rejected_objectives:
        blockers.append("rejected_objectives")

    mandate = _load_required_config(
        manifest.mandate_path,
        "mandate_path",
        load_mandate,
        blockers,
    )
    thesis = _load_required_config(
        manifest.thesis_path,
        "thesis_path",
        load_thesis,
        blockers,
    )
    descriptor_set = _load_required_config(
        manifest.descriptor_set_path,
        "descriptor_set_path",
        load_descriptor_set,
        blockers,
    )
    sleeve = _load_required_config(
        manifest.sleeve_path,
        "sleeve_path",
        load_sleeve,
        blockers,
    )
    target = _load_required_config(
        manifest.target_path,
        "target_path",
        load_target,
        blockers,
    )
    portfolio = _load_required_config(
        manifest.portfolio_path,
        "portfolio_path",
        load_portfolio,
        blockers,
    )
    cost_model = _load_required_config(
        manifest.cost_model_path,
        "cost_model_path",
        load_cost_model,
        blockers,
    )

    if mandate is not None:
        bound_ids["mandate_id"] = mandate.id
    if thesis is not None:
        bound_ids["thesis_id"] = thesis.id
    if descriptor_set is not None:
        bound_ids["descriptor_set_id"] = descriptor_set.id
    if sleeve is not None:
        bound_ids["sleeve_id"] = sleeve.id
    if target is not None:
        bound_ids["target_id"] = target.id
    if portfolio is not None:
        bound_ids["portfolio_id"] = portfolio.id
    if cost_model is not None:
        bound_ids["cost_model_id"] = cost_model.id

    if descriptor_set is not None and thesis is not None:
        if descriptor_set.thesis_id != thesis.id:
            blockers.append("descriptor_set_thesis_mismatch")
    if descriptor_set is not None and target is not None:
        if descriptor_set.target_id != target.id:
            blockers.append("descriptor_set_target_mismatch")
    if sleeve is not None and mandate is not None:
        if sleeve.mandate_id != mandate.id:
            blockers.append("sleeve_mandate_mismatch")
    if sleeve is not None and thesis is not None:
        if sleeve.thesis_id != thesis.id:
            blockers.append("sleeve_thesis_mismatch")
    if sleeve is not None and descriptor_set is not None:
        if sleeve.descriptor_set_id != descriptor_set.id:
            blockers.append("sleeve_descriptor_set_mismatch")
    if sleeve is not None and target is not None:
        if sleeve.target_id != target.id:
            blockers.append("sleeve_target_mismatch")
    if portfolio is not None and mandate is not None:
        if portfolio.mandate_id != mandate.id:
            blockers.append("portfolio_mandate_mismatch")
    if portfolio is not None and sleeve is not None:
        if sleeve.id not in portfolio.sleeves:
            blockers.append("portfolio_sleeve_missing")
    if target is not None and cost_model is not None:
        if target.cost_model != cost_model.id:
            blockers.append("target_cost_model_mismatch")

    _validate_json_artifact(
        manifest.data_quality_audit_path,
        "data_quality_audit_path",
        "market_data_quality_audit",
        blockers,
        evidence_paths,
    )
    _validate_json_artifact(
        manifest.daily_backtest_path,
        "daily_backtest_path",
        "portfolio_backtest_result",
        blockers,
        evidence_paths,
    )
    _validate_promotion_replay_path(
        manifest.promotion_replay_path,
        blockers,
        evidence_paths,
        required=manifest.promotion_review_requested,
    )

    promotion_review_allowed = (
        manifest.promotion_review_requested
        and not blockers
        and bool(manifest.daily_backtest_path.strip())
    )
    return StrategyGenerationGuardrailResult(
        manifest=manifest,
        valid=not blockers,
        promotion_review_allowed=promotion_review_allowed,
        blockers=blockers,
        rejected_objectives=rejected_objectives,
        bound_ids=bound_ids,
        evidence_paths=evidence_paths,
    )


def validate_generated_strategy_manifest(
    path: Path | str,
) -> StrategyGenerationGuardrailResult:
    result = evaluate_generated_strategy_manifest(path)
    if result.valid:
        return result
    details = ", ".join(result.blockers)
    if result.rejected_objectives:
        rejected = ", ".join(result.rejected_objectives)
        raise ValueError(
            "Generated strategy manifest failed guardrails: "
            f"rejected objectives: {rejected}; blockers: {details}"
        )
    raise ValueError(f"Generated strategy manifest failed guardrails: {details}")


def _load_required_config(
    path: str,
    blocker: str,
    loader: Callable[[Path | str], T],
    blockers: list[str],
) -> T | None:
    if not path.strip():
        blockers.append(blocker)
        return None
    target = _resolve_project_path(path)
    if not target.exists():
        blockers.append(blocker)
        return None
    return loader(target)


def _validate_json_artifact(
    path: str,
    blocker: str,
    expected_artifact_type: str,
    blockers: list[str],
    evidence_paths: dict[str, str],
) -> None:
    if not path.strip():
        blockers.append(blocker)
        return
    target = _resolve_project_path(path)
    if not target.exists():
        blockers.append(blocker)
        return
    with target.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if int(payload.get("schema_version", 0)) != 1:
        blockers.append(blocker)
        return
    if str(payload.get("artifact_type", "")) != expected_artifact_type:
        blockers.append(blocker)
        return
    evidence_paths[blocker] = str(target)


def _validate_promotion_replay_path(
    path: str,
    blockers: list[str],
    evidence_paths: dict[str, str],
    *,
    required: bool,
) -> None:
    if not path.strip():
        if required:
            blockers.append("promotion_replay_path")
        return
    target = _resolve_project_path(path)
    if not target.exists():
        blockers.append("promotion_replay_path")
        return
    if target.suffix == ".toml":
        with target.open("rb") as handle:
            payload = tomllib.load(handle)
    else:
        with target.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    if int(payload.get("schema_version", 0)) != 1:
        blockers.append("promotion_replay_path")
        return
    if str(payload.get("artifact_type", "")) != "portfolio_promotion_replay_case":
        blockers.append("promotion_replay_path")
        return
    evidence_paths["promotion_replay_path"] = str(target)


def _resolve_project_path(path: Path | str) -> Path:
    target = Path(path)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    return target
