from __future__ import annotations

from pathlib import Path
import tomllib

from .models import (
    CostModel,
    DecayMonitor,
    Descriptor,
    DescriptorSet,
    ExecutionPolicy,
    Mandate,
    PortfolioConstructionModel,
    PortfolioRecipe,
    PromotionGate,
    RegimeOverlay,
    ResidualTarget,
    RiskModel,
    Sleeve,
    Thesis,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_ROOT = PROJECT_ROOT / "config"


def _read_toml(path: Path | str) -> dict:
    target = Path(path)
    if not target.is_absolute():
        target = PROJECT_ROOT / target
    with target.open("rb") as handle:
        return tomllib.load(handle)


def list_configs(group: str) -> list[Path]:
    return sorted((CONFIG_ROOT / group).glob("*.toml"))


def load_mandate(path: Path | str) -> Mandate:
    return Mandate.from_toml(_read_toml(path))


def load_thesis(path: Path | str) -> Thesis:
    return Thesis.from_toml(_read_toml(path))


def load_descriptor(path: Path | str) -> Descriptor:
    return Descriptor.from_toml(_read_toml(path))


def load_descriptor_set(path: Path | str) -> DescriptorSet:
    return DescriptorSet.from_toml(_read_toml(path))


def load_cost_model(path: Path | str) -> CostModel:
    return CostModel.from_toml(_read_toml(path))


def load_execution_policy(path: Path | str) -> ExecutionPolicy:
    return ExecutionPolicy.from_toml(_read_toml(path))


def load_target(path: Path | str) -> ResidualTarget:
    return ResidualTarget.from_toml(_read_toml(path))


def load_risk_model(path: Path | str) -> RiskModel:
    return RiskModel.from_toml(_read_toml(path))


def load_promotion_gate(path: Path | str) -> PromotionGate:
    return PromotionGate.from_toml(_read_toml(path))


def load_regime_overlay(path: Path | str) -> RegimeOverlay:
    return RegimeOverlay.from_toml(_read_toml(path))


def load_sleeve(path: Path | str) -> Sleeve:
    return Sleeve.from_toml(_read_toml(path))


def load_portfolio_construction_model(path: Path | str) -> PortfolioConstructionModel:
    return PortfolioConstructionModel.from_toml(_read_toml(path))


def load_portfolio(path: Path | str) -> PortfolioRecipe:
    return PortfolioRecipe.from_toml(_read_toml(path))


def load_decay_monitor(path: Path | str) -> DecayMonitor:
    return DecayMonitor.from_toml(_read_toml(path))
