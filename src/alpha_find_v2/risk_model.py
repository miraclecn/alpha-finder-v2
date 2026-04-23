from __future__ import annotations

from dataclasses import dataclass, field

from .models import RiskModel


@dataclass(slots=True)
class RiskModelSnapshot:
    factor_returns: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class AssetRiskObservation:
    asset_id: str
    forward_return: float
    exposures: dict[str, float] = field(default_factory=dict)


@dataclass(slots=True)
class RiskDecomposition:
    asset_id: str
    forward_return: float
    components: dict[str, float] = field(default_factory=dict)
    common_return: float = 0.0
    residual_return: float = 0.0


class ConfiguredRiskModelResidualizer:
    def __init__(self, risk_model: RiskModel) -> None:
        self.risk_model = risk_model

    def decompose(
        self,
        observation: AssetRiskObservation,
        snapshot: RiskModelSnapshot,
    ) -> RiskDecomposition:
        components = {
            component: 0.0
            for component in self.risk_model.residual_components
        }
        missing_factor_returns: list[str] = []

        for exposure_key, loading in observation.exposures.items():
            if loading == 0.0:
                continue

            factor = self.risk_model.factor_for_key(exposure_key)
            if factor is None:
                raise ValueError(f"Unsupported factor exposure key: {exposure_key}")
            if exposure_key not in snapshot.factor_returns:
                missing_factor_returns.append(exposure_key)
                continue

            components.setdefault(factor.component, 0.0)
            components[factor.component] += loading * snapshot.factor_returns[exposure_key]

        if missing_factor_returns:
            joined = ", ".join(sorted(missing_factor_returns))
            raise ValueError(f"Missing factor returns for used exposures: {joined}")

        common_return = sum(
            components.get(component, 0.0)
            for component in self.risk_model.residual_components
        )
        return RiskDecomposition(
            asset_id=observation.asset_id,
            forward_return=observation.forward_return,
            components=components,
            common_return=common_return,
            residual_return=observation.forward_return - common_return,
        )
