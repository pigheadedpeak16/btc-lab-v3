from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class EnsembleMember:
    name: str
    p_up: float
    weight: float = 1.0

    def __post_init__(self) -> None:
        if not 0.0 < self.p_up < 1.0:
            raise ValueError("p_up must be strictly between 0 and 1")
        if self.weight < 0:
            raise ValueError("weight must be non-negative")


def logit_ensemble(members: tuple[EnsembleMember, ...]) -> float:
    """Weighted logit pool for future validated ensembles.

    It is intentionally not one of the seven baseline strategies. Phase 9 must
    determine weights using training data only before it can be activated.
    """
    if not members:
        raise ValueError("at least one ensemble member is required")
    total_weight = sum(m.weight for m in members)
    if total_weight <= 0:
        raise ValueError("ensemble total weight must be positive")
    pooled = 0.0
    for member in members:
        pooled += member.weight * math.log(member.p_up / (1.0 - member.p_up))
    pooled /= total_weight
    if pooled >= 0:
        z = math.exp(-pooled)
        return 1.0 / (1.0 + z)
    z = math.exp(pooled)
    return z / (1.0 + z)
