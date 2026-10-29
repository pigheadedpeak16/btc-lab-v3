"""Phase 5.9.3 causal, paper-only, prospective binary BUY edge filter.

The input probability is an UNCALIBRATED Monte Carlo prediction. This module
subtracts an explicit policy safety haircut, not a statistically estimated
confidence interval, and fails closed when any executable input is missing.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal
from math import isfinite


@dataclass(frozen=True)
class EdgeGateConfig:
    probability_haircut: float = 0.08
    slippage_allowance: float = 0.02
    minimum_net_edge: float = 0.03


@dataclass(frozen=True)
class EdgeAssessment:
    accepted: bool
    reason: str
    probability_raw: float | None = None
    probability_conservative: float | None = None
    effective_price: float | None = None
    fee_per_share: float | None = None
    slippage_allowance: float | None = None
    net_edge_per_share: float | None = None
    minimum_net_edge: float | None = None
    submitted_shares: float | None = None
    filled_shares: float | None = None
    market_min_order_size: float | None = None

    def to_context(self) -> dict[str, object]:
        return asdict(self)


def assess_executable_edge(
    *,
    side: str,
    raw_probability: float | None,
    submitted_shares: Decimal,
    filled_shares: Decimal,
    effective_price: Decimal | None,
    total_fee: Decimal,
    market_min_order_size: Decimal | None,
    config: EdgeGateConfig,
) -> EdgeAssessment:
    """Price/fee/depth aware decision at the actual attempted paper execution.

    No extra assumed liquidity; FAK partial fills below published minimum fail.
    A top-of-book quote alone is insufficient: effective_price is walked L2 VWAP.
    """
    if side != "BUY":
        return EdgeAssessment(False, "EDGE_BUY_ONLY")
    if raw_probability is None or not isfinite(raw_probability) or not 0 <= raw_probability <= 1:
        return EdgeAssessment(False, "EDGE_MODEL_MISSING_OR_INVALID")
    if market_min_order_size is None or market_min_order_size <= 0:
        return EdgeAssessment(False, "EDGE_MIN_ORDER_UNKNOWN")
    if submitted_shares < market_min_order_size:
        return EdgeAssessment(False, "EDGE_SUBMITTED_BELOW_MINIMUM")
    if filled_shares < market_min_order_size:
        return EdgeAssessment(False, "EDGE_LIQUIDITY_BELOW_MINIMUM")
    if effective_price is None or not 0 < effective_price < 1 or total_fee < 0:
        return EdgeAssessment(False, "EDGE_PRICE_OR_FEE_INVALID")
    if filled_shares > submitted_shares:
        return EdgeAssessment(False, "EDGE_FILL_EXCEEDS_SUBMITTED")
    if not (0 <= config.probability_haircut < 1 and 0 <= config.slippage_allowance < 1 and 0 < config.minimum_net_edge < 1):
        raise ValueError("Invalid frozen edge policy")
    conservative_probability = max(0.0, raw_probability - config.probability_haircut)
    fee_per_share = total_fee / filled_shares
    edge = Decimal(str(conservative_probability)) - effective_price - fee_per_share - Decimal(str(config.slippage_allowance))
    allowed = edge >= Decimal(str(config.minimum_net_edge))
    return EdgeAssessment(
        accepted=allowed,
        reason="EDGE_ACCEPT" if allowed else "EDGE_INSUFFICIENT_MARGIN",
        probability_raw=raw_probability,
        probability_conservative=conservative_probability,
        effective_price=float(effective_price),
        fee_per_share=float(fee_per_share),
        slippage_allowance=config.slippage_allowance,
        net_edge_per_share=float(edge),
        minimum_net_edge=config.minimum_net_edge,
        submitted_shares=float(submitted_shares),
        filled_shares=float(filled_shares),
        market_min_order_size=float(market_min_order_size),
    )
