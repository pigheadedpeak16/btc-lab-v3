"""Paper-only, single-session conservative bankroll budget for Phase 5.9.4.

This deliberately does not communicate with exchanges, wallets or private keys.
It does not imply cross-session live protection; a real-money adapter is absent.
"""
from __future__ import annotations
from dataclasses import dataclass
from decimal import Decimal

@dataclass
class PaperSessionBudget:
    starting_cash: Decimal = Decimal("50")
    nightly_risk_budget: Decimal = Decimal("10")
    protected_floor: Decimal = Decimal("25")
    cumulative_loss_cutoff: Decimal = Decimal("25")
    cash: Decimal = Decimal("50")
    committed_this_session: Decimal = Decimal("0")
    unsettled_commitment: Decimal = Decimal("0")
    settled_pnl: Decimal = Decimal("0")
    stopped: bool = False

    def check(self, worst_case_cost: Decimal) -> str | None:
        if self.stopped or -self.settled_pnl >= self.cumulative_loss_cutoff:
            return "BANKROLL_LOSS_CIRCUIT_OPEN"
        if not worst_case_cost.is_finite() or worst_case_cost <= 0:
            return "BANKROLL_INVALID_COST"
        if self.committed_this_session + worst_case_cost > self.nightly_risk_budget:
            return "BANKROLL_NIGHTLY_BUDGET"
        if self.cash - worst_case_cost < self.protected_floor:
            return "BANKROLL_PROTECTED_FLOOR"
        return None

    def commit(self, worst_case_cost: Decimal) -> str | None:
        reason = self.check(worst_case_cost)
        if reason is not None:
            return reason
        self.cash -= worst_case_cost
        self.committed_this_session += worst_case_cost
        self.unsettled_commitment += worst_case_cost
        return None

    def settle(self, *, cost: Decimal, payout: Decimal) -> None:
        if cost < 0 or payout < 0 or cost > self.unsettled_commitment:
            raise ValueError("BANKROLL_INVALID_SETTLEMENT")
        self.unsettled_commitment -= cost
        self.cash += payout
        self.settled_pnl += payout - cost
        if -self.settled_pnl >= self.cumulative_loss_cutoff:
            self.stopped = True

    def snapshot(self) -> dict[str, str]:
        return {key: str(getattr(self, key)) for key in (
            "starting_cash", "nightly_risk_budget", "protected_floor",
            "cumulative_loss_cutoff", "cash", "committed_this_session",
            "unsettled_commitment", "settled_pnl", "stopped"
        )}
