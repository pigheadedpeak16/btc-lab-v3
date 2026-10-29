from __future__ import annotations

import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal
from typing import Any, Iterable

from .models import ROUND_SECONDS, WalletActivity, WalletTrade

ZERO = Decimal("0")


def _f(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _price_band(price: Decimal | None) -> str:
    if price is None:
        return "UNKNOWN"
    if price < Decimal("0.20"):
        return "0.00-0.20"
    if price < Decimal("0.40"):
        return "0.20-0.40"
    if price < Decimal("0.60"):
        return "0.40-0.60"
    if price < Decimal("0.80"):
        return "0.60-0.80"
    return "0.80-1.00"


def _timing_band(seconds_remaining: float | None) -> str:
    if seconds_remaining is None:
        return "UNKNOWN"
    if seconds_remaining > 240:
        return "240-300s"
    if seconds_remaining > 180:
        return "180-240s"
    if seconds_remaining > 120:
        return "120-180s"
    if seconds_remaining > 60:
        return "60-120s"
    if seconds_remaining > 30:
        return "30-60s"
    if seconds_remaining > 10:
        return "10-30s"
    return "0-10s"


@dataclass(frozen=True)
class RoundBehavior:
    slug: str
    round_start_us: int
    round_end_us: int
    first_action_us: int | None
    last_action_us: int | None
    buy_up_shares: float
    buy_down_shares: float
    sell_up_shares: float
    sell_down_shares: float
    buy_up_cost: float
    buy_down_cost: float
    trade_actions: int
    redeem_actions: int
    classification: str
    traded_both_sides: bool
    had_sell: bool
    had_redeem: bool
    final_60s_trade_actions: int


@dataclass(frozen=True)
class WalletForensicsReport:
    wallet: str
    source_activity_rows: int
    source_trade_rows: int
    btc5m_activity_rows: int
    btc5m_trade_actions: int
    btc5m_rounds: int
    up_only_rounds: int
    down_only_rounds: int
    both_side_rounds: int
    buy_only_rounds_with_redeem: int
    rounds_with_sell: int
    final_60s_trade_actions: int
    median_trade_price: float | None
    median_trade_size: float | None
    total_buy_cost_estimate: float
    total_sell_proceeds_estimate: float
    price_bands: dict[str, int]
    timing_bands: dict[str, int]
    event_types: dict[str, int]
    hypothesis_evidence: dict[str, dict[str, Any]]
    limitations: tuple[str, ...]
    rounds: tuple[RoundBehavior, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def analyze_wallet_rows(
    wallet: str,
    activities: Iterable[WalletActivity],
    trades: Iterable[WalletTrade] = (),
) -> WalletForensicsReport:
    activity_rows = list(activities)
    trade_rows = list(trades)
    btc_rows = [row for row in activity_rows if row.is_btc_5m]
    btc_trades = [row for row in btc_rows if row.is_trade]

    grouped: dict[str, list[WalletActivity]] = defaultdict(list)
    for row in btc_rows:
        start = row.btc_round_start_us
        slug = row.slug or row.event_slug
        if start is not None and slug:
            grouped[slug].append(row)

    round_results: list[RoundBehavior] = []
    for slug, rows in grouped.items():
        start_us = next(r.btc_round_start_us for r in rows if r.btc_round_start_us is not None)
        end_us = start_us + ROUND_SECONDS * 1_000_000
        buy_up = buy_down = sell_up = sell_down = ZERO
        buy_up_cost = buy_down_cost = ZERO
        trade_actions = redeem_actions = final_60 = 0
        action_times: list[int] = []
        for row in rows:
            if row.timestamp_us is not None:
                action_times.append(row.timestamp_us)
            etype = row.event_type.upper()
            if etype == "REDEEM":
                redeem_actions += 1
            if etype != "TRADE":
                continue
            trade_actions += 1
            if row.timestamp_us is not None and 0 <= end_us - row.timestamp_us <= 60_000_000:
                final_60 += 1
            size = row.size or ZERO
            price = row.price or ZERO
            side = (row.side or "").upper()
            outcome = row.outcome
            if side == "BUY" and outcome == "UP":
                buy_up += size
                buy_up_cost += size * price
            elif side == "BUY" and outcome == "DOWN":
                buy_down += size
                buy_down_cost += size * price
            elif side == "SELL" and outcome == "UP":
                sell_up += size
            elif side == "SELL" and outcome == "DOWN":
                sell_down += size
        has_up = buy_up > ZERO
        has_down = buy_down > ZERO
        if has_up and has_down:
            classification = "BOTH"
        elif has_up:
            classification = "UP_ONLY"
        elif has_down:
            classification = "DOWN_ONLY"
        else:
            classification = "NO_BUY"
        had_sell = (sell_up + sell_down) > ZERO
        round_results.append(
            RoundBehavior(
                slug=slug,
                round_start_us=start_us,
                round_end_us=end_us,
                first_action_us=min(action_times) if action_times else None,
                last_action_us=max(action_times) if action_times else None,
                buy_up_shares=float(buy_up),
                buy_down_shares=float(buy_down),
                sell_up_shares=float(sell_up),
                sell_down_shares=float(sell_down),
                buy_up_cost=float(buy_up_cost),
                buy_down_cost=float(buy_down_cost),
                trade_actions=trade_actions,
                redeem_actions=redeem_actions,
                classification=classification,
                traded_both_sides=classification == "BOTH",
                had_sell=had_sell,
                had_redeem=redeem_actions > 0,
                final_60s_trade_actions=final_60,
            )
        )

    round_results.sort(key=lambda r: r.round_start_us)
    trade_prices = [float(r.price) for r in btc_trades if r.price is not None]
    trade_sizes = [float(r.size) for r in btc_trades if r.size is not None]
    price_bands = Counter(_price_band(r.price) for r in btc_trades)
    timing_bands: Counter[str] = Counter()
    buy_cost = ZERO
    sell_proceeds = ZERO
    for row in btc_trades:
        if row.timestamp_us is not None:
            start = row.btc_round_start_us
            if start is not None:
                seconds_remaining = ((start + ROUND_SECONDS * 1_000_000) - row.timestamp_us) / 1_000_000
                timing_bands[_timing_band(seconds_remaining)] += 1
        size = row.size or ZERO
        price = row.price or ZERO
        if (row.side or "").upper() == "BUY":
            buy_cost += size * price
        elif (row.side or "").upper() == "SELL":
            sell_proceeds += size * price

    classifications = Counter(r.classification for r in round_results)
    rounds_with_sell = sum(1 for r in round_results if r.had_sell)
    buy_only_redeem = sum(
        1 for r in round_results if r.had_redeem and not r.had_sell and r.classification in {"UP_ONLY", "DOWN_ONLY", "BOTH"}
    )
    final_60 = sum(r.final_60s_trade_actions for r in round_results)
    rounds_n = len(round_results)
    both_ratio = _ratio(classifications["BOTH"], rounds_n)
    one_sided_ratio = _ratio(classifications["UP_ONLY"] + classifications["DOWN_ONLY"], rounds_n)
    final60_ratio = _ratio(final_60, len(btc_trades))
    sell_round_ratio = _ratio(rounds_with_sell, rounds_n)

    hypothesis_evidence = {
        "two_sided_accumulation": {
            "metric": "fraction_of_btc5m_rounds_with_buys_on_both_outcomes",
            "value": round(both_ratio, 6),
            "strength": "HIGH" if both_ratio >= 0.60 else "MEDIUM" if both_ratio >= 0.25 else "LOW",
        },
        "directional_one_sided": {
            "metric": "fraction_of_btc5m_rounds_with_buying_on_only_one_outcome",
            "value": round(one_sided_ratio, 6),
            "strength": "HIGH" if one_sided_ratio >= 0.75 else "MEDIUM" if one_sided_ratio >= 0.45 else "LOW",
        },
        "late_settlement_focus": {
            "metric": "fraction_of_btc5m_trade_actions_in_final_60_seconds",
            "value": round(final60_ratio, 6),
            "strength": "HIGH" if final60_ratio >= 0.50 else "MEDIUM" if final60_ratio >= 0.25 else "LOW",
        },
        "active_exit_behavior": {
            "metric": "fraction_of_btc5m_rounds_with_any_sell_activity",
            "value": round(sell_round_ratio, 6),
            "strength": "HIGH" if sell_round_ratio >= 0.50 else "MEDIUM" if sell_round_ratio >= 0.20 else "LOW",
        },
        "maker_market_making": {
            "metric": "requires_quote/L2 attribution beyond wallet activity alone",
            "value": None,
            "strength": "UNDETERMINED",
        },
    }

    limitations = (
        "Public wallet activity reveals executed economic events, not hidden signals or decisions that resulted in no trade.",
        "Cancelled and unfilled limit orders cannot be reconstructed from wallet activity or Polygon settlement transactions alone.",
        "Maker/taker and queue-position inference requires synchronized historical L2 around each fill; wallet-only statistics are insufficient.",
        "This phase does not provide historical authoritative Chainlink TWAP/PTB for old rounds, so it does not claim to recover the target's fair-value model.",
        "Strategy-hypothesis labels are descriptive diagnostics, not proof of the target's proprietary algorithm.",
    )

    return WalletForensicsReport(
        wallet=wallet.lower(),
        source_activity_rows=len(activity_rows),
        source_trade_rows=len(trade_rows),
        btc5m_activity_rows=len(btc_rows),
        btc5m_trade_actions=len(btc_trades),
        btc5m_rounds=rounds_n,
        up_only_rounds=classifications["UP_ONLY"],
        down_only_rounds=classifications["DOWN_ONLY"],
        both_side_rounds=classifications["BOTH"],
        buy_only_rounds_with_redeem=buy_only_redeem,
        rounds_with_sell=rounds_with_sell,
        final_60s_trade_actions=final_60,
        median_trade_price=statistics.median(trade_prices) if trade_prices else None,
        median_trade_size=statistics.median(trade_sizes) if trade_sizes else None,
        total_buy_cost_estimate=float(buy_cost),
        total_sell_proceeds_estimate=float(sell_proceeds),
        price_bands=dict(sorted(price_bands.items())),
        timing_bands=dict(sorted(timing_bands.items())),
        event_types=dict(sorted(Counter(row.event_type for row in btc_rows).items())),
        hypothesis_evidence=hypothesis_evidence,
        limitations=limitations,
        rounds=tuple(round_results),
    )
