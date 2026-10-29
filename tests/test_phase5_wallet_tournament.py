from __future__ import annotations

from btc_v3.forensics.analysis import WalletForensicsReport
from btc_v3.forensics.tournament import build_tournament_row, tournament_manifest


def _report(wallet: str, rounds: int = 100, trades: int = 500) -> WalletForensicsReport:
    return WalletForensicsReport(
        wallet=wallet,
        source_activity_rows=trades,
        source_trade_rows=trades,
        btc5m_activity_rows=trades,
        btc5m_trade_actions=trades,
        btc5m_rounds=rounds,
        up_only_rounds=40,
        down_only_rounds=30,
        both_side_rounds=30,
        buy_only_rounds_with_redeem=50,
        rounds_with_sell=10,
        final_60s_trade_actions=200,
        median_trade_price=0.55,
        median_trade_size=20.0,
        total_buy_cost_estimate=1000.0,
        total_sell_proceeds_estimate=200.0,
        price_bands={"0.50-0.60": trades},
        timing_bands={"60-120s": trades // 2, "30-60s": trades // 2},
        event_types={"TRADE": trades},
        hypothesis_evidence={},
        limitations=(),
        rounds=(),
    )


def test_copyability_stays_unscored_without_live_events():
    wallet = "0x1111111111111111111111111111111111111111"
    row = build_tournament_row(
        wallet=wallet,
        label="A",
        report=_report(wallet),
        profile_stats={"all_time_pnl": 1000, "trades": 200},
        volume_stats={"volume_usdc": 100000},
        live_events=[],
        copy_curve={},
    )
    assert row.copyability_score is None
    assert row.copyability_state == "NO_LIVE_EVENTS"
    assert row.inference_usefulness_score > 0


def test_copyability_rewards_low_lag_low_drift_and_context_match():
    wallet = "0x2222222222222222222222222222222222222222"
    events = [
        {"observable_lag_us": 300_000, "copy_price_delta": "0.004", "market_context_matches": 1}
        for _ in range(20)
    ]
    row = build_tournament_row(
        wallet=wallet,
        label="B",
        report=_report(wallet, rounds=1000, trades=5000),
        profile_stats={"all_time_pnl": 500, "trades": 5000},
        volume_stats={"volume_usdc": 1_000_000},
        live_events=events,
        copy_curve={250: {"observations": 20, "median_abs_price_delta": 0.006, "context_match_rate": 1.0}},
    )
    assert row.copyability_state == "MEASURED"
    assert row.copyability_score == 100.0
    assert row.inference_usefulness_score >= 85.0


def test_rankings_keep_raw_profit_separate_from_copyability():
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    bad_copy = [
        {"observable_lag_us": 8_000_000, "copy_price_delta": "0.08", "market_context_matches": 1}
        for _ in range(20)
    ]
    good_copy = [
        {"observable_lag_us": 400_000, "copy_price_delta": "0.003", "market_context_matches": 1}
        for _ in range(20)
    ]
    a = build_tournament_row(
        wallet=wallet_a,
        label="rich-but-late",
        report=_report(wallet_a),
        profile_stats={"all_time_pnl": 100000},
        volume_stats={},
        live_events=bad_copy,
        copy_curve={},
    )
    b = build_tournament_row(
        wallet=wallet_b,
        label="smaller-copyable",
        report=_report(wallet_b),
        profile_stats={"all_time_pnl": 20000},
        volume_stats={},
        live_events=good_copy,
        copy_curve={},
    )
    manifest = tournament_manifest([a, b])
    assert manifest["rankings"]["raw_all_time_pnl"][0] == wallet_a
    assert manifest["rankings"]["observable_copyability"][0] == wallet_b
