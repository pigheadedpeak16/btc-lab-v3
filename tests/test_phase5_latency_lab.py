from decimal import Decimal

from btc_v3.forensics.latency import (
    CorrelationResult,
    LatencyLabConfig,
    MarketTradeCandidate,
    correlate_market_trade,
    latency_verdict,
)
from btc_v3.forensics.models import WalletTrade


def _trade(**overrides):
    data = dict(
        trade_id="w1",
        wallet="0x" + "1" * 40,
        condition_id="0xabc",
        token_id="token-up",
        side="BUY",
        outcome="UP",
        price=Decimal("0.57"),
        size=Decimal("12.5"),
        timestamp_us=1_700_000_000_000_000,
        transaction_hash="0xtx",
        title="Bitcoin Up or Down",
        slug="btc-updown-5m-1700000000",
        event_slug=None,
        maker_address=None,
        taker_address=None,
        attribution="TAKER",
        raw_json="{}",
    )
    data.update(overrides)
    return WalletTrade(**data)


def _candidate(**overrides):
    data = dict(
        market_trade_id=7,
        slug="btc-updown-5m-1700000000",
        token_id="token-up",
        source_trade_id="0xtx",
        source_ts_us=1_700_000_000_050_000,
        received_at_us=1_700_000_000_100_000,
        price="0.57",
        size="12.5",
        taker_side="BUY",
    )
    data.update(overrides)
    return MarketTradeCandidate(**data)


def test_latency_config_allows_100ms_polling_but_not_10ms():
    LatencyLabConfig(poll_ms=100).validate()
    try:
        LatencyLabConfig(poll_ms=10).validate()
    except ValueError as exc:
        assert "poll_ms" in str(exc)
    else:
        raise AssertionError("10ms polling should be rejected")


def test_correlate_prefers_unique_transaction_hash():
    result = correlate_market_trade(_trade(), [_candidate()], window_us=1_500_000)
    assert result.quality == "EXACT_TX"
    assert result.candidate is not None
    assert result.candidate.market_trade_id == 7


def test_correlate_falls_back_to_unique_signature():
    trade = _trade(transaction_hash=None)
    result = correlate_market_trade(trade, [_candidate(source_trade_id=None)], window_us=1_500_000)
    assert result.quality == "UNIQUE_SIGNATURE"
    assert result.candidate is not None


def test_correlate_refuses_ambiguous_signature():
    trade = _trade(transaction_hash=None)
    rows = [
        _candidate(market_trade_id=1, source_trade_id=None),
        _candidate(market_trade_id=2, source_trade_id=None, received_at_us=1_700_000_000_120_000),
    ]
    result = correlate_market_trade(trade, rows, window_us=1_500_000)
    assert result.quality == "AMBIGUOUS_SIGNATURE"
    assert result.candidate is None


def test_latency_verdict_requires_enough_samples_and_p90_under_200():
    small = latency_verdict([100_000] * 5, min_samples=20)
    assert small.verdict == "INSUFFICIENT_SAMPLES"

    good = latency_verdict([80_000, 90_000, 100_000, 110_000, 120_000] * 4, min_samples=20)
    assert good.verdict == "CONSISTENT_SUB_200MS"
    assert good.p90_ms is not None and good.p90_ms <= 200.0
    assert good.under_200ms_fraction == 1.0

    mixed = latency_verdict([100_000] * 11 + [450_000] * 9, min_samples=20)
    assert mixed.verdict == "OCCASIONAL_SUB_200MS"

    bad = latency_verdict([300_000] * 20, min_samples=20)
    assert bad.verdict == "NOT_SUB_200MS"


def test_correlate_allows_wallet_fill_size_to_differ_from_aggregate_clob_trade():
    trade = _trade(transaction_hash=None, size=Decimal("3.0"))
    candidate = _candidate(source_trade_id=None, size="15.1")
    result = correlate_market_trade(trade, [candidate], window_us=2_500_000)
    assert result.quality == "UNIQUE_PRICE_TIME"
    assert result.candidate is not None


def test_correlate_price_time_still_refuses_ambiguity():
    trade = _trade(transaction_hash=None, size=Decimal("3.0"))
    rows = [
        _candidate(market_trade_id=10, source_trade_id=None, size="15.1"),
        _candidate(market_trade_id=11, source_trade_id=None, size="8.0", source_ts_us=1_700_000_000_060_000),
    ]
    result = correlate_market_trade(trade, rows, window_us=2_500_000)
    assert result.quality == "AMBIGUOUS_PRICE_TIME"
    assert result.candidate is None
