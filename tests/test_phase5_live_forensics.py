from collections import deque
from decimal import Decimal

from btc_v3.forensics.live import (
    LiveForensicsConfig,
    LiveTelemetrySample,
    SeenTradeIds,
    last_sample_at_or_before,
)
from btc_v3.forensics.models import WalletTrade


def trade(trade_id: str, ts: int) -> WalletTrade:
    return WalletTrade(
        trade_id=trade_id,
        wallet="0xabc",
        condition_id=None,
        token_id=None,
        side="BUY",
        outcome="UP",
        price=Decimal("0.55"),
        size=Decimal("2"),
        timestamp_us=ts,
        transaction_hash=None,
        title="Bitcoin Up or Down - 5m",
        slug="btc-updown-5m-1000",
        event_slug=None,
        maker_address=None,
        taker_address=None,
        attribution=None,
        raw_json="{}",
    )


def sample(sample_id: str, ts: int) -> LiveTelemetrySample:
    return LiveTelemetrySample(
        sample_id=sample_id,
        session_id="s",
        wallet="0xabc",
        sample_ts_us=ts,
        slug="btc-updown-5m-1000",
        round_start_us=1,
        round_end_us=2,
        seconds_remaining=1.0,
        state="READY",
        unavailable_reason=None,
        ptb=100.0,
        official_twap=101.0,
        coinbase_price=101.0,
        kraken_price=101.0,
        predictor_price=101.0,
        p_up_mc=0.7,
        p_up_analytic=0.7,
        distance_dollars=1.0,
        returns_json="{}",
        volatility_json="{}",
        lab_json="{}",
        book_json="{}",
    )


def test_seen_trade_ids_only_emits_new_rows_in_time_order():
    seen = SeenTradeIds({"old"})
    rows = [trade("b", 20), trade("old", 10), trade("a", 15)]
    out = seen.mark_new(rows)
    assert [row.trade_id for row in out] == ["a", "b"]
    assert seen.mark_new(rows) == []


def test_last_sample_at_or_before_is_strictly_causal():
    samples = deque([sample("a", 100), sample("b", 200), sample("c", 300)])
    assert last_sample_at_or_before(samples, 250).sample_id == "b"
    assert last_sample_at_or_before(samples, 200).sample_id == "b"
    assert last_sample_at_or_before(samples, 50) is None


def test_live_forensics_config_rejects_overaggressive_polling():
    cfg = LiveForensicsConfig(poll_seconds=0.1)
    try:
        cfg.validate()
    except ValueError as exc:
        assert "poll_seconds" in str(exc)
    else:
        raise AssertionError("expected validation failure")


def test_default_live_forensics_config_is_valid():
    LiveForensicsConfig().validate()
