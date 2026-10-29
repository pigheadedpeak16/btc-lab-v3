from pathlib import Path

from btc_v3.forensics.live import LiveTelemetrySample, TargetWalletEvent
from btc_v3.forensics.store import ForensicsStore
from btc_v3.storage.db import initialize_project_databases


def test_live_forensics_schema_round_trip(tmp_path: Path):
    initialize_project_databases(tmp_path)
    with ForensicsStore.open_project(tmp_path) as store:
        store.start_live_session(
            session_id="s1",
            wallet="0xabc",
            requested_wallet="0xabc",
            started_at_us=100,
            config={"poll_seconds": 1.0},
        )
        sample = LiveTelemetrySample(
            sample_id="sample1",
            session_id="s1",
            wallet="0xabc",
            sample_ts_us=150,
            slug="btc-updown-5m-1000",
            round_start_us=100,
            round_end_us=400,
            seconds_remaining=250.0,
            state="READY",
            unavailable_reason=None,
            ptb=100.0,
            official_twap=101.0,
            coinbase_price=101.0,
            kraken_price=101.0,
            predictor_price=101.0,
            p_up_mc=0.7,
            p_up_analytic=0.71,
            distance_dollars=1.0,
            returns_json="{}",
            volatility_json="{}",
            lab_json="{}",
            book_json="{}",
        )
        store.save_live_sample(sample)
        event = TargetWalletEvent(
            event_id="e1",
            session_id="s1",
            wallet="0xabc",
            trade_id="t1",
            trade_ts_us=149,
            detected_at_us=160,
            observable_lag_us=11,
            slug="btc-updown-5m-1000",
            side="BUY",
            outcome="UP",
            price="0.55",
            size="2",
            attribution="TAKER",
            pre_trade_sample_id="sample1",
            detection_sample_id="sample1",
            current_executable_price="0.56",
            copy_price_delta="0.01",
            market_context_matches=True,
            raw_json="{}",
        )
        store.save_target_event(event)
        assert store.live_session_counts("s1") == (1, 1)
        store.finish_live_session(
            session_id="s1",
            ended_at_us=200,
            target_event_count=1,
            state="STOPPED",
        )
