from types import SimpleNamespace

from btc_v3.forensics.store import ForensicsStore
from btc_v3.storage.db import initialize_project_databases


def test_latency_tables_store_correlated_events(tmp_path):
    initialize_project_databases(tmp_path)
    wallet = "0x" + "a" * 40
    with ForensicsStore.open_project(tmp_path) as store:
        store.start_latency_session(
            session_id="s1",
            wallet=wallet,
            requested_wallet=wallet,
            started_at_us=100,
            config={"poll_ms": 100},
        )
        store.save_latency_poll(SimpleNamespace(
            poll_id="p1", session_id="s1", seq=1, request_started_us=100, response_received_us=180,
            rtt_us=80, status_code=200, retry_after_ms=None, trace_id="trace", row_count=1,
            newest_trade_ts_us=150, error_text=None,
        ))
        store.save_latency_event(SimpleNamespace(
            latency_event_id="e1", session_id="s1", wallet=wallet, trade_id="t1", trade_ts_us=100,
            api_first_seen_us=250, api_request_started_us=170, api_rtt_us=80, slug="btc-updown-5m-1",
            token_id="tok", side="BUY", outcome="UP", price="0.57", size="5", transaction_hash="0xtx",
            market_trade_id=1, market_received_at_us=100, market_source_ts_us=90, market_to_wallet_us=150_000,
            correlation_quality="EXACT_TX", correlation_candidate_count=1, detection_sample_id=None,
            current_executable_price="0.58", copy_price_delta="0.01", raw_json="{}",
        ))
        assert store.correlated_wallet_latencies(wallet) == [150_000]
        store.finish_latency_session(
            session_id="s1", ended_at_us=500, poll_count=1, target_event_count=1,
            rate_limit_count=0, service_unavailable_count=0,
            verdict={"verdict": "INSUFFICIENT_SAMPLES"}, state="STOPPED",
        )
