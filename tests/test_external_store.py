from pathlib import Path

from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.external_store import ExternalStore


def test_external_store_persists_predictor_ticks(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    with ExternalStore(paths["external"]) as store:
        store.persist_coinbase_tick(
            source_ts_us=10,
            received_at_us=20,
            receive_sequence=1,
            price="86000.12345678",
            raw_payload={"x": 1},
        )
        store.persist_kraken_tick(
            source_ts_us=11,
            received_at_us=21,
            receive_sequence=1,
            price="86001.2",
            raw_payload={"x": 2},
        )
        health = store.current_health()
    assert health.coinbase_count == 1
    assert health.kraken_count == 1
    assert health.latest_coinbase_at_us == 20
    assert health.latest_kraken_at_us == 21


def test_causal_price_reference_rejects_future_observation(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    with ExternalStore(paths["external"]) as store:
        store.persist_price_reference(
            slug="btc-updown-5m-100",
            round_start_us=100_000_000,
            round_end_us=400_000_000,
            price_to_beat="86000.123456789012345678",
            reference_ts_us=100_000_000,
            observed_at_us=105_000_000,
            source="CHAINLINK_BTC_USD_TWAP_60S",
            source_stream="prices.crypto.twap:btcusd:60s",
            mapping_method="EXACT_ROUND_START_SOURCE_TIMESTAMP",
            raw_payload={"value": "86000.123456789012345678"},
        )
        assert store.causal_price_reference("btc-updown-5m-100", 104_999_999) is None
        ref = store.causal_price_reference("btc-updown-5m-100", 105_000_000)
    assert ref is not None
    assert ref.price_to_beat == "86000.123456789012345678"
