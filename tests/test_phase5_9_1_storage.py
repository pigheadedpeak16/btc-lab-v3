from __future__ import annotations

import gzip
import json
from pathlib import Path

from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.feeds.raw_archive import RawJsonlArchive
from btc_v3.state.book_state import OrderBookState
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore

MICROS = 1_000_000


def _market() -> DiscoveredMarket:
    return DiscoveredMarket(
        slug="btc-updown-5m-1000",
        event_id="1",
        market_id="m1",
        condition_id="0xabc",
        question="Bitcoin Up or Down",
        rules_text=None,
        round_start_us=1000 * MICROS,
        round_end_us=1300 * MICROS,
        up_token_id="up",
        down_token_id="down",
        active=True,
        closed=False,
        raw_event={},
        raw_market={},
    )


def test_phase591_market_schema_uses_compact_storage_profile(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        assert store.storage_profile() == "compact_raw_ref_v1"
        tables = {
            str(r[0])
            for r in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        assert "storage_health_snapshots" in tables


def test_raw_archive_returns_exact_compact_reference(tmp_path: Path) -> None:
    raw_root = tmp_path / "data" / "raw"
    payload = {"event_type": "price_change", "blob": "x" * 100_000}
    ts = 1_760_000_000_000_000
    with RawJsonlArchive(raw_root, "polymarket_clob", flush_record_limit=1) as archive:
        ref = archive.write(ts, payload)

    assert ref["storage"] == "raw_jsonl_gzip_ref_v1"
    assert ref["component"] == "polymarket_clob"
    assert ref["record_seq"] == 1
    assert len(json.dumps(ref)) < 400

    gz = tmp_path / "data" / ref["path"]
    with gzip.open(gz, "rt", encoding="utf-8") as fh:
        row = json.loads(fh.readline())
    assert row["archive_session_id"] == ref["archive_session_id"]
    assert row["record_seq"] == ref["record_seq"]
    assert row["payload"] == payload


def test_normalized_book_rows_store_reference_not_duplicate_full_payload(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = _market()
    huge_payload = {"event_type": "price_change", "blob": "y" * 250_000}
    received = 1100 * MICROS

    raw_root = tmp_path / "data" / "raw"
    with RawJsonlArchive(raw_root, "polymarket_clob", flush_record_limit=1) as archive:
        raw_ref = archive.write(received, huge_payload)

    state = OrderBookState(market.slug, market.up_token_id, "UP")
    state.apply_snapshot(
        bids=[{"price": "0.49", "size": "100"}],
        asks=[{"price": "0.51", "size": "100"}],
        received_at_us=received,
        server_ts_us=received,
        book_hash=None,
    )
    change = {"asset_id": market.up_token_id, "side": "BUY", "price": "0.49", "size": "90"}
    state.apply_price_change(
        order_side="BUY",
        price="0.49",
        size="90",
        received_at_us=received + 1,
        server_ts_us=received + 1,
    )

    with MarketStore.open_project(tmp_path) as store:
        store.persist_market(market, received)
        store.persist_book_delta(
            state,
            received_at_us=received + 1,
            server_ts_us=received + 1,
            receive_sequence=1,
            delta=change,
            raw_payload=raw_ref,
        )
        stored = store.conn.execute("SELECT raw_payload FROM book_deltas LIMIT 1").fetchone()[0]

    assert "250000" not in stored
    assert "yyyy" not in stored
    assert "raw_jsonl_gzip_ref_v1" in stored
    assert len(stored) < 500


def test_storage_health_snapshot_is_lightweight_and_persisted(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        first = store.capture_storage_health(root=tmp_path, observed_at_us=1_000_000)
        store.persist_market(_market(), 2_000_000)
        second = store.capture_storage_health(root=tmp_path, observed_at_us=181_000_000)
        latest = store.latest_storage_health()

    assert first.free_bytes > 0
    assert second.db_bytes >= 0
    assert second.db_growth_bytes_per_hour is not None
    assert latest is not None
    assert latest.observed_at_us == 181_000_000
    assert latest.warning_level in {
        "OK",
        "LOW_DISK",
        "CRITICAL_LOW_DISK",
        "HIGH_DB_GROWTH",
        "OVERNIGHT_CAPACITY_RISK",
    }
