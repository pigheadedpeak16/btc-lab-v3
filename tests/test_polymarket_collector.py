import asyncio
from pathlib import Path

from btc_v3.feeds.polymarket import parse_btc_5m_event
from btc_v3.feeds.polymarket_collector import PolymarketBookCollector
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore


class FakeRest:
    async def get_order_book(self, token_id: str) -> dict:
        return {
            "market": "0x1",
            "asset_id": token_id,
            "timestamp": "2.0",
            "hash": "rest-hash",
            "bids": [{"price": "0.40", "size": "5"}],
            "asks": [{"price": "0.60", "size": "6"}],
            "min_order_size": "5",
            "tick_size": "0.01",
            "neg_risk": False,
        }


def market_fixture():
    slug = "btc-updown-5m-1791181500"
    event = {
        "id": "e1",
        "slug": slug,
        "title": "BTC Up or Down 5m",
        "markets": [{
            "id": "m1",
            "slug": slug,
            "question": "BTC Up or Down 5m",
            "conditionId": "0x1",
            "outcomes": ["Up", "Down"],
            "clobTokenIds": ["up", "down"],
            "active": True,
        }],
    }
    return parse_btc_5m_event(event, slug)


def test_book_then_delta_persists_full_depth(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()
    with MarketStore.open_project(tmp_path) as store:
        store.persist_market(market, 1)
        collector = PolymarketBookCollector(
            root=tmp_path,
            market=market,
            store=store,
            rest=FakeRest(),
        )
        collector.receive_sequence = 1
        asyncio.run(collector._process_event({
            "event_type": "book",
            "market": "0x1",
            "asset_id": "up",
            "timestamp": "1.0",
            "hash": "h1",
            "bids": [{"price": "0.40", "size": "2"}, {"price": "0.41", "size": "3"}],
            "asks": [{"price": "0.60", "size": "4"}, {"price": "0.59", "size": "5"}],
        }, 2_000_000))
        collector.receive_sequence = 2
        asyncio.run(collector._process_event({
            "event_type": "price_change",
            "market": "0x1",
            "timestamp": "1.1",
            "price_changes": [{
                "asset_id": "up",
                "price": "0.42",
                "size": "7",
                "side": "BUY",
            }],
        }, 2_100_000))
        cp = store.conn.execute("SELECT COUNT(*) FROM book_checkpoints").fetchone()[0]
        deltas = store.conn.execute("SELECT COUNT(*) FROM book_deltas").fetchone()[0]
        state = collector.states["up"]
        assert cp == 1
        assert deltas == 1
        assert str(state.best_bid.price) == "0.42"
        collector._raw.close()


def test_delta_before_snapshot_fails_closed_then_resyncs_in_background(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()
    with MarketStore.open_project(tmp_path) as store:
        store.persist_market(market, 1)
        collector = PolymarketBookCollector(
            root=tmp_path,
            market=market,
            store=store,
            rest=FakeRest(),
        )

        async def exercise() -> None:
            collector.receive_sequence = 1
            await collector._process_event({
                "event_type": "price_change",
                "market": "0x1",
                "timestamp": "2.1",
                "price_changes": [{
                    "asset_id": "up",
                    "price": "0.41",
                    "size": "8",
                    "side": "BUY",
                }],
            }, 2_200_000)
            # The event handler returns without blocking on HTTP. The unknown delta
            # is intentionally not applied without a baseline.
            assert collector.counters.ignored_unsynced_deltas == 1
            assert "up" in collector._resync_tasks
            await asyncio.gather(*list(collector._resync_tasks.values()))

        asyncio.run(exercise())
        state = collector.states["up"]
        assert state.synchronized
        assert str(state.best_bid.price) == "0.40"
        gaps = store.conn.execute("SELECT reason,recovered_at_us FROM feed_gap_events").fetchall()
        assert gaps[0][0] == "DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT"
        assert gaps[0][1] is not None
        collector._raw.close()


def test_price_comparison_is_numeric_not_string_formatting() -> None:
    assert PolymarketBookCollector._prices_equal("0.370", "0.37")
    assert PolymarketBookCollector._prices_equal("1.0", "1")
    assert not PolymarketBookCollector._prices_equal("0.37", "0.38")


def test_run_for_requests_graceful_stop_instead_of_cancelling(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()

    class SlowFinishingCollector(PolymarketBookCollector):
        async def run(self) -> None:
            # Simulate an in-flight integrity/resync operation that must be allowed to finish.
            await asyncio.sleep(0.03)
            self.states[market.up_token_id].synchronized = True
            self.states[market.down_token_id].synchronized = True

    with MarketStore.open_project(tmp_path) as store:
        collector = SlowFinishingCollector(
            root=tmp_path,
            market=market,
            store=store,
            rest=FakeRest(),
        )
        asyncio.run(collector.run_for(0.005))
        assert collector._stop_requested.is_set()
        assert collector.states[market.up_token_id].synchronized
        assert collector.states[market.down_token_id].synchronized
        collector._raw.close()



def test_post_subscribe_resync_establishes_both_books(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()
    with MarketStore.open_project(tmp_path) as store:
        store.persist_market(market, 1)
        collector = PolymarketBookCollector(
            root=tmp_path,
            market=market,
            store=store,
            rest=FakeRest(),
        )
        asyncio.run(collector._prime_rest_snapshots())
        assert not collector.states[market.up_token_id].synchronized
        assert not collector.states[market.down_token_id].synchronized
        asyncio.run(collector._post_subscribe_resync())
        assert collector.states[market.up_token_id].synchronized
        assert collector.states[market.down_token_id].synchronized
        assert collector.states[market.up_token_id].best_bid is not None
        assert collector.states[market.down_token_id].best_ask is not None
        reasons = [row[0] for row in store.conn.execute("SELECT reason FROM feed_gap_events ORDER BY id").fetchall()]
        assert reasons.count("POST_SUBSCRIBE_INITIAL_SYNC") == 2
        collector._raw.close()


def test_bba_mismatch_retains_levels_but_blocks_execution_until_resync(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()

    class SlowRest(FakeRest):
        async def get_order_book(self, token_id: str) -> dict:
            await asyncio.sleep(0.05)
            return await super().get_order_book(token_id)

    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=SlowRest())

        async def exercise() -> None:
            await collector._process_event({
                "event_type": "book",
                "asset_id": "up",
                "timestamp": "1.0",
                "bids": [{"price": "0.40", "size": "5"}],
                "asks": [{"price": "0.60", "size": "5"}],
            }, 1_000_000)
            state = collector.states["up"]
            assert state.synchronized
            await collector._process_event({
                "event_type": "best_bid_ask",
                "asset_id": "up",
                "timestamp": "1.1",
                "best_bid": "0.41",
                "best_ask": "0.59",
            }, 1_100_000)
            # Keep the last levels for inspection, but Stage 3 quarantines them
            # until a full snapshot resolves contradictory quote evidence.
            assert state.synchronized
            assert not state.depth_reliable
            assert str(state.best_bid.price) == "0.40"
            assert collector.counters.bba_mismatches == 1
            assert "up" in collector._resync_tasks
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert state.synchronized

        asyncio.run(exercise())
        collector._raw.close()


def test_failed_soft_resync_preserves_last_good_book(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()

    class FailingRest:
        async def get_order_book(self, token_id: str) -> dict:
            await asyncio.sleep(0.01)
            raise RuntimeError("temporary REST failure")

    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=FailingRest())

        async def exercise() -> None:
            await collector._process_event({
                "event_type": "book",
                "asset_id": "up",
                "timestamp": "1.0",
                "bids": [{"price": "0.40", "size": "5"}],
                "asks": [{"price": "0.60", "size": "5"}],
            }, 1_000_000)
            state = collector.states["up"]
            collector._schedule_resync("up", "TEST_SOFT_RESYNC")
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert state.synchronized
            assert str(state.best_bid.price) == "0.40"
            assert str(state.best_ask.price) == "0.60"
            assert collector.counters.resync_failures == 1

        asyncio.run(exercise())
        collector._raw.close()


def test_transient_empty_rest_response_cannot_overwrite_nonempty_bba_book(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()

    class EmptyRest:
        async def get_order_book(self, token_id: str) -> dict:
            return {
                "asset_id": token_id,
                # Same source time as the contradictory BBA. An older quote
                # must not veto a genuinely newer one-sided full snapshot.
                "timestamp": "1.1",
                "bids": [],
                "asks": [],
            }

    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=EmptyRest())

        async def exercise() -> None:
            await collector._process_event({
                "event_type": "book",
                "asset_id": "up",
                "timestamp": "1.0",
                "bids": [{"price": "0.40", "size": "5"}],
                "asks": [{"price": "0.60", "size": "5"}],
            }, 1_000_000)
            await collector._process_event({
                "event_type": "best_bid_ask",
                "asset_id": "up",
                "timestamp": "1.1",
                "best_bid": "0.40",
                "best_ask": "0.60",
            }, 1_100_000)
            state = collector.states["up"]
            collector._schedule_resync("up", "TEST_EMPTY_REST")
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert state.synchronized
            assert str(state.best_bid.price) == "0.40"
            assert str(state.best_ask.price) == "0.60"
            assert collector.counters.resync_failures == 1

        asyncio.run(exercise())
        collector._raw.close()


def test_older_book_snapshot_cannot_roll_state_backward(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()
    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=FakeRest())

        async def exercise() -> None:
            await collector._process_event({
                "event_type": "book",
                "asset_id": "up",
                "timestamp": "2.0",
                "bids": [{"price": "0.45", "size": "5"}],
                "asks": [{"price": "0.55", "size": "5"}],
            }, 2_000_000)
            await collector._process_event({
                "event_type": "book",
                "asset_id": "up",
                "timestamp": "1.0",
                "bids": [{"price": "0.40", "size": "5"}],
                "asks": [{"price": "0.60", "size": "5"}],
            }, 2_100_000)
            state = collector.states["up"]
            assert str(state.best_bid.price) == "0.45"
            assert str(state.best_ask.price) == "0.55"
            assert state.last_server_ts_us is not None
            assert state.last_server_ts_us > 0

        asyncio.run(exercise())
        collector._raw.close()


def test_repeated_bba_mismatch_storm_never_flaps_ready_book_offline(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    market = market_fixture()

    class SlowRest(FakeRest):
        async def get_order_book(self, token_id: str) -> dict:
            await asyncio.sleep(0.02)
            return await super().get_order_book(token_id)

    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=SlowRest())

        async def exercise() -> None:
            for token_id in ("up", "down"):
                await collector._process_event({
                    "event_type": "book",
                    "asset_id": token_id,
                    "timestamp": "1.0",
                    "bids": [{"price": "0.40", "size": "5"}],
                    "asks": [{"price": "0.60", "size": "5"}],
                }, 1_000_000)
            for i in range(20):
                await collector._process_event({
                    "event_type": "best_bid_ask",
                    "asset_id": "down" if i % 2 else "up",
                    "timestamp": str(1.1 + i / 1_000_000),
                    "best_bid": "0.41",
                    "best_ask": "0.59",
                }, 1_100_000 + i)
                assert collector.states["up"].synchronized
                assert collector.states["down"].synchronized
                assert collector.states["up"].best_bid is not None
                assert collector.states["down"].best_ask is not None
            if collector._resync_tasks:
                await asyncio.gather(*list(collector._resync_tasks.values()))
            assert collector.states["up"].synchronized
            assert collector.states["down"].synchronized

        asyncio.run(exercise())
        collector._raw.close()
