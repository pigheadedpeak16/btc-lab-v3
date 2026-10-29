"""Regression coverage for Phase 5.9.2b collector stability; paper-only."""
import asyncio
import json
import time

from btc_v3.feeds.polymarket_collector import PolymarketBookCollector
from btc_v3.feeds.kraken import KrakenCollector
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore
from btc_v3.storage.external_store import ExternalStore
from btc_v3.feeds.polymarket import parse_btc_5m_event


class FakeRest:
    async def get_order_book(self, token_id):
        return {
            "asset_id": token_id, "timestamp": "2000",
            "bids": [{"price": "0.40", "size": "5"}],
            "asks": [{"price": "0.60", "size": "6"}],
            "min_order_size": "5", "tick_size": "0.01",
        }


def market_fixture():
    slug = "btc-updown-5m-1791181500"
    event = {
        "id": "e1", "slug": slug,
        "markets": [{
            "id": "m1", "slug": slug, "question": "BTC Up or Down 5m",
            "conditionId": "0x1", "outcomes": ["Up", "Down"],
            "clobTokenIds": ["up", "down"], "active": True,
        }],
    }
    return parse_btc_5m_event(event, slug)


def test_ws_progress_updates_are_throttled_without_losing_message_freshness(tmp_path):
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        market = market_fixture()
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=FakeRest())
        calls = []
        original = store.set_status
        def status(**kw):
            calls.append(kw)
            return original(**kw)
        store.set_status = status
        timestamp = 1_000_000_000
        for i in range(200):
            collector.last_message_at_us = timestamp + i * 500
            collector._publish_progress_if_due(collector.last_message_at_us)
        assert len(calls) == 1
        collector.last_message_at_us = timestamp + 1_100_000
        collector._publish_progress_if_due(collector.last_message_at_us)
        assert len(calls) == 2
        assert calls[-1]['last_message_at_us'] == timestamp + 1_100_000
        collector._raw.close()


def test_soft_bba_resync_storm_is_rate_limited_per_token(tmp_path):
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        market = market_fixture()
        collector = PolymarketBookCollector(root=tmp_path, market=market, store=store, rest=FakeRest())
        async def run():
            collector._schedule_resync('up', 'TOP_OF_BOOK_MISMATCH_SOFT')
            await asyncio.gather(*list(collector._resync_tasks.values()))
            collector._schedule_resync('up', 'TOP_OF_BOOK_MISMATCH_SOFT')
            assert collector.counters.resync_requests == 1
            assert collector.counters.soft_resync_suppressed == 1
            collector._schedule_resync('down', 'TOP_OF_BOOK_MISMATCH_SOFT')
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert collector.counters.resync_requests == 2
            collector._schedule_resync('up', 'DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT')
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert collector.counters.resync_requests == 3
        asyncio.run(run())
        collector._raw.close()


def test_rest_failure_has_reason_diagnostics_and_soft_gap_is_closed(tmp_path):
    initialize_project_databases(tmp_path)
    class FailingRest:
        async def get_order_book(self, token_id):
            raise OSError('simulated network failure')
    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market_fixture(), store=store, rest=FailingRest())
        state = collector.states['up']
        state.apply_snapshot(bids=[{'price':'.4','size':'5'}],asks=[{'price':'.6','size':'6'}],
            received_at_us=1, server_ts_us=1, book_hash='fixture')
        asyncio.run(collector._rest_resync('up', 'TOP_OF_BOOK_MISMATCH_SOFT'))
        assert state.synchronized
        assert collector.counters.resync_failure_causes.get('OSError') == 1
        rows = store.conn.execute('SELECT reason, recovered_at_us FROM feed_gap_events ORDER BY id').fetchall()
        assert rows[0][0] == 'TOP_OF_BOOK_MISMATCH_SOFT'
        assert rows[0][1] is not None  # no phantom permanent book outage
        assert any(r[0] == 'REST_RESYNC_FAILED' for r in rows)
        collector._raw.close()


def test_kraken_feed_reports_trade_quiet_without_inventing_ticks(tmp_path):
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        collector = KrakenCollector(root=tmp_path, store=store)
        collector.last_message_at_us = 20_000_000
        collector.last_tick_at_us = 10_000_000
        details = collector._freshness_details(21_000_000)
        assert details['connection_message_age_seconds'] == 1.0
        assert details['trade_tick_age_seconds'] == 11.0
        assert details['trade_quiet_over_5s'] is True
        assert collector.tick_count == 0
        collector.last_tick_source_ts_us = 8_000_000
        assert collector._freshness_details(21_000_000)['trade_source_age_seconds'] == 13.0
        collector._raw.close()


def test_local_websocket_burst_preserves_every_book_delta(tmp_path):
    """Synthetic real WS transport: burst processing cannot silently lose deltas."""
    import websockets
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        market = market_fixture()

        async def scenario():
            n = 180
            async def serve_stream(ws):
                await ws.recv()  # subscription
                await ws.send(json.dumps({
                    'event_type': 'book', 'asset_id': 'up',
                    'timestamp': '3000',
                    'bids': [{'price': '0.40', 'size': '5'}],
                    'asks': [{'price': '0.60', 'size': '5'}],
                }))
                for idx in range(n):
                    await ws.send(json.dumps({
                        'event_type': 'price_change',
                        'timestamp': str(3100 + idx),
                        'price_changes': [{
                            'asset_id': 'up', 'side': 'BUY',
                            'price': '0.40', 'size': str(10 + idx),
                        }],
                    }))
                try:
                    async for _ in ws:
                        pass
                except websockets.ConnectionClosed:
                    pass

            async with websockets.serve(serve_stream, '127.0.0.1', 0) as server:
                port = server.sockets[0].getsockname()[1]
                collector = PolymarketBookCollector(
                    root=tmp_path, market=market, store=store,
                    rest=FakeRest(), ws_url=f'ws://127.0.0.1:{port}',
                )
                job = asyncio.create_task(collector.run())
                try:
                    deadline = asyncio.get_running_loop().time() + 10
                    while collector.counters.book_deltas != n and asyncio.get_running_loop().time() < deadline:
                        await asyncio.sleep(.02)
                    assert collector.counters.book_deltas == n
                    assert str(collector.states['up'].best_bid.size) == str(10 + n - 1)
                finally:
                    await collector.request_stop()
                    await asyncio.wait_for(job, 5)
                assert store.conn.execute('SELECT COUNT(*) FROM book_deltas').fetchone()[0] == n
                assert collector.counters.raw_messages == n + 1
                assert store.conn.execute('SELECT COUNT(*) FROM collector_sessions WHERE session_id=?',
                                          (collector.session_id,)).fetchone()[0] == 1

        asyncio.run(scenario())
