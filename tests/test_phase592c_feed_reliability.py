"""Failure-oriented Phase 5.9.2c tests: network backpressure and strictly causal price failures."""
import asyncio
import json
import time
from pathlib import Path

import pytest

from btc_v3.feeds.polymarket_collector import PolymarketBookCollector
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore
from btc_v3.features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from btc_v3.storage.external_store import ExternalStore
from tests.test_phase592b_feed_reliability import market_fixture, FakeRest


def test_exponential_soft_rest_failure_cooldown_is_per_token(tmp_path):
    initialize_project_databases(tmp_path)
    class AlwaysStale:
        async def get_order_book(self, token):
            raise RuntimeError("STALE_REST_SNAPSHOT")
    with MarketStore.open_project(tmp_path) as store:
        collector = PolymarketBookCollector(root=tmp_path, market=market_fixture(), store=store, rest=AlwaysStale())
        collector.states['up'].synchronized = True
        async def scenario():
            collector._schedule_resync('up','TOP_OF_BOOK_MISMATCH_SOFT')
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert collector._soft_failure_streak['up'] == 1
            attempts = collector.counters.resync_requests
            collector._last_soft_resync_mono['up'] = 0.0
            collector._schedule_resync('up','TOP_OF_BOOK_MISMATCH_SOFT')
            assert collector.counters.resync_requests == attempts
            collector._schedule_resync('down', 'DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT')
            await asyncio.gather(*list(collector._resync_tasks.values()))
            assert collector.counters.resync_requests == attempts + 1
        asyncio.run(scenario())
        collector._raw.close()


def test_causal_tick_failure_includes_exact_historical_age(tmp_path):
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        store.persist_kraken_tick(source_ts_us=100_000_000, received_at_us=101_000_000,
            receive_sequence=1, price='100000', raw_payload={})
        engine = CausalBtcFeatureEngine(store)
        engine._lookup_stage = 'known_final_twap_window'
        with pytest.raises(FeatureUnavailable, match='STALE_KRAKEN_HISTORY'):
            engine._price_at('kraken', 110_000_000, 115_000_000)
        ctx = engine.last_unavailable_context
        assert ctx['source']=='kraken'
        assert ctx['stage']=='known_final_twap_window'
        assert ctx['age_us']==10_000_000
        assert ctx['allowed_age_us']==5_000_000
        assert ctx['target_ts_us'] <= ctx['decision_ts_us']


def test_background_receiver_handles_slow_sqlite_processing_without_skipping_deltas(tmp_path):
    import websockets
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        original = store.persist_book_delta
        def slow_persist(*args, **kwargs):
            time.sleep(.003)
            return original(*args,**kwargs)
        store.persist_book_delta = slow_persist
        async def scenario():
            count = 220
            async def sender(ws):
                await ws.recv()
                await ws.send(json.dumps({'event_type':'book','asset_id':'up','timestamp':'3000',
                  'bids':[{'price':'0.4','size':'6'}],'asks':[{'price':'0.6','size':'6'}]}))
                for i in range(count):
                    await ws.send(json.dumps({'event_type':'price_change',
                       'timestamp':str(3100+i),'price_changes':[{'asset_id':'up',
                       'side':'BUY','price':'0.4','size':str(i+10)}]}))
                try:
                    async for _ in ws:
                        pass
                except websockets.ConnectionClosed: pass
            async with websockets.serve(sender,'127.0.0.1',0) as server:
                port = server.sockets[0].getsockname()[1]
                collector = PolymarketBookCollector(root=tmp_path,market=market_fixture(),store=store,
                    rest=FakeRest(),ws_url=f'ws://127.0.0.1:{port}')
                job = asyncio.create_task(collector.run())
                try:
                    until=asyncio.get_running_loop().time()+10
                    while collector.counters.book_deltas < count and asyncio.get_running_loop().time() < until:
                        await asyncio.sleep(.04)
                    assert collector.counters.book_deltas == count
                    assert collector.counters.receive_queue_peak > 1
                    assert collector.counters.receive_queue_overflows == 0
                    assert collector.counters.raw_messages == count+1
                    assert store.conn.execute('select count(*) from book_deltas').fetchone()[0]==count
                finally:
                    await collector.request_stop()
                    await asyncio.wait_for(job, 5)
                collector._raw.close()
        asyncio.run(scenario())


def test_reader_overflow_is_explicit_and_fails_closed(tmp_path):
    """Never silently discard frames and continue trading on the old sequence."""
    import websockets
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        async def scenario():
            async def send_burst(ws):
                await ws.recv()
                for i in range(100):
                    await ws.send(json.dumps({'event_type':'price_change', 'timestamp':str(i+3000),
                      'price_changes':[{'asset_id':'up','side':'BUY','price':'0.4','size':str(i+1)}]}))
                try:
                    async for _ in ws: pass
                except websockets.ConnectionClosed: pass
            async with websockets.serve(send_burst,'127.0.0.1',0) as server:
                collector = PolymarketBookCollector(root=tmp_path,market=market_fixture(),store=store,
                    rest=FakeRest(),ws_url=f'ws://127.0.0.1:{server.sockets[0].getsockname()[1]}')
                collector._ws_queue_capacity = 3
                async def doomed():
                    # While the main loop deliberately does not drain, the dedicated
                    # receiver thread will hit the bounded high-water mark.
                    import queue as q
                    import threading
                    buffer = q.Queue(maxsize=3)
                    stop = threading.Event()
                    wake = asyncio.Event()
                    thread = threading.Thread(target=collector._socket_reader,
                        args=(buffer, stop, wake, asyncio.get_running_loop()),daemon=True)
                    thread.start()
                    try:
                        await asyncio.sleep(0.25)
                        assert collector.counters.receive_queue_overflows > 0
                        assert buffer.qsize() == 3
                    finally:
                        stop.set()
                        await asyncio.to_thread(thread.join,4)
                await doomed()
        asyncio.run(scenario())


def test_late_model_history_failure_locates_known_final_twap_sample(tmp_path):
    from btc_v3.models.settlement import TwapSettlementModel, SettlementModelConfig
    from tests.test_phase4_features import seed_store, market
    decision = 1285_000_000
    with seed_store(tmp_path) as store:
        for sec in range(1241,1286):
            store.persist_coinbase_tick(source_ts_us=sec*1_000_000,
                received_at_us=sec*1_000_000+1000, receive_sequence=sec, price='86000', raw_payload={})
            store.persist_settlement_observation(source_ts_us=sec*1_000_000,
                received_at_us=sec*1_000_000+1000, receive_sequence=sec,
                value='86000', source='CHAINLINK_BTC_USD_TWAP_60S',
                stream_id='prices.crypto.twap:btcusd:60s',raw_payload={})
        store.persist_kraken_tick(source_ts_us=1284_000_000,
            received_at_us=1284_000_100,receive_sequence=999,price='86002',raw_payload={})
        engine = CausalBtcFeatureEngine(store)
        f = engine.build(market=market(),decision_time_us=decision)
        assert f.kraken_price == 86002
        with pytest.raises(FeatureUnavailable,match='STALE_KRAKEN_HISTORY'):
            TwapSettlementModel(engine,SettlementModelConfig(paths=100)).evaluate(
                market=market(),decision_time_us=decision,features=f)
        ctx=engine.last_unavailable_context
        assert ctx['stage']=='known_final_twap_window'
        assert ctx['source']=='kraken'
        assert ctx['target_ts_us'] < decision
