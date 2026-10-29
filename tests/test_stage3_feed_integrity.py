"""Stage 3: deterministic feed failures against isolated stores and fake sockets."""
import asyncio
import json
import queue
import threading
import time
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest

from btc_v3.feeds import polymarket_collector as pc
from btc_v3.state.book_state import OrderBookState
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore
from btc_v3.forensics import forward_lab as f
from tests.test_polymarket_collector import FakeRest, market_fixture
from tests.test_stage1_integrity import lab, collector


def state():
    s = OrderBookState('s', 'up', 'UP')
    s.apply_snapshot(bids=[{'price': '.4', 'size': '5'}],
        asks=[{'price': '.6', 'size': '5'}], received_at_us=100_000_000,
        server_ts_us=90_000_000, book_hash='h', min_order_size='5')
    return s


@pytest.fixture
def feed(tmp_path):
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        c = pc.PolymarketBookCollector(root=tmp_path, market=market_fixture(), store=store, rest=FakeRest())
        try:
            yield c
        finally:
            c._raw.close()


def test_bba_watermark_does_not_discard_valid_depth_delta():
    s = state(); s.mark_verified(110_000_000, 105_000_000)
    assert s.apply_price_change(order_side='BUY', price='.4', size='99',
        received_at_us=111_000_000, server_ts_us=100_000_000)
    assert s.bids[Decimal('.4')] == 99


@pytest.mark.parametrize('auxiliary', ['bba', 'tick'])
def test_auxiliary_messages_do_not_refresh_depth_age(auxiliary):
    s = state()
    if auxiliary == 'bba': s.mark_verified(110_000_000, 105_000_000)
    else: s.apply_tick_size('.001', 110_000_000)
    assert s.age_seconds(111_000_000) == 21
    assert s.last_server_ts_us == 90_000_000


def test_duplicate_delta_cannot_keep_stale_depth_fresh():
    s = state()
    kw = dict(order_side='BUY', price='.4', size='9', server_ts_us=100_000_000, change_hash='d')
    s.apply_price_change(**kw, received_at_us=101_000_000)
    assert not s.apply_price_change(**kw, received_at_us=110_000_000)
    assert s.age_seconds(111_000_000) == 11


@pytest.mark.parametrize('bad', [
    [{'price': 'NaN', 'size': '5'}], [{'price': '1.1', 'size': '5'}],
    [{'price': '.7', 'size': '5'}], [{'size': '5'}]])
def test_invalid_snapshot_never_becomes_executable(bad):
    s = state()
    with pytest.raises(ValueError):
        s.apply_snapshot(bids=bad, asks=[{'price': '.6', 'size': '5'}],
            received_at_us=110_000_000, server_ts_us=100_000_000, book_hash='bad')
    assert not s.depth_reliable


def test_future_local_receive_time_is_not_clamped_fresh():
    assert state().age_seconds(99_000_000) is None


def test_bba_mismatch_blocks_depth_until_snapshot(feed):
    c = feed; c.states['up'] = state()
    async def scenario():
        await c._process_event({'event_type':'best_bid_ask', 'asset_id':'up',
            'timestamp':'100', 'best_bid':'.45', 'best_ask':'.6'}, 101_000_000)
        assert c.states['up'].age_seconds(101_000_000) is None
        assert not c.states['up'].depth_reliable
        await asyncio.gather(*list(c._resync_tasks.values()))
    asyncio.run(scenario())


def test_receiver_overflow_invalidates_before_main_loop_drains_error(feed, monkeypatch):
    c = feed; c.states['up'] = state()
    class WS:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def send(self, msg): pass
        def recv(self, timeout): return '{}'
    monkeypatch.setattr(pc, 'ws_sync_connect', lambda *a, **kw: WS())
    q = queue.Queue(maxsize=1); stop = threading.Event()
    loop = SimpleNamespace(call_soon_threadsafe=lambda fn: None)
    t = threading.Thread(target=c._socket_reader,args=(q,stop,SimpleNamespace(set=lambda:None),loop))
    t.start()
    try:
        deadline = time.monotonic()+2
        while c.counters.receive_queue_overflows == 0 and time.monotonic()<deadline: time.sleep(.001)
        assert c.counters.receive_queue_overflows > 0
        assert c.states['up'].age_seconds(100_000_000) is None
    finally:
        stop.set()
        while not q.empty():q.get_nowait()
        t.join(3)


@pytest.mark.parametrize('traffic', ['continuous', 'idle'])
@pytest.mark.parametrize('ack', [True, False])
def test_heartbeats_independent_of_inbound_messages(feed, monkeypatch, traffic, ack):
    c = feed; c.heartbeat_seconds=.1
    stop = threading.Event(); q=queue.Queue(maxsize=1000); clock=[0.0]; messages=[]; pong=[False]
    class WS:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def send(self, msg):
            messages.append(msg)
            if msg=='PING' and ack:pong[0]=True
        def recv(self, timeout):
            clock[0] += .025
            if clock[0] >= 1.0: stop.set()
            if pong[0]:pong[0]=False;return 'PONG'
            if traffic=='idle':raise TimeoutError()
            return '{}'
    monkeypatch.setattr(pc, 'ws_sync_connect',lambda *a, **kw:WS())
    monkeypatch.setattr(pc.time, 'monotonic',lambda:clock[0])
    c._socket_reader(q,stop,SimpleNamespace(set=lambda:None),SimpleNamespace(call_soon_threadsafe=lambda fn:None))
    assert messages.count('PING') >= 2
    errors=[x[1] for x in list(q.queue) if x[0]=='error']
    if not ack:assert any('HEARTBEAT_TIMEOUT' in str(e) for e in errors)
    else:assert not errors


@pytest.mark.parametrize('cause', ['asset', 'timeout', 'rate_limit', 'unavailable'])
def test_rest_resync_failure_is_classified_and_does_not_replace_depth(feed, cause):
    c=feed;c.states['up']=state(); before=dict(c.states['up'].bids)
    async def request(token):
        if cause=='timeout':raise asyncio.TimeoutError()
        if cause in {'rate_limit','unavailable'}:
            status=429 if cause=='rate_limit' else 503
            r=httpx.Response(status,request=httpx.Request('GET','https://fixture.invalid'))
            raise httpx.HTTPStatusError('fixture',request=r.request,response=r)
        return {'asset_id':'WRONG','timestamp':'200','bids':[{'price':'.45','size':'4'}],
                'asks':[{'price':'.6','size':'5'}]}
    c.rest=SimpleNamespace(get_order_book=request)
    asyncio.run(c._rest_resync('up','TEST'))
    labels={'asset':'REST_ASSET_MISMATCH','timeout':'TIMEOUT',
            'rate_limit':'REST_RATE_LIMIT','unavailable':'REST_UNAVAILABLE'}
    assert c.counters.resync_failure_causes.get(labels[cause])==1
    assert c.states['up'].bids==before


def test_rest_retries_obey_cooldown_even_when_book_unsynchronized(feed):
    async def scenario():
        async def fail(token):raise asyncio.TimeoutError()
        feed.rest=SimpleNamespace(get_order_book=fail)
        feed._schedule_resync('up','DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT')
        await asyncio.gather(*list(feed._resync_tasks.values()))
        feed._schedule_resync('up','DELTA_BEFORE_SYNCHRONIZED_SNAPSHOT')
        assert feed.counters.resync_requests==1
        await asyncio.gather(*list(feed._resync_tasks.values()))
    asyncio.run(scenario())


def test_taker_refuses_transport_fault_despite_synchronized_book(lab):
    ms, store, engine=lab;c=collector(1_010_100_000)
    fault=threading.Event();fault.set();c.states['up'].transport_fault=fault
    assert engine._execute_copy(engine.pending_copies[0],collector=c,market_store=ms,
        now_us=1_010_100_000) not in {'FILLED','PARTIAL'}
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0]==0


def test_runtime_clock_jump_latches_and_halts_new_execution(lab):
    from btc_v3.feeds.clock_health import RuntimeClockHealth
    guard=RuntimeClockHealth(utc_us=1_010_000_000,monotonic_seconds=0.0)
    guard.validate(1_011_000_000,1.0)
    with pytest.raises(RuntimeError,match='CLOCK_HEALTH_LOST'):guard.validate(1_020_000_000,2.0)
    with pytest.raises(RuntimeError,match='CLOCK_HEALTH_LOST'):guard.validate(1_012_000_000,2.0)


def test_disconnect_classification_does_not_blame_local_queue_for_1013(feed):
    exc = SimpleNamespace(rcvd=SimpleNamespace(code=1013))
    feed.counters.receive_queue_overflows = 4
    assert feed._disconnect_cause(exc) == ('REMOTE_1013', 1013)
    assert feed._disconnect_cause(RuntimeError('WS_READER_QUEUE_OVERFLOW')) == ('QUEUE_OVERFLOW', None)


def test_gap_closes_only_when_both_full_books_recovered(feed):
    gap=feed.store.record_gap(component=pc.COMPONENT,slug=feed.market.slug,
        detected_at_us=100_000_000,reason='TEST_LOSS',details={})
    feed._open_gap_ids.append(gap);feed._gap_started_us=100_000_000
    feed.states['up']=state()
    feed._recover_if_ready(101_000_000)
    assert feed._open_gap_ids == [gap]
    feed.states['down']=state()
    feed._recover_if_ready(102_000_000)
    assert feed._open_gap_ids == []
    assert feed.counters.gap_duration_us == 2_000_000


@pytest.mark.parametrize('batch_frame',[False,True])
def test_consumer_yields_within_processing_budget_preserving_order(feed, monkeypatch,batch_frame):
    virtual=[0.0];processed=[];batches=[];running=[True]
    def reader(q, stop, wake, loop):
        events=[{'i':i} for i in range(64)]
        if batch_frame:q.put(('message',100_000_000,json.dumps(events)))
        else:
            for event in events:q.put(('message',100_000_000,json.dumps(event)))
        q.put(('end',));loop.call_soon_threadsafe(wake.set)
    async def process(event, received_at_us, raw_ref):
        processed.append(event['i']);virtual[0]+=.003
    async def ticker():
        last=0
        while running[0]:
            await asyncio.sleep(0)
            batches.append(len(processed)-last);last=len(processed)
    monkeypatch.setattr(feed,'_socket_reader',reader)
    monkeypatch.setattr(feed,'_process_event',process)
    monkeypatch.setattr(feed,'_publish_progress_if_due',lambda _:None)
    monkeypatch.setattr(feed._raw,'write',lambda *a,**kw:{})
    monkeypatch.setattr(pc,'now_epoch_us',lambda:100_000_000)
    monkeypatch.setattr(pc.time,'monotonic',lambda:virtual[0])
    async def scenario():
        ticker_task=asyncio.create_task(ticker())
        await asyncio.sleep(0)
        try:
            with pytest.raises(RuntimeError,match='closed unexpectedly'):await feed._run_connection()
        finally:
            running[0]=False;await ticker_task
    asyncio.run(scenario())
    assert processed == list(range(64))
    print('max_contiguous_work_ms=',max(batches)*3)
    assert max(batches) <= 2


def test_session_rejection_reasons_are_counted(lab):
    ms,store,engine=lab;c=collector(1_010_100_000)
    c.states['up'].mark_suspect('LOST_DEPTH')
    reason=engine._execute_copy(engine.pending_copies[0],collector=c,market_store=ms,now_us=1_010_100_000)
    assert store.session_summary(engine.session_id)['rejection_reasons'][reason] == 1


def test_s8_aggressor_cannot_fill_during_feed_loss(lab):
    import inspect
    from tests.test_phase4_features import market
    ms,store,engine=lab;engine.position(f.STRATEGY_8,market())
    created=1_010_000_000
    engine.maker_orders['maker']=f.MakerOrder('maker',market().slug,'up','UP',Decimal('.5'),
        Decimal(5),Decimal(5),Decimal(0),created)
    store.save_order(order_id='maker',session_id=engine.session_id,strategy=f.STRATEGY_8,
        signal_id=None,slug=market().slug,token_id='up',outcome='UP',side='BUY',style='MAKER_GTC_PAPER',
        price_limit='.5',shares='5',created_at_us=created,scheduled_at_us=created,status='OPEN',reason=None,metadata={})
    ms.persist_trade(market(),token_id='up',source_ts_us=created+1,received_at_us=created+2,
        price='.5',size='20',taker_side='SELL',source_trade_id='t',raw_payload={})
    c=collector(created+3);c.states['up'].mark_suspect('LOSS')
    pool=SimpleNamespace(collector_for_slug=lambda slug:c)
    options={'collector_pool':pool} if 'collector_pool' in inspect.signature(engine.process_market_trades).parameters else {}
    assert engine.process_market_trades(ms,now_us=created+3,**options)==0
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0]==0
    assert store.session_summary(engine.session_id)['cancellation_reasons']['MAKER_BOOK_UNRELIABLE']==1


def test_runtime_guard_used_at_execution_rolls_back_pending_fill(lab):
    from btc_v3.feeds.clock_health import RuntimeClockHealth
    ms,store,engine=lab;guard=RuntimeClockHealth(utc_us=1_010_000_000,monotonic_seconds=0)
    engine.execution_clock=lambda:guard.validate(1_019_000_000,1)
    with pytest.raises(RuntimeError,match='CLOCK_HEALTH_LOST'):
        engine._execute_copy(engine.pending_copies[0],collector=collector(1_019_000_000),
            market_store=ms,now_us=1_019_000_000)
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0]==0
    assert f._termination_state(RuntimeError('CLOCK_HEALTH_LOST'))=='GUARD_STOPPED'


def test_nonadjacent_duplicate_delta_does_not_refresh_depth():
    s=state()
    for px,at in [('.4',101_000_000),('.39',102_000_000),('.4',110_000_000)]:
        s.apply_price_change(order_side='BUY',price=px,size='9',received_at_us=at,
            server_ts_us=100_000_000,change_hash='same')
    assert s.age_seconds(111_000_000)==11


def test_duplicate_full_stream_snapshot_does_not_refresh_depth():
    s=state()
    s.apply_snapshot(bids=[{'price':'.4','size':'5'}],asks=[{'price':'.6','size':'5'}],
        received_at_us=110_000_000,server_ts_us=90_000_000,book_hash='h')
    assert s.age_seconds(111_000_000)==21


def test_future_source_time_is_not_executable():
    s=state();s.last_server_ts_us=120_000_000
    assert s.age_seconds(111_000_000) is None


def test_old_bba_does_not_invalidate_newer_depth(feed):
    feed.states['up']=state()
    asyncio.run(feed._process_event({'event_type':'best_bid_ask','asset_id':'up',
        'timestamp':'80','best_bid':'.3','best_ask':'.8'},101_000_000))
    assert feed.states['up'].depth_reliable
    assert feed.counters.resync_requests==0


def test_rest_response_from_previous_connection_cannot_heal_loss(feed):
    async def request(token):
        feed._connection_generation+=1
        return await FakeRest().get_order_book(token)
    feed.rest=SimpleNamespace(get_order_book=request)
    asyncio.run(feed._rest_resync('up','TEST'))
    assert not feed.states['up'].depth_reliable
    assert feed.counters.resync_failure_causes['REST_CONNECTION_SUPERSEDED']==1


def test_failed_rest_is_not_recovery_and_matching_bba_cannot_heal(feed):
    feed.states['up']=state()
    async def fail(token):raise asyncio.TimeoutError()
    feed.rest=SimpleNamespace(get_order_book=fail)
    async def scenario():
        await feed._process_event({'event_type':'best_bid_ask','asset_id':'up',
            'timestamp':'100','best_bid':'.41','best_ask':'.6'},101_000_000)
        await asyncio.gather(*list(feed._resync_tasks.values()))
        await feed._process_event({'event_type':'best_bid_ask','asset_id':'up',
            'timestamp':'101','best_bid':'.4','best_ask':'.6'},102_000_000)
    asyncio.run(scenario())
    assert not feed.states['up'].depth_reliable
    assert feed._open_gap_ids


def test_incomplete_stream_snapshot_invalidates_depth(feed):
    feed.states['up']=state()
    with pytest.raises(ValueError,match='INCOMPLETE_SNAPSHOT'):
        asyncio.run(feed._process_event({'event_type':'book','asset_id':'up','timestamp':'100'},101_000_000))
    assert not feed.states['up'].depth_reliable


def test_rest_retry_after_is_respected(feed,monkeypatch):
    monkeypatch.setattr(pc.time,'monotonic',lambda:10.)
    async def fail(token):
        r=httpx.Response(429,headers={'Retry-After':'60'},request=httpx.Request('GET','https://fixture.invalid'))
        raise httpx.HTTPStatusError('fixture',request=r.request,response=r)
    feed.rest=SimpleNamespace(get_order_book=fail)
    asyncio.run(feed._rest_resync('up','TEST'))
    assert feed._soft_next_allowed_mono['up']>=70.


@pytest.mark.parametrize('age_us',[2_000_001,-1])
def test_queued_stale_or_future_receive_is_not_processed(feed,monkeypatch,age_us):
    processed=[]
    def reader(q,stop,wake,loop):
        q.put(('message',100_000_000-age_us,'{}'));q.put(('end',))
        loop.call_soon_threadsafe(wake.set)
    async def process(*a):processed.append(a)
    monkeypatch.setattr(feed,'_socket_reader',reader);monkeypatch.setattr(feed,'_process_event',process)
    monkeypatch.setattr(pc,'now_epoch_us',lambda:100_000_000)
    with pytest.raises(RuntimeError,match='WS_READER_(BACKLOG_STALE|RECEIVE_IN_FUTURE)'):
        asyncio.run(feed._run_connection())
    assert processed==[]
    assert not any(s.depth_reliable for s in feed.states.values())


def test_tick_ordering_is_independent_and_invalid_tick_fails_closed():
    import inspect
    s=state()
    has_source='server_ts_us' in inspect.signature(s.apply_tick_size).parameters
    s.apply_tick_size('.001',110_000_000,**({'server_ts_us':105_000_000} if has_source else {}))
    s.apply_tick_size('.01',111_000_000,**({'server_ts_us':100_000_000} if has_source else {}))
    assert s.tick_size==Decimal('.001')
    assert s.last_server_ts_us==90_000_000
    with pytest.raises(ValueError):s.apply_tick_size('NaN',112_000_000)
    assert not s.depth_reliable


def test_stale_rest_snapshot_is_classified(feed):
    feed.states['up']=state();feed.states['up'].last_server_ts_us=3_000_000_000
    asyncio.run(feed._rest_resync('up','TEST'))
    assert feed.counters.resync_failure_causes['STALE_REST_SNAPSHOT']==1
    assert feed.states['up'].last_server_ts_us==3_000_000_000


def test_session_diagnostics_include_feed_identity_and_validity(feed):
    feed.forward_session_id='forward-fixture';feed.states['up']=state()
    details=feed._details()
    assert details['forward_session_id']=='forward-fixture'
    assert details['collector_session_id']==feed.session_id
    assert details['tokens']['UP']['depth_integrity']=='VALID'
    assert all(k in details for k in ['disconnect_causes','receive_queue_overflows',
        'receive_processing_peak_ms','receive_backlog_peak_ms','resync_failure_causes',
        'gap_duration_us','open_gap_age_us','heartbeat_pings','heartbeat_pongs'])


def test_reconnect_rebuilds_both_books_after_high_volume_loss(feed,monkeypatch):
    feed.states['up']=state();feed.states['down']=state()
    gap=feed.store.record_gap(component=pc.COMPONENT,slug=feed.market.slug,
        detected_at_us=99_000_000,reason='QUEUE_OVERFLOW',details={})
    feed._open_gap_ids=[gap];feed._gap_started_us=99_000_000
    transitions=[]
    original=feed._process_event
    async def process(event,at,raw_ref):
        await original(event,at,raw_ref)
        transitions.append((event['event_type'],[s.depth_reliable for s in feed.states.values()],list(feed._open_gap_ids)))
    def reader(q,stop,wake,loop):
        q.put(('message',100_000_000,json.dumps({'event_type':'price_change','timestamp':'100',
            'price_changes':[{'asset_id':'up','side':'BUY','price':'.4','size':'99'}]})))
        for token in ('up','down'):
            q.put(('message',100_000_000,json.dumps({'event_type':'book','timestamp':'100','asset_id':token,
                'bids':[{'price':'.4','size':'5'}],'asks':[{'price':'.6','size':'5'}]})))
        for i in range(128):
            q.put(('message',100_000_000,json.dumps({'event_type':'price_change','timestamp':'100',
                'price_changes':[{'asset_id':'up','side':'BUY','price':'.4','size':str(6+i),'hash':str(i)}]})))
        q.put(('end',));loop.call_soon_threadsafe(wake.set)
    monkeypatch.setattr(feed,'_socket_reader',reader);monkeypatch.setattr(feed,'_process_event',process)
    monkeypatch.setattr(pc,'now_epoch_us',lambda:100_000_000)
    with pytest.raises(RuntimeError,match='closed unexpectedly'):asyncio.run(feed._run_connection())
    assert transitions[0][1]==[False,False]
    assert transitions[1][1]==[True,False] and transitions[1][2]==[gap]
    assert transitions[2][1]==[True,True] and transitions[2][2]==[]
    assert feed.counters.book_deltas==128
    assert feed.states['up'].bids[Decimal('.4')]==133
    assert feed.counters.gap_duration_us==1_000_000
    assert not any(s.depth_reliable for s in feed.states.values()) # closed transport


def test_rest_in_flight_cannot_overwrite_new_ws_baseline(feed):
    feed.states['up']=state()
    async def request(token):
        feed.states['up'].apply_snapshot(bids=[{'price':'.45','size':'8'}],
            asks=[{'price':'.55','size':'7'}],received_at_us=101_000_000,
            server_ts_us=100_000_000,book_hash='new-ws')
        return await FakeRest().get_order_book(token)
    feed.rest=SimpleNamespace(get_order_book=request)
    asyncio.run(feed._rest_resync('up','TEST'))
    assert feed.states['up'].best_bid.price==Decimal('.45')
    assert feed.counters.resync_superseded==1


@pytest.mark.parametrize('offset',[-3_000_000,1])
def test_telemetry_cannot_use_stale_or_future_depth_features(tmp_path,offset):
    from btc_v3.forensics import live
    from tests.test_phase4_features import seed_store,market
    cutoff=1_240_004_000;c=collector(cutoff+offset)
    with seed_store(tmp_path) as external:
        sample=live._sample_from_state(session_id='s',wallet='w',market=market(),
            book_collector=c,external_store=external,model_paths=100,decision_time_us=cutoff)
    assert sample.p_up_mc is not None
    assert json.loads(sample.book_json)['book_direction_score'] is None
    assert 'BOOK' in sample.state


def test_tournament_history_cannot_relabel_stale_book_as_fresh(lab,tmp_path):
    from btc_v3.storage.external_store import ExternalStore
    ms,store,engine=lab
    with ExternalStore.open_project(tmp_path) as external:
        tournament=f.FullForwardTournament(session_id=engine.session_id,store=store,
            paper_engine=engine,external_store=external,config=f.ForwardLabConfig())
        tournament.observe_book(collector=collector(1_010_000_000),now_us=1_013_000_000)
        assert not tournament.book_history


def test_depth_receive_and_processing_verification_clocks_are_distinct(feed,monkeypatch):
    monkeypatch.setattr(pc,'now_epoch_us',lambda:110_000_000)
    asyncio.run(feed._process_event({'event_type':'book','asset_id':'up','timestamp':'90',
        'bids':[{'price':'.4','size':'5'}],'asks':[{'price':'.6','size':'5'}]},100_000_000))
    s=feed.states['up']
    assert s.last_received_at_us==100_000_000
    assert s.last_verified_at_us==110_000_000
    assert s.last_server_ts_us==90_000_000
    assert s.age_seconds(111_000_000)==21 # neither delivery nor processing refreshes source time


def test_buffered_delta_superseded_by_full_snapshot_is_not_a_new_gap():
    s=state()
    s.apply_snapshot(bids=[{'price':'.45','size':'7'}],asks=[{'price':'.55','size':'8'}],
        received_at_us=110_000_000,server_ts_us=105_000_000,book_hash='new')
    assert not s.apply_price_change(order_side='BUY',price='.4',size='8',
        received_at_us=109_000_000,server_ts_us=100_000_000)
    assert s.depth_reliable and s.age_seconds(111_000_000)==6


def test_reconnect_does_not_spawn_second_reader_while_old_reader_is_alive(feed,monkeypatch):
    created=[]
    class StuckReader:
        def __init__(self,**kw):self.args=kw['args'];created.append(self)
        def start(self):
            q,stop,wake,loop=self.args;q.put(('end',));wake.set()
        def join(self,timeout):pass
        def is_alive(self):return True
    monkeypatch.setattr(pc,'threading',SimpleNamespace(Thread=StuckReader,Event=threading.Event))
    async def scenario():
        with pytest.raises(RuntimeError,match='closed unexpectedly'):await feed._run_connection()
        with pytest.raises(RuntimeError,match='WS_PREVIOUS_READER_STILL_ACTIVE'):await feed._run_connection()
    asyncio.run(scenario())
    assert len(created)==1


@pytest.mark.parametrize('changes',[None,'not-a-list',[None]])
def test_malformed_depth_event_cannot_leave_old_book_eligible(feed,changes):
    feed.states['up']=state()
    with pytest.raises(ValueError,match='INVALID_DEPTH_EVENT'):
        asyncio.run(feed._process_event({'event_type':'price_change','timestamp':'100',
            'price_changes':changes},101_000_000))
    assert not feed.states['up'].depth_reliable
