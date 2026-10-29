import asyncio
from decimal import Decimal
from types import SimpleNamespace
from pathlib import Path
from dataclasses import replace
import json

from tests.test_stage3_feed_integrity import feed,state
from tests.test_polymarket_collector import FakeRest
from btc_v3.feeds import polymarket_collector as pc


def test_future_rest_source_does_not_replace_valid_stream_depth(feed,monkeypatch):
    c=feed;c.states['up']=state();before=c.states['up'].bids.copy()
    # Exact source/receive offset reproduced in approved late archive record 5011.
    receive=1792943100255105;source=1792943100284000
    async def get(token):
        p=await FakeRest().get_order_book(token);p['timestamp']=str(source//1000);return p
    c.rest=SimpleNamespace(get_order_book=get)
    monkeypatch.setattr(pc,'now_epoch_us',lambda:receive)
    asyncio.run(c._rest_resync('up','TEST'))
    assert c.states['up'].last_server_ts_us==90_000_000
    assert c.states['up'].bids==before
    assert c.counters.resync_failure_causes.get('REST_FUTURE_SOURCE_TIME')==1


def test_old_bba_cannot_veto_newer_one_sided_authoritative_snapshot(feed,monkeypatch):
    c=feed;s=c.states['up'];s.mark_verified(1_000_000_000,1_000_000_000)
    c._last_bba['up']=(Decimal('.4'),Decimal('.6'))
    async def get(token):
        p=await FakeRest().get_order_book(token);p['timestamp']='2000';p['bids']=[];return p
    c.rest=SimpleNamespace(get_order_book=get);monkeypatch.setattr(pc,'now_epoch_us',lambda:2_000_001_000)
    asyncio.run(c._rest_resync('up','TEST'))
    assert s.depth_reliable and s.best_bid is None
    assert c.counters.resync_successes==1


def test_newer_bba_conflict_still_prevents_empty_rest_recovery(feed,monkeypatch):
    c=feed;s=c.states['up'];s.mark_verified(2_000_002_000,2_000_001_000)
    c._last_bba['up']=(Decimal('.4'),Decimal('.6'))
    async def get(token):
        p=await FakeRest().get_order_book(token);p['timestamp']='2000';p['bids']=[];return p
    c.rest=SimpleNamespace(get_order_book=get);monkeypatch.setattr(pc,'now_epoch_us',lambda:2_000_003_000)
    asyncio.run(c._rest_resync('up','TEST'))
    assert not s.depth_reliable and c.counters.resync_successes==0


def test_clock_inconsistency_is_reported_without_relaxing_execution_clock(feed,monkeypatch):
    c=feed;c.states['up']=state();receive=1792943100292281;source=receive+30000
    monkeypatch.setattr(pc,'now_epoch_us',lambda:receive)
    asyncio.run(c._process_event(dict(event_type='price_change',timestamp=str(source//1000),
        price_changes=[dict(asset_id='up',side='BUY',price='.4',size='9')]),receive))
    assert c.states['up'].age_seconds(receive) is None
    d=c._details()
    assert d['source_ahead_of_receive_events']==1
    assert d['source_ahead_peak_us']>0


def test_observed_rest_ws_clock_cases_keep_stream_watermark(feed,monkeypatch):
    fixture=json.loads((Path(__file__).parent/'fixtures/six_hour_clob_clock.json').read_text())
    assert len(fixture['cases'])==2
    from btc_v3.state.book_state import OrderBookState
    async def scenario(case):
        c=feed;token=case['baseline']['payload']['asset_id']
        c.market=replace(c.market,up_token_id=token)
        c.states={token:OrderBookState(c.market.slug,token,'UP'),'down':OrderBookState(c.market.slug,'down','DOWN')}
        clock=[case['baseline']['received_at_us']];monkeypatch.setattr(pc,'now_epoch_us',lambda:clock[0])
        monkeypatch.setattr(c,'_schedule_resync',lambda *a,**kw:None)
        await c._process_event(case['baseline']['payload'],clock[0])
        previous=c.states[token].last_server_ts_us
        async def get(token):return case['rest']['payload']
        c.rest=SimpleNamespace(get_order_book=get);clock[0]=case['rest']['received_at_us']
        await c._rest_resync(token,'OBSERVED_CASE')
        assert c.states[token].last_server_ts_us==previous
        clock[0]=case['following_delta']['received_at_us']
        await c._process_event(case['following_delta']['payload'],clock[0])
        assert c.states[token].integrity_reason!='OUT_OF_ORDER_DEPTH'
    for case in fixture['cases']:asyncio.run(scenario(case))
