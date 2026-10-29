import asyncio
import pytest
from tests.test_stage3_feed_integrity import feed,state
from btc_v3.feeds import polymarket_collector as pc


@pytest.mark.parametrize('kind',['book','price_change'])
def test_future_depth_cannot_become_valid_when_local_clock_catches_up(feed,monkeypatch,kind):
    c=feed;c.states['up']=state();c.states['up'].slug=c.market.slug
    receive=100_000_000;source=receive+30_000
    monkeypatch.setattr(pc,'now_epoch_us',lambda:receive+100_000)
    monkeypatch.setattr(c,'_schedule_resync',lambda *args,**kw:None)
    event=dict(event_type=kind,timestamp=str(source/1_000_000),asset_id='up',
        bids=[dict(price='.4',size='9')],asks=[dict(price='.6',size='9')],
        price_changes=[dict(asset_id='up',side='BUY',price='.4',size='9')])
    asyncio.run(c._process_event(event,receive))
    assert not c.states['up'].depth_reliable
    assert c.states['up'].last_server_ts_us==90_000_000
    assert c.states['up'].integrity_reason=='SOURCE_TIME_AFTER_RECEIPT'


def test_unknown_source_depth_never_refreshes_a_known_watermark(feed):
    c=feed;c.states['up']=state();c._schedule_resync=lambda *a,**kw:None
    asyncio.run(c._process_event(dict(event_type='book',asset_id='up',bids=[],asks=[]),101_000_000))
    assert not c.states['up'].depth_reliable
    assert c.states['up'].last_server_ts_us==90_000_000


def test_future_tick_change_cannot_modify_execution_constraints(feed,monkeypatch):
    c=feed;c.states['up']=state();original=c.states['up'].tick_size
    monkeypatch.setattr(c,'_schedule_resync',lambda *a,**kw:None)
    asyncio.run(c._process_event(dict(event_type='tick_size_change',asset_id='up',timestamp='101.03',new_tick_size='.001'),101_000_000))
    assert c.states['up'].tick_size==original
    assert not c.states['up'].depth_reliable


def test_future_bba_cannot_poison_latest_hint_or_invalidate_good_depth(feed):
    c=feed;c.states['up']=state()
    asyncio.run(c._process_event(dict(event_type='best_bid_ask',asset_id='up',timestamp='101.03',best_bid='.1',best_ask='.9'),101_000_000))
    assert c.states['up'].depth_reliable and 'up' not in c._last_bba


def test_repeated_fast_failed_integrity_resyncs_are_rate_limited_per_token(feed,monkeypatch):
    calls=[];mono=[10.0]
    monkeypatch.setattr(pc.time,'monotonic',lambda:mono[0])
    async def failed(token,reason):calls.append((token,reason))
    monkeypatch.setattr(feed,'_rest_resync',failed)
    async def exercise():
        for i in range(100):
            feed._schedule_resync('up','SOURCE_TIME_AFTER_RECEIPT')
            await asyncio.gather(*list(feed._resync_tasks.values()))
        assert len(calls)==1
        mono[0]+=1.01
        feed._schedule_resync('up','SOURCE_TIME_AFTER_RECEIPT')
        await asyncio.gather(*list(feed._resync_tasks.values()))
        assert len(calls)==2
        feed._schedule_resync('down','SOURCE_TIME_AFTER_RECEIPT')
        await asyncio.gather(*list(feed._resync_tasks.values()))
        assert len(calls)==3
    asyncio.run(exercise())


def test_malformed_unverifiable_depth_cannot_bypass_gap_quarantine(feed,monkeypatch):
    feed.states['up']=state()
    monkeypatch.setattr(feed,'_schedule_resync',lambda *a,**kw:None)
    asyncio.run(feed._process_event(dict(event_type='price_change'),101_000_000))
    assert not feed.states['up'].depth_reliable
