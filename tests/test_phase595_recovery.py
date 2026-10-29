import asyncio
from types import SimpleNamespace
from decimal import Decimal
from tests.test_stage3_feed_integrity import feed,state
from btc_v3.feeds import polymarket_collector as pc


def test_delta_received_during_rest_cannot_be_overwritten_at_equal_source_time(feed,monkeypatch):
    c=feed;c.states['up']=state();monkeypatch.setattr(pc,'now_epoch_us',lambda:101_000_000)
    async def get(token):
        await c._process_event(dict(event_type='price_change',timestamp='100',price_changes=[dict(asset_id='up',side='BUY',price='.4',size='99')]),101_000_000)
        return dict(asset_id='up',timestamp='100',bids=[dict(price='.4',size='5')],asks=[dict(price='.6',size='5')])
    c.rest=SimpleNamespace(get_order_book=get)
    asyncio.run(c._rest_resync('up','TEST'))
    assert c.states['up'].bids[Decimal('.4')]==99
    assert c.counters.resync_superseded==1
