import asyncio
from decimal import Decimal

import pytest

from tests.test_stage3_feed_integrity import feed
from tests.test_polymarket_collector import FakeRest
from tests.test_polymarket_collector import market_fixture


def test_websocket_subscription_not_blocked_by_initial_rest_prime(feed,monkeypatch):
    c=feed
    async def scenario():
        release=asyncio.Event();connected=asyncio.Event()
        async def prime():await release.wait()
        async def connection():connected.set();c._stop_requested.set()
        monkeypatch.setattr(c,'_prime_rest_snapshots',prime)
        monkeypatch.setattr(c,'_run_connection',connection)
        task=asyncio.create_task(c.run())
        try:
            for _ in range(8):await asyncio.sleep(0)
            assert connected.is_set(),'Valid WS startup must not wait for two REST requests'
        finally:
            release.set();await asyncio.wait_for(task,1)
    asyncio.run(scenario())


def test_independent_constraint_requests_start_in_parallel_without_changing_depth(feed):
    c=feed
    assert hasattr(c,'_prime_rest_constraints'),'Missing metadata-only parallel startup'
    async def scenario():
        up_wait=asyncio.Event();down_started=asyncio.Event();calls=[]
        class Rest(FakeRest):
            async def get_order_book(self,token):
                calls.append(token)
                if token=='up':await up_wait.wait()
                else:down_started.set()
                return await super().get_order_book(token)
        c.rest=Rest();task=asyncio.create_task(c._prime_rest_constraints())
        try:
            for _ in range(8):await asyncio.sleep(0)
            assert down_started.is_set()
            assert not c.states['up'].synchronized and not c.states['down'].synchronized
        finally:up_wait.set();await task
        assert c.states['up'].min_order_size==Decimal(5)
        assert not c.states['up'].synchronized and c.states['up'].last_received_at_us is None
    asyncio.run(scenario())


def test_delayed_metadata_does_not_overwrite_newer_stream_tick(feed):
    c=feed
    assert hasattr(c,'_prime_rest_constraints'),'Missing race-safe metadata startup'
    async def scenario():
        class Rest(FakeRest):
            async def get_order_book(self,token):
                c.states[token].tick_size=Decimal('.001')
                return await super().get_order_book(token)
        c.rest=Rest();await c._prime_rest_constraints()
        assert c.states['up'].tick_size==Decimal('.001')
    asyncio.run(scenario())


@pytest.mark.parametrize('invalid',['NaN','Infinity','-Infinity'])
def test_nonfinite_startup_constraints_reject_without_unhandled_task_failure(feed,invalid):
    c=feed
    class Rest(FakeRest):
        async def get_order_book(self,token):
            payload=await super().get_order_book(token)
            payload['tick_size']=invalid
            return payload
    c.rest=Rest();asyncio.run(c._prime_rest_constraints())
    assert c._startup_constraints_errors=={'UP':'STARTUP_CONSTRAINTS_UNAVAILABLE','DOWN':'STARTUP_CONSTRAINTS_UNAVAILABLE'}
    assert all(s.tick_size is None for s in c.states.values())


def test_live_discovery_does_not_wait_for_irrelevant_future_candidates():
    from btc_v3.feeds.polymarket import PolymarketPublicRest
    from datetime import datetime,timezone
    m=market_fixture();calls=[]
    class Rest(PolymarketPublicRest):
        async def get_event_by_slug(self,slug):
            calls.append(slug)
            assert slug==m.slug,'Live discovery must return before future requests'
            return dict(id='e1',slug=slug,title='BTC Up or Down 5m',markets=[dict(id=m.market_id,slug=slug,conditionId=m.condition_id,
                outcomes=['Up','Down'],clobTokenIds=list(m.token_ids),active=True)])
    rest=Rest(client=object())
    found=asyncio.run(rest.discover_btc_5m(now=datetime.fromtimestamp((m.round_start_us+1000000)/1e6,timezone.utc)))
    assert found.slug==m.slug and len(calls)==1


def test_collector_publishes_separate_first_up_and_down_readiness(feed):
    c=feed
    async def scenario():
        await c._process_event(dict(event_type='book',asset_id='up',timestamp='1000',hash='first',
            bids=[{'price':'.4','size':'5'}],asks=[{'price':'.6','size':'5'}]),2_000_000_000)
    asyncio.run(scenario())
    details=c._details()
    assert 'startup_readiness' in details
    assert details['startup_readiness']['first_up_snapshot']['received_us']==2_000_000_000
    assert 'first_down_snapshot' not in details['startup_readiness']
