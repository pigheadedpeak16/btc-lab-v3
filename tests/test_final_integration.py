import asyncio
from dataclasses import replace

from tests.test_stage7_replay import START




def test_verified_upcoming_subscription_does_not_switch_current(tmp_path,monkeypatch):
    from btc_v3.forensics import latency as m
    from tests.research_fixtures import market
    class Store:
        def persist_market(self,*args):pass
    class Collector:
        def __init__(self,**kw):self.market=kw['market']
        async def run(self):await asyncio.Event().wait()
    monkeypatch.setattr(m,'PolymarketBookCollector',Collector)
    async def scenario():
        pool=m.OverlapCollectorPool(root=tmp_path,rest=None,store=Store(),grace_seconds=2)
        a=market();b=replace(a,slug=f'btc-updown-5m-{a.round_end_us//1_000_000}',
            round_start_us=a.round_end_us,round_end_us=a.round_end_us+300_000_000,up_token_id='new-up',down_token_id='new-down')
        try:
            await pool.ensure_current(a)
            await pool.prepare_upcoming(b,current=a,at_us=a.round_end_us-30_000_000)
            assert pool.current('BTC')[0].slug==a.slug
            assert pool.collector_for_slug(b.slug) is not None
            prepared=pool.collector_for_slug(b.slug)
            await pool.ensure_current(b)
            assert pool.current('BTC')[1] is prepared
            assert pool.slots[a.slug].retire_after_mono is not None
        finally:
            for slot in pool.slots.values():slot.task.cancel()
            await asyncio.gather(*(s.task for s in pool.slots.values()),return_exceptions=True)
    asyncio.run(scenario())


def test_dashboard_keeps_bounded_readiness_without_leaking_arbitrary_details():
    import json
    from btc_v3.forensics.dashboard_v4 import public_feed_details,HTML
    details=public_feed_details(json.dumps(dict(startup_readiness={'process_start_us':1,'latest':{}},secret='MUST_NOT_DISPLAY')))
    assert details['startup_readiness']['process_start_us']==1
    assert 'secret' not in details
    assert 'Current readiness checklist' in HTML


def test_dashboard_reads_clob_status_by_bounded_metadata_table(tmp_path):
    import json
    from btc_v3.storage.db import initialize_project_databases
    from btc_v3.storage.market_store import MarketStore
    from btc_v3.forensics.dashboard_v4 import snapshot
    initialize_project_databases(tmp_path)
    with MarketStore.open_project(tmp_path) as store:
        store.set_status(component='polymarket_clob',session_id='fixture',slug='public',state='SYNCING',
            updated_at_us=START,last_message_at_us=START,last_book_at_us=None,reconnect_count=0,error_count=0,
            details={'startup_readiness':{'process_start_us':START,'latest':{}},'secret':'MUST_NOT_DISPLAY'})
    view=snapshot(tmp_path,now_us=START+1_000_000)
    clob=[r for r in view['feeds'] if r['component']=='polymarket_clob']
    assert len(clob)==1 and clob[0]['observation_state']=='RECENT_OBSERVATION'
    assert 'startup_readiness' in json.loads(clob[0]['details_json'])
    assert 'MUST_NOT_DISPLAY' not in json.dumps(view)
