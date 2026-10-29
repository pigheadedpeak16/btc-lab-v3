import asyncio
import importlib
from datetime import datetime,timezone

import httpx
import pytest

from tests.test_stage7_replay import START,END
from tests.research_fixtures import market


def api():
    assert importlib.util.find_spec('btc_v3.operations.public_feed'), 'Missing bounded public feed adapter'
    return importlib.import_module('btc_v3.operations.public_feed')


class Rest:
    async def discover_btc_5m(self,**kwargs):return market()
    async def get_clob_market_info(self,condition):return {'fd':{'r':'.07','e':1}}
    async def get_order_book(self,token):return dict(asset_id=token,timestamp=str((START+1_000_000)//1000),
        bids=[{'price':'.49','size':'100'}],asks=[{'price':'.50','size':'10'}],tick_size='.01',min_order_size='5')


def handler(request):
    assert request.method=='GET'
    assert 'authorization' not in request.headers
    if request.url.host=='api.exchange.coinbase.com':return httpx.Response(200,json={'price':'100','time':datetime.fromtimestamp((START+1_000_000)/1e6,timezone.utc).isoformat()})
    if request.url.host=='api.kraken.com':return httpx.Response(200,json={'error':[],'result':{'XXBTZUSD':[['100','1',(START+1_000_000)/1e6]],'last':'n'}})
    return httpx.Response(200,json={'data':[dict(event_slug=market().slug,token_id='up',outcome='UP',side='BUY',price='.65',size='10',
        timestamp=START//1_000_000,transaction_hash='tx')]})


def test_public_snapshots_capture_source_and_actual_wallet_response_clock():
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            feed=api().PublicSnapshotFeed(rest=Rest(),client=client,watch_wallet='0x'+'1'*40,clock_us=lambda:START+1_100_000)
            rows=await feed.poll()
            assert {'market','fee','book','tick','wallet'}<=set(r['kind'] for r in rows)
            signal=next(r for r in rows if r['kind']=='wallet')
            assert signal['received_at_us']==START+1_100_000 and signal['source_ts_us']==START
            assert signal['data']['economic_key']
    asyncio.run(run())


def test_expired_discovery_never_fetches_wallet_or_books():
    async def run():
        calls=[]
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:calls.append(r))) as client:
            feed=api().PublicSnapshotFeed(rest=Rest(),client=client,watch_wallet='0x'+'1'*40,clock_us=lambda:END+1)
            rows=await feed.poll()
            assert not calls and all(r['kind'] not in {'book','wallet'} for r in rows)
    asyncio.run(run())


def test_response_limits_and_network_faults_produce_gap_no_orders():
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r:httpx.Response(200,content=b'x'*1_000_001))) as client:
            feed=api().PublicSnapshotFeed(rest=Rest(),client=client,watch_wallet='0x'+'1'*40,clock_us=lambda:START+1_100_000)
            rows=await feed.poll()
            assert rows[-1]['kind']=='gap' and not any(r['kind']=='wallet' for r in rows)
    asyncio.run(run())


def test_public_feed_refuses_authenticated_client_without_reading_values():
    async def run():
        async with httpx.AsyncClient(headers={'Authorization':'fixture'}) as client:
            with pytest.raises(ValueError,match='AUTHENTICATED'):api().PublicSnapshotFeed(rest=Rest(),client=client,watch_wallet='0x'+'1'*40)
    asyncio.run(run())
