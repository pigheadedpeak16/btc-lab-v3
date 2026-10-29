import asyncio
from types import SimpleNamespace
import pytest

from btc_v3.forensics import forward_lab as f
from btc_v3.storage.db import initialize_project_databases
from tests.test_polymarket_collector import market_fixture


def test_legacy_runner_starts_books_and_references_before_wallet_baseline(tmp_path,monkeypatch):
    from btc_v3.feeds import clock_health
    monkeypatch.setattr(clock_health,'require_host_sync',lambda:{'state':'SERVICE_REPORTS_SYNCHRONIZED'})
    initialize_project_databases(tmp_path);state={'books':False,'refs':0}
    class StartupObserved(Exception):pass
    class Resolver:
        async def __aenter__(self):return self
        async def __aexit__(self,*args):pass
        async def resolve_proxy_wallet(self,*args):return 'PUBLIC_FIXTURE',{}
    class Rest(Resolver):
        async def discover_btc_5m(self,*args,**kw):return market_fixture()
        async def get_clob_market_info(self,*args):return {'fd':{'r':'.07','e':1}}
    class Poller(Resolver):
        async def poll(self,**kw):
            assert state['books'],'Wallet baseline must not block initial CLOB subscription'
            await asyncio.sleep(0)
            assert state['refs']==3,'Independent reference collectors must already be running'
            raise StartupObserved()
    class Ref:
        def __init__(self,**kw):self.stop=asyncio.Event()
        async def run(self):state['refs']+=1;await self.stop.wait()
        async def request_stop(self):self.stop.set()
    class Pool:
        def __init__(self,**kw):self.completed_diagnostics=[]
        async def ensure_current(self,m):state['books']=True
        async def stop(self):pass
    for name,cls in [('PolymarketDataClient',Resolver),('PolymarketPublicRest',Rest),('FastAttributionPoller',Poller),
        ('CoinbaseCollector',Ref),('KrakenCollector',Ref),('AuthenticatedTwapCollector',Ref),('OverlapCollectorPool',Pool)]:
        monkeypatch.setattr(f,name,cls)
    with pytest.raises(StartupObserved):
        asyncio.run(f.run_forward_wallet_lab(tmp_path,requested_wallet='PUBLIC_FIXTURE',seconds=0,
            config=f.ForwardLabConfig(strict_market_minimum=True)))
