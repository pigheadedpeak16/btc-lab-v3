"""Foreground, finite public diagnostics. No account, wallet or order adapter.

Only new isolated output roots are allowed. This is not executable replay or a
funded experiment: book/reference prerequisites are measured, not fill success.
"""
import argparse
import asyncio
from collections import deque
import hashlib
import json
from pathlib import Path
import shutil
import time

MIN_FREE_BYTES = 8 * 2**30
from ..feeds.clock_health import RuntimeClockHealth
from ..feeds.coinbase import CoinbaseCollector
from ..feeds.kraken import KrakenCollector
from ..feeds.polymarket import PolymarketPublicRest
from ..feeds.polymarket_collector import FEED_POLICY_REVISION
from ..forensics.latency import OverlapCollectorPool
from ..state.readiness import assess_readiness
from ..storage.db import initialize_project_databases
from ..storage.market_store import MarketStore
from ..storage.external_store import ExternalStore
from ..timeutil import now_epoch_us

PROBE_POLICY_REVISION='stage9_unfunded_startup_diagnostics_v1'


class ObservedExternalStore(ExternalStore):
    def __init__(self,path):
        super().__init__(path);self.observations=deque(maxlen=2000)
    def _observe(self,source,kw):
        self.observations.append(dict(source=source,source_us=kw['source_ts_us'],received_us=kw['received_at_us'],price=kw['price']))
    def persist_coinbase_tick(self,**kw):
        super().persist_coinbase_tick(**kw);self._observe('coinbase',kw)
    def persist_kraken_tick(self,**kw):
        super().persist_kraken_tick(**kw);self._observe('kraken',kw)


async def run_probe(output,*,seconds=420,authorized=False,free_bytes=None):
    if not authorized:return dict(state='DISABLED',funded_account_activated=False)
    if type(seconds) is not int or not 5<=seconds<=900:raise ValueError('PROBE_DURATION_LIMIT')
    output=Path(output)
    if output.exists():raise ValueError('PROBE_OUTPUT_MUST_BE_NEW')
    free_bytes=free_bytes or (lambda:shutil.disk_usage(output.parent).free)
    if free_bytes()<MIN_FREE_BYTES:return dict(state='LOW_DISK_SPACE_GUARD',funded_account_activated=False)
    started=now_epoch_us();mono=time.monotonic();deadline=mono+seconds
    guard=RuntimeClockHealth(utc_us=started,monotonic_seconds=mono)
    output.mkdir(parents=True,exist_ok=False)
    source_root=Path(__file__).resolve().parents[2]
    source_files=sorted(list((source_root/'btc_v3').rglob('*.py'))+[source_root/'pyproject.toml'])
    manifest={p.relative_to(source_root).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    source_sha=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    report=dict(state='RUNNING',policy_revision=PROBE_POLICY_REVISION,feed_policy_revision=FEED_POLICY_REVISION,
        source_sha256=source_sha,probe_entry_us=started,requested_seconds=seconds,funded_account_activated=False,
        orders=0,wallet_authentication=False,official_ptb='NOT_CONNECTED',markets={},discovery_failures=0,
        execution_eligibility='NOT_EVALUATED: no account, signals or strategy evidence',
        limitations=['No pre-fix live control; differences across feeds/markets are not causal performance proof.',
            'Collector construction origin differs from OS process creation; wrapper records process launch separately.'])
    (output/'manifest.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    initialize_project_databases(output)
    with MarketStore.open_project(output) as store,ObservedExternalStore(output/'data/v3_external.sqlite') as external:
        async with PolymarketPublicRest(timeout_seconds=3) as rest:
            pool=OverlapCollectorPool(root=output,rest=rest,store=store,grace_seconds=2)
            cb=CoinbaseCollector(root=output,store=external);kr=KrakenCollector(root=output,store=external)
            tasks=[asyncio.create_task(cb.run()),asyncio.create_task(kr.run())]
            discovery=None;next_discovery=mono;last_slug=None;last_storage=mono
            async def observe():
                nonlocal discovery,next_discovery,last_slug,last_storage
                with (output/'readiness.jsonl').open('x',encoding='utf-8') as journal:
                    while time.monotonic()<deadline:
                        now=now_epoch_us();mnow=time.monotonic();guard.validate(now,mnow)
                        if free_bytes()<MIN_FREE_BYTES:raise ValueError('LOW_DISK_SPACE_GUARD')
                        if mnow-last_storage>=1:
                            used=sum(p.stat().st_size for p in output.rglob('*') if p.is_file())
                            if used>128_000_000:raise ValueError('PROBE_STORAGE_CAP')
                            last_storage=mnow
                        if any(t.done() for t in tasks):raise ValueError('PUBLIC_REFERENCE_TASK_STOPPED')
                        await pool.check_tasks();await pool.prune()
                        if discovery is None and mnow>=next_discovery:
                            discovery=asyncio.create_task(asyncio.wait_for(rest.discover_btc_5m(candidate_offsets=(0,)),timeout=3))
                        if discovery is not None and discovery.done():
                            try:
                                market=discovery.result()
                                if not market.round_start_us<=now<market.round_end_us:raise ValueError('DISCOVERY_NOT_LIVE')
                                if market.slug!=last_slug:
                                    await pool.ensure_current(market);last_slug=market.slug
                                    report['markets'].setdefault(market.slug,dict(discovered_us=now,discovery_latency_ms=(mnow-mono)*1000))
                            except Exception:report['discovery_failures']+=1
                            discovery=None;next_discovery=mnow+2
                        # Discovery/subscription awaits may admit newer feed input.
                        # Use the actual fresh diagnostic cutoff after those awaits.
                        now=now_epoch_us();mnow=time.monotonic()
                        market,collector=pool.current('BTC')
                        view=assess_readiness(market,{} if collector is None else collector.states,at_us=now,ticks=list(external.observations))
                        if collector is not None:
                            telemetry=collector.readiness.observe(market,collector.states,at_us=now)
                            report['markets'][market.slug]['readiness']=telemetry
                        journal.write(json.dumps(dict(at_us=now,elapsed_ms=(mnow-mono)*1000,readiness=view),sort_keys=True)+'\n')
                        journal.flush()
                        await asyncio.sleep(min(.25,max(0,deadline-time.monotonic())))
            try:
                await asyncio.wait_for(observe(),timeout=max(.001,deadline-time.monotonic()))
                report['state']='COMPLETE_BOUNDED_DIAGNOSTICS'
            except asyncio.TimeoutError:report['state']='COMPLETE_BOUNDED_DIAGNOSTICS'
            except Exception as exc:report['state']=str(exc) if isinstance(exc,ValueError) else 'PUBLIC_DIAGNOSTICS_FAILURE'
            finally:
                # Stop admission at the monotonic limit; cleanup emits final status only.
                report['observation_ended_us']=now_epoch_us();report['observed_seconds']=time.monotonic()-mono
                if discovery is not None:discovery.cancel();await asyncio.gather(discovery,return_exceptions=True)
                for task in tasks:task.cancel()
                await asyncio.gather(*tasks,return_exceptions=True)
                await pool.stop()
                report['collector_diagnostics']=pool.completed_diagnostics
                report['reference_counts']={'coinbase':cb.tick_count,'kraken':kr.tick_count}
    report['cleanup_ended_us']=now_epoch_us()
    (output/'report.json').write_text(json.dumps(report,indent=2,sort_keys=True),encoding='utf-8')
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--seconds',type=int,default=420)
    parser.add_argument('--authorized-unfunded',action='store_true')
    args=parser.parse_args()
    result=asyncio.run(run_probe(args.output,seconds=args.seconds,authorized=args.authorized_unfunded))
    print(json.dumps({k:v for k,v in result.items() if k not in {'markets','collector_diagnostics'}},indent=2))
    return 0 if result['state']=='COMPLETE_BOUNDED_DIAGNOSTICS' else 3

if __name__=='__main__':raise SystemExit(main())
