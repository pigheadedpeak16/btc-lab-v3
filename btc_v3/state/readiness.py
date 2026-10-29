"""Read-only staged diagnostics. Readiness never authorizes an order or changes clocks."""
from collections import Counter
from decimal import Decimal,InvalidOperation
import time

READINESS_POLICY_REVISION='stage9_layered_readiness_v1'


def reference_readiness(ticks,*,at_us,required_sources=('coinbase','kraken')):
    if len(ticks)>2000:raise ValueError('REFERENCE_HISTORY_LIMIT')
    sources={}
    for source in required_sources:
        supplied=[r for r in ticks if r.get('source')==source];rows=[];excluded=0
        for row in supplied:
            src,recv=row.get('source_us'),row.get('received_us')
            if (type(src) is not int or type(recv) is not int or not 0<=src<=recv<=at_us):excluded+=1;continue
            try:price=Decimal(str(row['price']))
            except (InvalidOperation,KeyError):excluded+=1;continue
            if not price.is_finite() or price<=0:excluded+=1;continue
            if at_us-src<=60_000_000:rows.append(row)
        rows.sort(key=lambda r:(r['source_us'],r['received_us']))
        latest=rows[-1] if rows else None
        state='MISSING_OBSERVATIONS'
        if latest:
            if at_us-latest['source_us']>2_000_000:state='SOURCE_STALE'
            elif len(rows)<2 or latest['source_us']-rows[0]['source_us']<1_000_000:state='WARMING_HISTORY'
            else:state='READY'
        sources[source]=dict(state=state,observations=len(rows),distinct_source_times=len({r['source_us'] for r in rows}),
            excluded_noncausal_or_invalid=excluded,source_age_seconds=None if latest is None else (at_us-latest['source_us'])/1e6,
            receive_age_seconds=None if latest is None else (at_us-latest['received_us'])/1e6,
            history_span_seconds=0 if not rows else (rows[-1]['source_us']-rows[0]['source_us'])/1e6)
    return dict(ready=bool(sources) and all(r['state']=='READY' for r in sources.values()),sources=sources,
        scope='60-second causal regime features; not the Stage 6 official TWAP/model history')


def book_readiness(market,state,outcome,at_us):
    result=dict(state='BOOK_UNAVAILABLE',reason='SNAPSHOT_MISSING',synchronized=False,two_sided=False,constraints_known=False,
        source_age_seconds=None,receive_age_seconds=None)
    if state is None:return result
    if state.slug!=market.slug or state.token_id not in market.token_ids or market.outcome_for_token(state.token_id)!=outcome:
        return {**result,'reason':'MARKET_TOKEN_IDENTITY_MISMATCH'}
    if not state.depth_reliable:return {**result,'reason':'DEPTH_UNSYNCHRONIZED','integrity_reason':state.integrity_reason}
    if state.last_server_ts_us is None:return {**result,'reason':'SOURCE_TIME_MISSING'}
    if state.last_received_at_us is None or not state.last_server_ts_us<=state.last_received_at_us<=at_us:
        return {**result,'reason':'CLOCK_INVALID'}
    age=state.age_seconds(at_us)
    result.update(synchronized=True,source_age_seconds=(at_us-state.last_server_ts_us)/1e6,
        receive_age_seconds=(at_us-state.last_received_at_us)/1e6)
    if age is None or not 0<=age<=2:return {**result,'reason':'DEPTH_STALE'}
    result['constraints_known']=bool(state.tick_size is not None and state.tick_size.is_finite() and state.tick_size>0
        and state.min_order_size is not None and state.min_order_size.is_finite() and state.min_order_size>0)
    result['two_sided']=state.best_bid is not None and state.best_ask is not None
    return {**result,'state':'BOOK_READY' if result['two_sided'] else 'SYNCED_ONE_SIDED',
        'reason':'READY' if result['two_sided'] else 'MISSING_BIDS' if state.best_bid is None else 'MISSING_ASKS'}


def assess_readiness(market,states,*,at_us,ticks=(),strategy=None):
    if market is None:return dict(policy_revision=READINESS_POLICY_REVISION,discovery={'state':'MARKET_UNAVAILABLE'},execution_book_inputs_ready=False)
    up=book_readiness(market,states.get(market.up_token_id),'UP',at_us)
    down=book_readiness(market,states.get(market.down_token_id),'DOWN',at_us)
    live=market.round_start_us<=at_us<market.round_end_us
    model_required=strategy is not None and strategy not in {'S9_BOOK_CONFIRM_75','S9_PRICE60_80'}
    return dict(policy_revision=READINESS_POLICY_REVISION,discovery={'state':'LIVE_MARKET' if live else 'UPCOMING' if at_us<market.round_start_us else 'MARKET_EXPIRED'},
        up_book=up,down_book=down,btc_reference=reference_readiness(ticks,at_us=at_us),
        official_reference={'state':'UNAVAILABLE','required_by_signal':model_required,'reason':'No verified official PTB supplied to diagnostic'},
        model={'state':'NOT_REQUIRED_BY_SIGNAL' if not model_required else 'NOT_EVALUATED','calibration':'NOT_ASSUMED'},
        strategy={'state':'NOT_EVALUATED','id':strategy,'reason':'Signal/evidence/risk eligibility is separate from book synchronization'},
        execution={'state':'NOT_EVALUATED','reason':'Final sizing, fees, risk, fresh-clock and strategy guards required'},
        execution_book_inputs_ready=live and all(r['state']=='BOOK_READY' and r['constraints_known'] for r in (up,down)))


class ReadinessTimeline:
    def __init__(self,*,started_us,started_mono=None,monotonic=time.monotonic):
        self.started_us=started_us;self.monotonic=monotonic
        self.started_mono=monotonic() if started_mono is None else started_mono
        self.first={};self.reasons=Counter();self.latest=None

    def observe(self,market,states,*,at_us):
        view=assess_readiness(market,states,at_us=at_us);self.latest=view
        self.first.setdefault('market_discovered_us',at_us)
        for outcome,token in [('up',market.up_token_id),('down',market.down_token_id)]:
            state=states.get(token);key='first_'+outcome+'_snapshot'
            if state is not None and state.depth_reliable and key not in self.first:
                self.first[key]=dict(source_us=state.last_server_ts_us,received_us=state.last_received_at_us,processed_us=at_us)
            reason=view[outcome+'_book']['reason']
            if reason!='READY':self.reasons[outcome+':'+reason]+=1
        if all(view[k]['state'] in {'BOOK_READY','SYNCED_ONE_SIDED'} for k in ('up_book','down_book')):
            if 'first_paired_book_us' not in self.first:
                self.first['first_paired_book_us']=at_us
                self.first['first_paired_book_latency_ms']=max(0,(self.monotonic()-self.started_mono)*1000)
        if view['execution_book_inputs_ready']:
            self.first.setdefault('first_execution_book_inputs_us',at_us)
        return self.snapshot()

    def snapshot(self):
        return dict(process_start_us=self.started_us,**self.first,latest=self.latest,rejection_checks=dict(self.reasons),
            scope='Measured book prerequisites; not funded strategy/account authorization')
