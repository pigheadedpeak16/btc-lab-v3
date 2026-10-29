"""Offline replay reuses frozen shadow execution without a funded account.

Timers run before the next external event; ties process the supplied capture event
sequence before timers. Restart is bounded deterministic recomputation, not resume
of an external trading process. Scratch stores are small and retained for audit.
"""
from dataclasses import asdict
from decimal import Decimal
from itertools import groupby
from pathlib import Path
from types import SimpleNamespace
import json
import tempfile

from .contracts import ReplayManifest, validate_events, fingerprint, plain, REPLAY_POLICY_REVISION
from ..feeds.polymarket import DiscoveredMarket
from ..feeds.official_resolution import validate_resolution
from ..state.book_state import OrderBookState
from ..storage.db import initialize_project_databases
from ..storage.market_store import MarketStore
from ..storage.external_store import ExternalStore
from ..forensics.forward_store import ForwardWalletStore
from ..forensics.models import WalletTrade
from ..forensics import forward_lab as f


def records(conn,sql,args=()):
    cur=conn.execute(sql,args);return [dict(zip([c[0] for c in cur.description],row)) for row in cur.fetchall()]


def replay(manifest,rows,*,checkpoint=None,strategy_definition=None):
    m=ReplayManifest.from_dict(asdict(manifest) if isinstance(manifest,ReplayManifest) else manifest)
    events=validate_events(m,rows)
    configured=m.strategy_id=='CONFIGURED_RESEARCH'
    config=f.ForwardLabConfig(strict_market_minimum=True,enable_full_tournament=configured)
    if configured:
        from ..strategies.lab_profiles import verified_definition
        profile=verified_definition(strategy_definition)
        config=f.ForwardLabConfig(strict_market_minimum=True,enable_full_tournament=True,
            model_paths=profile.common.monte_carlo_paths,max_book_age_seconds=profile.common.max_quote_age_seconds,
            tournament_execution_delay_ms=int(profile.common.execution_delay_seconds*1000))
    policies=dict(replay=REPLAY_POLICY_REVISION,replay_execution='research_shadow_replay_v3',execution=f.EXECUTION_POLICY_REVISION,
        execution_admission=f.EXECUTION_ADMISSION_REVISION,
        accounting=f.ACCOUNTING_POLICY_REVISION,feed=f.FEED_POLICY_REVISION,model=f.MODEL_POLICY_REVISION,
        settlement=f.RESOLUTION_POLICY_REVISION,config_hash=f.phase591_config_hash(config))
    if configured:policies.update(strategy_config_sha256=strategy_definition['config_sha256'],strategy_definition_sha256=fingerprint(strategy_definition),
        tournament_execution='phase595_configured_shadow_v1')
    cursor=dict(manifest_sha256=m.fingerprint,events=len(events),prefix_sha256=fingerprint([e.to_dict() for e in events]),
        runtime_policy_sha256=fingerprint(policies))
    if checkpoint is not None and checkpoint!=cursor:raise ValueError('REPLAY_CHECKPOINT_MISMATCH')
    scratch=Path(tempfile.mkdtemp(prefix='btc-replay-'))
    initialize_project_databases(scratch)
    clock=[m.start_us];collectors={};fees=set();seen=set();baselines=set();notes=[];unsupported=[]
    if not events:unsupported.append('EMPTY_EVENT_STREAM')
    if m.input_completeness=='INCOMPLETE_CAPTURE':unsupported.append('INCOMPLETE_CAPTURE')
    with MarketStore.open_project(scratch) as markets,ForwardWalletStore.open_project(scratch) as store,ExternalStore.open_project(scratch) as external:
        session='replay_'+m.fingerprint[:20]
        store.start_session(session_id=session,wallet='OFFLINE_PAPER',started_at_us=m.start_us,config=asdict(m))
        engine=f.ForwardPaperEngine(session_id=session,config=config,store=store,execution_clock=lambda:clock[0])
        pool=SimpleNamespace(collector_for_slug=lambda slug:collectors.get(slug))
        tournament=None
        if configured:
            from ..strategies.configured_research import ConfiguredResearch
            tournament=ConfiguredResearch(engine=engine,external_store=external,definition=strategy_definition)
        def reject(e,reason):
            notes.append(dict(event_id=e.event_id,at_us=clock[0],reason=reason,slug=e.slug))

        def timers(limit,inclusive=True):
            while True:
                times=[p.scheduled_at_us for p in engine.pending_copies]
                if tournament is not None:
                    nxt=tournament.next_timer(collectors,clock[0])
                    if nxt is not None:times.append(nxt)
                if not times:break
                due=min(times)
                if due>limit or (due==limit and not inclusive):break
                clock[0]=max(clock[0],due)
                retained=[]
                for p in engine.pending_copies:
                    if (p.scheduled_at_us<=clock[0] and (p.confirmed_at_us is not None or not p.require_book_confirmation)
                        and p.slug not in fees):
                        store.update_order_status(p.order_id,status='REJECTED',reason='FEE_UNAVAILABLE')
                        notes.append(dict(event_id=p.signal_id,at_us=clock[0],reason='FEE_UNAVAILABLE',slug=p.slug))
                        unsupported.append('FEE_UNAVAILABLE')
                    else:retained.append(p)
                engine.pending_copies=retained
                engine.execute_due_copies(collector_pool=pool,market_store=markets,now_us=clock[0])
                if tournament is not None:tournament.advance(collectors=collectors,pool=pool,market_store=markets,at_us=clock[0])

        for at,group in groupby(events,key=lambda e:e.processed_at_us):
            timers(at,inclusive=False);clock[0]=at
            for e in group:
                data=plain(e.data);c=collectors.get(e.slug)
                if e.kind=='market':
                    start,end=int(data['round_start_us']),int(data['round_end_us'])
                    if (end-start!=300_000_000 or start%300_000_000 or e.slug!=f'btc-updown-5m-{start//1_000_000}'
                        or data['up_token_id']==data['down_token_id']):raise ValueError('REPLAY_MARKET_IDENTITY')
                    market=DiscoveredMarket(slug=e.slug,event_id=None,market_id=str(data['market_id']),condition_id=str(data['condition_id']),
                        question='BTC offline replay',rules_text=None,round_start_us=start,round_end_us=end,
                        up_token_id=str(data['up_token_id']),down_token_id=str(data['down_token_id']),active=True,closed=False,raw_event={},raw_market={})
                    if c is not None:
                        if c.market!=market:raise ValueError('REPLAY_MARKET_CONFLICT')
                        continue
                    markets.persist_market(market,at)
                    states={token:OrderBookState(slug=e.slug,token_id=token,outcome=outcome) for outcome,token in [('UP',market.up_token_id),('DOWN',market.down_token_id)]}
                    collectors[e.slug]=SimpleNamespace(market=market,states=states)
                elif c is None:
                    reject(e,'MARKET_IDENTITY_UNAVAILABLE');unsupported.append('MARKET_IDENTITY_UNAVAILABLE')
                elif e.kind=='fee':
                    rate=Decimal(str(data['rate']));exponent=data['exponent']
                    if not rate.is_finite() or rate<0 or type(exponent) is not int or exponent<0:raise ValueError('REPLAY_INVALID_FEE')
                    engine.set_market_fee(slug=e.slug,rate=rate,exponent=exponent,source='CAUSAL_REPLAY_OBSERVATION');fees.add(e.slug)
                elif e.kind in {'book','delta'}:
                    state=c.states.get(str(data['token_id']))
                    if state is None:reject(e,'BOOK_MARKET_MISMATCH');unsupported.append('BOOK_MARKET_MISMATCH');continue
                    if e.source_ts_us is None:state.mark_unsynchronized();reject(e,'BOOK_SOURCE_TIME_UNAVAILABLE');continue
                    if e.kind=='book' and state.last_server_ts_us is not None and e.source_ts_us<state.last_server_ts_us:
                        state.mark_suspect('OUT_OF_ORDER_SNAPSHOT');reject(e,'OUT_OF_ORDER_SNAPSHOT');continue
                    try:
                        if e.kind=='book':
                            state.apply_snapshot(bids=data['bids'],asks=data['asks'],received_at_us=e.received_at_us,
                                server_ts_us=e.source_ts_us,book_hash=data.get('book_hash'),tick_size=data.get('tick_size'),min_order_size=data.get('min_order_size'))
                            baselines.add((e.slug,str(data['token_id'])))
                        else:state.apply_price_change(order_side=data['side'],price=data['price'],size=data['size'],
                            received_at_us=e.received_at_us,server_ts_us=e.source_ts_us,change_hash=data.get('change_hash'))
                    except ValueError as exc:state.mark_suspect('INVALID_REPLAY_DEPTH');reject(e,str(exc))
                elif e.kind=='gap':
                    for state in c.states.values():state.mark_unsynchronized()
                    reject(e,'FEED_INVALIDATED:'+str(data.get('reason','UNKNOWN')))
                elif e.kind=='wallet':
                    key=data.get('economic_key')
                    if not key:reject(e,'WALLET_ECONOMIC_ID_UNAVAILABLE');unsupported.append('WALLET_ECONOMIC_ID_UNAVAILABLE');continue
                    if key in seen:reject(e,'DUPLICATE_WALLET_OBSERVATION');continue
                    seen.add(key)
                    if any((e.slug,t) not in baselines for t in c.market.token_ids):unsupported.append('BOOK_BASELINE_MISSING')
                    if (data.get('token_id') not in c.market.token_ids
                        or c.market.outcome_for_token(data.get('token_id'))!=data.get('outcome') or data.get('side')!='BUY'):
                        reject(e,'SIGNAL_MARKET_MISMATCH');continue
                    if not c.market.round_start_us<=at<c.market.round_end_us:reject(e,'MARKET_EXPIRED');continue
                    row=WalletTrade.from_api('OFFLINE_PAPER',dict(event_slug=e.slug,token_id=data['token_id'],outcome=data['outcome'],side='BUY',
                        price=data['price'],size=data['shares'],timestamp=None if e.source_ts_us is None else e.source_ts_us//1_000_000,transaction_hash=key))
                    if m.strategy_id=='S9_PRICE60_80' and not Decimal('.60')<=row.price<Decimal('.80'):reject(e,'OUTSIDE_PRICE_BAND');continue
                    store.save_signal(signal_id=e.event_id,session_id=session,economic_key=key,source='OFFLINE_REPLAY',
                        source_first_seen_us=e.received_at_us,trade_ts_us=e.source_ts_us,slug=e.slug,token_id=row.token_id,side='BUY',
                        outcome=row.outcome,target_price=str(row.price),target_size=str(row.size),transaction_hash=key,
                        publication_lag_lower_us=None,publication_lag_upper_us=None,detection_sample_id=None,raw_json=row.raw_json)
                    strategies=[] if configured else [m.strategy_id]
                    for strategy in strategies:
                        if strategy=='S9_PRICE60_80' and not Decimal('.60')<=row.price<Decimal('.80'):continue
                        engine.schedule_copy(strategy=strategy,signal_id=e.event_id,row=row,first_seen_us=e.received_at_us,lower_lag_us=None,
                            delay_ms=75 if strategy=='S9_BOOK_CONFIRM_75' else 25,require_book_confirmation=strategy=='S9_BOOK_CONFIRM_75')
                elif e.kind=='resolution':
                    result=validate_resolution(data,c.market,observed_at_us=e.received_at_us)
                    for (strategy,slug),pos in engine.positions.items():
                        if slug==e.slug and pos.pnl is None:
                            pos.settlement_evidence=dict(kind='CALLER_SUPPLIED_VALIDATED_BINARY_RESOLUTION',
                                evidence_ref=result.evidence_ref,observed_at_us=e.received_at_us,exchange_payout='NOT_APPLICABLE_SHADOW')
                            pos.settle(resolved_side=result.outcome,at_us=at);engine._persist_position(strategy,pos)
                elif e.kind=='payout':
                    reject(e,'PAYOUT_NOT_APPLICABLE_TO_SHADOW_RESEARCH')
                elif e.kind=='tick':
                    source=data['source']
                    if source not in {'coinbase','kraken'}:raise ValueError('REPLAY_EXTERNAL_SOURCE')
                    getattr(external,'persist_'+source+'_tick')(source_ts_us=e.source_ts_us,received_at_us=e.received_at_us,
                        receive_sequence=e.sequence,price=data['price'],raw_payload={})
                elif e.kind=='oracle':
                    from ..feeds.settlement_reference import PriceReferenceMapper,SettlementObservation,OFFICIAL_SOURCE,POLYMARKET_STREAM
                    PriceReferenceMapper(external).ingest_observation(c.market,SettlementObservation(e.source_ts_us,e.received_at_us,
                        data['value'],OFFICIAL_SOURCE,POLYMARKET_STREAM,{}),e.sequence)
                elif e.kind=='prediction':
                    reject(e,'PREDICTION_NOT_REQUIRED_BY_FROZEN_BENCHMARK')
            if tournament is not None:tournament.advance(collectors=collectors,pool=pool,market_store=markets,at_us=at)
            timers(at)
        timers(m.end_us);clock[0]=m.end_us
        research_coverage=[]
        if tournament is not None:
            if store.conn.execute("SELECT 1 FROM wallet_forward_orders WHERE reason='FEE_UNAVAILABLE' LIMIT 1").fetchone():unsupported.append('FEE_UNAVAILABLE')
            observed=records(store.conn,'SELECT slug,strategy,reason FROM wallet_forward_orders')
            for slug,c in sorted(collectors.items()):
                arms=[]
                for strategy in sorted(tournament.strategies):
                    rows=[r for r in observed if r['slug']==slug and r['strategy']==strategy]
                    row=rows[0] if len(rows)==1 else None
                    reason=None if row is None else row['reason']
                    ready=row is not None and not (reason or '').startswith('MISSED_DECISION_WINDOW')
                    arms.append(dict(strategy_id=strategy,state='DECISION_RECORDED' if ready else 'CAUSAL_INPUTS_UNAVAILABLE',reason=reason))
                supported=all(a['state']=='DECISION_RECORDED' for a in arms)
                research_coverage.append(dict(slug=slug,state='SUPPORTED' if supported else 'UNSUPPORTED_INPUTS',arms=arms))
                if not supported:
                    unsupported.append('TOURNAMENT_CAUSAL_MODEL_INPUTS_UNAVAILABLE')
        fills=records(store.conn,'SELECT order_id,strategy,slug,token_id,outcome,fill_ts_us AS at_us,shares,notional,fee FROM wallet_forward_fills ORDER BY rowid')
        orders=records(store.conn,'SELECT order_id,strategy,slug,status,reason,shares AS submitted_shares FROM wallet_forward_orders ORDER BY rowid')
        decisions=records(store.conn,'SELECT signal_id AS event_id,decided_at_us AS at_us,slug,decision,reason FROM wallet_strategy_decision_events LEFT JOIN wallet_forward_signals USING(session_id,signal_id) ORDER BY wallet_strategy_decision_events.id')
        decisions=sorted(notes+decisions,key=lambda x:(x['at_us'],x.get('event_id') or ''))
        research_rounds=records(store.conn,'SELECT strategy,slug,pnl,fees,up_shares,down_shares,details_json FROM wallet_forward_rounds')
        for row in research_rounds:row['details']=json.loads(row.pop('details_json'))
        return dict(replay_policy_revision=REPLAY_POLICY_REVISION,policies=policies,manifest=asdict(m),checkpoint=cursor,
            status='UNSUPPORTED_INPUTS' if unsupported else 'SUPPORTED_SYNTHETIC_OR_CURATED',
            unsupported_reasons=sorted(set(unsupported)),fills=fills,orders=orders,decisions=decisions,
            research_coverage=research_coverage,
            research_rounds=research_rounds,
            limitations=['Frozen BOOK/PRICE BUY takers and immutable configured strategy definitions only.',
                'Caller-supplied event provenance is not independently authenticated.',
                'Restart recomputes a bounded immutable prefix; it does not resume a live process.',
                'Independent shadow PnL requires an observed validated resolution; no bankroll or adaptive switching.'])
