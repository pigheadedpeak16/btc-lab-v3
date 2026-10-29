"""Stage 4 execution regressions; all SQLite files are isolated in tmp_path."""
import json
from dataclasses import asdict, replace
from decimal import Decimal as D
from types import SimpleNamespace

import pytest

from btc_v3.forensics import forward_lab as f
from btc_v3.storage.external_store import ExternalStore
from tests.test_stage1_integrity import lab, collector
from tests.test_phase4_features import market
from tests.test_stage2_integrity import maker

AT = 1_010_100_000


def literal(engine, **changes):
    pending = replace(engine.pending_copies[0], strategy=f.STRATEGY_9A, **changes)
    # Keep the isolated order ledger consistent with the arm being exercised.
    with engine.store.conn:
        engine.store.conn.execute('update wallet_forward_orders set strategy=?,shares=? where order_id=?',
            (pending.strategy, str(pending.target_size), pending.order_id))
    return pending


@pytest.mark.parametrize('fault', ['token', 'outcome', 'book_slug', 'book_token', 'collector_market'])
def test_copy_rejects_unverified_execution_identity(lab, fault):
    ms, store, engine = lab
    p = literal(engine)
    c = collector(AT)
    if fault == 'token': p.token_id = 'alien'
    if fault == 'outcome': p.outcome = 'DOWN'
    if fault == 'book_slug': c.states['up'].slug = 'btc-updown-5m-1300'
    if fault == 'book_token': c.states['up'].token_id = 'alien'
    if fault == 'collector_market': c.market = replace(market(), slug='btc-updown-5m-1300')
    assert engine._execute_copy(p, collector=c, market_store=ms, now_us=AT) == 'EXECUTION_MARKET_MISMATCH'
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


def test_depth_weighted_copy_never_exceeds_notional_cap(lab):
    ms, store, engine = lab
    p = literal(engine, target_size=D(200))
    c = collector(AT)
    c.states['up'].asks = {D('.5'): D(1), D('.9'): D(200)}
    assert engine._execute_copy(p, collector=c, market_store=ms, now_us=AT) in {'FILLED', 'PARTIAL'}
    cost = D(store.conn.execute('select notional from wallet_forward_fills').fetchone()[0])
    assert cost <= D(50)


def test_actual_rounded_submission_recorded_for_every_copy_arm(lab):
    ms, store, engine = lab
    p = replace(engine.pending_copies[0], target_size=D('20.009'))
    engine.config = replace(engine.config, strict_market_minimum=True)
    assert engine._execute_copy(p, collector=collector(AT), market_store=ms, now_us=AT) == 'FILLED'
    order_size = D(store.conn.execute('select shares from wallet_forward_orders').fetchone()[0])
    fill_size = D(store.conn.execute('select shares from wallet_forward_fills').fetchone()[0])
    assert order_size == fill_size == D('20.00')


def test_v2_sizes_to_walked_depth_and_fees_instead_of_rejecting_affordable_trade(lab):
    ms, store, engine = lab
    p = replace(engine.pending_copies[0], strategy=f.STRATEGY_9_EDGE_BOOK_V2)
    c = collector(AT)
    c.states['up'].asks = {D('.5'): D(1), D('.7'): D(100)}
    result = engine._execute_copy(p, collector=c, market_store=ms, now_us=AT)
    assert result in {'FILLED', 'PARTIAL'}
    assert D(0) < engine.edge_v2_bankroll.committed_this_session <= D(10)


def test_same_arm_cannot_reuse_unchanged_displayed_liquidity(lab):
    ms, store, engine = lab
    first = engine.pending_copies[0]
    c = collector(AT)
    c.states['up'].asks = {D('.5'): D(20)}
    assert engine._execute_copy(first, collector=c, market_store=ms, now_us=AT) == 'FILLED'
    second = replace(first, order_id='second')
    store.save_order(order_id='second', session_id=engine.session_id, strategy=first.strategy,
        signal_id=first.signal_id, slug=first.slug, token_id=first.token_id, outcome='UP', side='BUY',
        style='FAK_PAPER', price_limit='.5', shares='20', created_at_us=AT,
        scheduled_at_us=AT, status='SCHEDULED', reason=None, metadata={})
    assert engine._execute_copy(second, collector=c, market_store=ms, now_us=AT+1) == 'NO_EXECUTABLE_LIQUIDITY'


def test_fak_partial_records_cancelled_remainder(lab):
    ms, store, engine = lab
    c = collector(AT)
    c.states['up'].asks = {D('.5'): D(6)}
    assert engine._execute_copy(engine.pending_copies[0], collector=c, market_store=ms, now_us=AT) == 'PARTIAL'
    row = store.conn.execute('select status,reason from wallet_forward_orders').fetchone()
    assert tuple(row) == ('PARTIAL', 'FAK_REMAINDER_CANCELLED')


def test_maker_trade_must_occur_after_quote_not_only_arrive_after_it(lab):
    ms, store, engine = lab
    pos, order = maker(lab)
    ms.conn.execute('update market_trades set source_ts_us=?', (order.created_at_us-1,))
    ms.conn.commit()
    assert engine.process_market_trades(ms, now_us=1_102_000_000) == 0
    assert pos.up_shares == 0 and order.queue_ahead == 1


@pytest.mark.parametrize('fault', ['missing_source', 'source_after_receive', 'receive_after_processing'])
def test_maker_rejects_unprovable_trade_chronology(lab, fault):
    ms, store, engine = lab
    pos, order = maker(lab)
    if fault == 'missing_source': ms.conn.execute('update market_trades set source_ts_us=NULL')
    elif fault == 'source_after_receive': ms.conn.execute('update market_trades set source_ts_us=received_at_us+1')
    else: ms.conn.execute('update market_trades set received_at_us=1103000000')
    ms.conn.commit()
    assert engine.process_market_trades(ms, now_us=1_102_000_000) == 0
    assert pos.up_shares == 0 and order.queue_ahead == 1


def test_duplicate_public_print_cannot_fill_maker_twice(lab):
    ms, store, engine = lab
    pos, order = maker(lab)
    assert engine.process_market_trades(ms, now_us=1_102_000_000) == 1
    ms.persist_trade(market(), token_id='up', source_ts_us=1_101_000_000,
        received_at_us=1_102_500_000, price='.5', size='3', taker_side='SELL',
        source_trade_id='trade', raw_payload={})
    assert engine.process_market_trades(ms, now_us=1_103_000_000) == 0
    assert pos.up_shares == D(2)


def quote_book(at=AT):
    c = collector(at)
    for s in c.states.values():
        s.bids = {D('.4'): D(1)}
        s.asks = {D('.6'): D(100)}
        s.tick_size = D('.01')
    return c


def test_maker_open_quotes_reserve_round_exposure(lab):
    ms, store, engine = lab
    engine.config = replace(engine.config, grid_prices=(.25,.35), grid_orders_per_side=2,
        grid_clip_shares=10, max_total_round_notional=8, max_unpaired_shares=100,
        strict_market_minimum=True)
    c = quote_book()
    engine.refresh_strategy8(market=market(), collector=c, sample=None, now_us=AT)
    # Let the highest quote fill; lower OPEN quotes still reserve their exposure.
    ms.persist_trade(market(), token_id='up', source_ts_us=AT+10, received_at_us=AT+20,
        price='.35', size='10', taker_side='SELL', source_trade_id='fill', raw_payload={})
    engine.process_market_trades(ms, now_us=AT+30)
    engine.refresh_strategy8(market=market(), collector=c, sample=None, now_us=AT+40)
    pos = engine.positions[(f.STRATEGY_8, market().slug)]
    reserved = sum(o.price*o.remaining_shares for o in engine.maker_orders.values())
    assert pos.total_buy_cost + reserved <= D(8)


def test_maker_open_quotes_reserve_same_side_share_limit(lab):
    ms, store, engine = lab
    engine.config = replace(engine.config, grid_prices=(.25,.35), grid_orders_per_side=2,
        grid_clip_shares=48, max_unpaired_shares=48, strict_market_minimum=True)
    engine.refresh_strategy8(market=market(), collector=quote_book(), sample=None, now_us=AT)
    for outcome in ('UP','DOWN'):
        assert sum(o.remaining_shares for o in engine.maker_orders.values() if o.outcome==outcome) <= D(48)


@pytest.mark.parametrize('fault', ['unknown_minimum', 'below_minimum', 'off_tick', 'unknown_tick'])
def test_maker_does_not_submit_ineligible_quote(lab, fault):
    ms, store, engine = lab
    engine.config = replace(engine.config, strict_market_minimum=True)
    c = quote_book()
    if fault == 'below_minimum': engine.config = replace(engine.config, grid_clip_shares=2)
    for s in c.states.values():
        if fault == 'unknown_minimum': s.min_order_size = None
        if fault == 'off_tick': s.tick_size = D('.1')
        if fault == 'unknown_tick': s.tick_size = None
    engine.refresh_strategy8(market=market(), collector=c, sample=None, now_us=AT)
    assert not engine.maker_orders


def test_tournament_rejects_token_outcome_mismatch(lab, tmp_path):
    ms, store, engine = lab
    with ExternalStore.open_project(tmp_path) as external:
        t = f.FullForwardTournament(session_id=engine.session_id, store=store,
            paper_engine=engine, external_store=external, config=engine.config)
        name, cfg = f.full_tournament_specs(15)[0]
        t._schedule(strategy=name, cfg=cfg.common, market=market(), outcome='UP',
            ask=D('.5'), now_us=AT-1_000_000, metadata={})
        t.pending[0].token_id='alien'
        assert t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:collector(AT)),
            market_store=ms, now_us=AT).get('EXECUTION_MARKET_MISMATCH') == 1


def test_sell_preserves_remaining_average_basis():
    p=f.RoundPosition('s',0,10)
    p.record_fill(outcome='UP',side='BUY',shares=D(10),notional=D(4),fee=D(0))
    p.record_fill(outcome='UP',side='SELL',shares=D(5),notional=D(3),fee=D(0))
    assert p.avg_buy('UP') == D('.4')
    assert p.total_buy_cost == D(4)


@pytest.mark.parametrize('size', ['NaN', 'Infinity', '-1', '0'])
def test_every_copy_arm_rejects_invalid_size_cleanly(lab, size):
    ms, store, engine = lab
    p = literal(engine, target_size=D(size))
    assert engine._execute_copy(p, collector=collector(AT), market_store=ms, now_us=AT) == 'NO_EXECUTABLE_SIZE'


@pytest.mark.parametrize('late,reason', [(1_300_000_000, 'MARKET_EXPIRED'), (AT+3_000_000, 'STALE_BOOK')])
def test_copy_rechecks_clock_after_edge_assessment(lab, monkeypatch, late, reason):
    ms, store, engine = lab
    clock=[AT]
    engine.execution_clock=lambda:clock[0]
    original=f.assess_executable_edge
    def delayed(**kwargs):
        result=original(**kwargs)
        clock[0]=late
        return result
    monkeypatch.setattr(f,'assess_executable_edge',delayed)
    p=replace(engine.pending_copies[0], strategy=f.STRATEGY_9_EDGE_BOOK_V2)
    assert engine._execute_copy(p,collector=collector(AT),market_store=ms,now_us=AT)==reason
    assert engine.edge_v2_bankroll.committed_this_session==0
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0]==0


def test_maker_rejects_token_outcome_mismatch_before_queue_mutation(lab):
    ms, store, engine = lab
    pos, order=maker(lab)
    order.outcome='DOWN'
    assert engine.process_market_trades(ms,now_us=1_102_000_000)==0
    assert order.queue_ahead==1 and pos.down_shares==0


def test_maker_rechecks_clock_before_submission(lab):
    ms, store, engine = lab
    calls=iter([AT,1_300_000_000])
    engine.execution_clock=lambda:next(calls,1_300_000_000)
    engine.refresh_strategy8(market=market(),collector=quote_book(),sample=None,now_us=AT)
    assert not engine.maker_orders


def test_tournament_history_never_crosses_market_rollover(lab,tmp_path):
    ms,store,engine=lab
    with ExternalStore.open_project(tmp_path) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,
            paper_engine=engine,external_store=external,config=engine.config)
        t.observe_book(collector=collector(AT),now_us=AT)
        c=collector(AT+1_000_000)
        c.market=replace(market(),slug='btc-updown-5m-1300',round_start_us=1_300_000_000,
            round_end_us=1_600_000_000)
        for state in c.states.values():state.slug=c.market.slug
        t.observe_book(collector=c,now_us=AT+1_000_000)
        assert t._previous_book(now_us=AT+1_000_000,lookback_seconds=1) is None


def test_strict_taker_rejects_unknown_tick(lab):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    c=collector(AT)
    c.states['up'].tick_size=None
    assert engine._execute_copy(engine.pending_copies[0],collector=c,
        market_store=ms,now_us=AT)=='MARKET_TICK_UNKNOWN'


def test_forward_command_defaults_to_strict_submission_rules():
    from btc_v3.cli import build_parser, cmd_forward_wallet_lab
    import inspect
    assert build_parser().parse_args(['forward-wallet-lab']).strict_market_minimum is True
    assert inspect.signature(cmd_forward_wallet_lab).parameters['strict_market_minimum'].default is True


def test_maker_fill_exports_submission_minimum_and_conditional_queue_model(lab):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    c=quote_book()
    engine.refresh_strategy8(market=market(),collector=c,sample=None,now_us=AT)
    ms.persist_trade(market(),token_id='up',source_ts_us=AT+1,received_at_us=AT+2,
        price='.35',size='.01',taker_side='SELL',source_trade_id='partial',raw_payload={})
    assert engine.process_market_trades(ms,now_us=AT+3)==1
    m=json.loads(store.conn.execute('select metadata_json from wallet_forward_fills').fetchone()[0])
    assert m['market_min_order_size']=='5'
    assert m['submitted_shares']=='48.00'
    assert m['minimum_submission_eligible'] is True
    assert m['execution_certainty']=='CONDITIONAL_PUBLIC_PRINT_QUEUE_ESTIMATE'


def test_copy_records_tick_valid_actual_limit(lab):
    ms,store,engine=lab
    p=literal(engine,target_size=D(20),target_price=D('.5'),deterioration_cap=D('.035'))
    assert engine._execute_copy(p,collector=collector(AT),market_store=ms,now_us=AT)=='FILLED'
    row=store.conn.execute('select price_limit from wallet_forward_orders').fetchone()
    assert D(row[0])==D('.53')


@pytest.mark.parametrize('minimum',['NaN','Infinity','-1','0'])
def test_invalid_market_minimum_fails_closed(lab,minimum):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    c=collector(AT);c.states['up'].min_order_size=D(minimum)
    assert engine._execute_copy(engine.pending_copies[0],collector=c,
        market_store=ms,now_us=AT)=='MARKET_MINIMUM_UNKNOWN'


def test_edge_book_fault_after_assessment_cannot_mutate_cash(lab,monkeypatch):
    ms,store,engine=lab
    c=collector(AT)
    original=f.assess_executable_edge
    def fault(**kwargs):
        result=original(**kwargs)
        c.states['up'].mark_suspect('transport loss')
        return result
    monkeypatch.setattr(f,'assess_executable_edge',fault)
    p=replace(engine.pending_copies[0],strategy=f.STRATEGY_9_EDGE_BOOK_V2)
    assert engine._execute_copy(p,collector=c,market_store=ms,now_us=AT)=='BOOK_UNRELIABLE'
    assert engine.edge_v2_bankroll.committed_this_session==0


def test_execution_decision_uses_actual_fill_time_after_processing_delay(lab,monkeypatch):
    ms,store,engine=lab
    pending=engine.pending_copies[0];pending.require_book_confirmation=False
    clock=[AT];engine.execution_clock=lambda:clock[0]
    original=f.walk_taker_fak
    def slow(*args,**kwargs):
        result=original(*args,**kwargs);clock[0]=AT+500_000
        return result
    monkeypatch.setattr(f,'walk_taker_fak',slow)
    engine.execute_due_copies(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:collector(AT)),
        market_store=ms,now_us=AT)
    filled=store.conn.execute('select fill_ts_us from wallet_forward_fills').fetchone()[0]
    event=store.conn.execute("select decided_at_us,context_json from wallet_strategy_decision_events where decision='EXECUTED'").fetchone()
    assert event[0]==filled
    assert json.loads(event[1])['execution_at_us']==filled


def test_forward_runner_refuses_non_strict_config_before_any_network(tmp_path,monkeypatch):
    import asyncio
    def forbidden(*args,**kwargs):
        pytest.fail('non-strict runner reached network client creation')
    monkeypatch.setattr(f,'PolymarketDataClient',forbidden)
    with pytest.raises(ValueError,match='STRICT_EXECUTION_REQUIRED'):
        asyncio.run(f.run_forward_wallet_lab(tmp_path,requested_wallet='test',seconds=0,
            config=f.ForwardLabConfig(strict_market_minimum=False)))


def test_session_summary_exposes_unsettled_exposure(lab):
    ms,store,engine=lab
    engine._execute_copy(engine.pending_copies[0],collector=collector(AT),market_store=ms,now_us=AT)
    summary=store.session_summary(engine.session_id)
    strategy=summary['strategies'][engine.pending_copies[0].strategy]
    assert strategy['unsettled_rounds']==1
    assert strategy['unsettled_purchase_cost']==10
    assert summary['pnl_scope']=='SETTLED_ONLY_UNSETTLED_EXPOSURE_EXCLUDED'


def test_tournament_actual_limit_follows_current_tick(lab,tmp_path):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    c=collector(AT);c.states['up'].tick_size=D('.1')
    with ExternalStore.open_project(tmp_path) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,paper_engine=engine,
            external_store=external,config=engine.config)
        name,cfg=f.full_tournament_specs(15)[0]
        t._schedule(strategy=name,cfg=cfg.common,market=market(),outcome='UP',ask=D('.5'),
            now_us=AT-1_000_000,metadata={})
        t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:c),market_store=ms,now_us=AT)
        limit=store.conn.execute('select price_limit from wallet_forward_orders where strategy=?',(name,)).fetchone()[0]
        assert D(limit)==D('.5')


@pytest.mark.parametrize('kind',['wrong_market','future_sample'])
def test_maker_cannot_use_another_market_or_future_model_sample(lab,kind):
    ms,store,engine=lab
    sample=SimpleNamespace(slug='wrong' if kind=='wrong_market' else market().slug,
        sample_ts_us=AT+1 if kind=='future_sample' else AT,p_up_mc=0.0)
    engine.refresh_strategy8(market=market(),collector=quote_book(),sample=sample,now_us=AT)
    assert {o.outcome for o in engine.maker_orders.values()}=={'UP','DOWN'}


@pytest.mark.parametrize('fault',['unknown_tick','nan_minimum','infinite_minimum'])
def test_tournament_fails_closed_on_invalid_submission_metadata(lab,tmp_path,fault):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    c=collector(AT)
    if fault=='unknown_tick': c.states['up'].tick_size=None
    else: c.states['up'].min_order_size=D('NaN' if fault=='nan_minimum' else 'Infinity')
    reason='MARKET_TICK_UNKNOWN' if fault=='unknown_tick' else 'MARKET_MINIMUM_UNKNOWN'
    with ExternalStore.open_project(tmp_path) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,paper_engine=engine,
            external_store=external,config=engine.config)
        name,cfg=f.full_tournament_specs(15)[0]
        t._schedule(strategy=name,cfg=cfg.common,market=market(),outcome='UP',ask=D('.5'),
            now_us=AT-1_000_000,metadata={})
        assert t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:c),
            market_store=ms,now_us=AT).get(reason)==1


def test_strict_copy_nan_quantity_is_rejection_not_runner_crash(lab):
    ms,store,engine=lab
    engine.config=replace(engine.config,strict_market_minimum=True)
    assert engine._execute_copy(literal(engine,target_size=D('NaN')),collector=collector(AT),
        market_store=ms,now_us=AT)=='NO_EXECUTABLE_SIZE'


def test_extreme_finite_signal_size_is_bounded_by_order_cap(lab):
    ms,store,engine=lab
    assert engine._execute_copy(literal(engine,target_size=D('1e100')),collector=collector(AT),
        market_store=ms,now_us=AT)=='FILLED'
    assert D(store.conn.execute('select notional from wallet_forward_fills').fetchone()[0])<=D(50)


def another_order(store,engine,p,*,order_id='next',strategy=None,size='20'):
    strategy=strategy or p.strategy
    store.save_order(order_id=order_id,session_id=engine.session_id,strategy=strategy,signal_id=p.signal_id,
        slug=p.slug,token_id=p.token_id,outcome=p.outcome,side=p.side,style='FAK_PAPER',price_limit='.5',
        shares=size,created_at_us=AT,scheduled_at_us=AT,status='SCHEDULED',reason=None,metadata={})
    return replace(p,order_id=order_id,strategy=strategy,target_size=D(size))


@pytest.mark.parametrize('observation',['identical_snapshot','size_increase','independent_arm'])
def test_virtual_liquidity_requires_evidence_to_replenish(lab,observation):
    ms,store,engine=lab
    p=engine.pending_copies[0];c=collector(AT)
    c.states['up'].asks={D('.5'):D(20)}
    assert engine._execute_copy(p,collector=c,market_store=ms,now_us=AT)=='FILLED'
    if observation=='identical_snapshot':
        c.states['up'].apply_snapshot(bids=[{'price':'.49','size':'100'}],asks=[{'price':'.5','size':'20'}],
            received_at_us=AT+1,server_ts_us=AT+1,book_hash='new',min_order_size='5',tick_size='.01')
        second=another_order(store,engine,p)
        expected='NO_EXECUTABLE_LIQUIDITY'
    elif observation=='size_increase':
        c.states['up'].asks[D('.5')]=D(25)
        second=another_order(store,engine,p,size='5');expected='FILLED'
    else:
        second=another_order(store,engine,p,strategy=f.STRATEGY_9_BOOK_CONFIRM_75);expected='FILLED'
    assert engine._execute_copy(second,collector=c,market_store=ms,now_us=AT+1)==expected
    assert c.states['up'].asks[D('.5')]==(D(25) if observation=='size_increase' else D(20))


def test_failed_fill_does_not_consume_virtual_liquidity(lab,monkeypatch):
    ms,store,engine=lab
    p=engine.pending_copies[0];c=collector(AT);c.states['up'].asks={D('.5'):D(20)}
    original=store.save_fill
    def failed(**kwargs):
        original(**kwargs);raise OSError('isolated disk failure')
    monkeypatch.setattr(store,'save_fill',failed)
    with pytest.raises(OSError):engine._execute_copy(p,collector=c,market_store=ms,now_us=AT)
    monkeypatch.setattr(store,'save_fill',original)
    assert engine._execute_copy(p,collector=c,market_store=ms,now_us=AT+1)=='FILLED'
    assert store.reconcile_session(engine.session_id)['ok'] is True


def test_maker_same_transaction_with_distinct_trade_times_is_not_collapsed(lab):
    ms,store,engine=lab
    pos,order=maker(lab)
    assert engine.process_market_trades(ms,now_us=1_102_000_000)==1
    ms.persist_trade(market(),token_id='up',source_ts_us=1_102_000_000,received_at_us=1_102_500_000,
        price='.5',size='2',taker_side='SELL',source_trade_id='trade',raw_payload={})
    assert engine.process_market_trades(ms,now_us=1_103_000_000)==1
    assert pos.up_shares==D(4)


def test_maker_repeated_refresh_keeps_existing_quote_priority(lab):
    ms,store,engine=lab
    c=quote_book()
    engine.refresh_strategy8(market=market(),collector=c,sample=None,now_us=AT)
    before={key:asdict(o) for key,o in engine.maker_orders.items()}
    engine.refresh_strategy8(market=market(),collector=c,sample=None,now_us=AT+1)
    assert before=={key:asdict(o) for key,o in engine.maker_orders.items()}


@pytest.mark.parametrize('terminal',['REJECTED','CANCELLED'])
def test_terminal_copy_cannot_be_revived_by_late_callback(lab,terminal):
    ms,store,engine=lab
    p=engine.pending_copies[0]
    if terminal=='REJECTED':
        assert engine._execute_copy(p,collector=collector(AT-3_000_000),market_store=ms,now_us=AT)=='STALE_BOOK'
    else: store.update_order_status(p.order_id,status='CANCELLED',reason='TEST_CANCELLATION')
    assert engine._execute_copy(p,collector=collector(AT),market_store=ms,now_us=AT)=='ORDER_TERMINAL'
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0]==0


def test_rejected_tournament_order_cannot_be_revived(lab,tmp_path):
    ms,store,engine=lab
    with ExternalStore.open_project(tmp_path) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,paper_engine=engine,
            external_store=external,config=engine.config)
        name,cfg=f.full_tournament_specs(15)[0]
        t._schedule(strategy=name,cfg=cfg.common,market=market(),outcome='UP',ask=D('.5'),
            now_us=AT-1_000_000,metadata={})
        p=t.pending[0]
        t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:collector(AT-10_000_000)),
            market_store=ms,now_us=AT)
        t.pending.append(p)
        assert t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:collector(AT)),
            market_store=ms,now_us=AT).get('ORDER_TERMINAL')==1


def test_confirmation_callback_cannot_reopen_cancelled_order(lab):
    ms,store,engine=lab
    p=engine.pending_copies[0]
    store.update_order_status(p.order_id,status='CANCELLED',reason='TEST_CANCELLATION')
    engine.execute_due_copies(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:collector(AT)),
        market_store=ms,now_us=AT)
    assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0]=='CANCELLED'
