"""Stage 1 regressions. All stores belong to pytest temporary directories."""
import ast
import inspect
from dataclasses import asdict, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from btc_v3.forensics import forward_lab as f, live
from btc_v3.storage.external_store import ExternalStore
from tests.test_phase593_edge_book import _session, _book
from tests.test_phase4_features import market, seed_store


@pytest.fixture
def lab(tmp_path):
    ms, store, engine, _ = _session(tmp_path)
    try:
        yield ms, store, engine
    finally:
        store.close()
        ms.close()


def collector(at):
    return SimpleNamespace(market=market(), states={
        'up': _book(at, 'UP', 'up'), 'down': _book(at, 'DOWN', 'down')})


@pytest.mark.parametrize('at', [1_300_000_000, 1_300_000_001])
def test_copy_rejects_execution_at_or_after_deadline(lab, at):
    ms, store, engine = lab
    p = engine.pending_copies[0]
    assert engine._execute_copy(p, collector=collector(at), market_store=ms,
                                now_us=at) == 'MARKET_EXPIRED'
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


@pytest.mark.parametrize('side', ['BUY', 'SELL'])
def test_settlement_finalizes_position_against_late_callbacks(side):
    p = f.RoundPosition('x', 0, 10)
    p.record_fill(outcome='UP', side='BUY', shares=Decimal(5),
                  notional=Decimal(2), fee=Decimal('.1'))
    assert p.settle(resolved_side='UP', at_us=10) == Decimal('2.9')
    before = asdict(p)
    assert p.settle(resolved_side='DOWN', at_us=20) == Decimal('2.9')
    with pytest.raises(ValueError, match='POSITION_SETTLED'):
        p.record_fill(outcome='UP', side=side, shares=Decimal(1),
                      notional=Decimal('.5'), fee=Decimal('.01'))
    assert asdict(p) == before


def test_settled_copy_rejected_without_new_ledger_fill(lab):
    ms, store, engine = lab
    pending = engine.pending_copies[0]
    pos = engine.position_by_slug(pending.strategy, pending.slug, ms)
    pos.settle(resolved_side='UP', at_us=1_010_000_000)
    assert engine._execute_copy(pending, collector=collector(1_011_000_000),
        market_store=ms, now_us=1_011_000_000) == 'POSITION_SETTLED'
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


@pytest.mark.parametrize('processing_at,expected', [
    (1_300_000_000, 'MARKET_EXPIRED'), (1_014_000_000, 'STALE_BOOK')])
def test_copy_refreshes_execution_clock_after_loop_delay(lab, processing_at, expected):
    ms, store, engine = lab
    # Inject the actual processing clock; supplied loop timestamp is stale.
    engine.execution_clock = lambda: processing_at
    assert engine._execute_copy(engine.pending_copies[0], collector=collector(1_010_000_000),
        market_store=ms, now_us=1_010_100_000) == expected


@pytest.mark.parametrize('remaining', ['0', '.00001'])
def test_v2_budget_exhaustion_rejects_before_walker(lab, monkeypatch, remaining):
    ms, store, engine = lab
    p = replace(engine.pending_copies[0], strategy=f.STRATEGY_9_EDGE_BOOK_V2)
    engine.edge_v2_bankroll.committed_this_session = Decimal(10) - Decimal(remaining)
    def forbidden(*a, **kw):
        pytest.fail('zero-sized V2 request reached book walker')
    monkeypatch.setattr(f, 'walk_taker_fak', forbidden)
    assert engine._execute_copy(p, collector=collector(1_010_100_000),
        market_store=ms, now_us=1_010_100_000) == 'EDGE_NO_EXECUTABLE_SIZE'


def runner_detection_block(namespace):
    """Exercise the actual runner call-site, without starting its coroutine/feeds."""
    tree = ast.parse(inspect.getsource(f.run_forward_wallet_lab))
    assignments = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id in
                           {'detection_market', 'detection_collector', 'detection_sample'}
                           or isinstance(t, ast.Tuple) and any(isinstance(x, ast.Name)
                           and x.id == 'detection_market' for x in t.elts) for t in n.targets)]
    assignments.sort(key=lambda n: n.lineno)
    exec(compile(ast.Module(body=assignments, type_ignores=[]), '<runner detection>', 'exec'), namespace)
    return namespace['detection_sample']


def detection_namespace(monkeypatch):
    old = replace(market(), slug='btc-updown-5m-900', round_start_us=900_000_000,
                  round_end_us=1_200_000_000)
    row = SimpleNamespace(slug=old.slug, event_slug=old.slug, token_id='up', outcome='UP')
    old_collector = SimpleNamespace(market=old)
    pool = SimpleNamespace(current=lambda asset: (market(), collector(1_210_000_000)),
                           collector_for_slug=lambda slug: old_collector if slug == old.slug else None)
    ns = dict(vars(f))
    ns.update(pool=pool, row=row, batch=SimpleNamespace(response_received_us=1_199_000_000),
              session_id='s', wallet='w', read_store=None, config=f.ForwardLabConfig(),
              _sample_from_state=lambda **kw: SimpleNamespace(**kw))
    monkeypatch.setattr(f, '_sample_from_state', ns['_sample_from_state'])
    return ns, old


def test_runner_model_is_bound_to_signal_observability_time(monkeypatch):
    ns, old = detection_namespace(monkeypatch)
    sample = runner_detection_block(ns)
    assert getattr(sample, 'decision_time_us', None) == ns['batch'].response_received_us


def test_runner_uses_signal_market_during_rollover(monkeypatch):
    ns, old = detection_namespace(monkeypatch)
    sample = runner_detection_block(ns)
    assert sample.market.slug == old.slug


def test_sample_explicit_cutoff_excludes_future_observations(tmp_path, monkeypatch):
    cutoff = 1_240_004_000
    with seed_store(tmp_path) as store:
        monkeypatch.setattr(live, 'now_epoch_us', lambda: cutoff + 2_000_000)
        first = live._sample_from_state(session_id='s', wallet='w', market=market(),
            book_collector=None, external_store=store, model_paths=100,
            decision_time_us=cutoff)
        for source in ('coinbase', 'kraken'):
            persist = getattr(store, 'persist_' + source + '_tick')
            # Individually violate receive and source cutoffs.
            persist(source_ts_us=cutoff-1, received_at_us=cutoff+1,
                    receive_sequence=999, price='99000', raw_payload={})
            persist(source_ts_us=cutoff+1, received_at_us=cutoff-1,
                    receive_sequence=1000, price='98000', raw_payload={})
        second = live._sample_from_state(session_id='s', wallet='w', market=market(),
            book_collector=None, external_store=store, model_paths=100,
            decision_time_us=cutoff)
        assert first.p_up_mc is not None
        assert first.sample_ts_us == second.sample_ts_us == cutoff
        assert first.predictor_price == second.predictor_price
        assert first.p_up_mc == second.p_up_mc


@pytest.mark.parametrize('settled', [False, True])
def test_tournament_rejects_expired_or_finalized_position(lab, tmp_path, settled):
    ms, store, engine = lab
    with ExternalStore.open_project(tmp_path) as external:
        t = f.FullForwardTournament(session_id=engine.session_id, store=store,
            paper_engine=engine, external_store=external, config=f.ForwardLabConfig())
        name, cfg = f.full_tournament_specs(15)[0]
        t._schedule(strategy=name, cfg=cfg.common, market=market(), outcome='UP',
                    ask=Decimal('.5'), now_us=1_299_000_000, metadata={})
        if settled:
            engine.position(name, market()).settle(resolved_side='UP', at_us=1_299_000_000)
        at = 1_299_500_000 if settled else 1_300_000_000
        pool = SimpleNamespace(collector_for_slug=lambda slug: collector(at))
        stats = t.execute_due(collector_pool=pool, market_store=ms, now_us=at)
        assert stats.get('POSITION_SETTLED' if settled else 'MARKET_EXPIRED') == 1


@pytest.mark.parametrize('settled', [False, True])
def test_s8_late_aggressor_cannot_mutate_closed_position(lab, settled):
    ms, store, engine = lab
    pos = engine.position(f.STRATEGY_8, market())
    created = 1_299_000_000
    at = 1_299_500_000 if settled else 1_300_000_000
    if settled:
        pos.settle(resolved_side='UP', at_us=created)
    order = f.MakerOrder('maker', market().slug, 'up', 'UP', Decimal('.5'),
                        Decimal(5), Decimal(5), Decimal(0), created)
    engine.maker_orders['maker'] = order
    store.save_order(order_id='maker', session_id=engine.session_id, strategy=f.STRATEGY_8,
        signal_id=None, slug=market().slug, token_id='up', outcome='UP', side='BUY',
        style='MAKER_GTC_PAPER', price_limit='.5', shares='5', created_at_us=created,
        scheduled_at_us=created, status='OPEN', reason=None, metadata={})
    ms.persist_trade(market(), token_id='up', source_ts_us=at, received_at_us=at,
        price='.5', size='5', taker_side='SELL', source_trade_id='late', raw_payload={})
    engine.execution_clock = lambda: at
    assert engine.process_market_trades(ms) == 0
    assert pos.up_shares == 0
    assert order.status == 'CANCELLED'


def test_s8_records_actual_processing_timestamp(lab):
    ms, store, engine = lab
    m = market()
    engine.position(f.STRATEGY_8, m)
    order = f.MakerOrder('maker', m.slug, 'up', 'UP', Decimal('.5'),
        Decimal(5), Decimal(5), Decimal(0), 1_100_000_000)
    engine.maker_orders['maker'] = order
    store.save_order(order_id='maker', session_id=engine.session_id, strategy=f.STRATEGY_8,
        signal_id=None, slug=m.slug, token_id='up', outcome='UP', side='BUY',
        style='MAKER_GTC_PAPER', price_limit='.5', shares='5', created_at_us=1_100_000_000,
        scheduled_at_us=1_100_000_000, status='OPEN', reason=None, metadata={})
    ms.persist_trade(m, token_id='up', source_ts_us=1_101_000_000,
        received_at_us=1_101_000_000, price='.5', size='5', taker_side='SELL',
        source_trade_id='trade', raw_payload={})
    engine.execution_clock = lambda: 1_102_000_000
    assert engine.process_market_trades(ms) == 1
    assert store.conn.execute('select fill_ts_us from wallet_forward_fills').fetchone()[0] == 1_102_000_000


@pytest.mark.parametrize('size', ['NaN', 'Infinity', '0', '-1'])
def test_v2_invalid_quantity_is_clean_rejection(lab, size):
    ms, store, engine = lab
    p = replace(engine.pending_copies[0], strategy=f.STRATEGY_9_EDGE_BOOK_V2,
                target_size=Decimal(size))
    assert engine._execute_copy(p, collector=collector(1_010_100_000),
        market_store=ms, now_us=1_010_100_000) == 'EDGE_NO_EXECUTABLE_SIZE'


@pytest.mark.parametrize('kind,expected', [
    ('missing', 'SIGNAL_MARKET_UNAVAILABLE'),
    ('wrong_slug', 'SIGNAL_MARKET_MISMATCH'),
    ('wrong_token', 'SIGNAL_MARKET_MISMATCH'),
    ('wrong_outcome', 'SIGNAL_MARKET_MISMATCH'),
    ('expired', 'SIGNAL_MARKET_NOT_LIVE_AT_DETECTION')])
def test_unverified_signal_market_has_no_probability(lab, tmp_path, kind, expected):
    ms, store, engine = lab
    m = replace(market(), slug='btc-updown-5m-900', round_start_us=900_000_000,
                round_end_us=1_200_000_000)
    c = SimpleNamespace(market=m)
    row = SimpleNamespace(slug=m.slug, event_slug=m.slug, token_id='up', outcome='UP')
    if kind == 'wrong_slug':
        c.market = market()
    if kind == 'wrong_token':
        row.token_id = 'alien'
    if kind == 'wrong_outcome':
        row.outcome = 'DOWN'
    pool = SimpleNamespace(collector_for_slug=lambda slug: None if kind == 'missing' else c)
    with ExternalStore.open_project(tmp_path) as external:
        sample = f._wallet_detection_sample(row=row, pool=pool,
            observed_at_us=1_200_000_000 if kind == 'expired' else 1_199_000_000,
            session_id='s', wallet='w', external_store=external, model_paths=100)
    assert sample.p_up_mc is None
    assert sample.unavailable_reason == expected


def test_copy_rechecks_clock_after_book_walk(lab, monkeypatch):
    ms, store, engine = lab
    clock = [1_299_000_000]
    engine.execution_clock = lambda: clock[0]
    original = f.walk_taker_fak
    def slow_walk(*args, **kwargs):
        plan = original(*args, **kwargs)
        clock[0] = 1_300_000_000
        return plan
    monkeypatch.setattr(f, 'walk_taker_fak', slow_walk)
    assert engine._execute_copy(engine.pending_copies[0], collector=collector(clock[0]),
        market_store=ms, now_us=clock[0]) == 'MARKET_EXPIRED'
    assert engine.edge_v2_bankroll.committed_this_session == 0
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


def test_tournament_rechecks_processing_clock_after_walk(lab, tmp_path, monkeypatch):
    ms, store, engine = lab
    clock = [1_299_000_000]
    engine.execution_clock = lambda: clock[0]
    original = f.walk_taker_fak
    def slow_walk(*args, **kwargs):
        plan = original(*args, **kwargs)
        clock[0] = 1_300_000_000
        return plan
    monkeypatch.setattr(f, 'walk_taker_fak', slow_walk)
    with ExternalStore.open_project(tmp_path) as external:
        t = f.FullForwardTournament(session_id=engine.session_id, store=store,
            paper_engine=engine, external_store=external, config=f.ForwardLabConfig())
        name, cfg = f.full_tournament_specs(15)[0]
        t._schedule(strategy=name, cfg=cfg.common, market=market(), outcome='UP',
                    ask=Decimal('.5'), now_us=1_298_000_000, metadata={})
        pool = SimpleNamespace(collector_for_slug=lambda slug: collector(1_299_000_000))
        assert t.execute_due(collector_pool=pool, market_store=ms,
                             now_us=1_299_000_000) == {'MARKET_EXPIRED': 1}
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


def test_successful_copy_records_fresh_execution_timestamp(lab):
    ms, store, engine = lab
    engine.execution_clock = lambda: 1_011_000_000
    assert engine._execute_copy(engine.pending_copies[0], collector=collector(1_011_000_000),
        market_store=ms, now_us=1_010_000_000) == 'FILLED'
    assert store.conn.execute('select fill_ts_us from wallet_forward_fills').fetchone()[0] == 1_011_000_000


def test_same_response_two_markets_keep_distinct_sample_ids(tmp_path):
    with seed_store(tmp_path) as store:
        samples = [live._sample_from_state(session_id='s', wallet='w', market=m,
            book_collector=None, external_store=store, model_paths=100,
            decision_time_us=1_240_004_000) for m in
            (market(), replace(market(), slug='btc-updown-5m-900'))]
    assert samples[0].sample_id != samples[1].sample_id
