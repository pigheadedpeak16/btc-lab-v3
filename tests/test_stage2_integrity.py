"""Archive and ledger fault-injection tests; no production data is opened."""
import gzip
import json
import sqlite3
import subprocess
import sys
from dataclasses import asdict, replace
from decimal import Decimal
from types import SimpleNamespace

import pytest

from btc_v3.feeds.raw_archive import RawJsonlArchive
from btc_v3.forensics import forward_lab as f
from btc_v3.storage.external_store import ExternalStore
from tests.test_phase593_edge_book import _session, _book
from tests.test_phase4_features import market


@pytest.fixture
def lab(tmp_path):
    ms, store, engine, _ = _session(tmp_path)
    try:
        yield ms, store, engine
    finally:
        store.close()
        ms.close()


def book(at=1_010_100_000):
    return SimpleNamespace(states={'up': _book(at, 'UP', 'up'),
                                  'down': _book(at, 'DOWN', 'down')})


def test_overlapping_archive_writers_remain_independently_readable(tmp_path):
    a = RawJsonlArchive(tmp_path/'data/raw', 'polymarket_clob', flush_record_limit=1)
    b = RawJsonlArchive(tmp_path/'data/raw', 'polymarket_clob', flush_record_limit=1)
    refs = []
    try:
        for i in range(6):
            refs.append((a if i % 2 == 0 else b).write(1_760_000_000_000_000+i, {'i': i}))
    finally:
        a.close(); b.close()
    records = []
    for path in {ref['path'] for ref in refs}:
        with gzip.open(tmp_path/'data'/path, 'rt', encoding='utf-8') as fh:
            records.extend(json.loads(line) for line in fh)
    assert sorted(row['payload']['i'] for row in records) == list(range(6))
    assert refs[0]['path'] != refs[1]['path']


def test_archive_never_appends_to_legacy_file(tmp_path):
    root = tmp_path/'data/raw'; day = root/'2025-10-09'; day.mkdir(parents=True)
    legacy = day/'polymarket_clob.jsonl.gz'; legacy.write_bytes(b'historical sentinel')
    before = legacy.read_bytes()
    with RawJsonlArchive(root, 'polymarket_clob') as archive:
        ref = archive.write(1_760_000_000_000_000, {'i': 1})
    assert legacy.read_bytes() == before
    assert (tmp_path/'data'/ref['path']) != legacy


def test_archive_reopen_uses_new_segment(tmp_path):
    a = RawJsonlArchive(tmp_path/'data/raw', 'polymarket_clob')
    first = a.write(1_760_000_000_000_000, {'i': 1}); a.close()
    second = a.write(1_760_000_000_000_001, {'i': 2}); a.close()
    assert first['path'] != second['path']


@pytest.mark.parametrize('v2', [False, True])
@pytest.mark.parametrize('failure', ['save_fill', 'update_order_status', 'upsert_round'])
def test_copy_failure_rolls_back_entire_ledger_and_memory(lab, monkeypatch, v2, failure):
    ms, store, engine = lab
    p = engine.pending_copies[0]
    if v2:
        p.strategy = f.STRATEGY_9_EDGE_BOOK_V2
    pos = engine.position_by_slug(p.strategy, p.slug, ms)
    before = asdict(pos); budget = engine.edge_v2_bankroll.snapshot()
    original = getattr(store, failure)
    def fail_after_write(*a, **kw):
        original(*a, **kw)
        raise OSError('injected disk failure')
    monkeypatch.setattr(store, failure, fail_after_write)
    with pytest.raises(OSError, match='injected'):
        engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000)
    assert asdict(pos) == before
    assert engine.edge_v2_bankroll.snapshot() == budget
    assert not engine.edge_v2_costs_by_slug
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0
    assert store.conn.execute('select count(*) from wallet_forward_rounds').fetchone()[0] == 0
    assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0] == 'SCHEDULED'
    monkeypatch.setattr(store, failure, original)
    assert engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000) == 'FILLED'
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 1


def test_duplicate_copy_callback_is_idempotent(lab):
    ms, store, engine = lab
    p = engine.pending_copies[0]
    assert engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000) == 'FILLED'
    before = asdict(engine.positions[(p.strategy, p.slug)])
    assert engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_001) == 'ALREADY_EXECUTED'
    assert asdict(engine.positions[(p.strategy, p.slug)]) == before
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 1


def test_copy_batch_failure_preserves_pending_callbacks(lab, monkeypatch):
    ms, store, engine = lab
    p = engine.pending_copies[0]; p.require_book_confirmation = False
    pool = SimpleNamespace(collector_for_slug=lambda slug: book())
    def fail(*a, **kw): raise OSError('injected')
    monkeypatch.setattr(store, 'save_fill', fail)
    with pytest.raises(OSError):
        engine.execute_due_copies(collector_pool=pool, market_store=ms, now_us=1_010_100_000)
    assert engine.pending_copies == [p]
    assert not engine.positions


def maker(lab):
    ms, store, engine = lab; m = market()
    pos = engine.position(f.STRATEGY_8, m)
    o = f.MakerOrder('maker', m.slug, 'up', 'UP', Decimal('.5'), Decimal(5),
                     Decimal(5), Decimal(1), 1_100_000_000)
    engine.maker_orders['maker'] = o
    store.save_order(order_id='maker', session_id=engine.session_id, strategy=f.STRATEGY_8,
        signal_id=None, slug=m.slug, token_id='up', outcome='UP', side='BUY',
        style='MAKER_GTC_PAPER', price_limit='.5', shares='5', created_at_us=o.created_at_us,
        scheduled_at_us=o.created_at_us, status='OPEN', reason=None, metadata={})
    ms.persist_trade(m, token_id='up', source_ts_us=1_101_000_000, received_at_us=1_101_000_000,
        price='.5', size='3', taker_side='SELL', source_trade_id='trade', raw_payload={})
    return pos, o


def test_maker_failure_restores_queue_cursor_and_position(lab, monkeypatch):
    ms, store, engine = lab; pos, o = maker(lab)
    before = asdict(pos); before_order = asdict(o)
    original = store.save_fill
    def fail(*a, **kw): original(*a, **kw); raise OSError('injected')
    monkeypatch.setattr(store, 'save_fill', fail)
    with pytest.raises(OSError): engine.process_market_trades(ms, now_us=1_102_000_000)
    assert asdict(pos) == before
    assert asdict(o) == before_order
    assert engine.last_market_trade_id == 0
    assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0
    monkeypatch.setattr(store, 'save_fill', original)
    assert engine.process_market_trades(ms, now_us=1_102_000_000) == 1
    assert pos.up_shares == Decimal(2)
    assert engine.process_market_trades(ms, now_us=1_102_000_001) == 0


def test_tournament_failure_restores_pending_and_fill_state(lab, tmp_path, monkeypatch):
    ms, store, engine = lab
    with ExternalStore.open_project(tmp_path) as external:
        t = f.FullForwardTournament(session_id=engine.session_id, store=store,
            paper_engine=engine, external_store=external, config=f.ForwardLabConfig())
        name, cfg = f.full_tournament_specs(15)[0]
        t._schedule(strategy=name, cfg=cfg.common, market=market(), outcome='UP',
                    ask=Decimal('.5'), now_us=1_009_000_000, metadata={})
        pending = list(t.pending)
        original = store.update_order_status
        def fail(*a, **kw): original(*a, **kw); raise OSError('injected')
        monkeypatch.setattr(store, 'update_order_status', fail)
        with pytest.raises(OSError):
            t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug: book()),
                          market_store=ms, now_us=1_010_100_000)
        assert t.pending == pending
        assert not engine.positions
        assert store.conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0


def test_settlement_failure_restores_budget_and_finalization(lab, tmp_path, monkeypatch):
    ms, store, engine = lab
    p = engine.pending_copies[0]; p.strategy = f.STRATEGY_9_EDGE_BOOK_V2
    engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000)
    pos = engine.positions[(p.strategy, p.slug)]; before = asdict(pos)
    budget = engine.edge_v2_bankroll.snapshot(); costs = dict(engine.edge_v2_costs_by_slug)
    with ExternalStore.open_project(tmp_path) as external:
        external.persist_price_reference(slug=p.slug, round_start_us=1_000_000_000,
            round_end_us=1_300_000_000, price_to_beat='100', reference_ts_us=1_000_000_000,
            observed_at_us=1_000_000_000, source='CHAINLINK_BTC_USD_TWAP_60S', source_stream='prices.crypto.twap:btcusd:60s', mapping_method='EXACT_ROUND_START_SOURCE_TIMESTAMP', raw_payload={})
        external.persist_settlement_observation(source_ts_us=1_300_000_000,
            received_at_us=1_300_000_000, receive_sequence=1, value='101', source='CHAINLINK_BTC_USD_TWAP_60S',
            stream_id='prices.crypto.twap:btcusd:60s', raw_payload={})
        original = store.upsert_round
        def fail(*a, **kw): original(*a, **kw); raise OSError('injected')
        monkeypatch.setattr(store, 'upsert_round', fail)
        with pytest.raises(OSError): engine.settle_ready(external_store=external, now_us=1_301_000_000)
        assert asdict(pos) == before
        assert engine.edge_v2_bankroll.snapshot() == budget
        assert engine.edge_v2_costs_by_slug == costs
        assert store.conn.execute('select pnl from wallet_forward_rounds').fetchone()[0] is None


def test_duplicate_scheduling_cannot_reset_executed_order(lab):
    ms, store, engine = lab
    p = engine.pending_copies[0]
    engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000)
    store.save_order(order_id=p.order_id, session_id=engine.session_id, strategy=p.strategy,
        signal_id=p.signal_id, slug=p.slug, token_id=p.token_id, outcome=p.outcome, side=p.side,
        style='FAK_PAPER', price_limit=None, shares=str(p.target_size),
        created_at_us=p.created_at_us, scheduled_at_us=p.scheduled_at_us, status='SCHEDULED',
        reason=None, metadata={})
    assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0] == 'FILLED'
    assert engine._execute_copy(p, collector=book(), market_store=ms,
        now_us=1_010_100_001) == 'ALREADY_EXECUTED'


def test_store_refuses_overwrite_of_finalized_position(lab):
    ms, store, engine = lab
    p = engine.position('example', market())
    p.settle(resolved_side='UP', at_us=1_300_000_000)
    engine._persist_position('example', p)
    p.up_shares = Decimal(99)  # Simulate a stale caller bypassing the engine API.
    with pytest.raises(ValueError, match='POSITION_SETTLED'):
        engine._persist_position('example', p)
    assert store.conn.execute('select up_shares from wallet_forward_rounds').fetchone()[0] == '0'


def test_reconciliation_detects_drift_without_writing(lab):
    ms, store, engine = lab
    engine._execute_copy(engine.pending_copies[0], collector=book(), market_store=ms,
                         now_us=1_010_100_000)
    assert store.reconcile_session(engine.session_id)['ok'] is True
    with store.conn:
        store.conn.execute("UPDATE wallet_forward_rounds SET up_shares='999'")
    changes = store.conn.total_changes
    result = store.reconcile_session(engine.session_id)
    assert result['ok'] is False
    assert any(i['reason'] == 'POSITION_LEDGER_MISMATCH' for i in result['issues'])
    assert store.conn.total_changes == changes


def test_nested_transaction_failure_does_not_commit_inner_writes(lab):
    ms, store, engine = lab
    original = store.conn.execute('select status from wallet_forward_orders').fetchone()[0]
    with pytest.raises(OSError):
        with store.transaction():
            store.update_order_status(engine.pending_copies[0].order_id, status='FILLED')
            raise OSError('injected outer failure')
    assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0] == original


def test_process_crash_cannot_leave_half_a_fill(tmp_path):
    script = '''
import os,sys
from pathlib import Path
from tests.test_phase593_edge_book import _session, _book
from types import SimpleNamespace
ms, store, engine, _ = _session(Path(sys.argv[1]))
original = store.save_fill
def crash(*a, **kw):
    original(*a, **kw)
    os._exit(73)
store.save_fill = crash
c = SimpleNamespace(states={'up':_book(1010100000,'UP','up'),'down':_book(1010100000,'DOWN','down')})
engine._execute_copy(engine.pending_copies[0],collector=c,market_store=ms,now_us=1010100000)
'''
    run = subprocess.run([sys.executable, '-B', '-c', script, str(tmp_path)],
                         capture_output=True, text=True, timeout=30)
    assert run.returncode == 73, run.stderr
    with sqlite3.connect(tmp_path/'research/v3_research.sqlite') as conn:
        assert conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0
        assert conn.execute('select count(*) from wallet_forward_rounds').fetchone()[0] == 0
        assert conn.execute('select status from wallet_forward_orders').fetchone()[0] == 'SCHEDULED'


def test_second_connection_never_sees_partial_execution(lab, monkeypatch):
    ms, store, engine = lab
    original = store.save_fill
    def observe(*a, **kw):
        with sqlite3.connect(store.path) as reader:
            assert reader.execute('select count(*) from wallet_forward_rounds').fetchone()[0] == 0
        return original(*a, **kw)
    monkeypatch.setattr(store, 'save_fill', observe)
    engine._execute_copy(engine.pending_copies[0], collector=book(), market_store=ms,
                         now_us=1_010_100_000)
    with sqlite3.connect(store.path) as reader:
        assert reader.execute('select count(*) from wallet_forward_rounds').fetchone()[0] == 1
        assert reader.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 1


def test_reconciliation_validates_settlement_pnl(lab):
    ms, store, engine = lab
    pending = engine.pending_copies[0]
    engine._execute_copy(pending, collector=book(), market_store=ms, now_us=1_010_100_000)
    pos = engine.positions[(pending.strategy, pending.slug)]
    pos.settle(resolved_side='UP', at_us=1_300_000_000)
    engine._persist_position(pending.strategy, pos)
    assert store.session_summary(engine.session_id)['integrity']['ok'] is True
    with store.conn:
        store.conn.execute("UPDATE wallet_forward_rounds SET pnl='999'")
    assert any(i['reason'] == 'SETTLEMENT_PNL_MISMATCH'
               for i in store.reconcile_session(engine.session_id)['issues'])


def test_partial_maker_and_cancelled_remainder_reconcile(lab):
    ms, store, engine = lab; pos, order = maker(lab)
    assert engine.process_market_trades(ms, now_us=1_102_000_000) == 1
    assert store.reconcile_session(engine.session_id)['ok'] is True
    engine.process_market_trades(ms, now_us=1_300_000_000)
    assert order.status == 'CANCELLED'
    assert store.reconcile_session(engine.session_id)['ok'] is True


def test_nested_savepoint_failure_can_be_caught_without_leaking_writes(lab):
    ms, store, engine = lab; oid = engine.pending_copies[0].order_id
    with store.transaction():
        store.update_order_status(oid, status='CONFIRMED')
        with pytest.raises(OSError):
            with store.transaction():
                store.update_order_status(oid, status='FILLED')
                raise OSError('inner failure')
        assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0] == 'CONFIRMED'
    assert store.conn.execute('select status from wallet_forward_orders').fetchone()[0] == 'CONFIRMED'


def test_commit_failure_restores_memory_and_database(lab):
    ms, store, engine = lab
    raw_conn = store.conn
    class FailOuterRelease:
        target = None
        failed = False
        def __getattr__(self, name): return getattr(raw_conn, name)
        def __enter__(self): return raw_conn.__enter__()
        def __exit__(self, *args): return raw_conn.__exit__(*args)
        def execute(self, sql, *args):
            if sql.startswith('SAVEPOINT ') and self.target is None:
                self.target = sql.split()[-1]
            if sql == f'RELEASE SAVEPOINT {self.target}' and not self.failed:
                self.failed = True
                raise sqlite3.OperationalError('injected commit failure')
            return raw_conn.execute(sql, *args)
    store.conn = FailOuterRelease()
    p = engine.pending_copies[0]
    with pytest.raises(sqlite3.OperationalError, match='commit failure'):
        engine._execute_copy(p, collector=book(), market_store=ms, now_us=1_010_100_000)
    assert not engine.positions
    assert raw_conn.execute('select count(*) from wallet_forward_fills').fetchone()[0] == 0
    assert raw_conn.execute('select count(*) from wallet_forward_rounds').fetchone()[0] == 0
    assert raw_conn.in_transaction is False


def test_failed_quote_creation_leaves_no_ghost_orders(lab, monkeypatch):
    ms, store, engine = lab
    original = store.save_order
    def fail(*a, **kw): original(*a, **kw); raise OSError('injected quote failure')
    monkeypatch.setattr(store, 'save_order', fail)
    with pytest.raises(OSError):
        engine.refresh_strategy8(market=market(), collector=book(1_010_000_000),
            sample=None, now_us=1_010_000_000)
    assert not engine.maker_orders
    assert not engine.positions
    assert store.conn.execute("select count(*) from wallet_forward_orders where style='MAKER_GTC_PAPER'").fetchone()[0] == 0


@pytest.mark.parametrize('column,value,reason', [
    ('fee', '-1', 'INVALID_FILL'), ('notional', '999', 'FILL_NOTIONAL_MISMATCH'),
    ('token_id', 'alien', 'FILL_ORDER_IDENTITY_MISMATCH')])
def test_reconciliation_reports_invalid_fill_fields(lab, column, value, reason):
    ms, store, engine = lab
    engine._execute_copy(engine.pending_copies[0], collector=book(), market_store=ms,
                         now_us=1_010_100_000)
    with store.conn:
        store.conn.execute(f'UPDATE wallet_forward_fills SET {column}=?', (value,))
    assert any(i['reason'] == reason for i in store.reconcile_session(engine.session_id)['issues'])
