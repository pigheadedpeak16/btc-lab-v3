"""5.9.2a regressions for first-wallet-signal and stale dashboard session selection."""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from btc_v3.forensics import browser_dashboard
from btc_v3.forensics.forward_lab import STRATEGY_9A
from btc_v3.forensics.forward_lab import ForwardLabConfig, ForwardPaperEngine
from btc_v3.forensics.models import WalletTrade
from btc_v3.storage.market_store import MarketStore
from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.state.book_state import OrderBookState


def books(now_us: int, mode: str):
    def make(side, bids, asks):
        book = OrderBookState(slug='btc-updown-5m-1000', token_id=side.lower(), outcome=side)
        book.apply_snapshot(bids=[{'price':'0.49','size':str(bids)}],
                            asks=[{'price':'0.50','size':str(asks)}],
                            received_at_us=now_us, server_ts_us=now_us, book_hash='book')
        return book
    return SimpleNamespace(states={'up':make('UP',100,100), 'down':make('DOWN',100,100)})


def schedule(tmp_path, probability):
    initialize_project_databases(tmp_path)
    market = DiscoveredMarket(slug='btc-updown-5m-1000',event_id='e',market_id='m',
        condition_id='c',question='BTC',rules_text=None,round_start_us=1_000_000_000,
        round_end_us=1_300_000_000,up_token_id='up',down_token_id='down',
        active=True,closed=False,raw_event={},raw_market={})
    market_store = MarketStore.open_project(tmp_path)
    market_store.persist_market(market,1_000_000_000)
    store = ForwardWalletStore.open_project(tmp_path)
    store.start_session(session_id='paper',wallet='0x'+'1'*40,
        started_at_us=1_000_000_000,config={})
    trade = WalletTrade.from_api('0x'+'1'*40,{'timestamp':1001,'event_slug':market.slug,
        'token_id':'up','side':'BUY','outcome':'Up','price':'0.65','size':'19',
        'transaction_hash':'0xabc'})
    store.save_signal(signal_id='sig',session_id='paper',economic_key='e',source='trades',
        source_first_seen_us=1_010_000_000,trade_ts_us=trade.timestamp_us,
        slug=market.slug,token_id=trade.token_id,side=trade.side,outcome=trade.outcome,
        target_price='0.65',target_size='19',transaction_hash='0xabc',
        publication_lag_lower_us=0,publication_lag_upper_us=0,
        detection_sample_id=None,raw_json=trade.raw_json)
    engine = ForwardPaperEngine(session_id='paper',config=ForwardLabConfig(),store=store)
    engine.schedule_phase590_wallet_variants(signal_id='sig',row=trade,
        first_seen_us=1_010_000_000,lower_lag_us=0,
        market_store=market_store,detection_p_up_mc=probability)
    return market_store,store,engine,None
from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.forensics.store import ForensicsStore
from btc_v3.forensics.live import LiveTelemetrySample
from btc_v3.storage.db import initialize_project_databases


@pytest.mark.parametrize('side,missing_executable', [('BUY',False),('SELL',False),('BUY',True),('SELL',True)])
def test_execution_context_is_side_aware_and_handles_empty_books(tmp_path,side,missing_executable):
    market, store, engine, _ = schedule(tmp_path,.7)
    try:
        pending = next(p for p in engine.pending_copies if p.strategy == STRATEGY_9A)
        pending.side = side
        collector = books(1_010_100_000,'neutral')
        state = collector.states['up']
        state.min_order_size = Decimal('5')
        if missing_executable:
            # Simulate one-sided state while retaining the other side.
            if side == 'BUY':
                state.asks.clear()
            else:
                state.bids.clear()
        context = engine._copy_execution_context(pending,collector=collector,now_us=1_010_100_000)
        if missing_executable:
            assert context['top_min_order_notional'] is None
            assert context['executable_price'] is None
        else:
            expected = Decimal('0.50') if side=='BUY' else Decimal('0.49')
            assert Decimal(context['top_min_order_notional']) == Decimal('5') * expected
            assert Decimal(context['executable_price']) == expected
    finally:
        store.close(); market.close()


def test_first_wallet_signal_executes_and_is_audited(tmp_path):
    market, store, engine, _ = schedule(tmp_path,.7)
    try:
        pending = next(p for p in engine.pending_copies if p.strategy == STRATEGY_9A)
        engine.pending_copies = [pending]
        collector = books(1_010_100_000,'neutral')
        collector.states['up'].min_order_size = Decimal('5')
        pool = SimpleNamespace(collector_for_slug=lambda slug:collector)
        stats = engine.execute_due_copies(collector_pool=pool,market_store=market,now_us=1_010_100_000)
        assert stats.get(f'{STRATEGY_9A}:FILLED',0) == 1
        events = store.conn.execute('SELECT decision,context_json FROM wallet_strategy_decision_events WHERE strategy=? ORDER BY id',(STRATEGY_9A,)).fetchall()
        assert [e[0] for e in events] == ['ACCEPT','EXECUTED']
        assert 'top_min_order_notional' in events[-1][1]
        assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills WHERE strategy=?',(STRATEGY_9A,)).fetchone()[0] == 1
    finally:
        store.close();market.close()


def _seed_session(root, session_id, started_at_us, sample_at_us, state='RUNNING'):
    with ForwardWalletStore.open_project(root) as wallet:
        wallet.start_session(session_id=session_id,wallet='0xabc',started_at_us=started_at_us,config={})
        if state != 'RUNNING':
            wallet.finish_session(session_id=session_id,ended_at_us=started_at_us+1,state=state,source_stats={},summary={})
    with ForensicsStore.open_project(root) as forensic:
        forensic.start_live_session(session_id=session_id,wallet='0xabc',requested_wallet='0xabc',started_at_us=started_at_us,config={})
        if sample_at_us is not None:
            forensic.save_live_sample(LiveTelemetrySample(sample_id='sample_'+session_id,session_id=session_id,wallet='0xabc',sample_ts_us=sample_at_us,slug='test',round_start_us=None,round_end_us=None,seconds_remaining=None,state='READY',unavailable_reason=None,ptb=None,official_twap=None,coinbase_price=None,kraken_price=None,predictor_price=None,p_up_mc=None,p_up_analytic=None,distance_dollars=None,returns_json='{}',volatility_json='{}',lab_json='{}',book_json='{}'))


def test_dashboard_selects_newest_session_not_old_marked_running(tmp_path, monkeypatch):
    initialize_project_databases(tmp_path)
    now = 2_000_000_000
    monkeypatch.setattr(browser_dashboard.time,'time',lambda:now)
    _seed_session(tmp_path,'old',1_000_000_000,None,'RUNNING')
    _seed_session(tmp_path,'new',now*1_000_000-10_000_000,now*1_000_000-1_000_000,'RUNNING')
    status=browser_dashboard.collect(tmp_path)
    assert status['session_id'] == 'new'
    assert status['state'] == 'LIVE'
    assert status['raw_state'] == 'RUNNING'
    assert status['sample_age_seconds'] == 1.0
    assert browser_dashboard.strategy_detail(tmp_path,'S9A_LITERAL_MIRROR')['session_id'] == 'new'


def test_dashboard_marks_stale_running_session_not_live(tmp_path,monkeypatch):
    initialize_project_databases(tmp_path)
    now = 2_000_000_000
    monkeypatch.setattr(browser_dashboard.time,'time',lambda:now)
    _seed_session(tmp_path,'abandoned',now*1_000_000-100_000_000,now*1_000_000-90_000_000,'RUNNING')
    status=browser_dashboard.collect(tmp_path)
    assert status['state'] == 'STALE'
    assert status['raw_state'] == 'RUNNING'
    assert status['sample_age_seconds'] == 90.0


def test_dashboard_stopped_newest_not_hidden_behind_abandoned_older(tmp_path,monkeypatch):
    initialize_project_databases(tmp_path)
    now=2_000_000_000
    monkeypatch.setattr(browser_dashboard.time,'time',lambda:now)
    _seed_session(tmp_path,'old',now*1_000_000-300_000_000,now*1_000_000-260_000_000,'RUNNING')
    _seed_session(tmp_path,'new',now*1_000_000-150_000_000,now*1_000_000-120_000_000,'STOPPED')
    status=browser_dashboard.collect(tmp_path)
    assert status['session_id'] == 'new'
    assert status['state'] == 'STOPPED'


def test_terminal_status_is_truthful_and_preserves_disk_guard():
    import asyncio
    from btc_v3.forensics.forward_lab import _termination_state
    assert _termination_state(None) == 'STOPPED'
    assert _termination_state(KeyboardInterrupt()) == 'STOPPED'
    assert _termination_state(asyncio.CancelledError()) == 'STOPPED'
    assert _termination_state(RuntimeError('LOW_DISK_SPACE_GUARD: 7.8 GiB')) == 'GUARD_STOPPED'
    assert _termination_state(NameError("name 'best' is not defined")) == 'CRASHED'
