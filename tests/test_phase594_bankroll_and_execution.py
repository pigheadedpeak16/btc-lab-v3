"""Phase 5.9.4 paper-only, strict execution and account simulation tests."""
from decimal import Decimal
from dataclasses import replace
from types import SimpleNamespace

from btc_v3.forensics.forward_lab import (
    ForwardLabConfig, STRATEGY_9_EDGE_BOOK_V2, STRATEGY_9_EDGE_BOOK_V1,
    PHASE590_WALLET_STRATEGIES, PHASE_VERSION,
)
from btc_v3.strategies.paper_bankroll import PaperSessionBudget
from btc_v3.forensics.models import WalletTrade
from tests.test_phase593_edge_book import _session, _book


def test_new_phase_separate_from_legacy_controls():
    assert PHASE_VERSION == "5.9.5" # original signal controls remain frozen in the new execution cohort
    assert len(PHASE590_WALLET_STRATEGIES) == 24
    assert STRATEGY_9_EDGE_BOOK_V2 not in PHASE590_WALLET_STRATEGIES
    assert STRATEGY_9_EDGE_BOOK_V1 not in PHASE590_WALLET_STRATEGIES


def test_bankroll_commit_protects_cash_and_nightly_budget():
    b = PaperSessionBudget()
    assert b.commit(Decimal("4")) is None
    assert b.commit(Decimal("5")) is None
    assert b.check(Decimal("2")) == "BANKROLL_NIGHTLY_BUDGET"
    assert b.cash == Decimal("41")
    assert b.unsettled_commitment == Decimal("9")
    b.settle(cost=Decimal("9"), payout=Decimal("0"))
    assert b.settled_pnl == Decimal("-9")
    assert b.cash == Decimal("41")
    assert b.check(Decimal("2")) == "BANKROLL_NIGHTLY_BUDGET"


def test_bankroll_invalid_or_lifetime_risk_rejected():
    b = PaperSessionBudget()
    assert b.check(Decimal("NaN")) == "BANKROLL_INVALID_COST"
    assert b.check(Decimal("0")) == "BANKROLL_INVALID_COST"
    b.settled_pnl = Decimal("-25")
    assert b.check(Decimal("1")) == "BANKROLL_LOSS_CIRCUIT_OPEN"


def test_v2_upsize_small_wallet_signal_and_record_true_order_size(tmp_path):
    ms, store, engine, _ = _session(tmp_path, size='2')
    try:
        engine.config = replace(engine.config, strict_market_minimum=True)
        trade = WalletTrade.from_api('0x'+'1'*40, {
            'timestamp':1001, 'event_slug':'btc-updown-5m-1000',
            'token_id':'up','side':'BUY','outcome':'Up','price':'.50','size':'2',
            'transaction_hash':'0x593',
        })
        assert engine.schedule_phase594_edge_v2(
            signal_id='s', row=trade, first_seen_us=1_010_000_000,
            lower_lag_us=0, detection_p_up_mc=.95,
        ) == 'SCHEDULED'
        pending = [p for p in engine.pending_copies if p.strategy == STRATEGY_9_EDGE_BOOK_V2][0]
        state = _book(1_010_100_000, 'UP', 'up')
        collector = SimpleNamespace(states={'up':state,'down':_book(1_010_100_000,'DOWN','down')})
        assert engine._execute_copy(pending, collector=collector, market_store=ms,now_us=1_010_100_000)=='FILLED'
        order = store.conn.execute('SELECT shares FROM wallet_forward_orders WHERE order_id=?',(pending.order_id,)).fetchone()
        assert Decimal(str(order[0])) == Decimal('5')
        assert engine.edge_v2_bankroll.committed_this_session > Decimal('2.5')
    finally:
        store.close();ms.close()


def test_strict_mode_rejects_invalid_legacy_submission_without_fill(tmp_path):
    ms, store, engine, result = _session(tmp_path,size='2')
    try:
        engine.config=replace(engine.config, strict_market_minimum=True)
        pending=engine.pending_copies[0]
        collector=SimpleNamespace(states={'up':_book(1_010_100_000,'UP','up'), 'down':_book(1_010_100_000,'DOWN','down')})
        assert engine._execute_copy(pending,collector=collector,market_store=ms,now_us=1_010_100_000)=='SUBMITTED_BELOW_MARKET_MINIMUM'
        assert store.conn.execute('SELECT count(*) FROM wallet_forward_fills WHERE order_id=?',(pending.order_id,)).fetchone()[0]==0
    finally:
        store.close();ms.close()


def test_v2_rejects_high_price_or_unprofitable_trade(tmp_path):
    ms,store,engine,_=_session(tmp_path, size='2')
    try:
        trade=WalletTrade.from_api('0x'+'1'*40, {
            'timestamp':1001,'event_slug':'btc-updown-5m-1000','token_id':'up',
            'side':'BUY','outcome':'Up','price':'.50','size':'2','transaction_hash':'0x593',
        })
        engine.schedule_phase594_edge_v2(signal_id='s',row=trade,first_seen_us=1_010_000_000,lower_lag_us=0,detection_p_up_mc=.60)
        pending=[p for p in engine.pending_copies if p.strategy==STRATEGY_9_EDGE_BOOK_V2][0]
        collector=SimpleNamespace(states={'up':_book(1_010_100_000,'UP','up'), 'down':_book(1_010_100_000,'DOWN','down')})
        assert engine._execute_copy(pending,collector=collector,market_store=ms,now_us=1_010_100_000)=='EDGE_INSUFFICIENT_MARGIN'
        assert engine.edge_v2_bankroll.committed_this_session==0
    finally:
        store.close();ms.close()
