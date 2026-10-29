"""Phase 5.9.3 policy and real engine integration regressions (paper only)."""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from btc_v3.strategies.edge_gate import EdgeGateConfig, assess_executable_edge
from btc_v3.forensics.forward_lab import (
    ForwardLabConfig, ForwardPaperEngine, STRATEGY_9_EDGE_BOOK_V1,
    STRATEGY_9_BOOK_CONFIRM_75, PHASE590_WALLET_STRATEGIES,
    phase591_config_hash,
)
from btc_v3.forensics.models import WalletTrade
from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore
from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.state.book_state import OrderBookState


def check(p=.90, submitted='20', filled='20', price='0.50', fee='0.20', min_size='5', side='BUY'):
    return assess_executable_edge(
        side=side, raw_probability=p,
        submitted_shares=Decimal(submitted), filled_shares=Decimal(filled),
        effective_price=Decimal(price), total_fee=Decimal(fee),
        market_min_order_size=None if min_size is None else Decimal(min_size),
        config=EdgeGateConfig(),
    )


def test_edge_positive_and_auditable():
    result = check()
    assert result.accepted
    assert result.reason == 'EDGE_ACCEPT'
    assert result.net_edge_per_share == pytest.approx(.90-.08-.50-.01-.02)
    assert result.to_context()['market_min_order_size'] == 5


@pytest.mark.parametrize('change,reason',[
    ({'p':None}, 'EDGE_MODEL_MISSING_OR_INVALID'),
    ({'p':float('nan')}, 'EDGE_MODEL_MISSING_OR_INVALID'),
    ({'min_size':None}, 'EDGE_MIN_ORDER_UNKNOWN'),
    ({'submitted':'4'}, 'EDGE_SUBMITTED_BELOW_MINIMUM'),
    ({'filled':'4'}, 'EDGE_LIQUIDITY_BELOW_MINIMUM'),
    ({'price':'0.82'}, 'EDGE_INSUFFICIENT_MARGIN'),
    ({'side':'SELL'}, 'EDGE_BUY_ONLY'),
])
def test_edge_fail_closed(change,reason):
    assert check(**change).reason == reason


def _book(now, outcome, token, ask_size=100):
    state = OrderBookState(slug='btc-updown-5m-1000', token_id=token, outcome=outcome)
    state.apply_snapshot(
        bids=[{'price':'.49','size':'100'}],
        asks=[{'price':'.50','size':str(ask_size)}],
        received_at_us=now, server_ts_us=now, book_hash='x',
        min_order_size='5',
        tick_size='.01',
    )
    return state


def _session(tmp_path, probability=.90, side='BUY', size='20'):
    initialize_project_databases(tmp_path)
    market = DiscoveredMarket(
        slug='btc-updown-5m-1000', event_id='e', market_id='m', condition_id='c',
        question='btc', rules_text=None, round_start_us=1_000_000_000,
        round_end_us=1_300_000_000, up_token_id='up', down_token_id='down',
        active=True, closed=False, raw_event={}, raw_market={},
    )
    market_store = MarketStore.open_project(tmp_path)
    market_store.persist_market(market, 1_000_000_000)
    store = ForwardWalletStore.open_project(tmp_path)
    store.start_session(session_id='p593',wallet='0x'+'1'*40,
                        started_at_us=1_000_000_000,config={})
    trade=WalletTrade.from_api('0x'+'1'*40,{
        'timestamp':1001,'event_slug':market.slug,'token_id':'up',
        'side':side,'outcome':'Up','price':'.50','size':size,
        'transaction_hash':'0x593',
    })
    store.save_signal(
        signal_id='s',session_id='p593',economic_key='e',source='trades',
        source_first_seen_us=1_010_000_000, trade_ts_us=trade.timestamp_us,
        slug=market.slug, token_id=trade.token_id, side=trade.side,
        outcome=trade.outcome, target_price='.50', target_size=size,
        transaction_hash='0x593', publication_lag_lower_us=0,
        publication_lag_upper_us=0, detection_sample_id=None, raw_json=trade.raw_json,
    )
    engine=ForwardPaperEngine(session_id='p593',config=ForwardLabConfig(),store=store)
    result=engine.schedule_phase593_edge(
        signal_id='s',row=trade,first_seen_us=1_010_000_000,
        lower_lag_us=0,detection_p_up_mc=probability,
    )
    return market_store,store,engine,result


def test_old_24_arm_registry_and_hash_preserved_as_control():
    assert len(PHASE590_WALLET_STRATEGIES) == 24
    assert STRATEGY_9_BOOK_CONFIRM_75 in PHASE590_WALLET_STRATEGIES
    assert STRATEGY_9_EDGE_BOOK_V1 not in PHASE590_WALLET_STRATEGIES
    assert phase591_config_hash(ForwardLabConfig()) != phase591_config_hash(ForwardLabConfig(edge_v1_min_net_edge=.05))


def test_new_arm_schedules_only_after_causal_model(tmp_path):
    ms,store,engine,result=_session(tmp_path,probability=None)
    try:
        assert result == 'EDGE_CAUSAL_MODEL_MISSING'
        assert not engine.pending_copies
        row=store.conn.execute('SELECT decision, reason FROM wallet_strategy_decision_events WHERE strategy=?',(STRATEGY_9_EDGE_BOOK_V1,)).fetchone()
        assert tuple(row)==('REJECT','EDGE_CAUSAL_MODEL_MISSING')
    finally:
        store.close();ms.close()


def test_new_arm_never_paper_fills_sell(tmp_path):
    ms,store,engine,result=_session(tmp_path,side='SELL')
    try:
        assert result == 'EDGE_BUY_ONLY'
        assert not engine.pending_copies
    finally:
        store.close();ms.close()


def test_new_arm_rejects_nonexecutable_partial_fill_without_ledger_credit(tmp_path):
    ms,store,engine,result=_session(tmp_path)
    try:
        assert result=='SCHEDULED'
        pending=engine.pending_copies[0]
        up=_book(1_010_100_000,'UP','up',ask_size=4)
        down=_book(1_010_100_000,'DOWN','down')
        collector=SimpleNamespace(states={'up':up,'down':down})
        reason=engine._execute_copy(pending, collector=collector, market_store=ms,
                                    now_us=1_010_100_000)
        assert reason=='EDGE_LIQUIDITY_BELOW_MINIMUM'
        assert store.conn.execute('SELECT count(*) FROM wallet_forward_fills WHERE strategy=?',(STRATEGY_9_EDGE_BOOK_V1,)).fetchone()[0]==0
    finally:
        store.close();ms.close()


def test_new_arm_executes_positive_causal_buy_and_records_edge(tmp_path):
    ms,store,engine,result=_session(tmp_path)
    try:
        pending=engine.pending_copies[0]
        up=_book(1_010_100_000,'UP','up')
        down=_book(1_010_100_000,'DOWN','down')
        collector=SimpleNamespace(states={'up':up,'down':down})
        reason=engine._execute_copy(pending,collector=collector,market_store=ms,now_us=1_010_100_000)
        assert reason=='FILLED'
        record=store.conn.execute('SELECT metadata_json FROM wallet_forward_fills WHERE strategy=?',(STRATEGY_9_EDGE_BOOK_V1,)).fetchone()
        assert record is not None
        import json
        payload=json.loads(record[0])
        assert payload['confirmation']['phase593_edge_gate']['accepted'] is True
        assert payload['filled_below_market_min_order_size'] is False
    finally:
        store.close();ms.close()


def test_new_arm_requires_book_confirmation_before_exec(tmp_path):
    ms,store,engine,result=_session(tmp_path)
    try:
        collector=SimpleNamespace(states={
            'up':_book(1_010_075_000,'UP','up'),
            'down':_book(1_010_075_000,'DOWN','down'),
        })
        pool=SimpleNamespace(collector_for_slug=lambda slug:collector)
        # Neutral L2 fails book agreement; edge model must not override it.
        stats=engine.execute_due_copies(collector_pool=pool,market_store=ms,
                                        now_us=1_010_075_000)
        assert stats[f'{STRATEGY_9_EDGE_BOOK_V1}:BOOK_DIRECTION_DISAGREES']==1
        assert not engine.pending_copies
        assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills WHERE strategy=?',
                                  (STRATEGY_9_EDGE_BOOK_V1,)).fetchone()[0]==0
    finally:
        store.close();ms.close()


def test_new_arm_book_confirmation_then_delayed_edge_execution(tmp_path):
    ms,store,engine,result=_session(tmp_path)
    try:
        up=_book(1_010_075_000,'UP','up',ask_size=10)
        down=_book(1_010_075_000,'DOWN','down',ask_size=100)
        up.bids[Decimal('.49')]=Decimal('100')
        down.bids[Decimal('.49')]=Decimal('10')
        collector=SimpleNamespace(states={'up':up,'down':down})
        pool=SimpleNamespace(collector_for_slug=lambda slug:collector)
        stats=engine.execute_due_copies(collector_pool=pool,market_store=ms,now_us=1_010_075_000)
        assert stats[f'{STRATEGY_9_EDGE_BOOK_V1}:CONFIRMED']==1
        assert engine.pending_copies[0].scheduled_at_us == 1_010_100_000
        assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills WHERE strategy=?',
                                  (STRATEGY_9_EDGE_BOOK_V1,)).fetchone()[0]==0
        stats=engine.execute_due_copies(collector_pool=pool,market_store=ms,now_us=1_010_100_000)
        assert stats[f'{STRATEGY_9_EDGE_BOOK_V1}:PARTIAL']==1
        assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills WHERE strategy=?',
                                  (STRATEGY_9_EDGE_BOOK_V1,)).fetchone()[0]==1
    finally:
        store.close();ms.close()
