"""New 5.9.2 prospective router and dashboard accounting regressions."""
from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from btc_v3.forensics.browser_dashboard import collect, compute_metrics, strategy_detail
from btc_v3.forensics.forward_lab import (
    ForwardLabConfig, ForwardPaperEngine, STRATEGY_9_BOOK_MODEL60,
    STRATEGY_9_BOOK_MODEL65, STRATEGY_9_MODEL60, STRATEGY_9_MODEL65,
    STRATEGY_9_PRICE60_80, STRATEGY_9_ROUTER_V1,
)
from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.forensics.models import WalletTrade
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore
from btc_v3.state.book_state import OrderBookState


class R(dict):
    def __getitem__(self, key):
        return dict.__getitem__(self,key)


def test_dashboard_market_level_accounting_and_unsettled_exclusion():
    rows = [
        R(strategy='S9_MODEL60',slug='m1',pnl='10',fees='1',up_cost='35',down_cost='15',round_end_us=1),
        R(strategy='S9_MODEL60',slug='m2',pnl='-5',fees='2',up_cost='30',down_cost='20',round_end_us=2),
        R(strategy='S9_MODEL60',slug='m3',pnl=None,fees='10',up_cost='40',down_cost='0',round_end_us=3),
        R(strategy='S5_BASE_H240_BOOK_IMBALANCE',slug='m4',pnl='0',fees='0',up_cost='5',down_cost='5',round_end_us=4),
    ]
    fills = [R(strategy='S9_MODEL60',fills=11),R(strategy='S5_BASE_H240_BOOK_IMBALANCE',fills=2)]
    by = {r['strategy']:r for r in compute_metrics(rows,fills)}
    x = by['S9_MODEL60']
    assert (x['markets'],x['wins'],x['losses'],x['unsettled'],x['fills']) == (2,1,1,1,11)
    assert x['pnl'] == 5 and x['notional'] == 100 and x['roi_pct'] == 5
    assert x['fees'] == 3 and x['max_dd'] == -5
    assert by['S5_BASE_H240_BOOK_IMBALANCE']['breakeven'] == 1
    assert by['S5_BASE_H240_BOOK_IMBALANCE']['group'] == 'normal'


def _market(tmp_path):
    from btc_v3.feeds.polymarket import DiscoveredMarket
    return DiscoveredMarket(
        slug='btc-updown-5m-1000',event_id='e',market_id='m',condition_id='c',question='BTC',rules_text=None,
        round_start_us=1_000_000_000,round_end_us=1_300_000_000,
        up_token_id='up',down_token_id='down',active=True,closed=False,raw_event={},raw_market={},
    )


def schedule(tmp_path, prob, price='0.65', outcome='UP'):
    initialize_project_databases(tmp_path)
    market = _market(tmp_path)
    market_store = MarketStore.open_project(tmp_path)
    store = ForwardWalletStore.open_project(tmp_path)
    market_store.persist_market(market,1000_000_000)
    store.start_session(session_id='paper',wallet='0x'+'1'*40,started_at_us=1000_000_000,config={})
    row = WalletTrade.from_api('0x'+'1'*40,{'timestamp':1001,'event_slug':market.slug,
        'token_id':'up' if outcome=='UP' else 'down','side':'BUY','outcome':outcome.title(),
        'price':price,'size':'19','transaction_hash':'0xabc'})
    store.save_signal(signal_id='sig',session_id='paper',economic_key='e',source='trades',
        source_first_seen_us=1010_000_000,trade_ts_us=row.timestamp_us,slug=market.slug,
        token_id=row.token_id,side=row.side,outcome=row.outcome,target_price=price,target_size='19',
        transaction_hash='0xabc',publication_lag_lower_us=0,publication_lag_upper_us=0,
        detection_sample_id=None,raw_json=row.raw_json)
    engine = ForwardPaperEngine(session_id='paper',config=ForwardLabConfig(),store=store)
    decisions = engine.schedule_phase590_wallet_variants(signal_id='sig',row=row,
        first_seen_us=1010_000_000,lower_lag_us=0,market_store=market_store,detection_p_up_mc=prob)
    return market_store,store,engine,decisions


def test_new_model_and_price_arms_freeze_causally(tmp_path):
    market, store, engine, decisions = schedule(tmp_path, 0.64)
    try:
        assert decisions[STRATEGY_9_MODEL60] == 'SCHEDULED'
        assert decisions[STRATEGY_9_MODEL65] == 'MODEL_BELOW_65_OR_MISSING'
        assert decisions[STRATEGY_9_BOOK_MODEL60] == 'SCHEDULED'
        assert decisions[STRATEGY_9_BOOK_MODEL65] == 'MODEL_BELOW_65_OR_MISSING'
        assert decisions[STRATEGY_9_PRICE60_80] == 'SCHEDULED'
        assert decisions[STRATEGY_9_ROUTER_V1] == 'SCHEDULED'
        events = store.conn.execute('SELECT COUNT(*) FROM wallet_strategy_decision_events WHERE session_id=?',('paper',)).fetchone()[0]
        assert events == 24
        assert len(engine.pending_copies) > 0
    finally:
        store.close();market.close()


def test_new_arms_do_not_backfill_missing_model(tmp_path):
    market, store, engine, decisions = schedule(tmp_path, None, price='0.35')
    try:
        assert decisions[STRATEGY_9_MODEL60] == 'MODEL_BELOW_60_OR_MISSING'
        assert decisions[STRATEGY_9_BOOK_MODEL60] == 'MODEL_BELOW_60_OR_MISSING'
        assert decisions[STRATEGY_9_PRICE60_80] == 'PRICE_OUTSIDE_60_80'
        assert decisions[STRATEGY_9_ROUTER_V1] == 'ROUTER_NO_PRIMARY_OR_FALLBACK'
    finally:
        store.close();market.close()


def test_down_signal_uses_complementary_probability(tmp_path):
    market, store, engine, decisions = schedule(tmp_path, .35, outcome='DOWN')
    try:
        assert decisions[STRATEGY_9_MODEL60] == 'SCHEDULED'
        assert decisions[STRATEGY_9_MODEL65] == 'SCHEDULED'
    finally:
        store.close();market.close()


def books(now_us, mode):
    def make(side,bids,asks):
        b=OrderBookState(slug='btc-updown-5m-1000',token_id=side.lower(),outcome=side)
        b.apply_snapshot(bids=[{'price':'0.49','size':str(bids)}],
                         asks=[{'price':'0.50','size':str(asks)}],
                         received_at_us=now_us,server_ts_us=now_us,book_hash='book')
        return b
    if mode=='positive':
        up,down=make('UP',100,10),make('DOWN',10,100)
    elif mode=='negative':
        up,down=make('UP',10,100),make('DOWN',100,10)
    else:
        up,down=make('UP',100,100),make('DOWN',100,100)
    return SimpleNamespace(states={'up':up,'down':down})


@pytest.mark.parametrize('mode,prob,expected,route',[
    ('positive',.7,'CONFIRMED','BOOK_MODEL60_PRIMARY'),
    ('neutral',.4,'CONFIRMED','PRICE60_80_NEUTRAL_OR_LOW_MODEL_FALLBACK'),
    ('negative',.9,'ROUTER_V1_OPPOSITE_BOOK_VETO',None),
])
def test_router_v1_primary_fallback_and_opposite_veto(tmp_path,mode,prob,expected,route):
    market, store, engine, decisions = schedule(tmp_path, prob, price='.65')
    try:
        pending = next(p for p in engine.pending_copies if p.strategy==STRATEGY_9_ROUTER_V1)
        result = engine._router_v1_decision(pending,collector=books(1010_100_000,mode),now_us=1010_100_000)
        assert result == expected
        if route:
            assert pending.router_stage == route
            assert pending.scheduled_at_us == 1010_125_000
            ev = store.conn.execute('SELECT decision FROM wallet_strategy_decision_events WHERE strategy=? ORDER BY id', (STRATEGY_9_ROUTER_V1,)).fetchall()
            assert [x[0] for x in ev] == ['ACCEPT','CONFIRMED']
        else:
            assert pending.confirmed_at_us is None
    finally:
        store.close();market.close()


def test_router_missing_book_is_not_neutral_fallback(tmp_path):
    market, store, engine, _ = schedule(tmp_path,.7,price='.65')
    try:
        pending=next(p for p in engine.pending_copies if p.strategy==STRATEGY_9_ROUTER_V1)
        assert engine._router_v1_decision(pending,collector=SimpleNamespace(states={}),now_us=1010_100_000) == 'ROUTER_V1_BOOK_UNAVAILABLE'
    finally:
        store.close();market.close()


def test_browser_status_and_detail_are_read_only(tmp_path):
    market, store, engine, decisions = schedule(tmp_path,.6)
    try:
        data=collect(tmp_path)
        assert data['paper_only'] is True
        assert data['raw_state']=='RUNNING'
        assert data['state']=='STALE'  # old synthetic 1970 test session is not a live heartbeat
        assert 'S9_ROUTER_V1' in {r['strategy'] for r in data['strategies']}
        assert strategy_detail(tmp_path,'S9_MODEL60')['rejections']==[]
        with pytest.raises(ValueError):
            strategy_detail(tmp_path,'../private')
    finally:
        store.close();market.close()
