from dataclasses import replace
from types import SimpleNamespace
import pytest

from btc_v3.forensics import forward_lab as f
from btc_v3 import cli
from tests.test_stage1_integrity import lab,collector
from tests.test_phase4_features import market


def churn(engine,store,ms,*,cycles=6):
    for i in range(cycles):
        at=1_010_100_000+i*300_000;c=collector(at)
        engine.refresh_strategy8(market=market(),collector=c,sample=None,now_us=at)
        for state in c.states.values():state.mark_suspect('TEST_DEPTH_LOSS')
        pool=SimpleNamespace(collector_for_slug=lambda slug:c)
        engine.process_market_trades(market_store=ms,now_us=at+1000,collector_pool=pool)
    return store.conn.execute("SELECT COUNT(*) FROM wallet_forward_orders WHERE strategy=?",(f.STRATEGY_8,)).fetchone()[0]


def test_legacy_short_recovery_bursts_reproduce_cancel_recreate_without_fills(lab):
    ms,store,engine=lab
    assert churn(engine,store,ms)>6
    assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills WHERE strategy=?',(f.STRATEGY_8,)).fetchone()[0]==0
    assert store.conn.execute("SELECT COUNT(*) FROM wallet_forward_orders WHERE strategy=? AND status!='CANCELLED'",(f.STRATEGY_8,)).fetchone()[0]==0


def test_disabled_operational_maker_policy_writes_no_churn(lab):
    ms,store,engine=lab
    assert hasattr(engine.config,'s8_research_policy'),'Missing explicit operational S8 exclusion policy'
    engine.config=replace(engine.config,s8_research_policy='DISABLED_V1')
    assert churn(engine,store,ms)==0


def test_operational_cli_excludes_s8_until_explicit_research_opt_in():
    args=cli.build_parser().parse_args(['forward-wallet-lab'])
    assert getattr(args,'s8_research_policy',None)=='DISABLED_V1'


def test_maker_churn_diagnostics_match_committed_cancellations(lab):
    ms,store,engine=lab;count=churn(engine,store,ms)
    assert hasattr(engine,'maker_cancellations'),'Missing bounded live maker cancellation diagnostics'
    assert sum(engine.maker_cancellations.values())==count
