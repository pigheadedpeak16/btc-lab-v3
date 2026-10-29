from dataclasses import replace
from types import SimpleNamespace
import pytest
from tests.test_stage1_integrity import lab,collector
from tests.test_phase593_edge_book import _book
from btc_v3.forensics import forward_lab as f
from btc_v3.state.readiness import book_readiness
from btc_v3.storage.external_store import ExternalStore
from decimal import Decimal


def test_recent_receipt_does_not_refresh_stale_source(lab):
    ms,store,engine=lab;at=1_010_100_000;c=collector(at)
    c.states['up'].last_server_ts_us=at-8_000_000
    assert engine._execute_copy(engine.pending_copies[0],collector=c,market_store=ms,now_us=at)=='STALE_BOOK'
    assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_fills').fetchone()[0]==0


@pytest.mark.parametrize('source',[None,1_010_200_000])
def test_missing_or_noncausal_source_never_gets_an_execution_age(source):
    s=_book(1_010_000_000,'UP','up');s.last_server_ts_us=source
    assert s.age_seconds(1_011_000_000) is None


def test_both_age_clocks_are_checked_at_exact_threshold():
    s=_book(1_010_000_000,'UP','up');s.last_server_ts_us=1_009_000_000
    assert s.age_seconds(1_011_000_000)==2
    assert s.age_seconds(1_011_000_001)>2


def test_confirmation_cannot_bypass_other_side_invalidated_before_fill(lab):
    ms,store,engine=lab;at=1_010_100_000;c=collector(at)
    p=replace(engine.pending_copies[0],strategy=f.STRATEGY_9_BOOK_CONFIRM_75,require_book_confirmation=True,confirmed_at_us=at-25_000)
    c.states['down'].mark_unsynchronized()
    assert engine._execute_copy(p,collector=c,market_store=ms,now_us=at)=='PAIRED_BOOK_UNAVAILABLE'


def test_readiness_uses_same_source_freshness_as_execution():
    at=1_010_000_000;c=collector(at);c.states['up'].last_server_ts_us=at-3_000_000
    assert book_readiness(c.market,c.states['up'],'UP',at)['reason']=='DEPTH_STALE'


def test_book_strategy_tournament_revalidates_both_sides(lab):
    ms,store,engine=lab;at=1_010_100_000;c=collector(at)
    with ExternalStore.open_project(ms.path.parents[1]) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,paper_engine=engine,external_store=external,config=engine.config)
        t._schedule(strategy='S5_BASE_H180_BOOK_IMBALANCE',cfg=f._forward_common(horizon=180,wallet_informed=False),
            market=c.market,outcome='UP',ask=Decimal('.50'),now_us=at-100_000,metadata={})
        c.states['down'].mark_unsynchronized()
        result=t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:c),market_store=ms,now_us=at)
        assert result=={'PAIRED_BOOK_UNAVAILABLE':1}
