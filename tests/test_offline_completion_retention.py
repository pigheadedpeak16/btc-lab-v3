from dataclasses import asdict
from decimal import Decimal
import pytest
from tests.test_stage1_integrity import lab
from btc_v3.forensics import forward_lab as f


def test_evicted_settled_position_cannot_reopen_on_late_callback(lab):
    ms,store,engine=lab
    pending=engine.pending_copies.pop()
    engine.pending_copies.clear()
    p=engine.position_by_slug(pending.strategy,pending.slug,ms)
    p.record_fill(outcome='UP',side='BUY',shares=Decimal(5),notional=Decimal(2),fee=Decimal('.1'))
    p.settle(resolved_side='UP',at_us=p.round_end_us)
    engine._persist_position(pending.strategy,p)
    before=asdict(p)
    engine.positions.clear()  # Simulates bounded cache eviction or a new engine.
    restored=engine.position_by_slug(pending.strategy,pending.slug,ms)
    assert asdict(restored)==before
    with pytest.raises(ValueError,match='POSITION_SETTLED'):
        restored.record_fill(outcome='UP',side='BUY',shares=Decimal(1),notional=Decimal('.4'),fee=Decimal(0))
    assert restored.settle(resolved_side='DOWN',at_us=p.round_end_us+1)==Decimal('2.9')


def test_retention_discards_only_durably_finalized_cache(lab):
    ms,store,engine=lab
    p=engine.position_by_slug('settled',engine.pending_copies[0].slug,ms)
    p.settle(resolved_side='DOWN',at_us=p.round_end_us)
    engine._persist_position('settled',p)
    engine.positions[('unpersisted',p.slug)]=f.RoundPosition(p.slug,p.round_start_us,p.round_end_us)
    engine.positions[('unpersisted',p.slug)].settle(resolved_side='UP',at_us=p.round_end_us)
    engine.prune_memory(now_us=p.round_end_us+600_000_001)
    assert ('settled',p.slug) not in engine.positions
    assert ('unpersisted',p.slug) in engine.positions
    assert store.conn.execute('SELECT COUNT(*) FROM wallet_forward_rounds WHERE strategy=?',('settled',)).fetchone()[0]==1


def test_uncached_open_position_requires_reconciliation_instead_of_reset(lab):
    ms,store,engine=lab
    pending=engine.pending_copies[0]
    p=engine.position_by_slug(pending.strategy,pending.slug,ms)
    engine._persist_position(pending.strategy,p)
    engine.positions.clear()
    with pytest.raises(ValueError,match='POSITION_STATE_RECOVERY_REQUIRED'):
        engine.position_by_slug(pending.strategy,pending.slug,ms)


def test_hundreds_of_settled_rounds_remain_durable_with_bounded_cache(lab):
    ms,store,engine=lab;engine.pending_copies.clear()
    for i in range(300):
        start=(1000+i*300)*1_000_000;slug=f'btc-updown-5m-{start//1_000_000}'
        p=f.RoundPosition(slug,start,start+300_000_000)
        p.record_fill(outcome='UP',side='BUY',shares=Decimal(5),notional=Decimal(2),fee=Decimal('.1'))
        p.settle(resolved_side='UP' if i%2 else 'DOWN',at_us=p.round_end_us)
        engine.positions[('s',slug)]=p;engine._persist_position('s',p)
        engine.prune_memory(now_us=p.round_end_us+1)
        assert len(engine.positions)<=3
    rows=store.conn.execute('SELECT COUNT(*),SUM(CAST(pnl AS REAL)) FROM wallet_forward_rounds WHERE strategy=?',('s',)).fetchone()
    assert rows[0]==300 and rows[1]==pytest.approx(120)
    last=engine.positions.copy()
    engine.positions[('unsettled','unknown')]=f.RoundPosition('unknown',0,1)
    with pytest.raises(RuntimeError,match='PAPER_RESOURCE_STATE_LIMIT'):engine.prune_memory(now_us=1,limit=2)
    assert engine.positions[('unsettled','unknown')].pnl is None
