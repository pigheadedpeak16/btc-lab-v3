from btc_v3.experiments.hashing import config_hash
from btc_v3.experiments.tournament import comparison_cohort
from btc_v3.types import StrategyFamily


def test_comparison_cohort_has_all_seven_families_at_same_horizon():
    t = comparison_cohort(horizon_seconds=60, min_edge=.03)
    assert len(t.strategies) == 7
    assert {s.strategy_family for s in t.strategies} == set(StrategyFamily)
    assert {s.common.decision_seconds_before_close for s in t.strategies} == {60}


def test_configs_are_immutable_and_hash_distinct():
    t = comparison_cohort(horizon_seconds=60, min_edge=.03)
    hashes = [config_hash(s) for s in t.strategies]
    assert len(set(hashes)) == 7


def test_horizon_change_changes_config_hash():
    a = comparison_cohort(horizon_seconds=60)
    b = comparison_cohort(horizon_seconds=45)
    assert config_hash(a.strategies[1]) != config_hash(b.strategies[1])
