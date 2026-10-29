from btc_v3.config import CommonExperimentConfig, FairValueParams, StrategyConfig
from btc_v3.experiments.hashing import canonical_json, config_hash
from btc_v3.types import StrategyFamily


def make_config() -> StrategyConfig:
    return StrategyConfig(
        strategy_family=StrategyFamily.FAIR_VALUE,
        common=CommonExperimentConfig(decision_seconds_before_close=60),
        params=FairValueParams(),
    )


def test_hash_is_stable() -> None:
    a = make_config()
    b = make_config()
    assert canonical_json(a) == canonical_json(b)
    assert config_hash(a) == config_hash(b)


def test_hash_changes_when_config_changes() -> None:
    a = make_config()
    b = StrategyConfig(
        strategy_family=StrategyFamily.FAIR_VALUE,
        common=CommonExperimentConfig(decision_seconds_before_close=45),
        params=FairValueParams(),
    )
    assert config_hash(a) != config_hash(b)
