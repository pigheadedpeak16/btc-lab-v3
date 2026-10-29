import sqlite3
from pathlib import Path

from btc_v3.config import CommonExperimentConfig, FairValueParams, StrategyConfig, TournamentConfig
from btc_v3.experiments.registry import load_experiment_config, register_experiment
from btc_v3.storage.db import initialize_project_databases
from btc_v3.types import StrategyFamily


def make_tournament(horizon: int = 60) -> TournamentConfig:
    strategy = StrategyConfig(
        strategy_family=StrategyFamily.FAIR_VALUE,
        common=CommonExperimentConfig(decision_seconds_before_close=horizon),
        params=FairValueParams(),
    )
    return TournamentConfig(experiment_name="phase1-test", strategies=(strategy,))


def test_same_frozen_config_resumes_same_experiment(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    cfg = make_tournament()
    first = register_experiment(paths["forward"], cfg)
    second = register_experiment(paths["forward"], cfg)
    assert first == second
    assert load_experiment_config(paths["forward"], first) == cfg


def test_changed_config_creates_new_experiment(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    first = register_experiment(paths["forward"], make_tournament(60))
    second = register_experiment(paths["forward"], make_tournament(45))
    assert first != second

    with sqlite3.connect(paths["forward"]) as conn:
        count = conn.execute("SELECT COUNT(*) FROM experiments").fetchone()[0]
    assert count == 2
