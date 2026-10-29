from btc_v3.config import StrategyLabConfig
from btc_v3.experiments.search_space import baseline_search_spaces
from btc_v3.experiments.tournament import comparison_matrix


def test_strategy_lab_default_horizons_are_unique_and_descending():
    cfg = StrategyLabConfig()
    assert cfg.decision_horizons_seconds == (240, 180, 120, 60, 30, 15)


def test_search_spaces_cover_all_seven_baseline_families():
    spaces = baseline_search_spaces()
    assert len(spaces) == 7
    assert len({space.family for space in spaces}) == 7
    assert all(space.dimensions for space in spaces)


def test_comparison_matrix_never_mixes_horizons():
    matrix = comparison_matrix()
    assert len(matrix) == 6
    for cohort in matrix:
        horizons = {s.common.decision_seconds_before_close for s in cohort.strategies}
        assert len(horizons) == 1
        assert len(cohort.strategies) == 7
