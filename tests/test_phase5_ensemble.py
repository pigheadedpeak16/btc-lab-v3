import pytest

from btc_v3.strategies.ensemble import EnsembleMember, logit_ensemble


def test_equal_weight_ensemble_of_equal_probabilities_is_identity():
    assert logit_ensemble((EnsembleMember("a", .7), EnsembleMember("b", .7))) == pytest.approx(.7)


def test_ensemble_stays_between_member_probabilities_for_positive_weights():
    p = logit_ensemble((EnsembleMember("a", .6, 1), EnsembleMember("b", .8, 2)))
    assert .6 < p < .8


def test_ensemble_rejects_empty_or_zero_weight():
    with pytest.raises(ValueError):
        logit_ensemble(())
    with pytest.raises(ValueError):
        logit_ensemble((EnsembleMember("a", .6, 0),))
