from btc_v3.models.calibration import LogisticCalibrator


def test_identity_calibrator_shape() -> None:
    c = LogisticCalibrator()
    assert abs(c.transform(0.2) - 0.2) < 1e-9
    assert abs(c.transform(0.8) - 0.8) < 1e-9


def test_train_only_calibrator_is_bounded() -> None:
    probs = [0.05, 0.15, 0.25, 0.40, 0.60, 0.75, 0.85, 0.95]
    labels = [0, 0, 0, 0, 1, 1, 1, 1]
    c = LogisticCalibrator.fit_train_only(probs, labels)
    assert c.fit_scope == "TRAIN_ONLY_REQUIRED"
    assert 0 <= c.transform(0.01) <= 1
    assert 0 <= c.transform(0.99) <= 1
    assert c.transform(0.8) > c.transform(0.2)
