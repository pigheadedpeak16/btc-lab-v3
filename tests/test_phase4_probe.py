from btc_v3.cli import _boundary_aware_probe_seconds

MICROS = 1_000_000


def test_probe_extends_through_next_boundary_when_current_ptb_may_be_missing() -> None:
    # Ten seconds after a five-minute boundary: 290s to next boundary + 75s safety/history.
    now_us = 310 * MICROS
    assert _boundary_aware_probe_seconds(now_us, 90) == 365.0


def test_probe_respects_longer_explicit_minimum() -> None:
    now_us = 310 * MICROS
    assert _boundary_aware_probe_seconds(now_us, 500) == 500.0


def test_probe_uses_next_boundary_even_when_started_exactly_on_boundary() -> None:
    now_us = 300 * MICROS
    assert _boundary_aware_probe_seconds(now_us, 90) == 375.0
