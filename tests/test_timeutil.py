from datetime import UTC, datetime

import pytest

from btc_v3.timeutil import from_epoch_us, to_epoch_us


def test_epoch_round_trip() -> None:
    value = datetime(2026, 10, 4, 12, 34, 56, 123456, tzinfo=UTC)
    assert from_epoch_us(to_epoch_us(value)) == value


def test_naive_datetime_rejected() -> None:
    with pytest.raises(ValueError):
        to_epoch_us(datetime(2026, 10, 4, 12, 0, 0))
