from __future__ import annotations

from datetime import UTC, datetime

MICROS_PER_SECOND = 1_000_000
MILLIS_PER_SECOND = 1_000


def utc_now() -> datetime:
    return datetime.now(UTC)


def now_epoch_us() -> int:
    return to_epoch_us(utc_now())


def to_epoch_us(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return int(value.astimezone(UTC).timestamp() * MICROS_PER_SECOND)


def from_epoch_us(value: int) -> datetime:
    return datetime.fromtimestamp(value / MICROS_PER_SECOND, tz=UTC)


def iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def epoch_us_to_iso(value: int | None) -> str:
    if value is None:
        return "-"
    return iso_utc(from_epoch_us(value))


def parse_source_timestamp_us(value: object) -> int | None:
    """Parse common Polymarket source timestamp encodings into epoch microseconds."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return to_epoch_us(value)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            numeric = float(text)
        except ValueError:
            normalized = text.replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(normalized)
            except ValueError:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return to_epoch_us(dt)

    absolute = abs(numeric)
    if absolute >= 1e17:  # nanoseconds
        return int(numeric / 1_000)
    if absolute >= 1e14:  # microseconds
        return int(numeric)
    if absolute >= 1e11:  # milliseconds
        return int(numeric * 1_000)
    return int(numeric * MICROS_PER_SECOND)
