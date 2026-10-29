from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

BTC_5M_SLUG_RE = re.compile(r"^btc-updown-5m-(\d+)$", re.IGNORECASE)
ETH_5M_SLUG_RE = re.compile(r"^eth-updown-5m-(\d+)$", re.IGNORECASE)
ROUND_SECONDS = 300


def pick(row: Mapping[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return default


def decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def text_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def normalize_outcome(value: Any) -> str | None:
    text = text_or_none(value)
    if text is None:
        return None
    upper = text.upper()
    if upper in {"UP", "YES"}:
        return "UP"
    if upper in {"DOWN", "NO"}:
        return "DOWN"
    return upper


def timestamp_us_from_row(row: Mapping[str, Any]) -> int | None:
    micros = pick(row, "timestamp_us", "timestamp_micros", "timestampMicros")
    if micros is not None:
        try:
            return int(micros)
        except (TypeError, ValueError):
            pass
    millis = pick(row, "timestamp_ms", "timestampMillis")
    if millis is not None:
        try:
            return int(millis) * 1_000
        except (TypeError, ValueError):
            pass
    seconds = pick(row, "timestamp", "time", "created_at", "createdAt")
    if seconds is None:
        return None
    try:
        numeric = Decimal(str(seconds))
    except (InvalidOperation, ValueError):
        return None
    # Epoch seconds are ~1e9.  Larger values are often milliseconds.
    if numeric >= Decimal("1000000000000"):
        return int(numeric * 1_000)
    return int(numeric * 1_000_000)


def parse_btc_5m_round_start_us(slug: str | None) -> int | None:
    if not slug:
        return None
    match = BTC_5M_SLUG_RE.match(slug)
    if match is None:
        return None
    return int(match.group(1)) * 1_000_000


def is_btc_5m_row(slug: str | None, event_slug: str | None, title: str | None) -> bool:
    for candidate in (slug, event_slug):
        if candidate and BTC_5M_SLUG_RE.match(candidate):
            return True
    if title:
        low = title.lower()
        return "bitcoin up or down" in low and ("5m" in low or "5 min" in low or "5-minute" in low)
    return False


def is_eth_5m_row(slug: str | None, event_slug: str | None, title: str | None) -> bool:
    for candidate in (slug, event_slug):
        if candidate and ETH_5M_SLUG_RE.match(candidate):
            return True
    if title:
        low = title.lower()
        return "ethereum up or down" in low and ("5m" in low or "5 min" in low or "5-minute" in low)
    return False


def is_crypto_5m_row(slug: str | None, event_slug: str | None, title: str | None) -> bool:
    return is_btc_5m_row(slug, event_slug, title) or is_eth_5m_row(slug, event_slug, title)


def canonical_json(value: Any) -> str:
    def default(obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        raise TypeError(f"not JSON serializable: {type(obj)!r}")

    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=default)


def stable_row_id(prefix: str, row: Mapping[str, Any]) -> str:
    payload = canonical_json(row).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()}"


@dataclass(frozen=True)
class WalletActivity:
    activity_id: str
    wallet: str
    event_type: str
    condition_id: str | None
    token_id: str | None
    side: str | None
    outcome: str | None
    price: Decimal | None
    size: Decimal | None
    usdc_size: Decimal | None
    timestamp_us: int | None
    transaction_hash: str | None
    title: str | None
    slug: str | None
    event_slug: str | None
    raw_json: str

    @property
    def is_trade(self) -> bool:
        return self.event_type.upper() == "TRADE"

    @property
    def is_btc_5m(self) -> bool:
        return is_btc_5m_row(self.slug, self.event_slug, self.title)

    @property
    def btc_round_start_us(self) -> int | None:
        for candidate in (self.slug, self.event_slug):
            parsed = parse_btc_5m_round_start_us(candidate)
            if parsed is not None:
                return parsed
        return None

    @classmethod
    def from_api(cls, wallet: str, row: Mapping[str, Any]) -> "WalletActivity":
        raw = dict(row)
        return cls(
            activity_id=stable_row_id("act", {"wallet": wallet.lower(), **raw}),
            wallet=wallet.lower(),
            event_type=str(pick(row, "type", "activity_type", "activityType", default="UNKNOWN")).upper(),
            condition_id=text_or_none(pick(row, "condition_id", "conditionId", "market")),
            token_id=text_or_none(pick(row, "token_id", "tokenId", "asset", "asset_id", "assetId")),
            side=(text_or_none(pick(row, "side")) or None),
            outcome=normalize_outcome(pick(row, "outcome", "outcome_label", "outcomeLabel")),
            price=decimal_or_none(pick(row, "price")),
            size=decimal_or_none(pick(row, "size", "tokens")),
            usdc_size=decimal_or_none(pick(row, "usdc_size", "usdcSize", "cash", "cash_usdc")),
            timestamp_us=timestamp_us_from_row(row),
            transaction_hash=text_or_none(pick(row, "transaction_hash", "transactionHash", "tx_hash", "txHash")),
            title=text_or_none(pick(row, "title", "question")),
            slug=text_or_none(pick(row, "slug", "market_slug", "marketSlug")),
            event_slug=text_or_none(pick(row, "event_slug", "eventSlug")),
            raw_json=canonical_json(raw),
        )


@dataclass(frozen=True)
class WalletTrade:
    trade_id: str
    wallet: str
    condition_id: str | None
    token_id: str | None
    side: str | None
    outcome: str | None
    price: Decimal | None
    size: Decimal | None
    timestamp_us: int | None
    transaction_hash: str | None
    title: str | None
    slug: str | None
    event_slug: str | None
    maker_address: str | None
    taker_address: str | None
    attribution: str | None
    raw_json: str

    @property
    def is_btc_5m(self) -> bool:
        return is_btc_5m_row(self.slug, self.event_slug, self.title)

    @property
    def is_eth_5m(self) -> bool:
        return is_eth_5m_row(self.slug, self.event_slug, self.title)

    @property
    def is_crypto_5m(self) -> bool:
        return is_crypto_5m_row(self.slug, self.event_slug, self.title)

    @classmethod
    def from_api(cls, wallet: str, row: Mapping[str, Any]) -> "WalletTrade":
        raw = dict(row)
        maker = text_or_none(pick(row, "maker_address", "makerAddress", "maker"))
        taker = text_or_none(pick(row, "taker_address", "takerAddress", "taker"))
        attribution = text_or_none(pick(row, "attribution", "liquidity_role", "liquidityRole", "role"))
        if attribution is None:
            wl = wallet.lower()
            if maker and maker.lower() == wl:
                attribution = "MAKER"
            elif taker and taker.lower() == wl:
                attribution = "TAKER"
        return cls(
            trade_id=stable_row_id("trd", {"wallet": wallet.lower(), **raw}),
            wallet=wallet.lower(),
            condition_id=text_or_none(pick(row, "condition_id", "conditionId", "market")),
            token_id=text_or_none(pick(row, "token_id", "tokenId", "asset", "asset_id", "assetId")),
            side=text_or_none(pick(row, "side")),
            outcome=normalize_outcome(pick(row, "outcome", "outcome_label", "outcomeLabel")),
            price=decimal_or_none(pick(row, "price")),
            size=decimal_or_none(pick(row, "size", "tokens")),
            timestamp_us=timestamp_us_from_row(row),
            transaction_hash=text_or_none(pick(row, "transaction_hash", "transactionHash", "tx_hash", "txHash")),
            title=text_or_none(pick(row, "title", "question")),
            slug=text_or_none(pick(row, "slug", "market_slug", "marketSlug")),
            event_slug=text_or_none(pick(row, "event_slug", "eventSlug")),
            maker_address=maker.lower() if maker else None,
            taker_address=taker.lower() if taker else None,
            attribution=attribution.upper() if attribution else None,
            raw_json=canonical_json(raw),
        )
