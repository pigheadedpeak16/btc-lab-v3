from datetime import UTC, datetime

import pytest

from btc_v3.feeds.polymarket import (
    MarketDiscoveryError,
    btc_5m_round_start_s,
    btc_5m_slug,
    parse_btc_5m_event,
)


def sample_event(slug: str) -> dict:
    return {
        "id": "event-1",
        "slug": slug,
        "title": "BTC Up or Down 5m",
        "active": True,
        "closed": False,
        "description": "Chainlink BTC/USD TWAP market",
        "markets": [
            {
                "id": "market-1",
                "slug": slug,
                "question": "Bitcoin Up or Down - 5 Minutes",
                "conditionId": "0xabc",
                "outcomes": '["Up","Down"]',
                "clobTokenIds": '["up-token","down-token"]',
                "active": True,
                "closed": False,
            }
        ],
    }


def test_round_alignment_and_slug() -> None:
    now = datetime(2026, 10, 5, 6, 29, 59, tzinfo=UTC)
    start = btc_5m_round_start_s(now)
    assert start == 1791181500
    assert btc_5m_slug(start) == "btc-updown-5m-1791181500"


def test_parse_btc_event_maps_up_down_tokens() -> None:
    slug = "btc-updown-5m-1791181500"
    market = parse_btc_5m_event(sample_event(slug), slug)
    assert market.up_token_id == "up-token"
    assert market.down_token_id == "down-token"
    assert market.round_end_us - market.round_start_us == 300_000_000
    assert market.condition_id == "0xabc"


def test_parse_rejects_misaligned_slug() -> None:
    slug = "btc-updown-5m-1791181501"
    with pytest.raises(MarketDiscoveryError, match="misaligned"):
        parse_btc_5m_event(sample_event(slug), slug)


def test_parse_rejects_ambiguous_outcomes() -> None:
    slug = "btc-updown-5m-1791181500"
    event = sample_event(slug)
    event["markets"][0]["outcomes"] = '["Yes","Maybe"]'
    with pytest.raises(MarketDiscoveryError, match="map Up/Down"):
        parse_btc_5m_event(event, slug)
