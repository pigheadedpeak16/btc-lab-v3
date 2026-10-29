from pathlib import Path

from btc_v3.feeds.polymarket import parse_btc_5m_event
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.market_store import MarketStore


def event(slug: str) -> dict:
    return {
        "id": "e1",
        "slug": slug,
        "title": "BTC Up or Down 5m",
        "active": True,
        "markets": [{
            "id": "m1",
            "slug": slug,
            "question": "BTC Up or Down 5m",
            "conditionId": "0x1",
            "outcomes": ["Up", "Down"],
            "clobTokenIds": ["u", "d"],
            "active": True,
        }],
    }


def test_market_persistence_and_health(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    slug = "btc-updown-5m-1791181500"
    market = parse_btc_5m_event(event(slug), slug)
    with MarketStore.open_project(tmp_path) as store:
        store.persist_market(market, 10)
        store.set_status(
            component="polymarket_clob",
            session_id="s1",
            slug=slug,
            state="SYNCING",
            updated_at_us=20,
            last_message_at_us=None,
            last_book_at_us=None,
            reconnect_count=2,
            error_count=1,
            details={"tokens": {}},
        )
        health = store.current_health()
    assert health.slug == slug
    assert health.up_token_id == "u"
    assert health.down_token_id == "d"
    assert health.collector_state == "SYNCING"
    assert health.reconnect_count == 2
