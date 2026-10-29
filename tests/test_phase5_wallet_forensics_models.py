from decimal import Decimal

from btc_v3.forensics.models import (
    WalletActivity,
    WalletTrade,
    is_btc_5m_row,
    parse_btc_5m_round_start_us,
)

WALLET = "0xdf4c6a942bd95bf903d6066b4ba7051e6f914f22"


def test_parse_btc_5m_slug() -> None:
    assert parse_btc_5m_round_start_us("btc-updown-5m-1791189000") == 1791189000_000_000
    assert parse_btc_5m_round_start_us("eth-updown-5m-1791189000") is None


def test_title_can_identify_btc_5m_family() -> None:
    assert is_btc_5m_row(None, None, "Bitcoin Up or Down 5m - October 5")
    assert not is_btc_5m_row(None, None, "Ethereum Up or Down - October 5")


def test_activity_normalizes_camel_case_and_decimals() -> None:
    row = {
        "type": "trade",
        "conditionId": "0xabc",
        "asset": "123",
        "side": "BUY",
        "outcome": "Yes",
        "price": Decimal("0.61"),
        "size": Decimal("4.25"),
        "timestamp": 1791189050,
        "transactionHash": "0xdead",
        "slug": "btc-updown-5m-1791189000",
    }
    parsed = WalletActivity.from_api(WALLET, row)
    assert parsed.event_type == "TRADE"
    assert parsed.outcome == "UP"
    assert parsed.price == Decimal("0.61")
    assert parsed.size == Decimal("4.25")
    assert parsed.timestamp_us == 1791189050_000_000
    assert parsed.btc_round_start_us == 1791189000_000_000


def test_trade_infers_maker_attribution_when_address_is_present() -> None:
    parsed = WalletTrade.from_api(
        WALLET,
        {
            "condition_id": "0xabc",
            "side": "BUY",
            "price": 0.5,
            "size": 10,
            "timestamp": 1791189050,
            "maker_address": WALLET.upper(),
        },
    )
    assert parsed.attribution == "MAKER"
