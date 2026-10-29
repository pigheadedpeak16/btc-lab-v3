from decimal import Decimal

from btc_v3.forensics.analysis import analyze_wallet_rows
from btc_v3.forensics.models import WalletActivity

WALLET = "0xdf4c6a942bd95bf903d6066b4ba7051e6f914f22"
START = 1791189000
SLUG = f"btc-updown-5m-{START}"


def activity(*, sec: int, side: str | None, outcome: str | None, price: str = "0.5", size: str = "5", kind: str = "TRADE") -> WalletActivity:
    row = {
        "type": kind,
        "conditionId": "0xabc",
        "side": side,
        "outcome": outcome,
        "price": Decimal(price) if side else None,
        "size": Decimal(size),
        "timestamp": START + sec,
        "transactionHash": f"0x{sec:064x}",
        "slug": SLUG,
        "title": "Bitcoin Up or Down - test",
    }
    return WalletActivity.from_api(WALLET, row)


def test_two_sided_round_and_final_minute_are_detected() -> None:
    rows = [
        activity(sec=40, side="BUY", outcome="Up", price="0.45", size="5"),
        activity(sec=100, side="BUY", outcome="Down", price="0.40", size="4"),
        activity(sec=260, side="BUY", outcome="Up", price="0.70", size="2"),
        activity(sec=301, side=None, outcome="Up", kind="REDEEM", size="7"),
    ]
    report = analyze_wallet_rows(WALLET, rows)
    assert report.btc5m_rounds == 1
    assert report.both_side_rounds == 1
    assert report.final_60s_trade_actions == 1
    assert report.buy_only_rounds_with_redeem == 1
    assert report.hypothesis_evidence["two_sided_accumulation"]["strength"] == "HIGH"


def test_sell_activity_is_not_misclassified_as_buying_other_side() -> None:
    rows = [
        activity(sec=10, side="BUY", outcome="Up", price="0.45", size="5"),
        activity(sec=200, side="SELL", outcome="Up", price="0.60", size="5"),
    ]
    report = analyze_wallet_rows(WALLET, rows)
    assert report.up_only_rounds == 1
    assert report.down_only_rounds == 0
    assert report.rounds_with_sell == 1
    assert report.hypothesis_evidence["active_exit_behavior"]["strength"] == "HIGH"


def test_non_btc_activity_is_excluded_from_round_behavior() -> None:
    row = WalletActivity.from_api(
        WALLET,
        {
            "type": "TRADE",
            "side": "BUY",
            "outcome": "Up",
            "price": 0.5,
            "size": 5,
            "timestamp": START,
            "slug": f"eth-updown-5m-{START}",
            "title": "Ethereum Up or Down",
        },
    )
    report = analyze_wallet_rows(WALLET, [row])
    assert report.btc5m_rounds == 0
    assert report.btc5m_trade_actions == 0


def test_maker_hypothesis_remains_undetermined_without_l2() -> None:
    report = analyze_wallet_rows(WALLET, [activity(sec=30, side="BUY", outcome="Up")])
    assert report.hypothesis_evidence["maker_market_making"]["strength"] == "UNDETERMINED"
