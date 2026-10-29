import inspect
from btc_v3.feeds.coinbase import CoinbaseCollector, COINBASE_WS_URL
from btc_v3.feeds.kraken import KrakenCollector, KRAKEN_WS_URL
from btc_v3.feeds.settlement_reference import AuthenticatedTwapCollector, SettlementReferencePolicy


def test_predictor_endpoints_remain_public_market_data_urls() -> None:
    assert COINBASE_WS_URL == "wss://ws-feed.exchange.coinbase.com"
    assert KRAKEN_WS_URL == "wss://ws.kraken.com/v2"


def test_predictor_collectors_expose_no_order_methods() -> None:
    forbidden = {"buy", "sell", "place_order", "cancel_order", "sign", "withdraw", "deposit"}
    assert not (forbidden & set(dir(CoinbaseCollector)))
    assert not (forbidden & set(dir(KrakenCollector)))


def test_reference_policy_allows_authentication_but_not_real_trading() -> None:
    policy = SettlementReferencePolicy()
    assert policy.authenticated_read_only_allowed is True
    assert "no order placement" in policy.reason.lower()


def test_reference_collector_exposes_no_execution_methods() -> None:
    forbidden = {
        "buy",
        "sell",
        "place_order",
        "create_order",
        "post_order",
        "cancel_order",
        "cancel_all",
        "withdraw",
        "deposit",
        "send_transaction",
    }
    assert not (forbidden & set(dir(AuthenticatedTwapCollector)))


def test_secure_client_is_forced_to_signer_eoa_mode() -> None:
    source = inspect.getsource(AuthenticatedTwapCollector._make_client_and_spec)
    assert "wallet=signer_address" in source
    assert "CryptoTwapPriceSpec" in source
