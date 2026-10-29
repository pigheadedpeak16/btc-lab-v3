from btc_v3.feeds.coinbase import parse_coinbase_ticker
from btc_v3.feeds.kraken import parse_kraken_trades


def test_coinbase_ticker_parser_preserves_price_and_timestamp() -> None:
    msg = {
        "type": "ticker",
        "product_id": "BTC-USD",
        "price": "86000.12000000",
        "time": "2026-10-05T07:00:01.123456Z",
    }
    tick = parse_coinbase_ticker(msg, 99)
    assert tick is not None
    assert tick.price == "86000.12000000"
    assert tick.received_at_us == 99
    assert tick.source_ts_us == 1791183601123456


def test_coinbase_ignores_wrong_product() -> None:
    assert parse_coinbase_ticker({"type": "ticker", "product_id": "ETH-USD", "price": "1"}, 1) is None


def test_kraken_trade_parser_handles_batch() -> None:
    msg = {
        "channel": "trade",
        "type": "update",
        "data": [
            {"symbol": "BTC/USD", "price": 86000.1, "timestamp": "2026-10-05T07:00:01.100000Z"},
            {"symbol": "BTC/USD", "price": 86000.2, "timestamp": "2026-10-05T07:00:01.200000Z"},
        ],
    }
    ticks = parse_kraken_trades(msg, 123)
    assert [t.price for t in ticks] == ["86000.1", "86000.2"]
    assert all(t.received_at_us == 123 for t in ticks)
    assert ticks[0].source_ts_us == 1791183601100000
    assert ticks[1].source_ts_us == 1791183601200000


def test_kraken_ignores_non_trade_message() -> None:
    assert parse_kraken_trades({"channel": "heartbeat", "type": "update"}, 1) == []
