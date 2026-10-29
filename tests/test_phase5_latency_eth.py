from btc_v3.forensics.models import WalletTrade
from btc_v3.feeds.polymarket import eth_5m_slug

def test_wallet_trade_recognizes_eth_5m():
    row = {"slug":"eth-updown-5m-1800000000","title":"Ethereum Up or Down - test","timestamp":1800000001,"price":"0.55","size":"2","side":"BUY","outcome":"Up"}
    trade = WalletTrade.from_api("0x" + "1" * 40, row)
    assert trade.is_eth_5m and trade.is_crypto_5m and not trade.is_btc_5m

def test_eth_slug_builder():
    assert eth_5m_slug(1800000000) == "eth-updown-5m-1800000000"
