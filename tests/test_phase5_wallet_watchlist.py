from __future__ import annotations

import pytest

from btc_v3.forensics.watchlist import DEFAULT_TARGET_WALLET, WalletWatchlist


def test_watchlist_seeds_default_target_and_persists(tmp_path):
    watchlist = WalletWatchlist.open_project(tmp_path)
    rows = watchlist.load()
    assert len(rows) == 1
    assert rows[0].wallet == DEFAULT_TARGET_WALLET.lower()
    assert rows[0].label == "98euf98a"
    assert rows[0].enabled is True
    assert watchlist.path.exists()


def test_watchlist_add_update_and_disable(tmp_path):
    watchlist = WalletWatchlist.open_project(tmp_path)
    wallet = "0x1111111111111111111111111111111111111111"
    added = watchlist.add(wallet, "candidate")
    assert added.label == "candidate"
    assert added.enabled is True
    disabled = watchlist.set_enabled(wallet, False)
    assert disabled.enabled is False
    loaded = {row.wallet: row for row in watchlist.load()}
    assert loaded[wallet].label == "candidate"
    assert loaded[wallet].enabled is False


def test_watchlist_rejects_invalid_wallet(tmp_path):
    with pytest.raises(ValueError):
        WalletWatchlist.open_project(tmp_path).add("not-a-wallet", "bad")
