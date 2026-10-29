from pathlib import Path

from btc_v3.forensics.analysis import analyze_wallet_rows
from btc_v3.forensics.models import WalletActivity
from btc_v3.forensics.store import ForensicsStore
from btc_v3.storage.db import initialize_project_databases

WALLET = "0xdf4c6a942bd95bf903d6066b4ba7051e6f914f22"


def test_forensic_tables_are_initialized(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with ForensicsStore.open_project(tmp_path) as store:
        names = {
            row[0]
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    assert "forensic_wallets" in names
    assert "forensic_activity" in names
    assert "forensic_rounds" in names
    assert "forensic_reports" in names


def test_store_deduplicates_activity_and_exports_report(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    row = WalletActivity.from_api(
        WALLET,
        {
            "type": "TRADE",
            "side": "BUY",
            "outcome": "Up",
            "price": 0.55,
            "size": 5,
            "timestamp": 1791189010,
            "slug": "btc-updown-5m-1791189000",
            "transactionHash": "0xabc",
        },
    )
    with ForensicsStore.open_project(tmp_path) as store:
        assert store.save_activities([row]) == 1
        assert store.save_activities([row]) == 0
        loaded = store.load_activities(WALLET)
        report = analyze_wallet_rows(WALLET, loaded)
        store.save_report(report)
        out = store.export(tmp_path, report)
    assert (out / "report.json").exists()
    assert (out / "rounds.csv").exists()
