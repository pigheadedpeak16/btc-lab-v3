import sqlite3
from pathlib import Path

from btc_v3.storage.db import initialize_project_databases


def table_names(path: Path) -> set[str]:
    with sqlite3.connect(path) as conn:
        return {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }


def pragma(path: Path, name: str) -> int | str:
    with sqlite3.connect(path) as conn:
        return conn.execute(f"PRAGMA {name}").fetchone()[0]


def test_initializes_four_databases(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    assert set(paths) == {"market", "external", "forward", "research"}
    assert all(path.exists() for path in paths.values())


def test_market_tables_exist(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    names = table_names(paths["market"])
    assert {"markets", "market_tokens", "book_checkpoints", "book_deltas", "market_resolutions", "collector_status"} <= names


def test_forward_integrity_tables_exist(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    names = table_names(paths["forward"])
    assert {"experiments", "strategy_configs", "decisions", "paper_orders", "paper_fills", "causality_audits"} <= names


def test_wal_enabled(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    assert str(pragma(paths["market"], "journal_mode")).lower() == "wal"


def test_external_phase3_tables_exist(tmp_path: Path) -> None:
    paths = initialize_project_databases(tmp_path)
    names = table_names(paths["external"])
    assert {"coinbase_ticks", "kraken_ticks", "settlement_reference_observations", "price_references", "external_status"} <= names
