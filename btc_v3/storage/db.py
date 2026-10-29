from __future__ import annotations

import sqlite3
from pathlib import Path
from contextlib import closing

from .schemas import EXTERNAL_SCHEMA, FORWARD_SCHEMA, MARKET_SCHEMA, RESEARCH_SCHEMA

SCHEMA_VERSION = "9"


def connect_sqlite(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _initialize(path: Path, schema: str, database_kind: str) -> None:
    with closing(connect_sqlite(path)) as conn, conn:
        conn.executescript(schema)
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('schema_version', ?)",
            (SCHEMA_VERSION,),
        )
        conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('database_kind', ?)",
            (database_kind,),
        )
        if database_kind == "market":
            conn.execute(
                "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('storage_profile', ?)",
                ("compact_raw_ref_v1",),
            )


def initialize_project_databases(root: Path) -> dict[str, Path]:
    paths = {
        "market": root / "data" / "v3_market.sqlite",
        "external": root / "data" / "v3_external.sqlite",
        "forward": root / "research" / "v3_forward.sqlite",
        "research": root / "research" / "v3_research.sqlite",
    }
    _initialize(paths["market"], MARKET_SCHEMA, "market")
    _initialize(paths["external"], EXTERNAL_SCHEMA, "external")
    _initialize(paths["forward"], FORWARD_SCHEMA, "forward")
    _initialize(paths["research"], RESEARCH_SCHEMA, "research")
    (root / "data" / "raw").mkdir(parents=True, exist_ok=True)
    (root / "research" / "datasets").mkdir(parents=True, exist_ok=True)
    (root / "logs").mkdir(parents=True, exist_ok=True)
    return paths
