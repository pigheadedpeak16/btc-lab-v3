from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing
from pathlib import Path

from ..config import TournamentConfig
from ..timeutil import to_epoch_us, utc_now
from ..storage.db import connect_sqlite
from .hashing import canonical_json, config_hash


class ExperimentConflictError(RuntimeError):
    pass


def register_experiment(forward_db: Path, config: TournamentConfig) -> str:
    tournament_hash = config_hash(config)
    tournament_json = canonical_json(config)
    now_us = to_epoch_us(utc_now())

    with closing(connect_sqlite(forward_db)) as conn, conn:
        existing = conn.execute(
            "SELECT experiment_id, tournament_config_json FROM experiments WHERE tournament_config_hash = ?",
            (tournament_hash,),
        ).fetchone()
        if existing:
            if existing[1] != tournament_json:
                raise ExperimentConflictError("hash collision or canonicalization mismatch")
            return str(existing[0])

        experiment_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO experiments(
                experiment_id, experiment_name, tournament_config_hash,
                tournament_config_json, schema_version, created_at_us,
                frozen_at_us, status
            ) VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                experiment_id,
                config.experiment_name,
                tournament_hash,
                tournament_json,
                config.schema_version,
                now_us,
                now_us,
                "ACTIVE",
            ),
        )

        for strategy in config.strategies:
            strategy_hash = config_hash(strategy)
            strategy_id = str(uuid.uuid5(uuid.UUID(experiment_id), strategy_hash))
            conn.execute(
                """
                INSERT INTO strategy_configs(
                    strategy_config_id, experiment_id, strategy_family,
                    config_hash, config_json, created_at_us
                ) VALUES(?,?,?,?,?,?)
                """,
                (
                    strategy_id,
                    experiment_id,
                    strategy.strategy_family.value,
                    strategy_hash,
                    canonical_json(strategy),
                    now_us,
                ),
            )
        return experiment_id


def load_experiment_config(forward_db: Path, experiment_id: str) -> TournamentConfig:
    with closing(sqlite3.connect(forward_db)) as conn:
        row = conn.execute(
            "SELECT tournament_config_json FROM experiments WHERE experiment_id = ?",
            (experiment_id,),
        ).fetchone()
    if row is None:
        raise KeyError(f"unknown experiment_id: {experiment_id}")
    return TournamentConfig.model_validate(json.loads(row[0]))
