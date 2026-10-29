from __future__ import annotations

from pathlib import Path

from btc_v3.features.btc import CausalBtcFeatureEngine
from btc_v3.models.settlement import SettlementModelConfig, TwapSettlementModel
from btc_v3.storage.external_store import ExternalStore
from test_phase4_features import MICROS, market, seed_store


def test_causal_expression_indexes_exist_and_avoid_temp_sort(tmp_path: Path) -> None:
    with seed_store(tmp_path) as store:
        names = {
            str(r[0])
            for r in store.conn.execute(
                "SELECT name FROM sqlite_master WHERE type='index'"
            ).fetchall()
        }
        assert "idx_coinbase_causal" in names
        assert "idx_kraken_causal" in names

        decision = 1240 * MICROS
        target = decision - 5 * MICROS
        plan = store.conn.execute(
            """
            EXPLAIN QUERY PLAN
            SELECT COALESCE(source_ts_us, received_at_us), received_at_us, price
            FROM coinbase_ticks INDEXED BY idx_coinbase_causal
            WHERE COALESCE(source_ts_us, received_at_us)<=?
              AND received_at_us<=?
            ORDER BY COALESCE(source_ts_us, received_at_us) DESC, received_at_us DESC, id DESC
            LIMIT 1
            """,
            (target, decision),
        ).fetchall()
        text = " ".join(str(x) for row in plan for x in row).upper()
        assert "IDX_COINBASE_CAUSAL" in text
        assert "TEMP B-TREE" not in text


def test_full_feature_and_settlement_model_use_bulk_tick_reads(tmp_path: Path) -> None:
    decision = 1240 * MICROS
    with seed_store(tmp_path) as store:
        statements: list[str] = []
        store.conn.set_trace_callback(statements.append)
        engine = CausalBtcFeatureEngine(store)
        features = engine.build(market=market(), decision_time_us=decision)
        result = TwapSettlementModel(
            engine,
            SettlementModelConfig(paths=500),
        ).evaluate(market=market(), decision_time_us=decision, features=features)
        store.conn.set_trace_callback(None)

        tick_selects = [
            s
            for s in statements
            if s.lstrip().upper().startswith("SELECT")
            and ("COINBASE_TICKS" in s.upper() or "KRAKEN_TICKS" in s.upper())
        ]
        # One bulk range query per exchange should serve the hundreds of causal
        # return/volatility/TWAP-grid lookups for this decision timestamp.
        assert len(tick_selects) <= 4, tick_selects
        assert 0.0 <= result.p_up <= 1.0


def test_tick_window_preserves_decision_time_causality(tmp_path: Path) -> None:
    from btc_v3.storage.db import initialize_project_databases

    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        store.persist_coinbase_tick(
            source_ts_us=100 * MICROS,
            received_at_us=101 * MICROS,
            receive_sequence=1,
            price="100",
            raw_payload={},
        )
        store.persist_coinbase_tick(
            source_ts_us=102 * MICROS,
            received_at_us=110 * MICROS,
            receive_sequence=2,
            price="200",
            raw_payload={},
        )
        rows = store.tick_window(
            source="coinbase",
            start_source_ts_us=90 * MICROS,
            end_source_ts_us=105 * MICROS,
            decision_time_us=105 * MICROS,
        )
        assert [row[3] for row in rows] == ["100"]
