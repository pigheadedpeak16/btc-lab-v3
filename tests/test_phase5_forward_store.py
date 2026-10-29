from __future__ import annotations

from pathlib import Path

from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.storage.db import initialize_project_databases


def test_forward_store_roundtrip(tmp_path: Path) -> None:
    initialize_project_databases(tmp_path)
    with ForwardWalletStore.open_project(tmp_path) as store:
        store.start_session(session_id="s", wallet="0xabc", started_at_us=1, config={"x": 1})
        assert store.save_signal(
            signal_id="sig",
            session_id="s",
            economic_key="econ",
            source="trades",
            source_first_seen_us=10,
            trade_ts_us=5,
            slug="btc-updown-5m-0",
            token_id="tok",
            side="BUY",
            outcome="UP",
            target_price="0.35",
            target_size="48",
            transaction_hash="0x1",
            publication_lag_lower_us=1,
            publication_lag_upper_us=2,
            detection_sample_id=None,
            raw_json="{}",
        )
        assert not store.save_signal(
            signal_id="sig2",
            session_id="s",
            economic_key="econ",
            source="activity",
            source_first_seen_us=11,
            trade_ts_us=5,
            slug="btc-updown-5m-0",
            token_id="tok",
            side="BUY",
            outcome="UP",
            target_price="0.35",
            target_size="48",
            transaction_hash="0x1",
            publication_lag_lower_us=1,
            publication_lag_upper_us=2,
            detection_sample_id=None,
            raw_json="{}",
        )
        store.save_order(
            order_id="o",
            session_id="s",
            strategy="S9A_LITERAL_MIRROR",
            signal_id="sig",
            slug="btc-updown-5m-0",
            token_id="tok",
            outcome="UP",
            side="BUY",
            style="FAK_PAPER",
            price_limit=None,
            shares="2",
            created_at_us=10,
            scheduled_at_us=11,
            status="FILLED",
            reason=None,
            metadata={},
        )
        store.save_fill(
            fill_id="f",
            order_id="o",
            session_id="s",
            strategy="S9A_LITERAL_MIRROR",
            slug="btc-updown-5m-0",
            token_id="tok",
            outcome="UP",
            side="BUY",
            fill_ts_us=12,
            shares="2",
            price="0.4",
            notional="0.8",
            fee="0.01",
            liquidity_role="TAKER_PAPER",
            metadata={},
        )
        store.upsert_round(
            session_id="s",
            strategy="S9A_LITERAL_MIRROR",
            slug="btc-updown-5m-0",
            round_start_us=0,
            round_end_us=300_000_000,
            up_shares="2",
            down_shares="0",
            up_cost="0.8",
            down_cost="0",
            sell_proceeds="0",
            fees="0.01",
            settled_side="UP",
            settled_at_us=400_000_000,
            pnl="1.19",
            details={},
        )
        summary = store.session_summary("s")
        assert summary["signals"] == 1
        assert summary["strategies"]["S9A_LITERAL_MIRROR"]["fills"] == 1
        assert summary["strategies"]["S9A_LITERAL_MIRROR"]["settled_pnl"] == 1.19
