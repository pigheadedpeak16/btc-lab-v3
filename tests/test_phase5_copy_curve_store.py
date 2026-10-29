from __future__ import annotations

from dataclasses import dataclass

from btc_v3.forensics.store import ForensicsStore
from btc_v3.storage.db import initialize_project_databases


@dataclass
class Quote:
    quote_id: str
    event_id: str
    session_id: str
    wallet: str
    delay_ms: int
    quote_ts_us: int
    slug: str
    side: str
    outcome: str
    target_price: str
    executable_price: str
    price_delta: str
    market_context_matches: bool
    details_json: str = "{}"


def test_copy_curve_is_aggregated_by_delay(tmp_path):
    initialize_project_databases(tmp_path)
    wallet = "0x1111111111111111111111111111111111111111"
    with ForensicsStore.open_project(tmp_path) as store:
        store.start_live_session(
            session_id="s1", wallet=wallet, requested_wallet=wallet, started_at_us=1, config={}
        )
        # Target event must exist because copy quotes use a foreign key.
        store.conn.execute(
            """
            INSERT INTO forensic_target_events(
                event_id,session_id,wallet,trade_id,detected_at_us,market_context_matches,raw_json
            ) VALUES('e1','s1',?,'t1',2,1,'{}')
            """,
            (wallet,),
        )
        store.conn.commit()
        store.save_copy_quote(Quote("q1", "e1", "s1", wallet, 250, 3, "slug", "BUY", "UP", "0.50", "0.51", "0.01", True))
        store.save_copy_quote(Quote("q2", "e1", "s1", wallet, 500, 4, "slug", "BUY", "UP", "0.50", "0.53", "0.03", True))
        curve = store.copy_curve_for_wallet(wallet)
    assert curve[250]["observations"] == 1
    assert curve[250]["median_abs_price_delta"] == 0.01
    assert curve[500]["median_abs_price_delta"] == 0.03
