from dataclasses import replace
from decimal import Decimal as D
from types import SimpleNamespace
import json

from tests.test_stage1_integrity import lab,collector
from tests.test_phase4_features import market,MICROS
from tests.test_stage6_data_model import reference
from btc_v3.storage.external_store import ExternalStore
from btc_v3.forensics import forward_lab as f


def test_copy_fill_records_book_clocks_at_actual_simulated_execution(lab):
    ms,store,engine=lab;at=1_010_100_000;c=collector(at)
    assert engine._execute_copy(engine.pending_copies[0],collector=c,market_store=ms,now_us=at) in {'FILLED','PARTIAL'}
    row=store.conn.execute('SELECT fill_ts_us,metadata_json FROM wallet_forward_fills').fetchone();d=json.loads(row[1])
    assert d['execution_at_us']==row[0]
    assert d['book_source_ts_us']==c.states['up'].last_server_ts_us
    assert d['book_received_at_us']==c.states['up'].last_received_at_us


def test_tournament_fill_records_fresh_book_clocks_and_tick(lab,tmp_path):
    ms,store,engine=lab;engine.config=replace(engine.config,strict_market_minimum=True)
    at=1_010_100_000;c=collector(at)
    with ExternalStore.open_project(tmp_path) as external:
        t=f.FullForwardTournament(session_id=engine.session_id,store=store,paper_engine=engine,external_store=external,config=engine.config)
        name,cfg=f.full_tournament_specs(15)[0]
        t._schedule(strategy=name,cfg=cfg.common,market=market(),outcome='UP',ask=D('.5'),now_us=at-1_000_000,metadata={})
        t.execute_due(collector_pool=SimpleNamespace(collector_for_slug=lambda slug:c),market_store=ms,now_us=at)
        row=store.conn.execute('SELECT fill_ts_us,metadata_json FROM wallet_forward_fills WHERE strategy=?',(name,)).fetchone()
        assert row is not None
        data=json.loads(row[1])
        assert data['execution_at_us']==row[0]
        assert data['book_source_ts_us']<=data['book_received_at_us']<=row[0]
        assert D(data['tick_size'])==c.states['up'].tick_size


def test_shadow_settlement_preserves_oracle_inputs_without_claiming_exchange_cash(lab,tmp_path):
    ms,store,engine=lab
    pos=engine.position_by_slug(f.STRATEGY_9_BOOK_CONFIRM_75,market().slug,ms)
    pos.record_fill(outcome='UP',side='BUY',shares=D(5),notional=D(2),fee=D('.1'))
    with ExternalStore.open_project(tmp_path) as external:
        reference(external)
        external.persist_settlement_observation(source_ts_us=1300*MICROS,received_at_us=1300*MICROS,
            receive_sequence=1,value='86050',source='CHAINLINK_BTC_USD_TWAP_60S',stream_id='prices.crypto.twap:btcusd:60s',raw_payload={})
        assert engine.settle_ready(external_store=external,now_us=1301*MICROS)
        raw=store.conn.execute('SELECT details_json FROM wallet_forward_rounds WHERE strategy=?',(f.STRATEGY_9_BOOK_CONFIRM_75,)).fetchone()[0]
        evidence=json.loads(raw)['settlement_evidence']
        assert evidence['kind']=='OBSERVED_TWAP_DERIVED_SHADOW_OUTCOME'
        assert evidence['official_resolution_status']=='NOT_INDEPENDENTLY_OBSERVED'
        assert evidence['exchange_payout_status']=='NO_EXCHANGE_PAYOUT_EVIDENCE'
        assert evidence['final_twap']['source_ts_us']==1300*MICROS
