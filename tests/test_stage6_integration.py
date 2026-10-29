import json
from dataclasses import replace
from decimal import Decimal
import pytest

from btc_v3.forensics import forward_lab as f, live
from btc_v3.features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from btc_v3.storage.external_store import ExternalStore
from tests.test_phase593_edge_book import _session
from test_phase4_features import market, seed_store, MICROS
from test_stage6_data_model import reference


def test_new_sessions_record_model_and_settlement_policy_in_hash():
    # Same config must not pool new probabilities with Stage 5 source behavior.
    old_hash='de267097d9b9aba8f03c0e84b1810d4988a3b5e556cdcb41012796d50c3b8e66'
    assert f.phase591_config_hash(f.ForwardLabConfig())!=old_hash


def test_telemetry_exposes_uncalibrated_model_revision(tmp_path):
    with seed_store(tmp_path) as store:
        s=live._sample_from_state(session_id='test',wallet='public',market=market(),book_collector=None,
            external_store=store,model_paths=100,decision_time_us=1240*MICROS)
        assert s.p_up_mc is not None
        payload=json.loads(s.lab_json)
        assert payload['model_policy_revision']=='stage6_causal_twap_v1'
        assert payload['calibration_state']=='UNCALIBRATED_RESEARCH_ONLY'
        assert payload['mc_standard_error']>=0


def test_missing_composite_history_explains_both_sources(tmp_path):
    with seed_store(tmp_path) as store:
        engine=CausalBtcFeatureEngine(store)
        with pytest.raises(FeatureUnavailable,match='MISSING_EXTERNAL_HISTORY'):
            engine.composite_price_at(900*MICROS,1240*MICROS,allow_single_source=True)
        context=engine.last_unavailable_context
        assert context['reason']=='MISSING_EXTERNAL_HISTORY'
        assert set(context['sources'])=={'coinbase','kraken'}
        assert context['target_ts_us']==900*MICROS


def test_conflicting_ptb_cannot_be_used_by_features(tmp_path):
    with seed_store(tmp_path) as store:
        reference(store,price_to_beat='1')
        assert store.causal_price_reference(market().slug,1240*MICROS) is None
        assert store.price_reference_exact(slug=market().slug,reference_ts_us=1000*MICROS,decision_time_us=1240*MICROS) is None


def test_computed_shadow_settlement_checks_reference_boundaries(tmp_path):
    ms,store,engine,_=_session(tmp_path)
    try:
        pos=engine.position_by_slug(f.STRATEGY_9_EDGE_BOOK_V1,market().slug,ms)
        pos.record_fill(outcome='UP',side='BUY',shares=Decimal(5),notional=Decimal(2),fee=Decimal('.1'))
        with ExternalStore.open_project(tmp_path) as external:
            reference(external,reference_ts_us=999*MICROS,observed_at_us=1000*MICROS)
            external.persist_settlement_observation(source_ts_us=1300*MICROS,received_at_us=1300*MICROS,
                receive_sequence=1,value='86050',source='CHAINLINK_BTC_USD_TWAP_60S',
                stream_id='prices.crypto.twap:btcusd:60s',raw_payload={})
            assert engine.settle_ready(external_store=external,now_us=1301*MICROS)==[]
            assert pos.pnl is None
    finally:
        store.close();ms.close()


@pytest.mark.parametrize('option,value',[('volatility_floor',float('nan')),('volatility_cap',-1),
    ('volatility_lookback_seconds',0),('paths',1.5)])
def test_invalid_model_configuration_fails_explicitly(option,value):
    with pytest.raises(ValueError,match='MODEL_CONFIG'):
        f.SettlementModelConfig(**{option:value})
