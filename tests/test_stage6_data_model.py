"""Stage 6: isolated causal data/model regressions; no project databases."""
from dataclasses import replace
from decimal import Decimal
import math

import numpy as np
import pytest

from btc_v3.features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from btc_v3.feeds.settlement_reference import PriceReferenceMapper, SettlementObservation
from btc_v3.models.settlement import SettlementModelConfig, TwapSettlementModel
from test_phase4_features import MICROS, market, seed_store


def reference(store, **changes):
    values=dict(slug=market().slug, round_start_us=market().round_start_us,
        round_end_us=market().round_end_us, price_to_beat='86020',
        reference_ts_us=market().round_start_us, observed_at_us=1239*MICROS,
        source='CHAINLINK_BTC_USD_TWAP_60S', source_stream='prices.crypto.twap:btcusd:60s',
        mapping_method='EXACT_ROUND_START_SOURCE_TIMESTAMP', raw_payload={})
    values.update(changes)
    return store.persist_price_reference(**values)


@pytest.mark.parametrize('query', ['latest_tick', 'tick_window', 'latest_settlement_observation', 'settlement_observation_exact'])
def test_source_time_never_exceeds_decision_even_if_target_does(tmp_path, query):
    with seed_store(tmp_path) as store:
        decision=1240*MICROS
        store.persist_coinbase_tick(source_ts_us=decision+MICROS,received_at_us=decision-1,
            receive_sequence=999,price='99999',raw_payload={})
        store.persist_settlement_observation(source_ts_us=decision+MICROS,received_at_us=decision-1,
            receive_sequence=999,value='99999',source='CHAINLINK_BTC_USD_TWAP_60S',
            stream_id='prices.crypto.twap:btcusd:60s',raw_payload={})
        if query=='latest_tick':
            row=store.latest_tick(source='coinbase',target_source_ts_us=decision+MICROS,decision_time_us=decision)
            assert row is None or row[0]<=decision
        elif query=='tick_window':
            rows=store.tick_window(source='coinbase',start_source_ts_us=decision-MICROS,
                end_source_ts_us=decision+MICROS,decision_time_us=decision)
            assert all(r[0]<=decision for r in rows)
        elif query=='latest_settlement_observation':
            row=store.latest_settlement_observation(target_source_ts_us=decision+MICROS,decision_time_us=decision)
            assert row is None or row[0]<=decision
        else:
            assert store.settlement_observation_exact(source_ts_us=decision+MICROS,decision_time_us=decision) is None


@pytest.mark.parametrize('source,stream', [('UNVERIFIED','prices.crypto.twap:btcusd:60s'),
    ('CHAINLINK_BTC_USD_TWAP_60S','wrong-stream')])
def test_untrusted_reference_does_not_replace_verified_reference(tmp_path,source,stream):
    with seed_store(tmp_path) as store:
        reference(store,source=source,source_stream=stream,price_to_beat='1')
        assert store.causal_price_reference(market().slug,1240*MICROS).price_to_beat=='86020.000000000000'


def test_conflicting_exact_official_observation_fails_closed(tmp_path):
    with seed_store(tmp_path) as store:
        store.persist_settlement_observation(source_ts_us=1239*MICROS,received_at_us=1239*MICROS+5000,
            receive_sequence=999,value='99999',source='CHAINLINK_BTC_USD_TWAP_60S',
            stream_id='prices.crypto.twap:btcusd:60s',raw_payload={})
        assert store.settlement_observation_exact(source_ts_us=1239*MICROS,decision_time_us=1240*MICROS) is None
        assert store.latest_settlement_observation(target_source_ts_us=1240*MICROS,decision_time_us=1240*MICROS) is None


@pytest.mark.parametrize('price',['0','-1','NaN','Infinity'])
def test_invalid_external_price_has_clear_unavailable_reason(tmp_path,price):
    with seed_store(tmp_path) as store:
        store.persist_coinbase_tick(source_ts_us=1240*MICROS-1,received_at_us=1240*MICROS-1,
            receive_sequence=999,price=price,raw_payload={})
        with pytest.raises(FeatureUnavailable,match='INVALID_COINBASE_PRICE'):
            CausalBtcFeatureEngine(store).build(market=market(),decision_time_us=1240*MICROS)


def test_reference_round_end_must_match_contract(tmp_path):
    with seed_store(tmp_path) as store:
        reference(store,round_end_us=market().round_end_us+300*MICROS)
        with pytest.raises(FeatureUnavailable,match='WRONG_BOUNDARY_PRICE_REFERENCE|NO_CAUSAL_PRICE_REFERENCE'):
            CausalBtcFeatureEngine(store).build(market=market(),decision_time_us=1240*MICROS)


def test_future_round_is_not_a_live_feature_state(tmp_path):
    with seed_store(tmp_path) as store:
        future=replace(market(),round_start_us=1241*MICROS)
        with pytest.raises(FeatureUnavailable,match='DECISION_BEFORE_ROUND_START'):
            CausalBtcFeatureEngine(store).build(market=future,decision_time_us=1240*MICROS)


@pytest.mark.parametrize('field,value',[('source','UNVERIFIED'),('stream_id','wrong'),('value','NaN'),('value','0')])
def test_mapper_rejects_bad_official_observation_before_persisting(tmp_path,field,value):
    with seed_store(tmp_path) as store:
        observation=SettlementObservation(market().round_start_us,1240*MICROS,'86000',
            'CHAINLINK_BTC_USD_TWAP_60S','prices.crypto.twap:btcusd:60s',{})
        before=store.conn.total_changes
        with pytest.raises(ValueError,match='INVALID_SETTLEMENT_OBSERVATION'):
            PriceReferenceMapper(store).ingest_observation(market(),replace(observation,**{field:value}),999)
        assert store.conn.total_changes==before


@pytest.mark.parametrize('change',[{'slug':'different-market'}, {'decision_time_us':1239*MICROS},
    {'seconds_remaining':59}, {'current_sources':('coinbase',)}, {'predictor_price':float('nan')}])
def test_model_rejects_mismatched_or_invalid_feature_state(tmp_path,change):
    with seed_store(tmp_path) as store:
        engine=CausalBtcFeatureEngine(store); f=engine.build(market=market(),decision_time_us=1240*MICROS)
        with pytest.raises(FeatureUnavailable,match='MODEL_'):
            TwapSettlementModel(engine,SettlementModelConfig(paths=100)).evaluate(
                market=market(),decision_time_us=1240*MICROS,features=replace(f,**change))


def test_official_anchor_uses_its_own_source_window(tmp_path):
    with seed_store(tmp_path) as store:
        engine=CausalBtcFeatureEngine(store);f=engine.build(market=market(),decision_time_us=1240*MICROS)
        result=TwapSettlementModel(engine,SettlementModelConfig(paths=100)).evaluate(
            market=market(),decision_time_us=1240*MICROS,features=f)
        anchor=f.official_twap_source_ts_us
        grid=engine.composite_grid(start_us=anchor-59*MICROS,end_us=anchor,decision_us=f.decision_time_us)
        assert result.oracle_scale==pytest.approx(f.official_twap/(sum(p for _,p in grid)/60),abs=1e-13)


def test_fractional_future_samples_use_exact_time_increments(tmp_path):
    with seed_store(tmp_path) as store:
        engine=CausalBtcFeatureEngine(store);decision=1240*MICROS+500000
        f=engine.build(market=market(),decision_time_us=decision)
        f=replace(f,realized_volatility={60:.002})
        result=TwapSettlementModel(engine,SettlementModelConfig(paths=1000)).evaluate(
            market=market(),decision_time_us=decision,features=f)
        times=np.array([(t*MICROS-decision)/MICROS for t in range(1241,1301)])
        increments=np.diff(np.r_[0,times]); rng=np.random.default_rng(result.seed)
        z=rng.standard_normal((1000,60));sigma=.002
        prices=f.predictor_price*np.exp(np.cumsum(-.5*sigma*sigma*increments+sigma*np.sqrt(increments)*z,axis=1))
        expected=prices.mean(axis=1)*result.oracle_scale
        assert result.simulated_twap_mean==pytest.approx(float(expected.mean()),abs=1e-8)


def test_no_volatility_window_silently_substitutes_for_requested_window(tmp_path):
    with seed_store(tmp_path) as store:
        engine=CausalBtcFeatureEngine(store);f=engine.build(market=market(),decision_time_us=1240*MICROS)
        with pytest.raises(FeatureUnavailable,match='VOLATILITY_WINDOW_UNAVAILABLE'):
            TwapSettlementModel(engine,SettlementModelConfig(paths=100)).evaluate(
                market=market(),decision_time_us=1240*MICROS,features=replace(f,realized_volatility={30:.001}))
