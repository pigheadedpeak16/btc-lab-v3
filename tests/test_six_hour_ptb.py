from dataclasses import replace
import json
import pytest

from tests.test_phase4_features import market,seed_store,MICROS
from tests.test_stage6_data_model import reference
from btc_v3.forensics.live import _sample_from_state
from btc_v3.features.btc import CausalBtcFeatureEngine,FeatureUnavailable
from btc_v3.storage.db import initialize_project_databases
from btc_v3.storage.external_store import ExternalStore


def sample(store,at=1240,contract=None):
    return _sample_from_state(session_id='fixture',wallet='public',market=contract or market(),book_collector=None,
        external_store=store,model_paths=100,decision_time_us=at*MICROS)


@pytest.mark.parametrize('disconnected',[False,True])
def test_known_ptb_survives_model_waiting_without_authorizing_prediction(tmp_path,disconnected):
    with seed_store(tmp_path) as store:
        assert sample(store).ptb==86020
        if disconnected:
            store.set_status(component='settlement_reference',session_id=None,state='DISCONNECTED',updated_at_us=1245*MICROS,
                last_message_at_us=1240*MICROS,last_tick_at_us=1240*MICROS,reconnect_count=1,error_count=0,details={})
        stale=sample(store,1246)
        assert stale.state=='WAITING' and stale.p_up_mc is None
        assert stale.ptb==86020,'Known market PTB must not disappear when predictors become stale'
        reference_view=json.loads(stale.lab_json)['market_reference']
        assert reference_view['state']=='VERIFIED_RETAINED' and reference_view['execution_eligible'] is False
        with pytest.raises(FeatureUnavailable):CausalBtcFeatureEngine(store).build(market=market(),decision_time_us=1246*MICROS)


def test_ptb_unknown_then_late_reference_becomes_observable(tmp_path):
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        assert sample(store).ptb is None
        reference(store,observed_at_us=1245*MICROS)
        assert sample(store,1244).ptb is None
        known=sample(store,1245)
        assert known.ptb==86020 and known.p_up_mc is None
        assert json.loads(known.lab_json)['market_reference']['observed_at_us']==1245*MICROS


def test_conflicting_reference_and_market_rollover_never_reuse_display_value(tmp_path):
    with seed_store(tmp_path) as store:
        reference(store,price_to_beat='1')
        conflicted=sample(store)
        assert conflicted.ptb is None
        assert json.loads(conflicted.lab_json)['market_reference']['state']=='CONFLICTING_OR_INVALID'
        nxt=replace(market(),slug='btc-updown-5m-1300',round_start_us=1300*MICROS,round_end_us=1600*MICROS)
        assert sample(store,1301,nxt).ptb is None


def test_future_reference_is_never_displayed_before_its_causal_boundary(tmp_path):
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        reference(store,observed_at_us=999*MICROS)
        assert sample(store,999).ptb is None
        assert sample(store,1240).ptb is None


def test_backdated_reference_receipt_cannot_authorize_model(tmp_path):
    with seed_store(tmp_path) as store:
        reference(store,observed_at_us=999*MICROS)
        assert store.causal_price_reference(market().slug,1240*MICROS) is None


def test_per_sample_health_diagnostics_do_not_count_entire_history(tmp_path):
    from btc_v3.forensics.live import _external_health_payload
    with seed_store(tmp_path) as store:
        queries=[];store.conn.set_trace_callback(queries.append)
        _external_health_payload(store,1240*MICROS)
        assert not any('COUNT(*)' in q.upper() or 'MAX(RECEIVED_AT_US)' in q.upper() for q in queries),queries


def test_display_separates_old_source_trade_from_fresh_network_receipt(tmp_path):
    from btc_v3.forensics.live import _external_health_payload
    initialize_project_databases(tmp_path)
    with ExternalStore.open_project(tmp_path) as store:
        store.set_status(component='kraken_btc_usd',session_id=None,state='RUNNING',updated_at_us=1239*MICROS,
            last_message_at_us=1239*MICROS,last_tick_at_us=1239*MICROS,reconnect_count=0,error_count=0,
            details={'latest_source_ts_us':1230*MICROS})
        view=_external_health_payload(store,1240*MICROS)['kraken_btc_usd']
        assert view['source_trade_age_s']==10 and view['tick_age_s']==1
