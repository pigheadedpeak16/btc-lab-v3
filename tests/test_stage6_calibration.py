"""Synthetic data tests for chronological diagnostics, never performance evidence."""
from copy import deepcopy
import json
import math

import pytest

from btc_v3.models import settlement as model


def report(rows,**changes):
    function=getattr(model,'calibration_report',None)
    assert function is not None,'Missing market-level chronological calibration diagnostics'
    options=dict(training_cutoff_us=300_000_000*120,evaluation_end_us=300_000_000*240,
        labels_cutoff_us=300_000_000*241)
    options.update(changes)
    return function(rows,**options)


def samples(count=240):
    rows=[]
    for i in range(count):
        start=i*300_000_000;end=start+300_000_000;decision=end-30_000_000
        rows.append(dict(slug=f'btc-updown-5m-{start//1000000}',round_start_us=start,round_end_us=end,
            decision_time_us=decision,latest_input_source_ts_us=decision-1000,
            latest_input_received_at_us=decision,p_up=.6,outcome='UP' if i%2 else 'DOWN',
            outcome_observed_at_us=end+1000,resolution_evidence_ref=f'official-snapshot-{i}',
            model_policy_revision='synthetic-test-v1',current_sources=['coinbase','kraken']))
    return rows


def cohort(result):
    assert len(result['cohorts'])==1
    return result['cohorts'][0]


def test_metrics_weight_markets_and_keep_training_separate_from_evaluation():
    result=report(samples());c=cohort(result)
    # Last training label arrives after cutoff, so it must not enter fitting.
    assert (c['training_markets'],c['evaluation_markets'])==(119,120)
    assert c['raw_metrics']['brier']==pytest.approx(.26)
    assert c['raw_metrics']['log_loss']==pytest.approx(-.5*(math.log(.6)+math.log(.4)))
    assert c['raw_metrics']['ece']==pytest.approx(.1)
    assert c['raw_metrics']['constant_half_brier']==.25
    assert c['calibrated_metrics']['brier']<c['raw_metrics']['brier']
    assert result['state']=='DIAGNOSTICS_ONLY_NOT_PROMOTED'
    assert c['calibration_state']=='OFFLINE_CANDIDATE_NOT_DEPLOYED'


def test_heldout_outcomes_cannot_change_fitted_mapping():
    rows=samples();first=cohort(report(rows))
    for row in rows[120:]: row['outcome']='UP'
    second=cohort(report(rows))
    assert first['training_bins']==second['training_bins']
    assert first['raw_metrics']!=second['raw_metrics']


def test_duplicate_market_samples_cannot_inflate_evidence():
    rows=samples();rows.append(deepcopy(rows[5]))
    with pytest.raises(ValueError,match='DUPLICATE_MARKET'):
        report(rows)


@pytest.mark.parametrize('change',[
    {'latest_input_source_ts_us':10**15},{'latest_input_received_at_us':10**15},
    {'p_up':float('nan')},{'p_up':1.1},{'outcome':'YES'},
    {'resolution_evidence_ref':''},{'outcome_observed_at_us':1},
    {'current_sources':['coinbase','coinbase']},
    {'model_policy_revision':''}, {'round_end_us':1},
])
def test_invalid_or_noncausal_samples_are_rejected(change):
    rows=samples();rows[2].update(change)
    with pytest.raises(ValueError,match='CALIBRATION_'):
        report(rows)


def test_late_or_missing_official_labels_are_not_scored():
    rows=samples()
    rows[122]['outcome_observed_at_us']=10**15
    rows[123]['outcome']=None; rows[123]['outcome_observed_at_us']=None
    result=report(rows)
    assert cohort(result)['evaluation_markets']==118
    assert result['excluded']['LABEL_NOT_AVAILABLE']==2


def test_single_source_revision_and_horizon_are_different_cohorts():
    rows=samples()
    rows[0]['current_sources']=['coinbase']
    rows[1]['model_policy_revision']='different'
    rows[2]['decision_time_us']-=1_000_000
    rows[2]['latest_input_source_ts_us']=rows[2]['decision_time_us']
    rows[2]['latest_input_received_at_us']=rows[2]['decision_time_us']
    result=report(rows)
    assert len(result['cohorts'])==4
    assert sum(c['training_markets'] for c in result['cohorts'])==119
    assert all(c['calibration_state']=='INSUFFICIENT_MARKETS' for c in result['cohorts'][1:]) or any(
        c['calibration_state']=='INSUFFICIENT_MARKETS' for c in result['cohorts'])


def test_sparse_bins_abstain_from_calibration_even_with_many_total_markets():
    rows=samples()
    for row in rows[120:]: row['p_up']=.99
    c=cohort(report(rows))
    assert c['calibrated_markets']==0 and c['calibrated_metrics'] is None
    assert c['calibration_abstentions']==120


def test_insufficient_markets_cannot_create_a_ready_model():
    c=cohort(report(samples(20)))
    assert c['calibration_state']=='INSUFFICIENT_MARKETS'
    assert c['calibrated_metrics'] is None


def test_cli_calibration_report_has_no_database_side_effects(tmp_path,capsys):
    from btc_v3 import cli
    function=getattr(cli,'cmd_model_calibration_report',None)
    assert function is not None,'Missing offline calibration report command'
    path=tmp_path/'observations.json';path.write_text(json.dumps(samples()))
    args=cli.build_parser().parse_args(['model-calibration-report','--input-json',str(path),
        '--training-cutoff-us',str(300_000_000*120),'--evaluation-end-us',str(300_000_000*240),
        '--labels-cutoff-us',str(300_000_000*241)])
    assert function(args)==0
    assert json.loads(capsys.readouterr().out)['state']=='DIAGNOSTICS_ONLY_NOT_PROMOTED'
    assert sorted(p.name for p in tmp_path.iterdir())==['observations.json']
