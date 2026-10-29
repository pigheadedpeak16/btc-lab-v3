import importlib.util
import importlib
from copy import deepcopy
import pytest
from tests.research_fixtures import tournament_events
from tests.test_stage7_replay import manifest,START


def api():
    assert importlib.util.find_spec('btc_v3.strategies.lab_profiles'),'Missing versioned research strategy profiles'
    return importlib.import_module('btc_v3.strategies.lab_profiles')


def test_catalog_create_edit_archive_preserves_versions_and_frozen_benchmarks(tmp_path):
    lab=api();before=lab.catalog(tmp_path)
    assert not list(tmp_path.rglob('*.sqlite')),'Reading empty catalog must not initialize a database'
    base=next(r for r in before['strategies'] if r['config']['strategy_family']=='fair_value_book_imbalance')
    saved=lab.save_profile(tmp_path,name='My imbalance',config=base['config'])
    changed=deepcopy(saved['config']);changed['common']['min_edge']=.04
    updated=lab.save_profile(tmp_path,name='My imbalance v2',config=changed,strategy_id=saved['strategy_id'],expected_revision=1)
    assert updated['revision']==2 and updated['config_sha256']!=saved['config_sha256']
    assert lab.get_profile(tmp_path,saved['strategy_id'],revision=1)==saved
    lab.archive_profile(tmp_path,saved['strategy_id'],expected_revision=2)
    assert all(r['strategy_id']!=saved['strategy_id'] for r in lab.catalog(tmp_path)['strategies'])
    assert lab.get_profile(tmp_path,saved['strategy_id'],revision=2)==updated
    with pytest.raises(ValueError,match='FROZEN'):lab.archive_profile(tmp_path,base['strategy_id'],expected_revision=1)


def test_catalog_rejects_conflicts_and_unsafe_or_unsupported_settings(tmp_path):
    lab=api();base=lab.catalog(tmp_path)['strategies'][0]
    saved=lab.save_profile(tmp_path,name='Test',config=base['config'])
    with pytest.raises(ValueError,match='CONFLICT'):
        lab.save_profile(tmp_path,name='Other',config=base['config'],strategy_id=saved['strategy_id'],expected_revision=0)
    for key,value in [('max_quote_age_seconds',20),('monte_carlo_paths',1000000),('predictor_price_source','coinbase')]:
        cfg=deepcopy(base['config']);cfg['common'][key]=value
        with pytest.raises(ValueError):lab.save_profile(tmp_path,name='Unsafe',config=cfg)
    cfg=deepcopy(base['config']);cfg['python_code']='__import__("os")'
    with pytest.raises(ValueError):lab.save_profile(tmp_path,name='Code',config=cfg)


def test_saved_parameters_drive_actual_causal_shadow_replay_without_account(tmp_path):
    lab=api();base=next(r for r in lab.catalog(tmp_path)['strategies'] if r['config']['strategy_family']=='fair_value_book_imbalance')
    cfg=deepcopy(base['config']);cfg['common'].update(shares=7,execution_delay_seconds=.2)
    saved=lab.save_profile(tmp_path,name='Seven shares',config=cfg)
    from btc_v3.replay.engine import replay
    result=replay(manifest(strategy_id='CONFIGURED_RESEARCH'),tournament_events(),strategy_definition=saved)
    assert result['status']=='SUPPORTED_SYNTHETIC_OR_CURATED',result['unsupported_reasons']
    assert len(result['fills'])==1 and float(result['fills'][0]['shares'])==5 # observed depth limits the FAK fill
    assert float(result['orders'][0]['submitted_shares'])==7
    assert result['fills'][0]['at_us']==START+120_200_000
    assert not {'account','funded_fills','selector_decisions'} & result.keys()
    assert result['policies']['strategy_config_sha256']==saved['config_sha256']
    blocked=deepcopy(cfg);blocked['params']['minimum_total_depth']=10000
    v2=lab.save_profile(tmp_path,name='No depth',config=blocked,strategy_id=saved['strategy_id'],expected_revision=1)
    output=replay(manifest(strategy_id='CONFIGURED_RESEARCH'),tournament_events(),strategy_definition=v2)
    assert not output['fills'] and lab.get_profile(tmp_path,saved['strategy_id'],revision=1)==saved


def test_shadow_replay_pnl_requires_exact_observed_resolution(tmp_path):
    from datetime import datetime,timezone
    from tests.test_stage7_replay import END,event
    from btc_v3.replay.engine import replay
    lab=api();base=next(r for r in lab.catalog(tmp_path)['strategies'] if r['config']['strategy_family']=='fair_value_book_imbalance')
    rows=tournament_events()
    result=replay(manifest(strategy_id='CONFIGURED_RESEARCH'),rows,strategy_definition=base)
    assert result['research_rounds'][0]['pnl'] is None
    rows.append(event('resolution',301_000_000,2000,dict(id='m',slug=rows[0]['slug'],conditionId='c',closed=True,
        endDate=datetime.fromtimestamp(END/1e6,timezone.utc).isoformat(),outcomes=['UP','DOWN'],clobTokenIds=['up','down'],
        outcomePrices=['1','0'],umaResolutionStatus='resolved')))
    settled=replay(manifest(strategy_id='CONFIGURED_RESEARCH'),rows,strategy_definition=base)
    assert float(settled['research_rounds'][0]['pnl'])==pytest.approx(2.31264)
    assert 'CALLER_SUPPLIED' in settled['research_rounds'][0]['details']['settlement_evidence']['kind']
