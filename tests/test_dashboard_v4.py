import importlib
import importlib.util
import json
import sqlite3
from decimal import Decimal as D
from pathlib import Path

import pytest

from tests.test_stage7_replay import START
from btc_v3.storage.db import initialize_project_databases
from btc_v3.forensics.forward_store import ForwardWalletStore


def api():
    assert importlib.util.find_spec('btc_v3.forensics.dashboard_v4') is not None,'Missing seven-view read-only V4 control center'
    return importlib.import_module('btc_v3.forensics.dashboard_v4')


def test_missing_project_is_safe_and_creates_no_database(tmp_path):
    result=api().snapshot(tmp_path,now_us=START)
    assert result['runner']['state']=='STOPPED'
    assert 'account' not in result
    assert list(tmp_path.iterdir())==[]


def test_stale_running_and_future_heartbeats_are_not_live():
    function=api().runner_status
    assert function({'state':'RUNNING','started_at_us':START},START,START+20_000_000)['state']=='STALE'
    assert function({'state':'RUNNING','started_at_us':START},START+1,START)['state']=='ERROR'
    assert function({'state':'STOPPED','started_at_us':START},START,START)['state']=='STOPPED'
    assert function({'state':'RUNNING','started_at_us':START},START,START+1000)['state']=='LIVE'






def test_strategy_metrics_distinguish_markets_and_profit_concentration():
    rows=[dict(strategy='S',slug='a',round_end_us=1,pnl='-2',fees='.1',up_cost='1',down_cost='0'),
        dict(strategy='S',slug='b',round_end_us=2,pnl='10',fees='.1',up_cost='1',down_cost='0'),
        dict(strategy='S',slug='c',round_end_us=3,pnl=None,fees='.1',up_cost='1',down_cost='0')]
    result=api().market_metrics(rows)[0]
    assert result['settled_markets']==2 and result['unsettled_markets']==1
    assert result['net_pnl']=='8' and result['leave_largest_winner_out']=='-2'
    assert result['drawdown']=='2'
    assert result['evidence']=='INSUFFICIENT_EVIDENCE'


def test_bad_numeric_data_is_flagged_not_replaced_by_zero():
    row=dict(strategy='S',slug='a',round_end_us=1,pnl='NaN',fees='0',up_cost='1',down_cost='0')
    assert api().market_metrics([row])[0]['evidence']=='INVALID_LEDGER_VALUE'


def test_new_ui_preserves_legacy_entry_point_and_has_all_views():
    from btc_v3.forensics import browser_dashboard
    html=api().HTML
    for label in ['Live Data','Strategies','Results','Diagnostics']:
        assert label in html
    assert 'PAPER ONLY' in html and '/api/v4/status' in html
    assert getattr(browser_dashboard,'V4_ENABLED',False),'Existing launcher must serve V4'


def test_snapshot_is_bounded_and_labels_policy_cohorts(tmp_path):
    initialize_project_databases(tmp_path)
    with ForwardWalletStore.open_project(tmp_path) as store:
        store.start_session(session_id='fixture',wallet='public',started_at_us=START,config={'execution_policy_revision':'fixture-policy'})
        store.conn.execute("UPDATE wallet_forward_sessions SET state='STOPPED'");store.conn.commit()
    result=api().snapshot(tmp_path,now_us=START)
    assert result['runner']['state']=='STOPPED'
    assert result['cohort']['execution_policy_revision']=='fixture-policy'
    assert result['shadow_scope']['maximum_round_rows']<=2000
    assert 'selector' not in result


def test_cache_is_bounded_and_reports_collection_age(tmp_path):
    clock=[100.0]
    cache=api().SnapshotCache(ttl_seconds=3,max_entries=2,clock=lambda:clock[0])
    first=cache.get(tmp_path,now_us=START)
    clock[0]=101.0
    second=cache.get(tmp_path,now_us=START+1_000_000)
    assert second['cache_age_seconds']==1
    assert first['observed_at_us']==second['observed_at_us']
    clock[0]=105
    assert cache.get(tmp_path,now_us=START+5_000_000)['cache_age_seconds']==0


def test_no_fill_rounds_cannot_inflate_independent_traded_market_count():
    row=dict(strategy='S',slug='a',round_end_us=1,pnl='0',fees='0',up_cost='0',down_cost='0')
    result=api().market_metrics([row])
    assert not result or result[0]['settled_markets']==0


def test_conflicting_market_rows_are_not_silently_deduplicated():
    row=dict(strategy='S',slug='a',round_end_us=1,pnl='2',fees='0',up_cost='1',down_cost='0')
    other={**row,'pnl':'100'}
    assert api().market_metrics([row,other])[0]['evidence']=='INVALID_LEDGER_VALUE'


def test_unreadable_research_database_reports_error_not_stopped(tmp_path):
    folder=tmp_path/'research';folder.mkdir();(folder/'v3_research.sqlite').write_text('not a database')
    result=api().snapshot(tmp_path,now_us=START)
    assert result['runner']['state']=='ERROR'


def test_feed_detail_whitelist_does_not_return_secrets_or_sdk_exceptions():
    function=getattr(api(),'public_feed_details',None);assert function is not None,'Missing public-field projection for feed metadata'
    result=function(json.dumps({'source':'official','stream':'BTC','exception':'synthetic secret value',
        'private_key':'synthetic-test-value','api_key':'synthetic-key','real_trading':'DISABLED'}))
    assert 'synthetic' not in json.dumps(result) and result['source']=='official'






def test_many_market_rows_are_capped_and_clearly_windowed(tmp_path):
    initialize_project_databases(tmp_path)
    with ForwardWalletStore.open_project(tmp_path) as store:
        store.start_session(session_id='bounded',wallet='fixture',started_at_us=START,config={'synthetic':True})
        with store.conn:
            store.conn.executemany('''INSERT INTO wallet_forward_rounds(session_id,strategy,slug,round_start_us,round_end_us,
                up_shares,down_shares,up_cost,down_cost,sell_proceeds,fees,pnl,details_json)
                VALUES('bounded','S',?,?,?,'5','0','2.5','0','0','.1','-2.6','{}')''',
                [(f'fixture-{i}',START+i*300_000_000,START+(i+1)*300_000_000) for i in range(5000)])
    result=api().snapshot(tmp_path,now_us=START)
    assert result['shadow_scope']['returned_round_rows']==2000
    assert result['strategies'][0]['settled_markets']==2000
    assert 'BOUNDED_ROWS' in result['shadow_scope']['scope']
