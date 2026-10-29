"""Product retirement and connected strategy replay; all IO uses temporary roots."""
import json
import pytest
from btc_v3.forensics import dashboard_v4 as dashboard
from btc_v3.forensics import lab_web
from btc_v3 import cli


def test_minimal_navigation_has_no_retired_product_features():
    assert "const names=['Live Data','Strategies','Results','Diagnostics']" in dashboard.HTML
    for text in ('$50','Adaptive selector','Account Audit','Selector Research','Historical Replay'):
        assert text not in dashboard.HTML


def test_retired_commands_and_funding_option_are_unavailable():
    parser=cli.build_parser()
    choices=parser._subparsers._group_actions[0].choices
    assert not {'paper-account-init','selector-decide','paper-run-public','replay-research',
                'paper-selector-account-init','paper-window-plan','research-fit','replay-native-capture'} & choices.keys()
    for args in (['paper-account-init'],['selector-decide'],['paper-run-public'],['replay-research'],
                 ['forward-wallet-lab','--funded-account-id','test']):
        with pytest.raises(SystemExit):parser.parse_args(args)


def test_empty_snapshot_never_offers_account_or_selector(tmp_path):
    data=dashboard.snapshot(tmp_path,now_us=1)
    assert not {'account','selector','settlements','operations','replays'} & data.keys()
    assert not list(tmp_path.iterdir())


def test_saved_workbench_run_is_listed_and_downloaded_without_replay(tmp_path):
    directory=tmp_path/'research/strategy_runs';directory.mkdir(parents=True)
    run=dict(run_id='a'*32,status='SUPPORTED_SYNTHETIC_OR_CURATED',profile=dict(name='Example',revision=2),
        manifest=dict(dataset_id='fixture',start_us=10,end_us=20),fills=[dict(fee='.1')],
        research_rounds=[dict(pnl='1.2')],unsupported_reasons=[])
    (directory/(run['run_id']+'.json')).write_text(json.dumps(run))
    result=lab_web.saved_runs(tmp_path)
    assert result['runs'][0]['run_id']==run['run_id']
    assert result['runs'][0]['settled_net_pnl']=='1.2'
    assert lab_web.saved_run(tmp_path,run['run_id'])==run
    with pytest.raises(ValueError):lab_web.saved_run(tmp_path,'../private')


def test_saved_run_reader_is_bounded_and_rejects_symlinks(tmp_path):
    directory=tmp_path/'research/strategy_runs';directory.mkdir(parents=True)
    (directory/('b'*32+'.json')).write_text('x'*2_000_001)
    (directory/('c'*32+'.json')).write_text('not json')
    data=lab_web.saved_runs(tmp_path)
    assert not data['runs'] and len(data['errors'])==2


def test_retired_schema_is_not_created_or_deleted_in_existing_stores(tmp_path):
    import sqlite3
    from btc_v3.storage.db import initialize_project_databases
    initialize_project_databases(tmp_path)
    path=tmp_path/'research/v3_research.sqlite'
    with sqlite3.connect(path) as conn:
        assert conn.execute("SELECT name FROM sqlite_master WHERE name LIKE 'paper_account%' OR name LIKE 'paper_selector%'").fetchall()==[]
        conn.execute('CREATE TABLE paper_accounts (historical_marker TEXT)')
        conn.execute("INSERT INTO paper_accounts VALUES ('preserve legacy history')")
    initialize_project_databases(tmp_path)
    with sqlite3.connect(path) as conn:
        assert conn.execute('SELECT historical_marker FROM paper_accounts').fetchone()[0]=='preserve legacy history'


def test_retired_modes_are_rejected_even_with_prebuilt_manifest():
    from btc_v3.replay.engine import replay
    from btc_v3.replay.contracts import ReplayManifest
    from tests.test_stage7_replay import manifest,events
    for strategy in ('S5_S6_RESEARCH_COHORT','S9_BUY_TAKER_COHORT'):
        with pytest.raises(ValueError,match='MANIFEST'):
            replay(ReplayManifest(**manifest(strategy_id=strategy)),events())


def test_legacy_account_table_is_not_queried_by_dashboard(tmp_path,monkeypatch):
    from btc_v3.storage.db import initialize_project_databases
    from contextlib import contextmanager
    initialize_project_databases(tmp_path)
    real=dashboard.reader;queries=[]
    @contextmanager
    def traced(path):
        with real(path) as conn:
            conn.set_trace_callback(queries.append);yield conn
    monkeypatch.setattr(dashboard,'reader',traced)
    dashboard.snapshot(tmp_path,now_us=1)
    assert not any('paper_account' in query or 'paper_selector' in query for query in queries)


def test_report_reader_refuses_symlinked_research_parent(tmp_path,monkeypatch):
    from pathlib import Path
    original=Path.is_symlink
    monkeypatch.setattr(Path,'is_symlink',lambda path:path==tmp_path/'research' or original(path))
    with pytest.raises(ValueError,match='SYMLINK'):
        lab_web.saved_runs(tmp_path)


def test_saved_history_caps_count_and_orders_newest_first(tmp_path):
    import os
    directory=tmp_path/'research/strategy_runs';directory.mkdir(parents=True)
    for i in range(22):
        run_id=format(i,'032x');path=directory/(run_id+'.json')
        path.write_text(json.dumps(dict(run_id=run_id,status='SUPPORTED_SYNTHETIC_OR_CURATED',
            profile=dict(name='Version '+str(i),revision=1),manifest=dict(dataset_id='fixture',start_us=1,end_us=2),
            fills=[],research_rounds=[])))
        # Windows file times have 100 ns granularity; use distinct milliseconds.
        os.utime(path,ns=(1_000_000_000+i*1_000_000,1_000_000_000+i*1_000_000))
    result=lab_web.saved_runs(tmp_path)
    assert len(result['runs'])==20 and result['limited'] is True
    assert result['runs'][0]['run_id']==format(21,'032x')
    assert result['runs'][-1]['run_id']==format(2,'032x')


def test_saved_history_caps_total_bytes_and_still_returns_valid_runs(tmp_path):
    directory=tmp_path/'research/strategy_runs';directory.mkdir(parents=True)
    for i in range(4):
        run_id=format(i,'032x')
        (directory/(run_id+'.json')).write_text(json.dumps(dict(run_id=run_id,status='SUPPORTED_SYNTHETIC_OR_CURATED',
            profile=dict(name='Example',revision=1),manifest=dict(dataset_id='fixture',start_us=1,end_us=2),
            fills=[],research_rounds=[],padding='x'*1_100_000)))
    result=lab_web.saved_runs(tmp_path)
    assert len(result['runs'])==3 and result['limited'] is True and not result['errors']
