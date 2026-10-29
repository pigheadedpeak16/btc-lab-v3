"""Native inputs stay read-only and never turn receive time into processing time."""
import importlib
import json
import sqlite3
import zipfile
from pathlib import Path

import pytest

from tests.test_stage7_replay import START, END, events, manifest


def api():
    from btc_v3.replay import readers
    assert hasattr(readers, 'inspect_native_window'), 'Missing bounded native-window adapter'
    return readers


def legacy(path):
    with sqlite3.connect(path) as conn:
        conn.executescript('''CREATE TABLE market_tokens(slug TEXT,side TEXT,token_id TEXT);
        CREATE UNIQUE INDEX idx_market_tokens_token ON market_tokens(token_id);
        CREATE TABLE book_checkpoints(id INTEGER PRIMARY KEY,token_id TEXT,received_at_us INTEGER,source_ts_us INTEGER);
        CREATE INDEX idx_book_cp_token_recv ON book_checkpoints(token_id,received_at_us);
        CREATE TABLE book_deltas(id INTEGER PRIMARY KEY,token_id TEXT,received_at_us INTEGER,source_ts_us INTEGER);
        CREATE INDEX idx_book_delta_token_recv ON book_deltas(token_id,received_at_us);''')
        slug=events()[0]['slug']
        conn.executemany('INSERT INTO market_tokens VALUES(?,?,?)', [(slug,'UP','up'),(slug,'DOWN','down')])
        conn.execute('INSERT INTO book_checkpoints VALUES(1,?,?,?)', ('up',START+1,START))


def test_legacy_native_observations_are_counted_but_not_fabricated_into_events(tmp_path):
    path=tmp_path/'market.sqlite';legacy(path)
    before=(path.read_bytes(),path.stat().st_mtime_ns,set(tmp_path.iterdir()))
    report=api().inspect_native_window(path,start_us=START,end_us=END)
    assert report['status']=='UNSUPPORTED_INPUTS'
    assert report['counts']['book_checkpoints']==1
    assert 'PROCESSING_CHRONOLOGY_NOT_CAPTURED' in report['unsupported_reasons']
    assert (path.read_bytes(),path.stat().st_mtime_ns,set(tmp_path.iterdir()))==before


def test_native_reader_refuses_uncheckpointed_wal_and_missing_files(tmp_path):
    path=tmp_path/'market.sqlite';legacy(path)
    Path(str(path)+'-wal').write_bytes(b'outstanding')
    with pytest.raises(ValueError,match='WAL'):api().inspect_native_window(path,start_us=START,end_us=END)
    missing=tmp_path/'absent.sqlite'
    with pytest.raises(ValueError,match='MISSING'):api().inspect_native_window(missing,start_us=START,end_us=END)
    assert not missing.exists()


def test_native_reader_requires_bounded_window_and_indexed_counts(tmp_path):
    path=tmp_path/'market.sqlite';legacy(path)
    with pytest.raises(ValueError,match='WINDOW'):api().inspect_native_window(path,start_us=START,end_us=END+1)
    with sqlite3.connect(path) as conn:conn.execute('DROP INDEX idx_book_cp_token_recv')
    report=api().inspect_native_window(path,start_us=START,end_us=END)
    assert report['coverage']['book_checkpoints']=='INDEX_REQUIRED'


def test_bounded_export_reads_only_window_and_compares_original_policy(tmp_path):
    path=tmp_path/'forward.zip'
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('00_session_manifest.json', json.dumps({'session_id':'s','execution_policy_revision':'stage3_depth_heartbeat_v1'}))
        z.writestr('01_wallet_signals.csv',f'signal_id,source_first_seen_us,slug\na,{START+1},{events()[0]["slug"]}\nb,{END+1},other\n')
        z.writestr('05_fills.csv',f'fill_id,strategy,slug,fill_ts_us,shares,notional,fee\nf,S9_BOOK_CONFIRM_75,{events()[0]["slug"]},{START+5},10,5,.1\n')
    report=api().read_forward_window(path,start_us=START,end_us=END)
    assert len(report['signals'])==1 and len(report['fills'])==1
    assert report['replay_supported'] is False
    compared=api().reconcile_forward({'policies':{'execution':'stage4_execution_realism_v1'},'fills':[]},report)
    assert compared['status']=='INCOMPARABLE_EXECUTION_POLICY'
    assert compared['recorded_fills']==1


def test_reconciliation_matches_economic_rows_and_flags_mismatches():
    fills=[dict(strategy='S9_BOOK_CONFIRM_75',slug='m',at_us=START,shares='10.0',notional='5',fee='.1')]
    recorded=dict(execution_policy_revision='p',fills=[dict(strategy='S9_BOOK_CONFIRM_75',slug='m',fill_ts_us=str(START),shares='10',notional='5.00',fee='.10')])
    result=api().reconcile_forward(dict(policies={'execution':'p'},fills=fills),recorded)
    assert result['status']=='MATCH'
    recorded['fills'][0]['fee']='.2'
    result=api().reconcile_forward(dict(policies={'execution':'p'},fills=fills),recorded)
    assert result['status']=='DISCREPANCY' and result['missing_recorded']==1


def test_real_export_manifest_layout_preserves_nested_original_revision(tmp_path):
    path=tmp_path/'real-layout.zip'
    with zipfile.ZipFile(path,'w') as z:
        z.writestr('00_session_manifest.json',json.dumps(dict(session_id='s',session=dict(
            config_json=json.dumps({'execution_policy_revision':'stage3_feed_integrity_v1'})),
            exported_summary='x'*350000)))
    assert api().read_forward_window(path,start_us=START,end_us=END)['execution_policy_revision']=='stage3_feed_integrity_v1'
