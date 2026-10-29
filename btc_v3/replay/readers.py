"""Bounded immutable input readers. No project DB constructor or network access."""
from pathlib import Path
from contextlib import closing
import json
import zipfile

from .contracts import ReplayManifest, ReplayEvent, validate_events


def _window(start_us,end_us):
    if (type(start_us) is not int or type(end_us) is not int
        or not 0<=start_us<end_us or end_us-start_us>300_000_000):
        raise ValueError('NATIVE_WINDOW_LIMIT')


def inspect_native_window(path,*,start_us,end_us,max_rows=2000):
    """Inventory a stopped legacy SQLite window using existing indexes only.

    Legacy normalized depth captures receipt but not application/processing clocks
    or the complete invalidation sequence. Counting it does NOT certify replay.
    immutable=1 deliberately refuses nonempty WAL/journal: never ignore pending data.
    """
    import sqlite3
    import time
    _window(start_us,end_us)
    if type(max_rows) is not int or not 1<=max_rows<=2000:raise ValueError('NATIVE_ROW_LIMIT')
    path=Path(path).resolve()
    if not path.is_file():raise ValueError('NATIVE_DATABASE_MISSING')
    companions=[Path(str(path)+suffix) for suffix in ('-wal','-journal')]
    if any(p.exists() and p.stat().st_size for p in companions):raise ValueError('NATIVE_UNCHECKPOINTED_WAL_OR_JOURNAL')
    before=(path.stat().st_size,path.stat().st_mtime_ns)
    counts={};coverage={};deadline=[time.monotonic()+.75]
    with closing(sqlite3.connect(path.as_uri()+'?mode=ro&immutable=1',uri=True,timeout=.1)) as conn:
        conn.execute('PRAGMA query_only=ON');conn.execute('PRAGMA cache_size=-256');conn.execute('PRAGMA temp_store=MEMORY')
        conn.set_progress_handler(lambda:int(time.monotonic()>deadline[0]),1000)
        tables={r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 128")}
        tokens=[]
        if 'market_tokens' in tables:
            slugs=[f'btc-updown-5m-{s}' for s in range(start_us//300_000_000*300,(end_us-1)//300_000_000*300+1,300)]
            for slug in slugs:
                tokens.extend(r[0] for r in conn.execute('SELECT token_id FROM market_tokens WHERE slug=? LIMIT 3',(slug,)))
        for table,index in [('book_checkpoints','idx_book_cp_token_recv'),('book_deltas','idx_book_delta_token_recv')]:
            deadline[0]=time.monotonic()+.75
            counts[table]=0
            if table not in tables:coverage[table]='TABLE_MISSING';continue
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",(index,)).fetchone():
                coverage[table]='INDEX_REQUIRED';continue
            try:
                for token in tokens:
                    n=conn.execute(f'''SELECT COUNT(*) FROM (SELECT 1 FROM {table} INDEXED BY {index}
                        WHERE token_id=? AND received_at_us>=? AND received_at_us<? LIMIT ?)''',
                        (token,start_us,end_us,max_rows+1)).fetchone()[0]
                    counts[table]+=n
                coverage[table]='CAPPED' if counts[table]>max_rows else 'BOUNDED_OBSERVATIONS'
            except sqlite3.OperationalError:coverage[table]='QUERY_BUDGET_OR_SCHEMA_UNSUPPORTED'
    if before!=(path.stat().st_size,path.stat().st_mtime_ns) or any(p.exists() and p.stat().st_size for p in companions):
        raise ValueError('NATIVE_SOURCE_CHANGED_DURING_READ')
    return dict(path=str(path),start_us=start_us,end_us=end_us,counts=counts,coverage=coverage,
        status='UNSUPPORTED_INPUTS',unsupported_reasons=['PROCESSING_CHRONOLOGY_NOT_CAPTURED',
        'COMPLETE_INVALIDATION_SEQUENCE_NOT_ESTABLISHED','WALLET_PROCESSING_ORDER_NOT_ESTABLISHED'],
        preservation='immutable read-only, no WAL/journal consumed; size/mtime unchanged')


def read_forward_window(path,*,start_us,end_us):
    """Capped CSV comparison evidence, never an executable liquidity adapter."""
    import csv
    import io
    _window(start_us,end_us)
    inventory=inspect_export(path)
    output=dict(start_us=start_us,end_us=end_us,signals=[],fills=[],replay_supported=False,
        execution_policy_revision=None,session_id=None,unsupported_reasons=inventory['reasons'])
    total=0
    with zipfile.ZipFile(path) as archive:
        if '00_session_manifest.json' in archive.namelist():
            info=archive.getinfo('00_session_manifest.json')
            if info.file_size>1_000_000:raise ValueError('FORWARD_MANIFEST_LIMIT')
            data=json.loads(archive.read(info))
            # Export versions put policy under session config/summary as well.
            configs=[data]
            for key in ('config','session','summary','manifest'):
                value=data.get(key)
                if isinstance(value,str):
                    try:value=json.loads(value)
                    except ValueError:value=None
                if isinstance(value,dict):configs.append(value)
            for value in tuple(configs):
                if isinstance(value.get('config_json'),str):
                    parsed=json.loads(value['config_json'])
                    if isinstance(parsed,dict):configs.append(parsed)
            for value in configs:
                output['session_id']=output['session_id'] or value.get('session_id')
                output['execution_policy_revision']=output['execution_policy_revision'] or value.get('execution_policy_revision')
        for filename,key,clock in [('01_wallet_signals.csv','signals','source_first_seen_us'),('05_fills.csv','fills','fill_ts_us')]:
            if filename not in archive.namelist():continue
            info=archive.getinfo(filename)
            if info.file_size>16_000_000:raise ValueError('FORWARD_INPUT_LIMIT')
            with archive.open(info) as raw:
                def lines():
                    nonlocal total
                    while line:=raw.readline(262145):
                        total+=len(line)
                        if len(line)>262144 or total>16_000_000:raise ValueError('FORWARD_INPUT_LIMIT')
                        yield line.decode('utf-8-sig')
                for row in csv.DictReader(lines()):
                    at=int(row[clock])
                    if start_us<=at<end_us:
                        if len(output[key])>=2000:raise ValueError('FORWARD_ROW_LIMIT')
                        # Whitelist research fields; raw payloads and wallets never exported.
                        fields=('signal_id','slug','token_id','side','outcome','source_first_seen_us','trade_ts_us','target_price','target_size') if key=='signals' else ('strategy','slug','token_id','outcome','fill_ts_us','shares','notional','fee')
                        output[key].append({k:row[k] for k in fields if k in row})
    return output


def reconcile_forward(replayed,recorded):
    """Compare economic multisets under equal execution revisions, never pool cohorts."""
    from collections import Counter
    from decimal import Decimal
    result=dict(recorded_fills=len(recorded['fills']),replayed_fills=len(replayed['fills']),
        recorded_execution_policy=recorded.get('execution_policy_revision'),
        replay_execution_policy=replayed['policies']['execution'])
    if not result['recorded_execution_policy'] or result['recorded_execution_policy']!=result['replay_execution_policy']:
        return {**result,'status':'INCOMPARABLE_EXECUTION_POLICY'}
    def key(row):
        values=[Decimal(str(row[n])) for n in ('shares','notional','fee')]
        if any(not v.is_finite() or v<0 for v in values):raise ValueError('INVALID_RECORDED_FILL')
        return (row['strategy'],row['slug'],int(row.get('at_us',row.get('fill_ts_us'))),*values)
    actual=Counter(map(key,replayed['fills']));old=Counter(map(key,recorded['fills']))
    return {**result,'status':'MATCH' if actual==old else 'DISCREPANCY',
        'missing_recorded':sum((old-actual).values()),'new_replay':sum((actual-old).values())}




def read_jsonl(path,*,max_bytes=16_000_000,max_events=50000):
    path=Path(path)
    if max_bytes>16_000_000 or max_events>50000 or path.stat().st_size>max_bytes:raise ValueError('REPLAY_INPUT_LIMIT')
    with path.open(encoding='utf-8-sig') as stream:
        line=stream.readline(262145)
        if len(line)>262144:raise ValueError('REPLAY_LINE_LIMIT')
        m=ReplayManifest.from_dict(json.loads(line));rows=[]
        while line:=stream.readline(262145):
            if len(line)>262144 or len(rows)>=max_events:raise ValueError('REPLAY_INPUT_LIMIT')
            rows.append(ReplayEvent.from_dict(json.loads(line)))
    return m,validate_events(m,rows)


def inspect_export(path):
    """Inspect names/headers only. Exported fills never become counterfactual depth."""
    with zipfile.ZipFile(path) as archive:
        infos=archive.infolist()
        if len(infos)>128:raise ValueError('REPLAY_EXPORT_MEMBER_LIMIT')
        names=[i.filename for i in infos]
        if any(Path(n).is_absolute() or '..' in Path(n).parts or '\\' in n for n in names):
            raise ValueError('REPLAY_UNSAFE_ARCHIVE_PATH')
        headers={}
        for info in infos:
            if info.filename.endswith('.csv'):
                with archive.open(info) as stream:headers[info.filename]=stream.readline(4096).decode('utf-8-sig',errors='replace').strip()
        # No automatic adapter from forward summaries to an original event stream.
        return dict(path=str(Path(path).resolve()),members=names,headers=headers,replay_supported=False,
            reasons=['ORIGINAL_EVENT_STREAM_MISSING','SOURCE_RECEIVE_PROCESSING_SEQUENCE_NOT_ESTABLISHED',
                'ORIGINAL_L2_AND_FAULT_STATE_NOT_ESTABLISHED','PAYOUT_TIMING_NOT_ESTABLISHED'])
