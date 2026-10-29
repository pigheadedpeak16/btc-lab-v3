"""Loopback strategy-workbench API. Never starts feeds or opens historical stores."""
from dataclasses import asdict,replace
from pathlib import Path
import json,re,threading,uuid,shutil,tempfile
from decimal import Decimal,InvalidOperation
from itertools import islice
from ..strategies.lab_profiles import catalog,save_profile,archive_profile,get_profile

_replay_lock=threading.Lock()


def _run_directory(root):
    root=Path(root).resolve()
    directory=root/'research/strategy_runs'
    if (root/'research').is_symlink() or directory.is_symlink():raise ValueError('LAB_SYMLINK_FORBIDDEN')
    return directory


def saved_run(root,run_id):
    """Read one bounded immutable report; IDs cannot name arbitrary files."""
    if not isinstance(run_id,str) or not re.fullmatch('[0-9a-f]{32}',run_id):raise ValueError('LAB_INVALID_RUN_ID')
    path=_run_directory(root)/(run_id+'.json')
    if path.is_symlink():raise ValueError('LAB_SYMLINK_FORBIDDEN')
    if not path.is_file():raise ValueError('LAB_RUN_NOT_FOUND')
    if path.stat().st_size>2_000_000:raise ValueError('LAB_REPLAY_OUTPUT_LIMIT')
    with path.open('rb') as stream:raw=stream.read(2_000_001)
    if len(raw)>2_000_000:raise ValueError('LAB_REPLAY_OUTPUT_LIMIT')
    result=json.loads(raw)
    if not isinstance(result,dict) or result.get('run_id')!=run_id:raise ValueError('LAB_INVALID_RUN_REPORT')
    return result


def saved_runs(root):
    """Latest 20 workbench runs, at most 4 MB read per explicit refresh."""
    directory=_run_directory(root)
    if not directory.is_dir():return dict(runs=[],errors=[],limited=False)
    paths=list(islice(directory.glob('*.json'),101))
    paths=sorted(paths,key=lambda p:p.lstat().st_mtime_ns,reverse=True)
    result=[];errors=[];used=0;limited=len(paths)>20
    for path in paths[:20]:
        try:
            if path.is_symlink():raise ValueError('LAB_SYMLINK_FORBIDDEN')
            size=path.stat().st_size
            if size>2_000_000:raise ValueError('LAB_REPLAY_OUTPUT_LIMIT')
            if used+size>4_000_000:limited=True;break
            used+=size;run=saved_run(root,path.stem)
            rounds=run.get('research_rounds',[]);settled=[r for r in rounds if r.get('pnl') is not None]
            net=sum((Decimal(str(r['pnl'])) for r in settled),Decimal(0))
            fees=sum((Decimal(str(r['fee'])) for r in run['fills']),Decimal(0))
            if not net.is_finite() or not fees.is_finite():raise ValueError('LAB_INVALID_RUN_REPORT')
            profile=run['profile'];manifest=run['manifest']
            result.append(dict(run_id=path.stem,name=profile['name'],revision=profile['revision'],status=run['status'],
                dataset=manifest['dataset_id'],start_us=manifest['start_us'],end_us=manifest['end_us'],
                fills=len(run['fills']),fees=str(fees),settled_markets=len(settled),unsettled_markets=len(rounds)-len(settled),
                settled_net_pnl=str(net) if settled else None,unsupported_reasons=run.get('unsupported_reasons',[])))
        except (ValueError,OSError,KeyError,TypeError,InvalidOperation):errors.append('LAB_RUN_REPORT_UNAVAILABLE:'+path.name)
    return dict(runs=result,errors=errors,limited=limited)


def error_code(exc):
    code=str(exc)
    return code if re.fullmatch('[A-Z][A-Z0-9_]{1,90}',code) else 'LAB_INVALID_CONFIGURATION_OR_INPUT'


def run_replay(root,body):
    from ..replay.contracts import ReplayManifest
    from ..replay.engine import replay
    if not _replay_lock.acquire(blocking=False):raise ValueError('LAB_REPLAY_BUSY')
    try:
        if any(shutil.disk_usage(p).free<8*2**30 for p in (root,tempfile.gettempdir())):raise ValueError('LAB_LOW_DISK_GUARD')
        profile=get_profile(root,body['strategy_id'],revision=body['revision'])
        text=body['input_jsonl']
        if not isinstance(text,str) or len(text.encode())>2_000_000:raise ValueError('LAB_REPLAY_INPUT_LIMIT')
        lines=text.splitlines()
        if not 2<=len(lines)<=5001:raise ValueError('LAB_REPLAY_EVENT_LIMIT')
        m=ReplayManifest.from_dict(json.loads(lines[0]))
        if m.end_us-m.start_us>1_800_000_000:raise ValueError('LAB_REPLAY_WINDOW_LIMIT')
        events=[json.loads(line) for line in lines[1:]]
        directory=_run_directory(root)
        if directory.exists() and sum(1 for _ in directory.glob('*.json'))>=100:raise ValueError('LAB_SAVED_RUN_LIMIT')
        result=replay(replace(m,strategy_id='CONFIGURED_RESEARCH'),events,strategy_definition=profile)
        result.update(profile=profile,run_id=uuid.uuid4().hex,mode='OFFLINE_SHADOW_RESEARCH',
            live_collection_started=False,source_authentication='CALLER_SUPPLIED_NOT_INDEPENDENTLY_AUTHENTICATED')
        encoded=json.dumps(result,allow_nan=False,sort_keys=True)
        if len(encoded.encode())>2_000_000:raise ValueError('LAB_REPLAY_OUTPUT_LIMIT')
        directory.mkdir(parents=True,exist_ok=True)
        with (directory/(result['run_id']+'.json')).open('x',encoding='utf-8') as stream:stream.write(encoded)
        return result
    finally:_replay_lock.release()


def dispatch(root,path,body):
    if path=='/api/lab/strategies':
        if set(body)-{'name','config','strategy_id','expected_revision'}:raise ValueError('LAB_UNKNOWN_FIELDS')
        return 201,save_profile(root,**body)
    if path=='/api/lab/strategies/archive':return 200,archive_profile(root,body['strategy_id'],expected_revision=body['expected_revision'])
    if path=='/api/lab/replay':return 200,run_replay(root,body)
    raise ValueError('LAB_ROUTE_UNKNOWN')
