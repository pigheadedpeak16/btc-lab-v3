from http.server import ThreadingHTTPServer
import threading,http.client,json
from contextlib import contextmanager
from btc_v3.forensics.browser_dashboard import handler_factory


@contextmanager
def server(root):
    s=ThreadingHTTPServer(('127.0.0.1',0),handler_factory(root));t=threading.Thread(target=s.serve_forever,daemon=True);t.start()
    try:yield s
    finally:s.shutdown();s.server_close();t.join(3)


def request(s,method,path,body=None,origin=True):
    c=http.client.HTTPConnection('127.0.0.1',s.server_port,timeout=10)
    headers={'Content-Type':'application/json'}
    if origin:headers['Origin']=f'http://127.0.0.1:{s.server_port}' if origin is True else origin
    c.request(method,path,None if body is None else json.dumps(body),headers)
    r=c.getresponse();data=r.read();status=r.status;c.close()
    return status,json.loads(data) if data and data.lstrip().startswith(b'{') else {'error':'NON_JSON_RESPONSE'}


def test_web_strategy_create_edit_archive_and_origin_guards(tmp_path):
    with server(tmp_path) as s:
        status,catalog=request(s,'GET','/api/lab/strategies');assert status==200
        assert not list(tmp_path.rglob('*.sqlite'))
        base=catalog['strategies'][0]
        status,saved=request(s,'POST','/api/lab/strategies',dict(name='Web strategy',config=base['config']))
        assert status==201 and saved['revision']==1
        for origin in (False,'https://evil.example'):
            status,_=request(s,'POST','/api/lab/strategies',dict(name='Rejected',config=base['config']),origin=origin)
            assert status==403
        status,_=request(s,'POST','/api/lab/strategies',dict(name='Conflict',config=base['config'],strategy_id=saved['strategy_id'],expected_revision=0))
        assert status==409
        status,result=request(s,'POST','/api/lab/strategies/archive',dict(strategy_id=saved['strategy_id'],expected_revision=1))
        assert status==200 and result['historical_versions_preserved'] is True


def test_web_replay_runs_saved_config_without_live_data_or_account(tmp_path):
    from tests.research_fixtures import tournament_events
    from tests.test_stage7_replay import manifest
    with server(tmp_path) as s:
        _,catalog=request(s,'GET','/api/lab/strategies')
        base=next(r for r in catalog['strategies'] if r['config']['strategy_family']=='fair_value_book_imbalance')
        text='\n'.join(json.dumps(r) for r in [manifest(strategy_id='CONFIGURED_RESEARCH')]+tournament_events())
        status,result=request(s,'POST','/api/lab/replay',dict(strategy_id=base['strategy_id'],revision=1,input_jsonl=text))
        assert status==200,result
        assert len(result['fills'])==1 and 'account' not in result
        assert result['run_id'] and result['profile']['config_sha256']==base['config_sha256']
        status,history=request(s,'GET','/api/lab/runs')
        assert status==200 and history['runs'][0]['run_id']==result['run_id']
        assert history['runs'][0]['settled_net_pnl'] is None
        status,download=request(s,'GET','/api/lab/runs?run_id='+result['run_id'])
        assert status==200 and download==result
        status,invalid=request(s,'GET','/api/lab/runs?run_id=..%2Fprivate')
        assert status==400 and invalid['error']=='LAB_INVALID_RUN_ID'
        assert not (tmp_path/'data').exists() and not (tmp_path/'research/v3_research.sqlite').exists()


def test_web_replay_low_disk_guard_runs_before_creating_any_stores(tmp_path,monkeypatch):
    import shutil
    from types import SimpleNamespace
    monkeypatch.setattr(shutil,'disk_usage',lambda p:SimpleNamespace(free=1))
    with server(tmp_path) as s:
        _,catalog=request(s,'GET','/api/lab/strategies')
        status,result=request(s,'POST','/api/lab/replay',dict(strategy_id=catalog['strategies'][0]['strategy_id'],revision=1,input_jsonl='{}\n{}'))
        assert status==400 and result['error']=='LAB_LOW_DISK_GUARD'
        assert not list(tmp_path.iterdir())


def test_two_tabs_cannot_overwrite_the_same_saved_revision(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    with server(tmp_path) as s:
        _,catalog=request(s,'GET','/api/lab/strategies')
        cfg=catalog['strategies'][0]['config']
        _,saved=request(s,'POST','/api/lab/strategies',dict(name='Original',config=cfg))
        def edit(name):return request(s,'POST','/api/lab/strategies',dict(name=name,config=cfg,strategy_id=saved['strategy_id'],expected_revision=1))[0]
        with ThreadPoolExecutor(max_workers=2) as pool:statuses=list(pool.map(edit,['Tab one','Tab two']))
        assert sorted(statuses)==[201,409]
        from btc_v3.strategies.lab_profiles import get_profile
        assert get_profile(tmp_path,saved['strategy_id'],revision=1)['name']=='Original'
