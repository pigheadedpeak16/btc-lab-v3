"""Bounded, read-only V4 projections; financial definitions live outside HTML."""
from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal,InvalidOperation
from pathlib import Path
from collections import OrderedDict
import json
import math
import shutil
import sqlite3
import threading
import time

DASHBOARD_REVISION='research_lab_minimal_v1'


@contextmanager
def reader(path):
    path=Path(path)
    if not path.is_file():raise FileNotFoundError(path)
    conn=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=.25)
    conn.row_factory=sqlite3.Row
    conn.execute('PRAGMA query_only=ON');conn.execute('PRAGMA busy_timeout=250')
    deadline=time.monotonic()+.35
    conn.set_progress_handler(lambda:int(time.monotonic()>deadline),1000)
    try:yield conn
    finally:conn.close()


def rows(conn,sql,args=()):
    return [dict(r) for r in conn.execute(sql,args)]


def table_exists(conn,name):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(name,)).fetchone() is not None


def runner_status(session,sample_ts_us,now_us):
    if not session:return dict(state='STOPPED',reason='NO_RECORDED_SESSION',heartbeat_age_seconds=None)
    if sample_ts_us is not None and sample_ts_us>now_us:return dict(state='ERROR',reason='FUTURE_HEARTBEAT',heartbeat_age_seconds=None)
    age=None if sample_ts_us is None else (now_us-sample_ts_us)/1e6
    state=str(session.get('state','UNKNOWN')).upper()
    if state=='RUNNING':state='LIVE' if age is not None and age<=15 else 'STALE'
    elif state in {'CRASHED','ERROR','FAILED'}:state='ERROR'
    else:state='STOPPED'
    return dict(state=state,reason='RECORDED_STATE_AND_SAMPLE_HEARTBEAT',heartbeat_age_seconds=age,
        process_liveness='NOT_INDEPENDENTLY_OS_VERIFIED')




def amount(value):
    number=Decimal(str(value))
    if not number.is_finite():raise ValueError('INVALID_LEDGER_VALUE')
    return number


def public_feed_details(raw):
    if not isinstance(raw,str) or len(raw)>32768:return {'state':'DETAILS_UNAVAILABLE_OR_TOO_LARGE'}
    try:data=json.loads(raw)
    except ValueError:return {'state':'MALFORMED_DETAILS'}
    allowed={'source','stream','slug','real_trading','state','observation_count','mapped_ptb_count',
        'reconnect_count','error_count','skip_reason','receive_queue_peak','receive_queue_overflows',
        'heartbeat_pings','heartbeat_pongs','resync_requests','resync_successes','resync_failures',
        'bba_mismatches','resync_superseded','source_ahead_of_receive_events','source_ahead_peak_us',
        'resync_rate_limited',
        'clock_scope','mapping_counter_scope','latest_source_ts_us'}
    if not isinstance(data,dict):return {}
    result={key:data[key] for key in data if key in allowed and
        (data[key] is None or type(data[key]) in (str,int,bool) or type(data[key]) is float and math.isfinite(data[key]))}
    startup=data.get('startup_readiness')
    if isinstance(startup,dict):
        result['startup_readiness']={k:v for k,v in startup.items() if k in {
            'process_start_us','market_discovered_us','first_up_snapshot','first_down_snapshot','first_paired_book_us',
            'first_paired_book_latency_ms','first_execution_book_inputs_us','latest','rejection_checks','scope'}}
    clocks=data.get('clock_correlation')
    if isinstance(clocks,dict):
        result['clock_correlation']={k:clocks[k] for k in ('policy_revision','events','source_after_receipt','source_after_receipt_peak_us',
            'queue_delay_peak_us','utc_monotonic_drift_us','max_utc_monotonic_drift_us','attribution','clock_correction_applied') if k in clocks}
        result['clock_correlation']['minute_bins']=[{k:v for k,v in b.items() if k in {'minute','events','future','future_peak_us','queue_peak_us','utc_monotonic_drift_us'} and type(v) is int}
            for b in clocks.get('minute_bins',[])[:720] if isinstance(b,dict)]
    return result


def sample_diagnostics(sample,*,now_us,runner_state):
    def decode(value):
        if not isinstance(value,str) or len(value)>32768:return {}
        try:
            obj=json.loads(value);return obj if isinstance(obj,dict) else {}
        except ValueError:return {}
    lab=decode(sample.get('lab_json'));book=decode(sample.get('book_json'))
    stamp=sample.get('sample_ts_us');age=None if type(stamp) is not int else (now_us-stamp)/1e6
    current=runner_state=='LIVE' and age is not None and 0<=age<=15
    state='HISTORICAL_STOPPED' if runner_state=='STOPPED' else 'CURRENT_OBSERVATION' if current else 'STALE_OR_UNVERIFIED_OBSERVATION'
    reference=lab.get('market_reference') or sample.get('reference_diagnostics') or {'state':'UNAVAILABLE' if sample.get('ptb') is None else 'LEGACY_VALUE_WITHOUT_RETAINED_PROVENANCE'}
    sources=[]
    for name,row in (lab.get('predictor_health') or {}).items():
        if not isinstance(row,dict):continue
        sources.append(dict(source=name,state=row.get('state'),source_age_seconds=row.get('source_trade_age_s'),
            receipt_age_seconds=row.get('tick_age_s'),message_age_seconds=row.get('message_age_s')))
    if isinstance(sample.get('source_health'),list):sources=sample['source_health'][:2]
    return dict(observation_state=state,age_seconds=age,reference=reference,sources=sources,
        model={'state':sample.get('model_state',sample.get('state','UNAVAILABLE')),'reason':sample.get('unavailable_reason'),
            'calibration':lab.get('calibration_state','NOT_VERIFIED')},
        book=book,execution={'state':'NOT_EVALUATED','eligible':False,'reason':'Display never authorizes current signal, edge, risk or fill feasibility'},
        maker=lab.get('maker',{'state':'NO_CURRENT_MAKER_COUNTER_OBSERVATION'}))


def market_metrics(round_rows):
    groups={}
    for row in round_rows:groups.setdefault(str(row['strategy']),[]).append(row)
    result=[]
    for strategy,group in sorted(groups.items()):
        try:
            unique={}
            for r in group:
                cost=amount(r['up_cost'])+amount(r['down_cost'])
                if cost==0 and amount(r['fees'])==0 and (r['pnl'] is None or amount(r['pnl'])==0):continue
                if cost<=0:raise ValueError('INVALID_LEDGER_VALUE')
                if r['slug'] in unique and r!=unique[r['slug']]:raise ValueError('CONFLICTING_MARKET_ROWS')
                unique[r['slug']]=r
            if not unique:continue
            settled=[r for r in unique.values() if r['pnl'] is not None]
            pnls=[amount(r['pnl']) for r in sorted(settled,key=lambda r:(r['round_end_us'],r['slug']))]
            cost=sum((amount(r['up_cost'])+amount(r['down_cost']) for r in settled),Decimal(0))
            fees=sum((amount(r['fees']) for r in settled),Decimal(0));net=sum(pnls,Decimal(0))
            wins=sum(p>0 for p in pnls);losses=sum(p<0 for p in pnls);best=max([Decimal(0),*pnls])
            equity=peak=drawdown=Decimal(0)
            for pnl in pnls:equity+=pnl;peak=max(peak,equity);drawdown=max(drawdown,peak-equity)
            n=len(pnls);z=1.959963984540054
            if n:
                rate=wins/n;den=1+z*z/n;mid=(rate+z*z/(2*n))/den;half=z*math.sqrt(rate*(1-rate)/n+z*z/(4*n*n))/den
                interval=[max(0,mid-half),min(1,mid+half)]
            else:interval=None
            result.append(dict(strategy=strategy,settled_markets=n,unsettled_markets=len(unique)-n,wins=wins,losses=losses,
                net_pnl=str(net),settled_purchase_cost=str(cost),fees=str(fees),return_on_settled_cost=None if cost<=0 else str(net/cost),
                drawdown=str(drawdown),hit_rate=None if not n else wins/n,descriptive_win_interval=interval,
                largest_winner=str(best),leave_largest_winner_out=str(net-best),
                evidence='INSUFFICIENT_EVIDENCE',scope='ONE_SESSION_WINDOWED_INDEPENDENT_SHADOW'))
        except (ValueError,InvalidOperation,TypeError):result.append(dict(strategy=strategy,evidence='INVALID_LEDGER_VALUE'))
    return result


def snapshot(root,*,now_us):
    root=Path(root);result=dict(dashboard_revision=DASHBOARD_REVISION,paper_only=True,observed_at_us=now_us,
        fixture_mode=(root/'.dashboard_fixture').exists(),runner=runner_status(None,None,now_us),cohort={},sample={},
        strategies=[],feeds=[],errors=[],decisions=[],
        shadow_scope={'maximum_round_rows':2000,'scope':'LATEST_SESSION; BOUNDED_ROWS; INDEPENDENT_SHADOW_ARMS'},
        storage={'free_gib':round(shutil.disk_usage(root).free/2**30,2),'emergency_guard_gib':8})
    path=root/'research/v3_research.sqlite'
    if path.is_file():
        try:
            with reader(path) as conn:
                session=rows(conn,'SELECT * FROM wallet_forward_sessions ORDER BY started_at_us DESC LIMIT 1') if table_exists(conn,'wallet_forward_sessions') else []
                if session:
                    s=session[0];sid=s['session_id'];result['session_id']=sid
                    result['cohort']=json.loads(s.get('config_json') or '{}')
                    samples=rows(conn,'SELECT * FROM forensic_live_samples WHERE session_id=? ORDER BY sample_ts_us DESC LIMIT 1',(sid,)) if table_exists(conn,'forensic_live_samples') else []
                    sample=samples[0] if samples else {};result['sample']=sample
                    result['runner']=runner_status(s,sample.get('sample_ts_us'),now_us)
                    result['sample']['seconds_remaining_now']=max(0,(sample.get('round_end_us',now_us)-now_us)/1e6) if sample.get('round_end_us') else None
                    result['sample']['market_is_current']=bool(sample.get('round_start_us') is not None and sample['round_start_us']<=now_us<(sample.get('round_end_us') or 0))
                    rounds=rows(conn,'''SELECT strategy,slug,round_end_us,pnl,fees,up_cost,down_cost FROM wallet_forward_rounds
                        WHERE session_id=? AND (CAST(up_shares AS REAL)>0 OR CAST(down_shares AS REAL)>0 OR pnl IS NOT NULL)
                        ORDER BY round_start_us DESC LIMIT 2000''',(sid,))
                    result['strategies']=market_metrics(rounds)
                    result['shadow_scope']['returned_round_rows']=len(rounds)
                    if table_exists(conn,'wallet_strategy_decision_events'):
                        result['decisions']=rows(conn,'SELECT strategy,decided_at_us,decision,reason FROM wallet_strategy_decision_events WHERE session_id=? ORDER BY decided_at_us DESC LIMIT 100',(sid,))
        except (sqlite3.Error,ValueError,KeyError):
            result['errors'].append('RESEARCH_SCHEMA_QUERY_UNAVAILABLE_OR_REFRESH_BUDGET_EXCEEDED')
            result['runner']={'state':'ERROR','reason':'RESEARCH_OBSERVATIONS_UNAVAILABLE'}
    external=root/'data/v3_external.sqlite'
    if external.is_file():
        try:
            with reader(external) as conn:
                result['feeds']=rows(conn,'SELECT component,state,updated_at_us,last_message_at_us,last_tick_at_us,reconnect_count,error_count,details_json FROM external_status LIMIT 16')
                for feed in result['feeds']:feed['details_json']=json.dumps(public_feed_details(feed.get('details_json')),allow_nan=False)
        except sqlite3.Error:result['errors'].append('EXTERNAL_HEALTH_UNAVAILABLE')
    market_path=root/'data/v3_market.sqlite'
    if market_path.is_file():
        try:
            with reader(market_path) as conn:
                if table_exists(conn,'collector_status'):
                    result['feeds'].extend(rows(conn,'''SELECT component,state,updated_at_us,last_message_at_us,
                        last_book_at_us AS last_tick_at_us,reconnect_count,error_count,details_json
                        FROM collector_status LIMIT 16'''))
                    for feed in result['feeds']:
                        feed['details_json']=json.dumps(public_feed_details(feed.get('details_json')),allow_nan=False)
        except sqlite3.Error:result['errors'].append('CLOB_HEALTH_UNAVAILABLE')
    for feed in result['feeds']:
        at=feed.get('updated_at_us')
        feed['observation_state']='FUTURE_CLOCK' if at is not None and at>now_us else 'STALE_OBSERVATION' if at is None or now_us-at>15_000_000 else 'RECENT_OBSERVATION'
    result['diagnostics']=sample_diagnostics(result['sample'],now_us=now_us,runner_state=result['runner']['state'])
    return result


class SnapshotCache:
    def __init__(self,*,ttl_seconds=3,max_entries=16,clock=time.monotonic):
        self.ttl=ttl_seconds;self.maximum=max_entries;self.clock=clock;self.cache=OrderedDict();self.lock=threading.Lock()
    def get(self,root,*,now_us):
        key=str(Path(root).resolve());now=self.clock()
        with self.lock:
            existing=self.cache.get(key)
            if existing is None or now-existing[0]>=self.ttl:
                self.cache[key]=(now,snapshot(root,now_us=now_us));self.cache.move_to_end(key)
                while len(self.cache)>self.maximum:self.cache.popitem(last=False)
            at,data=self.cache[key];result=deepcopy(data);result['cache_age_seconds']=max(0,now-at)
            return result


HTML=r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>BTC LAB · Strategy Research</title>
<style>
:root{--bg:#091018;--panel:#111c28;--border:#263747;--text:#e1eaf2;--muted:#91a5b8;--cyan:#64d9cd;--red:#ff929b;--yellow:#f1c883}*{box-sizing:border-box}[hidden]{display:none!important}body{margin:0;background:var(--bg);color:var(--text);font:14px "Segoe UI",system-ui,sans-serif}header{display:flex;align-items:center;gap:20px;padding:22px 32px;border-bottom:1px solid var(--border);background:#101a25}h1{font-size:21px;letter-spacing:.1em;margin:0}h1 span{color:var(--cyan)}.subtitle,small,.muted{color:var(--muted)}.tag{font-size:11px;letter-spacing:.12em;border:1px solid #35776f;color:var(--cyan);padding:6px 10px;border-radius:5px}.right{margin-left:auto}button{font:inherit;background:#1a2b3c;color:var(--text);border:1px solid var(--border);border-radius:5px;padding:9px 13px;cursor:pointer}button:focus-visible{outline:2px solid var(--cyan)}nav{display:flex;gap:3px;padding:14px 26px;overflow:auto;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--bg);z-index:2}nav button{white-space:nowrap;background:transparent;border-color:transparent;color:var(--muted)}nav button[aria-selected=true]{background:#173638;color:var(--cyan);border-color:#2c5d59}main{padding:24px 32px;max-width:1600px;margin:auto}.view{display:none}.view.active{display:block}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(195px,1fr));gap:12px;margin:16px 0 22px}.card,section{border:1px solid var(--border);border-radius:8px;background:var(--panel);padding:19px}.card b{display:block;font-size:25px;font-variant-numeric:tabular-nums;margin-top:9px;overflow-wrap:anywhere}.card small{font-size:11px;letter-spacing:.08em;text-transform:uppercase}section{margin-top:16px}h2{margin:0 0 8px;font-size:19px}h3{margin:0 0 12px;font-size:14px}.notice{border-left:3px solid var(--yellow);background:#24251f;padding:12px 16px;color:var(--yellow);margin-bottom:18px;line-height:1.6}.ok{color:var(--cyan)}.bad{color:var(--red)}.scroll{overflow:auto;max-height:540px}table{width:100%;border-collapse:collapse;white-space:nowrap;font-variant-numeric:tabular-nums}th,td{padding:12px 10px;border-bottom:1px solid var(--border);text-align:left}th{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);position:sticky;top:0;background:var(--panel)}tbody tr:hover{background:#192939}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:12px/1.7 Consolas,monospace;color:var(--muted);max-height:460px;overflow:auto}.empty{color:var(--muted);padding:35px 8px}footer{padding:22px 32px;color:var(--muted);font-size:12px;border-top:1px solid var(--border)}#fixture{display:none;color:var(--yellow)}@media(max-width:700px){header,main{padding:18px}.subtitle{display:none}.cards{grid-template-columns:1fr 1fr}.card b{font-size:20px}nav{padding:12px}}
</style></head><body>
<header><div><h1>BTC <span>LAB</span> / RESEARCH</h1><div class="subtitle">Polymarket · Coinbase · Kraken · Five-minute strategy testing</div></div><span class="tag">PAPER ONLY</span><span id="fixture">SYNTHETIC QA DATA</span><div class="right"><b id="runner">CONNECTING</b> <button id="pause">Pause refresh</button></div></header>
<nav role="tablist" id="tabs"></nav><main><div id="warning" role="status" class="notice" hidden></div><div id="views"></div></main><footer id="footer">Read-only · No order, account-reset or real-money controls · Bounded cached queries</footer>
<script>
const names=['Live Data','Strategies','Results','Diagnostics'];let selected=0,paused=false,busy=false,lastData=null;
const $=id=>document.getElementById(id),esc=x=>String(x??'—').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const money=x=>x===null||x===undefined?'Unknown':'$'+Number(x).toFixed(2);
function cards(items){return '<div class="cards">'+items.map(([k,v])=>'<div class="card"><small>'+esc(k)+'</small><b>'+esc(v)+'</b></div>').join('')+'</div>'}
function table(rows,fields){if(!rows?.length)return '<div class="empty">No recorded evidence is available for this view.</div>';return '<div class="scroll"><table><thead><tr>'+fields.map(([k,l])=>'<th>'+esc(l)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(r=>'<tr>'+fields.map(([k])=>'<td>'+esc(typeof r[k]==='object'?JSON.stringify(r[k]):r[k])+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>'}
function block(title,content){return '<section><h3>'+esc(title)+'</h3>'+content+'</section>'}function details(o){return '<pre>'+esc(JSON.stringify(o,null,2))+'</pre>'}
names.forEach((n,i)=>{$('tabs').insertAdjacentHTML('beforeend','<button role="tab" id="tab'+i+'" aria-controls="view'+i+'" aria-selected="'+(i===0)+'">'+esc(n)+'</button>');$('views').insertAdjacentHTML('beforeend','<article role="tabpanel" id="view'+i+'" class="view '+(i===0?'active':'')+'"><h2>'+esc(n)+'</h2><p class="muted">Loading bounded observations…</p></article>');$('tab'+i).onclick=()=>selectTab(i)});
function selectTab(i){selected=i;names.forEach((_,j)=>{$('tab'+j).setAttribute('aria-selected',String(i===j));$('view'+j).classList.toggle('active',i===j)});if(i===2&&typeof loadLabRuns==='function'&&!labRunsLoaded)loadLabRuns()}
function render(d){lastData=d;let diag=d.diagnostics||{},s=d.sample||{};const age=ts=>ts?Math.max(0,(d.observed_at_us-ts)/1e6).toFixed(1)+' s':'Unknown';
$('runner').textContent=d.runner.state;$('runner').className=d.runner.state==='LIVE'?'ok':d.runner.state==='ERROR'?'bad':'';$('fixture').style.display=d.fixture_mode?'inline':'none';
const warnings=[...(d.errors||[])];if(d.storage.free_gib<10)warnings.push('Low storage headroom: '+d.storage.free_gib+' GiB free; emergency guard is 8 GiB.');$('warning').textContent=warnings.join(' · ');$('warning').hidden=!warnings.length;
$('view0').innerHTML='<h2>Research overview</h2><p class="muted">'+esc(d.session_id||'No recorded session')+' · '+esc(d.sample?.market_is_current?'Current market':'Last observed market — may be expired')+'</p>'+cards([['Runner',d.runner.state],['Market',s.slug||'Unavailable'],['Remaining',s.seconds_remaining_now===null?'Unknown':(s.seconds_remaining_now??'—')+' s'],['Price to beat',s.ptb??'Unavailable'],['BTC distance',s.distance_dollars??'Unavailable'],['Model / book',s.state||'Unavailable'],['Latest sample age',age(s.sample_ts_us)],['Free disk',d.storage.free_gib+' GiB']])+block('Reference and model status',table([{layer:'Reference',state:diag.reference?.state},{layer:'Model',state:diag.model?.state,reason:diag.model?.reason},{layer:'Execution',state:diag.execution?.state,reason:diag.execution?.reason},{layer:'Observation',state:diag.observation_state}],[['layer','Layer'],['state','State'],['reason','Reason']]))+block('Current readiness checklist',details(s.readiness||{state:'NO_LAYERED_RUNTIME_OBSERVATION',reason:'See source health; old sessions do not contain new instrumentation'}));

$('view2').innerHTML='<div class="workbench-intro"><div><span class="eyebrow">Compare · Inspect · Export</span><h2>Results</h2><p>Saved strategy replay reports and observed shadow results. Unresolved markets stay unresolved; independent strategy arms are never combined into a portfolio.</p></div></div><section><div class="lab-toolbar"><h3>Saved replay runs</h3><button id="labRefreshRuns">Refresh runs</button></div><p class="muted">Runs created in Strategies appear here with their exact saved definition and execution policies.</p><div id="labRunsList"></div><div id="labSavedDetail"></div></section>'+block('Latest session · all strategy arms','<p class="muted">Latest session only · maximum 2,000 market rows. Compare results within the same execution policy. Small samples and descriptive uncertainty do not establish an edge.</p>'+table(d.strategies,[['strategy','Strategy'],['settled_markets','Settled'],['unsettled_markets','Unresolved'],['wins','Wins'],['losses','Losses'],['net_pnl','Net settled PnL'],['settled_purchase_cost','Purchase costs'],['fees','Fees'],['drawdown','Market drawdown'],['leave_largest_winner_out','Without largest winner'],['evidence','Evidence']]))+block('Session and coverage',details({session_id:d.session_id,cohort:d.cohort,scope:d.shadow_scope}));



$('view3').innerHTML='<h2>Diagnostics</h2><p class="muted">Source timestamps and receipt timestamps have different meanings. Resync scheduling counters are not HTTP-attempt counts.</p>'+table((d.feeds||[]).map(f=>({...f,message_age:age(f.last_message_at_us),tick_age:age(f.last_tick_at_us)})),[['component','Source'],['state','Collector state'],['observation_state','Observation freshness'],['message_age','Message age'],['tick_age','Tick age'],['reconnect_count','Reconnects'],['error_count','Errors']])+block('Source trade versus network receipt',table(diag.sources,[['source','Source'],['state','State'],['source_age_seconds','Source trade age (s)'],['receipt_age_seconds','Receipt age (s)'],['message_age_seconds','Network message age (s)']]))+block('UP / DOWN depth validity',table(Object.entries(diag.book?.health||diag.book||{}).filter(([k,v])=>['up','down'].includes(k)&&v&&typeof v==='object').map(([k,v])=>({outcome:k,...v})),[['outcome','Outcome'],['depth_reliable','Depth valid'],['integrity_reason','Integrity reason'],['source_age_seconds','Source age (s)'],['receive_age_seconds','Receipt age (s)'],['best_bid','Bid'],['best_ask','Ask']]))+block('Retained official reference',details(diag.reference))+block('Maker research participation',details(diag.maker))+block('Model readiness and causal history diagnostics',details({state:s.state,reason:s.unavailable_reason,lab:s.lab_json,book:s.book_json,readiness:s.readiness,generation:s.generation}))+block('Startup and rollover measurements',details((d.feeds||[]).map(f=>({source:f.component,details:f.details_json}))))+block('Source health details',details(d.feeds));
$('view3').innerHTML+=block('Latest strategy decisions · maximum 100',table(d.decisions,[['strategy','Strategy'],['decision','Decision'],['reason','Reason'],['decided_at_us','UTC µs']]))+block('Execution policy and session configuration',details(d.cohort))+block('Storage and query health',details({free_gib:d.storage.free_gib,emergency_guard_gib:8,cache_age_seconds:d.cache_age_seconds,warnings:d.errors}));
$('footer').textContent='Local strategy research · '+d.dashboard_revision+' · Collected '+new Date(d.observed_at_us/1000).toLocaleString()+' · Cache age '+d.cache_age_seconds+' s · '+(paused?'PAUSED':'3-second refresh')+' · Paper execution only';}
async function refresh(){if(paused||busy)return;busy=true;try{const response=await fetch('/api/v4/status',{cache:'no-store'});if(!response.ok)throw Error('HTTP '+response.status);render(await response.json())}catch(error){$('runner').textContent='ERROR';$('warning').hidden=false;$('warning').textContent='Data refresh unavailable: '+error.message+' · Previous values may be stale.'}finally{busy=false}}
$('pause').onclick=()=>{paused=!paused;$('pause').textContent=paused?'Resume refresh':'Pause refresh';if(!paused)refresh()};refresh();setInterval(refresh,3000);
</script></body></html>'''

from .workbench_ui import STYLE as WORKBENCH_STYLE,SCRIPT as WORKBENCH_SCRIPT
HTML=HTML.replace('</style>',WORKBENCH_STYLE+'</style>').replace('</script>',WORKBENCH_SCRIPT+'</script>')
