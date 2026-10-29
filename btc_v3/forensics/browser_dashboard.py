"""Local research dashboard: read-only evidence, isolated versioned strategy drafts.

Serves loopback only; HTML renders using DOM replacement rather than page reload,
therefore a reader's scroll position, selected horizon, and paused state persist.
No endpoint starts collectors, funds an account, or enables real orders. Strategy
edits and explicit offline replay use separate stores; historical ledgers are read-only.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import sqlite3
import time
import urllib.parse
from contextlib import closing
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .dashboard_v4 import HTML as V4_HTML,SnapshotCache

V4_ENABLED=True
V4_CACHE=SnapshotCache()


def read_conn(path: Path) -> sqlite3.Connection:
    # URI must stay read-only; immutable=1 is incorrect while live DB is updated.
    uri = f"file:{urllib.parse.quote(str(path.resolve()).replace(os.sep, '/'), safe='/:')}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, timeout=1)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA busy_timeout=750")
    return conn


def _f(x: object) -> float:
    try:
        n = float(x)  # type: ignore[arg-type]
        return n if math.isfinite(n) else 0.0
    except (TypeError, ValueError):
        return 0.0


def compute_metrics(round_rows, fill_rows=(), order_rows=(), decision_rows=()):
    """Single accounting definition for normal and wallet tables.

    Market win/loss is classified using *settled* NET pnl. Cost is only
    settled-market purchase cost; unclosed exposure is intentionally excluded.
    """
    by = {}
    for r in round_rows:
        # No-fill rounds are not 0-PnL settled trading outcomes.
        if 'filled_orders' in r.keys() and int(r['filled_orders'] or 0) <= 0:
            continue
        name = str(r['strategy'])
        d = by.setdefault(name, {'strategy': name, 'markets': 0, 'wins': 0,
            'losses': 0, 'breakeven': 0, 'unsettled': 0, 'pnl': 0.0, 'fees': 0.0,
            'notional': 0.0, 'max_dd': 0.0, '_pnl_stream': []})
        if r['pnl'] is None:
            d['unsettled'] += 1
            continue
        pnl = _f(r['pnl'])
        d['markets'] += 1
        d['wins'] += int(pnl > 1e-9)
        d['losses'] += int(pnl < -1e-9)
        d['breakeven'] += int(abs(pnl) <= 1e-9)
        d['pnl'] += pnl
        d['fees'] += _f(r['fees'])
        d['notional'] += _f(r['up_cost']) + _f(r['down_cost'])
        d['_pnl_stream'].append((int(r['round_end_us']), str(r['slug']), pnl))
    for r in fill_rows:
        d = by.setdefault(str(r['strategy']), {'strategy': str(r['strategy']), 'markets': 0,
            'wins': 0, 'losses': 0, 'breakeven': 0, 'unsettled': 0,
            'pnl': 0.0, 'fees': 0.0, 'notional': 0.0, 'max_dd': 0.0,
            '_pnl_stream': []})
        d['fills'] = int(r['fills'])
    for r in order_rows:
        d = by.setdefault(str(r['strategy']), {'strategy': str(r['strategy']), 'markets': 0,
            'wins': 0, 'losses': 0, 'breakeven': 0, 'unsettled': 0,
            'pnl': 0.0, 'fees': 0.0, 'notional': 0.0, 'max_dd': 0.0,
            '_pnl_stream': []})
        d['orders'] = int(r['orders'])
        d['rejected'] = int(r['rejected'])
    for r in decision_rows:
        d = by.setdefault(str(r['strategy']), {'strategy': str(r['strategy']), 'markets': 0,
            'wins': 0, 'losses': 0, 'breakeven': 0, 'unsettled': 0,
            'pnl': 0.0, 'fees': 0.0, 'notional': 0.0, 'max_dd': 0.0,
            '_pnl_stream': []})
        d['filter_rejects'] = int(r['rejected'])
    for d in by.values():
        running = peak = 0.0
        for _, _, pnl in sorted(d.pop('_pnl_stream')):
            running += pnl
            peak = max(peak, running)
            d['max_dd'] = min(d['max_dd'], running - peak)
        d['win_pct'] = 100 * d['wins'] / d['markets'] if d['markets'] else None
        d['roi_pct'] = 100 * d['pnl'] / d['notional'] if d['notional'] > 0 else None
        d['fills'] = d.get('fills', 0)
        d['orders'] = d.get('orders', 0)
        d['rejected'] = d.get('rejected', 0)
        d['filter_rejects'] = d.get('filter_rejects', 0)
        d['status'] = 'LOW SAMPLE' if d['markets'] < 30 else 'PAPER'
        d['group'] = 'normal' if '_BASE_' in d['strategy'] or d['strategy'].startswith('CTRL_') else 'wallet'
        d['horizon'] = int(d['strategy'].split('_H')[1][:3]) if '_H' in d['strategy'] else None
    return sorted(by.values(), key=lambda d: (d['group'], -d['pnl'], d['strategy']))


# A saved RUNNING value is not evidence that a crashed/stopped runner remains
# alive. The SQLite sample heartbeat is the authoritative dashboard liveness
# indicator. Use a tolerant window for occasional collector stalls.
LIVE_SAMPLE_MAX_AGE_SECONDS = 15.0


def _latest_session(conn: sqlite3.Connection) -> sqlite3.Row | None:
    # Newest run wins regardless of a stale, never-cleared RUNNING flag.
    return conn.execute('''SELECT session_id,state,started_at_us,ended_at_us
        FROM wallet_forward_sessions ORDER BY started_at_us DESC, rowid DESC LIMIT 1''').fetchone()


def _effective_session_state(sess: sqlite3.Row, sample_ts_us: int | None, now_us: int) -> tuple[str, float | None]:
    raw = str(sess['state']).upper()
    age = None if sample_ts_us is None else max(0.0, (now_us-int(sample_ts_us))/1e6)
    if raw == 'RUNNING':
        if age is not None and age <= LIVE_SAMPLE_MAX_AGE_SECONDS:
            return 'LIVE', age
        # Allow preflight and early bootstrap a short window to produce a sample.
        if age is None and max(0, (now_us-int(sess['started_at_us']))/1e6) <= LIVE_SAMPLE_MAX_AGE_SECONDS:
            return 'STARTING', None
        return 'STALE', age
    if raw in ('STOPPED','FINISHED','COMPLETE','COMPLETED','GUARD_STOPPED'):
        return 'STOPPED', age
    if raw in ('ERROR','FAILED','CRASHED'):
        return 'CRASHED', age
    return raw, age


def collect(root: Path, horizon: int | None = None) -> dict:
    path = root / 'research' / 'v3_research.sqlite'
    if not path.is_file():
        return {'state': 'WAITING', 'message': 'No research DB yet', 'strategies': [], 'feeds': []}
    with closing(read_conn(path)) as c:
        sess = _latest_session(c)
        if sess is None:
            return {'state': 'WAITING', 'message': 'No session yet', 'strategies': [], 'feeds': []}
        sid = str(sess['session_id'])
        rounds = c.execute('''SELECT r.strategy,r.slug,r.round_end_us,r.pnl,r.fees,r.up_cost,r.down_cost,
                (SELECT COUNT(*) FROM wallet_forward_fills f
                 WHERE f.session_id=r.session_id AND f.strategy=r.strategy AND f.slug=r.slug) AS filled_orders
            FROM wallet_forward_rounds r WHERE r.session_id=?''', (sid,)).fetchall()
        fills = c.execute('''SELECT strategy,COUNT(*) fills FROM wallet_forward_fills
            WHERE session_id=? GROUP BY strategy''', (sid,)).fetchall()
        orders = c.execute('''SELECT strategy,COUNT(*) orders,
            SUM(CASE WHEN status IN ('REJECTED','SKIPPED') THEN 1 ELSE 0 END) rejected
            FROM wallet_forward_orders WHERE session_id=? GROUP BY strategy''', (sid,)).fetchall()
        decision_rows = c.execute('''SELECT strategy,COUNT(*) rejected
            FROM wallet_signal_strategy_decisions WHERE session_id=? AND decision='REJECT'
            GROUP BY strategy''', (sid,)).fetchall()
        metrics = compute_metrics(rounds, fills, orders, decision_rows)
        # Keep readiness diagnostics bounded to the last 30 minutes of samples.
        latest_ts = c.execute('SELECT MAX(sample_ts_us) FROM forensic_live_samples WHERE session_id=?',(sid,)).fetchone()[0]
        coverage = []
        if latest_ts is not None:
            coverage = [dict(r) for r in c.execute('''SELECT state,COALESCE(unavailable_reason,'') reason,COUNT(*) count
                FROM forensic_live_samples WHERE session_id=? AND sample_ts_us>=?
                GROUP BY state,unavailable_reason ORDER BY count DESC''',
                (sid,int(latest_ts)-1_800_000_000))]
        if horizon is not None:
            metrics = [m for m in metrics if m['horizon'] == horizon or m['horizon'] is None]
        sample = c.execute('''SELECT sample_ts_us,slug,state AS mode,unavailable_reason AS reason FROM forensic_live_samples
            WHERE session_id=? ORDER BY sample_ts_us DESC LIMIT 1''', (sid,)).fetchone()
        latest_decisions = c.execute('''SELECT strategy,decision,reason,decided_at_us
            FROM wallet_strategy_decision_events WHERE session_id=?
            ORDER BY decided_at_us DESC,id DESC LIMIT 15''', (sid,)).fetchall()
        observed_us = int(time.time()*1e6)
        effective_state, sample_age_seconds = _effective_session_state(sess, latest_ts, observed_us)
        feed = []
        ext_path = root / 'data' / 'v3_external.sqlite'
        if ext_path.is_file():
            try:
                with closing(read_conn(ext_path)) as e:
                    feed = [dict(r) for r in e.execute('''SELECT component,state,last_message_at_us,
                       last_tick_at_us,reconnect_count,error_count FROM external_status''').fetchall()]
                    for entry in feed:
                        # Collector status table is project-global and can retain
                        # RUNNING after an ungraceful exit or across sessions.
                        if effective_state != 'LIVE':
                            entry['state'] = 'STALE' if effective_state == 'STALE' else 'STOPPED'
                        elif entry['state'] == 'RUNNING':
                            tick = entry['last_message_at_us']
                            if tick is None or (observed_us-int(tick)) > LIVE_SAMPLE_MAX_AGE_SECONDS*1e6:
                                entry['state'] = 'STALE'
            except sqlite3.Error:
                feed = []
        disk = shutil.disk_usage(root)
        market_file = root / 'data' / 'v3_market.sqlite'
        storage = {'free_gib': round(disk.free/(1024**3),2),
                   'market_logical_gib':round(market_file.stat().st_size/(1024**3),2) if market_file.is_file() else None,
                   'low_disk_guard_gib':8}
        return {'state': effective_state, 'raw_state': str(sess['state']), 'session_id': sid,
            'started_at_us': sess['started_at_us'], 'ended_at_us': sess['ended_at_us'],
            'sample_age_seconds': sample_age_seconds,
            'updated_at_us': observed_us, 'sample': dict(sample) if sample else {},
            'strategies': metrics, 'feeds': feed, 'decisions': [dict(r) for r in latest_decisions],
            'readiness_30m': coverage, 'storage': storage,
            'horizon': horizon, 'paper_only': True}


def strategy_detail(root: Path, strategy: str, limit: int = 80) -> dict:
    if not strategy or len(strategy) > 110 or not all(ch.isascii() and (ch.isalnum() or ch == '_') for ch in strategy):
        raise ValueError('invalid strategy')
    with closing(read_conn(root / 'research' / 'v3_research.sqlite')) as c:
        sess = _latest_session(c)
        if not sess:
            return {'session_id': None, 'rounds': [], 'rejections': []}
        sid = sess['session_id']
        rounds = [dict(r) for r in c.execute('''SELECT slug,pnl,up_cost,down_cost,fees,settled_side,round_end_us
                FROM wallet_forward_rounds WHERE session_id=? AND strategy=? ORDER BY round_end_us DESC LIMIT ?''', (sid,strategy,limit))]
        rejection = [dict(r) for r in c.execute('''SELECT reason, COUNT(*) count FROM wallet_signal_strategy_decisions
                WHERE session_id=? AND strategy=? AND decision='REJECT' GROUP BY reason ORDER BY count DESC LIMIT 15''', (sid,strategy))]
        return {'session_id': sid, 'rounds': rounds, 'rejections': rejection}

HTML = r'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>BTC Lab 5.9.2a | Paper research</title><style>
:root{color-scheme:dark;--bg:#10151e;--panel:#192231;--border:#344052;--txt:#dce6f5;--muted:#92a3b9;--green:#69d5a9;--red:#f48a93;--blue:#81bafa}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font:14px system-ui,Segoe UI,sans-serif}header{position:sticky;top:0;z-index:2;background:#111a28ef;border-bottom:1px solid var(--border);padding:13px 24px;display:flex;align-items:center;gap:14px;flex-wrap:wrap;backdrop-filter:blur(6px)}h1{font-size:18px;margin:0 20px 0 0}h2{font-size:16px;margin:0 0 12px}small,.muted{color:var(--muted)}main{max-width:1600px;margin:auto;padding:20px}section{background:var(--panel);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:18px}button,select{background:#243448;color:var(--txt);border:1px solid #4a5d75;padding:8px 12px;border-radius:8px;cursor:pointer}button:hover{background:#35506c}.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(185px,1fr));gap:12px}.metric{background:#121c29;padding:13px;border-radius:9px}.metric b{font-size:19px;display:block;margin-top:6px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums;white-space:nowrap}th,td{padding:10px 12px;text-align:right;border-bottom:1px solid #344052}th:first-child,td:first-child{text-align:left}th{color:#a8bcd1;position:sticky;top:0;background:#1a283a;cursor:pointer}tbody tr:hover{background:#2a384b;cursor:pointer}.positive{color:var(--green)}.negative{color:var(--red)}.warn{color:#e7c584}#status{font-weight:600}#details{white-space:pre-wrap;line-height:1.7}#messages{color:var(--muted)}.pill{padding:4px 9px;border-radius:8px;background:#254060;color:#9cc7ff;font-weight:700}button:focus-visible,select:focus-visible,tr:focus-visible{outline:2px solid var(--blue)}</style></head>
<body><header><h1>BTC LAB V3 · Forward Research (phase in session config)</h1><span class="pill">PAPER ONLY · NO REAL ORDERS</span><span id="status">Connecting...</span><label>Horizon <select id="horizon"><option value="">All</option><option value="240">T-240</option><option value="180">T-180</option><option value="120">T-120</option><option value="60">T-60</option><option value="30">T-30</option><option value="15">T-15</option></select></label><button id="pause">Pause updates</button><span id="messages">Updates every 2s. Scrolling is preserved.</span></header>
<main><section><h2>Session / operational status</h2><div class="cards" id="summary"></div></section>
<section><h2>Non-wallet strategies</h2><p class="muted">Same net settled-market accounting and column definitions as the wallet table.</p><div class="scroll"><table id="normal"></table></div></section>
<section><h2>Wallet and router strategies</h2><p class="muted">Wins/losses are settled-market outcomes, NOT individual fills. ROI = settled net PnL / settled purchases. Zero settled markets = no ROI.</p><div class="scroll"><table id="wallet"></table></div></section>
<section><h2>Selected strategy details</h2><div id="details">Click a strategy row to view recent settled markets and rejection reasons.</div></section>
<section><h2>Feed health</h2><div class="scroll"><table id="feeds"></table></div></section>
<section><h2>Latest decisions</h2><div class="scroll"><table id="decisions"></table></div></section></main>
<script>
const $=id=>document.getElementById(id), fields=[['strategy','Strategy'],['markets','Mkts'],['wins','Wins'],['losses','Losses'],['win_pct','Win %'],['fills','Fills'],['pnl','Net PnL'],['roi_pct','ROI %'],['fees','Fees'],['notional','Notional'],['max_dd','Max DD'],['status','Status']];
let paused=false, busy=false, sort={field:'pnl',reverse:true}, chosen=null;
const esc=x=>String(x??'-').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function num(x,k){if(x===null||x===undefined)return '—';if(['pnl','max_dd'].includes(k))return '$'+Number(x).toFixed(2);if(['notional','fees'].includes(k))return '$'+Number(x).toFixed(2);if(['win_pct','roi_pct'].includes(k))return Number(x).toFixed(1)+'%';return x}
function table(id,columns,rows,selectable=false){let h='<thead><tr>'+columns.map(([k,l])=>`<th data-k="${esc(k)}">${esc(l)}</th>`).join('')+'</tr></thead><tbody>';
for(const row of rows){h+='<tr'+(selectable?' tabindex="0" data-strategy="'+esc(row.strategy)+'"':'')+'>'+columns.map(([k])=>{let v=row[k],cl=(['pnl','roi_pct','max_dd'].includes(k)&&v!==null)?Number(v)<0?'negative':Number(v)>0?'positive':'':'';return `<td class="${cl}">${esc(num(v,k))}</td>`}).join('')+'</tr>'}$(id).innerHTML=h+'</tbody>';
if(selectable){$(id).querySelectorAll('tbody tr').forEach(tr=>{tr.onclick=()=>select(tr.dataset.strategy);tr.onkeydown=e=>{if(e.key==='Enter')select(tr.dataset.strategy)}})}
$(id).querySelectorAll('th[data-k]').forEach(th=>th.onclick=()=>{let k=th.dataset.k;sort={field:k,reverse:sort.field===k?!sort.reverse:true};refresh()})}
async function select(s){chosen=s;let r=await fetch('/api/strategy?name='+encodeURIComponent(s));let d=await r.json();$('details').textContent=s+'\n\nRejected decisions: '+d.rejections.map(x=>(x.reason||'UNKNOWN')+': '+x.count).join(', ')+'\n\nRecent markets:\n'+d.rounds.map(x=>x.slug+'  '+(x.pnl===null?'UNSETTLED':('$'+Number(x.pnl).toFixed(2)))+'  '+(x.settled_side||'-')).join('\n')}
function ordered(xs){return xs.sort((a,b)=>{let aa=a[sort.field],bb=b[sort.field];let v=typeof aa==='number'&&typeof bb==='number'?aa-bb:String(aa??'').localeCompare(String(bb??''));return sort.reverse?-v:v})}
function age(us){return us?Math.max(0,(Date.now()*1000-us)/1e6).toFixed(1)+'s':'—'}
async function refresh(){if(paused||busy)return;busy=true;try{let h=$('horizon').value;let r=await fetch('/api/status'+(h?'?horizon='+encodeURIComponent(h):''),{cache:'no-store'});if(!r.ok)throw Error('HTTP '+r.status);let d=await r.json();$('status').textContent=d.state+' · '+(d.session_id||'No session');$('summary').innerHTML=[['Research session',d.session_id||'—'],['Latest market',d.sample?.slug||'—'],['Model mode',d.sample?.mode||'—'],['Model issue',d.sample?.reason||'—'],['Last sample age',age(d.sample?.sample_ts_us)],['C: free',d.storage?.free_gib+' GiB'],['DB logical',d.storage?.market_logical_gib+' GiB'],['Readiness (30m)',Object.entries(d.readiness_30m?.reduce((acc,x)=>(acc[x.state]=(acc[x.state]||0)+x.count,acc),{})||{}).map(([state,count])=>state+':'+count).join(' · ')||'—'],['Report time',new Date().toLocaleTimeString()]].map(([k,v])=>`<div class="metric"><small>${esc(k)}</small><b>${esc(v)}</b></div>`).join('');
let ss=d.strategies||[];table('normal',fields,ordered(ss.filter(x=>x.group==='normal')),true);table('wallet',fields,ordered(ss.filter(x=>x.group==='wallet')),true);
table('feeds',[['component','Feed'],['state','Status'],['message_age','Message age'],['tick_age','Tick age'],['reconnect_count','Reconnects'],['error_count','Errors']],(d.feeds||[]).map(x=>({...x,message_age:age(x.last_message_at_us),tick_age:age(x.last_tick_at_us)})));
table('decisions',[['strategy','Strategy'],['decision','Decision'],['reason','Reason'],['age','Age']],(d.decisions||[]).map(x=>({...x,age:age(x.decided_at_us)})));$('messages').textContent='Updated '+new Date().toLocaleTimeString()+' · Scroll position stays fixed.';
}catch(e){$('messages').textContent='Read-only dashboard retry: '+e.message}finally{busy=false}}
$('pause').onclick=()=>{paused=!paused;$('pause').textContent=paused?'Resume updates':'Pause updates';$('messages').textContent=paused?'PAUSED — results frozen while you inspect.':'Resuming...';if(!paused)refresh()};$('horizon').onchange=refresh;
refresh();setInterval(refresh,2000);
</script></body></html>'''


def handler_factory(root: Path):
    class Handler(BaseHTTPRequestHandler):
        def _local_request(self):
            port=self.server.server_address[1]
            return self.client_address[0] in {'127.0.0.1','::1'} and self.headers.get('Host') in {f'127.0.0.1:{port}',f'localhost:{port}'}
        def _json_response(self,status,obj):
            body=json.dumps(obj,allow_nan=False).encode('utf-8')
            self.send_response(status);self._headers('application/json; charset=utf-8',len(body));self.end_headers();self.wfile.write(body)
        def _discard_small_body(self):
            # A rejected small POST must be drained before HTTP/1.0 closes on
            # Windows, otherwise unread data can reset the socket before its 403.
            try:
                size=int(self.headers.get('Content-Length','0'))
                if 0<size<=65536:
                    self.connection.settimeout(1);self.rfile.read(size)
            except (ValueError,OSError):pass
        def do_POST(self):
            from .lab_web import dispatch,error_code
            port=self.server.server_address[1]
            if not self._local_request() or self.headers.get('Origin') not in {f'http://127.0.0.1:{port}',f'http://localhost:{port}'}:
                self._discard_small_body()
                self._json_response(403,{'error':'LAB_SAME_ORIGIN_REQUIRED'});return
            if self.headers.get('Content-Type','').split(';')[0]!='application/json':
                self._discard_small_body()
                self._json_response(415,{'error':'LAB_JSON_REQUIRED'});return
            try:
                path=urllib.parse.urlsplit(self.path).path
                limit=2_100_000 if path=='/api/lab/replay' else 65536
                length=int(self.headers.get('Content-Length','0'))
                if not 0<length<=limit:raise ValueError('LAB_REQUEST_LIMIT')
                self.connection.settimeout(5)
                body=json.loads(self.rfile.read(length))
                if not isinstance(body,dict):raise ValueError('LAB_OBJECT_REQUIRED')
                status,obj=dispatch(root,path,body)
                self._json_response(status,obj)
            except (ValueError,KeyError,TypeError,OSError,sqlite3.Error) as exc:
                code=error_code(exc);status=409 if code=='LAB_REVISION_CONFLICT' else 429 if code.endswith(('LIMIT','BUSY')) else 400
                self._json_response(status,{'error':code})
        def do_GET(self):
            if not self._local_request():self._json_response(403,{'error':'LAB_LOCAL_HOST_REQUIRED'});return
            parsed = urllib.parse.urlsplit(self.path)
            if parsed.path == '/':
                body = V4_HTML.encode('utf8'); content_type = 'text/html; charset=utf-8'
            elif parsed.path=='/api/lab/strategies':
                from .lab_web import catalog,error_code
                try:self._json_response(200,catalog(root))
                except (ValueError,OSError,sqlite3.Error) as exc:self._json_response(400,{'error':error_code(exc)})
                return
            elif parsed.path=='/api/lab/clock':
                from ..feeds.clock_health import host_sync_status
                self._json_response(200,host_sync_status());return
            elif parsed.path=='/api/lab/runs':
                from .lab_web import saved_runs,saved_run,error_code
                try:
                    run_id=(urllib.parse.parse_qs(parsed.query).get('run_id') or [None])[0]
                    self._json_response(200,saved_runs(root) if run_id is None else saved_run(root,run_id))
                except (ValueError,OSError,KeyError,TypeError) as exc:
                    self._json_response(400,{'error':error_code(exc)})
                return
            elif parsed.path == '/api/v4/status':
                obj=V4_CACHE.get(root,now_us=time.time_ns()//1000)
                body=json.dumps(obj,allow_nan=False).encode();content_type='application/json; charset=utf-8'
            elif parsed.path == '/api/status':
                q = urllib.parse.parse_qs(parsed.query)
                h = (q.get('horizon') or [None])[0]
                try:
                    horizon = int(h) if h in ('240','180','120','60','30','15') else None
                    obj = collect(root,horizon)
                except (sqlite3.Error, OSError) as exc:
                    obj = {'state':'RETRY','strategies':[],'feeds':[],'error':str(exc)}
                body = json.dumps(obj, allow_nan=False).encode(); content_type = 'application/json; charset=utf-8'
            elif parsed.path == '/api/strategy':
                try:
                    name = (urllib.parse.parse_qs(parsed.query).get('name') or [''])[0]
                    obj = strategy_detail(root,name)
                    status = 200
                except (ValueError,sqlite3.Error,OSError) as exc:
                    obj = {'error':str(exc)}; status = 400
                body=json.dumps(obj,allow_nan=False).encode();content_type='application/json; charset=utf-8'
                self.send_response(status);self._headers(content_type,len(body));self.end_headers();self.wfile.write(body);return
            else:
                self.send_error(404);return
            self.send_response(200);self._headers(content_type,len(body));self.end_headers();self.wfile.write(body)
        def _headers(self, content_type, length):
            self.send_header('Content-Type',content_type)
            self.send_header('Content-Length',str(length))
            self.send_header('Cache-Control','no-store')
            self.send_header('X-Content-Type-Options','nosniff')
            self.send_header('Content-Security-Policy',"default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'")
        def log_message(self, fmt, *args):
            pass
    return Handler


def main():
    ap=argparse.ArgumentParser(description='Local BTC Lab strategy research dashboard; paper execution only')
    ap.add_argument('--root',type=Path,default=Path.cwd())
    ap.add_argument('--port',type=int,default=8592)
    ap.add_argument('--open',action='store_true',help='open local browser after bind')
    args=ap.parse_args()
    if not 1024 <= args.port <= 65535:
        ap.error('port must be 1024..65535')
    server=ThreadingHTTPServer(('127.0.0.1',args.port),handler_factory(args.root))
    print(f'BTC LAB V3 PAPER dashboard: http://127.0.0.1:{args.port}/',flush=True)
    print('Browser scrolling is preserved. Ctrl+C stops this dashboard only.',flush=True)
    if args.open:
        import threading
        import webbrowser
        threading.Timer(0.35, lambda: webbrowser.open(f'http://127.0.0.1:{args.port}/')).start()
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__=='__main__':
    main()
