"""Execute shipped dashboard JS against a small DOM contract; no server/network.

This verifies behavior and HTML output, not browser layout or visual acceptance.
"""
import json
import shutil
import subprocess
from pathlib import Path
import pytest
from btc_v3.forensics import dashboard_v4 as v4


def test_runtime_reference_and_waiting_model_are_projected_separately():
    s=dict(sample_ts_us=100,ptb='100',state='WAITING_CAUSAL_INPUTS',model_state='WAITING_CAUSAL_TOURNAMENT_INPUTS',
        reference_diagnostics=dict(state='VERIFIED_RETAINED',price_to_beat='100',execution_eligible=False),
        source_health=[dict(source='kraken',state='STALE',source_age_seconds=10,receipt_age_seconds=1)])
    result=v4.sample_diagnostics(s,now_us=110,runner_state='LIVE')
    assert result['reference']['state']=='VERIFIED_RETAINED'
    assert result['model']['state']=='WAITING_CAUSAL_TOURNAMENT_INPUTS'
    assert result['sources'][0]['source_age_seconds']==10
    assert result['execution']['eligible'] is False


def test_four_views_refresh_pause_all_arms_and_escaping(tmp_path):
    node=shutil.which('node')
    if node is None:pytest.skip('Node runtime unavailable for offline JS integration')
    data=v4.snapshot(tmp_path,now_us=100_000_000)
    data.update(sample=dict(ptb='86020',state='WAITING',unavailable_reason='STALE_KRAKEN_HISTORY'),
        strategies=[dict(strategy='S5_BASE_H180_BOOK_IMBALANCE',net_pnl='1'),dict(strategy='LEGACY_TEST_ARM',net_pnl='-1')],
        decisions=[dict(strategy='<unsafe>',decision='SKIP',reason='<script>unsafe()</script>')])
    data['diagnostics']=dict(reference=dict(state='VERIFIED_RETAINED'),model=dict(state='WAITING'),execution=dict(state='NOT_EVALUATED'))
    script=v4.HTML.split('<script>',1)[1].split('</script>',1)[0]
    harness=r'''
const vm=require('node:vm'),assert=require('node:assert/strict');
const nodes=new Map();
function parse(html){for(const m of html.matchAll(/id="([^"]+)"/g))if(!nodes.has(m[1]))make(m[1]);}
function make(id){let html='';const n={id,textContent:'',className:'',style:{},attrs:{},classes:new Set(),
 setAttribute(k,v){this.attrs[k]=v},querySelectorAll(){return []},insertAdjacentHTML(where,value){html+=value;parse(value)}};
 n.classList={toggle(k,on){if(on)n.classes.add(k);else n.classes.delete(k)}};
 Object.defineProperty(n,'innerHTML',{get(){return html},set(v){html=v;parse(v)}});nodes.set(id,n);return n;}
for(const id of ['tabs','views','runner','fixture','warning','footer','pause'])make(id);
let calls=0,fail=false;const fixture=FIXTURE;
const context=vm.createContext({document:{getElementById(id){assert(nodes.has(id),'Unknown DOM id '+id);return nodes.get(id)}},
 fetch:async(url,options)=>{calls++;assert.equal(options.cache,'no-store');
   if(fail)throw Error('OFFLINE_TEST_FAILURE');return {ok:true,json:async()=>url==='/api/v4/status'?fixture:url==='/api/lab/runs'?{runs:[],errors:[]}:url==='/api/lab/clock'?{state:'UNVERIFIED'}:{}}},setInterval(fn,ms){assert.equal(ms,3000);return 1},console});
vm.runInContext(SHIPPED,context);
(async()=>{
 await vm.runInContext('refresh()',context);await new Promise(resolve=>setImmediate(resolve));
 for(let i=0;i<4;i++){assert(nodes.get('view'+i).innerHTML.includes('<h2>'));nodes.get('tab'+i).onclick();
   for(let j=0;j<4;j++){assert.equal(nodes.get('tab'+j).attrs['aria-selected'],String(i===j));assert.equal(nodes.get('view'+j).classes.has('active'),i===j);}}
 assert(nodes.get('view0').innerHTML.includes('86020'));assert(nodes.get('view0').innerHTML.includes('WAITING'));
 assert(nodes.get('view2').innerHTML.includes('S5_BASE_H180_BOOK_IMBALANCE'));
 assert(nodes.get('view2').innerHTML.includes('LEGACY_TEST_ARM'));
 assert(nodes.get('view3').innerHTML.includes('&lt;script&gt;'));assert(!nodes.get('view3').innerHTML.includes('<script>unsafe'));
 nodes.get('pause').onclick();let before=calls;await vm.runInContext('refresh()',context);assert.equal(calls,before);
 assert.equal(nodes.get('pause').textContent,'Resume refresh');
 fail=true;nodes.get('pause').onclick();await new Promise(resolve=>setImmediate(resolve));
 assert.equal(nodes.get('runner').textContent,'ERROR');assert(nodes.get('warning').textContent.includes('Previous values may be stale'));
 assert(nodes.get('view0').innerHTML.includes('86020'));assert.equal(vm.runInContext('busy',context),false);
 console.log('4 views; tabs; retained PTB with WAITING; all arms; escaping; pause; failed refresh: PASS');
})().catch(e=>{console.error(e);process.exitCode=1});
'''.replace('FIXTURE',json.dumps(data)).replace('SHIPPED',json.dumps(script))
    target=tmp_path/'dashboard_contract.cjs';target.write_text(harness,encoding='utf-8')
    result=subprocess.run([node,str(target)],capture_output=True,text=True,timeout=15)
    assert result.returncode==0,result.stdout+result.stderr
