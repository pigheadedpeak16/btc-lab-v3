'use strict';
const installCommands={
windows:String.raw`git clone https://github.com/pigheadedpeak16/btc-lab-v3.git
cd btc-lab-v3
py -3.11 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -B -m btc_v3.forensics.browser_dashboard --root . --open`,
unix:`git clone https://github.com/pigheadedpeak16/btc-lab-v3.git
cd btc-lab-v3
python3 -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -B -m btc_v3.forensics.browser_dashboard --root . --open`
};
let currentOS='windows';
document.querySelectorAll('[data-os]').forEach(button=>button.addEventListener('click',()=>{
 currentOS=button.dataset.os;document.getElementById('install-code').textContent=installCommands[currentOS];
 document.querySelectorAll('[data-os]').forEach(b=>{const active=b===button;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active))});
 document.getElementById('copy-status').textContent='';
}));
document.getElementById('copy-install').addEventListener('click',async()=>{
 const status=document.getElementById('copy-status');
 try{await navigator.clipboard.writeText(installCommands[currentOS]);status.textContent='Copied to clipboard';}
 catch{status.textContent='Select and copy the commands above.';}
});
const views={
 'live-data':['Live Data','Observe the latest market, retained reference and independent source, model and execution states. Historical samples remain labeled.'],
 strategies:['Strategies','Create and version a strategy. Every editable parameter belongs to a typed signal family; fixed engine constraints are visible.'],
 results:['Results','Inspect the exact report saved by Strategies: version, input coverage, fees, fills and unresolved outcomes. Download without rerunning.'],
 diagnostics:['Diagnostics','Separate source freshness from delivery freshness. Inspect book integrity, model requirements, rejections and execution policies.']
};
document.querySelectorAll('[data-view]').forEach(button=>button.addEventListener('click',()=>{
 const view=button.dataset.view,[name,caption]=views[view],image=document.getElementById('product-shot');
 image.src='assets/'+view+'.png';image.alt=name+' view of BTC LAB using synthetic QA observations';
 const full=document.getElementById('product-full');full.href=image.src;full.setAttribute('aria-label','Open full '+name+' screenshot');
 document.getElementById('product-caption').textContent=caption;
 document.querySelectorAll('[data-view]').forEach(b=>{const active=b===button;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active))});
}));
document.querySelectorAll('[data-timing]').forEach(button=>button.addEventListener('click',()=>{
 const delayed=button.dataset.timing==='delayed',result=document.getElementById('timing-result');
 document.getElementById('clock-received').textContent=delayed?'12:00:02.900':'12:00:00.120';
 document.getElementById('clock-execution').textContent=delayed?'12:00:03.020':'12:00:00.320';
 document.getElementById('timing-state').textContent=delayed?'Rejected: source age is 3.02 s':'Source age passes: 0.32 s';
 document.getElementById('timing-reason').textContent=delayed?'Receipt is recent. The exchange observation is still too old.':'A new arrival does not get to rewrite the source clock.';
 result.classList.toggle('rejected',delayed);result.querySelector('.check-symbol').textContent=delayed?'×':'✓';
 document.querySelectorAll('[data-timing]').forEach(b=>{const active=b===button;b.classList.toggle('active',active);b.setAttribute('aria-pressed',String(active))});
}));
