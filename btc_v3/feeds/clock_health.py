"""Fail-closed runtime continuity check supplementing the UTC/NTP preflight."""
from __future__ import annotations

import math
import os,re,subprocess,threading,time

_sync_lock=threading.Lock()
_sync_cache=None


def parse_windows_sync(text):
    leap=re.search(r'^Leap Indicator:\s*(\d+)',text,re.M)
    stratum=re.search(r'^Stratum:\s*(\d+)',text,re.M)
    state='UNVERIFIED'
    if leap and stratum:
        state='SERVICE_REPORTS_SYNCHRONIZED' if int(leap[1])<3 and 1<=int(stratum[1])<=15 else 'NOT_SYNCHRONIZED'
    return dict(state=state,leap_indicator=None if leap is None else int(leap[1]),stratum=None if stratum is None else int(stratum[1]),
        scope='Local time-service report; absolute offset is not independently measured',clock_correction_applied=False)


def host_sync_status(*,refresh=False):
    global _sync_cache
    with _sync_lock:
        mono=time.monotonic()
        if not refresh and _sync_cache and 0<=mono-_sync_cache[0]<60:return dict(_sync_cache[1])
        result=dict(state='UNVERIFIED',scope='No supported time-service evidence',clock_correction_applied=False)
        try:
            if os.name=='nt':
                report=subprocess.run(['w32tm','/query','/status'],capture_output=True,text=True,timeout=2,creationflags=0x08000000)
                if report.returncode==0:result=parse_windows_sync(report.stdout)
            elif os.name=='posix':
                report=subprocess.run(['timedatectl','show','--property=NTPSynchronized','--value'],capture_output=True,text=True,timeout=2)
                result.update(state='SERVICE_REPORTS_SYNCHRONIZED' if report.returncode==0 and report.stdout.strip()=='yes' else 'UNVERIFIED')
        except (OSError,subprocess.TimeoutExpired):pass
        result['observed_at_us']=time.time_ns()//1000
        _sync_cache=(mono,result)
        return dict(result)


def require_host_sync():
    status=host_sync_status()
    if status['state']!='SERVICE_REPORTS_SYNCHRONIZED':raise RuntimeError('CLOCK_SYNC_UNVERIFIED: local time service is not synchronized or cannot be verified')
    return status


class ClockCorrelation:
    """Bounded paired-clock evidence. Never changes a clock or grants eligibility."""
    def __init__(self):
        self.first=None;self.latest=None;self.count=0;self.future=0;self.future_peak=0
        self.queue_peak=0;self.drift=0;self.max_drift=0;self.bins={}

    def observe(self,*,received_us,received_mono_ns,processed_us,processed_mono_ns,source_us,transport):
        if self.first is None:self.first=(received_us,received_mono_ns)
        self.count+=1;future=source_us is not None and source_us>received_us
        self.future+=future;self.future_peak=max(self.future_peak,0 if source_us is None else source_us-received_us)
        delay=(processed_mono_ns-received_mono_ns)//1000
        self.queue_peak=max(self.queue_peak,delay)
        self.drift=(received_us-self.first[0])-(received_mono_ns-self.first[1])//1000
        self.max_drift=max(self.max_drift,abs(self.drift))
        self.latest=dict(received_at_us=received_us,received_mono_ns=received_mono_ns,processed_at_us=processed_us,
            processed_mono_ns=processed_mono_ns,source_ts_us=source_us,transport=transport,queue_delay_us=delay,
            dispatch_utc_delta_us=processed_us-received_us,utc_monotonic_drift_us=self.drift)
        key=(received_mono_ns-self.first[1])//60_000_000_000
        if key not in self.bins:
            if len(self.bins)>=720:self.bins.pop(next(iter(self.bins)))
            self.bins[key]=dict(minute=key,events=0,future=0,future_peak_us=0,queue_peak_us=0,utc_monotonic_drift_us=0)
        b=self.bins[key];b['events']+=1;b['future']+=future;b['future_peak_us']=max(b['future_peak_us'],0 if source_us is None else source_us-received_us)
        b['queue_peak_us']=max(b['queue_peak_us'],delay);b['utc_monotonic_drift_us']=self.drift

    def snapshot(self):
        return dict(policy_revision='phase595_paired_transport_clocks_v1',events=self.count,source_after_receipt=self.future,
            source_after_receipt_peak_us=self.future_peak,queue_delay_peak_us=self.queue_peak,utc_monotonic_drift_us=self.drift,
            max_utc_monotonic_drift_us=self.max_drift,latest=self.latest,minute_bins=list(self.bins.values()),
            clock_correction_applied=False,attribution='UNRESOLVED_REMOTE_VS_LOCAL_ABSOLUTE_CLOCK')


class RuntimeClockHealth:
    def __init__(self, *, utc_us: int, monotonic_seconds: float) -> None:
        self.utc_us = utc_us
        self.monotonic_seconds = monotonic_seconds
        self.failed = False
        self.max_skew_us = 0.0
        self.host_sync={'observations':0,'latest':None,'transitions':[]}

    def record_host_sync(self,status):
        evidence={k:status[k] for k in ('state','observed_at_us','leap_indicator','stratum','scope','clock_correction_applied') if k in status}
        old=self.host_sync['latest'];self.host_sync['observations']+=1;self.host_sync['latest']=evidence
        if old is None or old.get('state')!=evidence.get('state'):
            self.host_sync['transitions'].append(dict(evidence))
            self.host_sync['transitions']=self.host_sync['transitions'][-32:]

    def validate(self, utc_us: int, monotonic_seconds: float) -> int:
        skew = (utc_us - self.utc_us) - (monotonic_seconds - self.monotonic_seconds) * 1_000_000
        self.max_skew_us = max(self.max_skew_us, abs(skew))
        if not math.isfinite(skew) or monotonic_seconds < self.monotonic_seconds or abs(skew) > 500_000:
            self.failed = True
        if self.failed:
            raise RuntimeError("CLOCK_HEALTH_LOST: UTC/monotonic continuity exceeded 0.5 seconds")
        return utc_us

    def details(self) -> dict:
        return {"healthy": not self.failed, "max_skew_us": self.max_skew_us,
                "limit_us": 500_000, "scope": "runtime_continuity_not_absolute_NTP",'host_sync':self.host_sync}
