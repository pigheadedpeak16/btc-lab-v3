import gzip,json
from btc_v3.feeds import clock_health as ch
from btc_v3.feeds.raw_archive import RawJsonlArchive


def test_progressive_drift_and_backlog_are_measured_without_clock_correction():
    assert hasattr(ch,'ClockCorrelation'),'Missing paired UTC/monotonic transport measurements'
    p=ch.ClockCorrelation()
    for i in range(180):
        recv=1_000_000_000+i*60_000_000-i*200
        p.observe(received_us=recv,received_mono_ns=i*60_000_000_000,processed_us=recv+900_000,
            processed_mono_ns=i*60_000_000_000+900_000_000,source_us=recv+50_000,transport='WS')
    d=p.snapshot()
    assert d['source_after_receipt']==180 and d['utc_monotonic_drift_us']==-35_800
    assert d['queue_delay_peak_us']==900_000
    assert d['clock_correction_applied'] is False
    assert d['attribution']=='UNRESOLVED_REMOTE_VS_LOCAL_ABSOLUTE_CLOCK'


def test_archive_preserves_transport_clocks_outside_original_payload(tmp_path):
    payload={'event_type':'book','timestamp':'100'}
    with RawJsonlArchive(tmp_path,'test') as a:
        a.write(100_000_000,payload,timing={'received_mono_ns':100,'processed_at_us':100_100_000,'processed_mono_ns':100_000_100,'connection_generation':2})
    path=next(tmp_path.glob('*/*.gz'))
    with gzip.open(path,'rt') as f:row=json.loads(f.readline())
    assert row['payload']==payload and row['timing']['connection_generation']==2
