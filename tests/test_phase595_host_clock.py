import asyncio
import pytest
from btc_v3.feeds import clock_health as ch
from btc_v3.forensics import forward_lab as f


def test_windows_unsynchronized_and_unknown_status_cannot_pass_preflight():
    assert hasattr(ch,'parse_windows_sync'),'Missing absolute sync preflight'
    assert ch.parse_windows_sync('Leap Indicator: 3(not synchronized)\nStratum: 0 (unspecified)')['state']=='NOT_SYNCHRONIZED'
    assert ch.parse_windows_sync('Leap Indicator: 0(no warning)\nStratum: 3 (secondary reference)')['state']=='SERVICE_REPORTS_SYNCHRONIZED'
    assert ch.parse_windows_sync('unknown localized output')['state']=='UNVERIFIED'


def test_unsynchronized_runner_stops_before_databases_clients_or_credentials(tmp_path,monkeypatch):
    assert hasattr(ch,'require_host_sync'),'Missing fail-closed clock check'
    def fail():raise RuntimeError('CLOCK_SYNC_UNVERIFIED')
    monkeypatch.setattr(ch,'require_host_sync',fail)
    with pytest.raises(RuntimeError,match='CLOCK_SYNC_UNVERIFIED'):
        asyncio.run(f.run_forward_wallet_lab(tmp_path,requested_wallet='public',seconds=1,config=f.ForwardLabConfig(strict_market_minimum=True)))
    assert not list(tmp_path.iterdir())
    assert f._termination_state(RuntimeError('CLOCK_SYNC_UNVERIFIED'))=='GUARD_STOPPED'


def test_cli_clock_guard_precedes_credentials_and_database_initialization(tmp_path,monkeypatch):
    from btc_v3 import cli
    def blocked():raise RuntimeError('CLOCK_SYNC_UNVERIFIED')
    monkeypatch.setattr(ch,'require_host_sync',blocked)
    original=cli.os.environ.get
    def guarded(key,*args):
        if key==cli.PRIVATE_KEY_ENV:pytest.fail('Unsynchronized startup inspected credential presence')
        return original(key,*args)
    monkeypatch.setattr(cli.os.environ,'get',guarded)
    assert cli.cmd_forward_wallet_lab(tmp_path,wallet='public',seconds=1,trades_poll_ms=100,
        activity_poll_ms=100,copy_delay_ms=75,model_paths=500)==3
    assert not list(tmp_path.iterdir())


def test_exported_clock_diagnostics_retain_absolute_service_state_and_transitions():
    guard=ch.RuntimeClockHealth(utc_us=1_000_000,monotonic_seconds=0)
    assert hasattr(guard,'record_host_sync'),'Host sync checks must remain inspectable in exported clock diagnostics'
    for i in range(100):guard.record_host_sync({'state':'SERVICE_REPORTS_SYNCHRONIZED','observed_at_us':1_000_000+i})
    guard.record_host_sync({'state':'NOT_SYNCHRONIZED','observed_at_us':2_000_000})
    d=guard.details()['host_sync']
    assert d['latest']['state']=='NOT_SYNCHRONIZED' and d['observations']==101
    assert len(d['transitions'])==2
