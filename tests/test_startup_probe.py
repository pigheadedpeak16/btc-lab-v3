import asyncio
import importlib
import pytest

def api():
    assert importlib.util.find_spec('btc_v3.operations.startup_probe'),'Missing bounded unfunded startup probe'
    return importlib.import_module('btc_v3.operations.startup_probe')

def test_disabled_probe_performs_no_io(tmp_path):
    path=tmp_path/'absent'
    assert asyncio.run(api().run_probe(path))['state']=='DISABLED'
    assert not path.exists()

@pytest.mark.parametrize('seconds',[0,901,True])
def test_duration_guard_before_network_or_files(tmp_path,seconds):
    with pytest.raises(ValueError,match='DURATION'):
        asyncio.run(api().run_probe(tmp_path/'absent',authorized=True,seconds=seconds))

def test_low_disk_probe_does_not_create_files(tmp_path):
    path=tmp_path/'absent'
    result=asyncio.run(api().run_probe(path,authorized=True,free_bytes=lambda:1))
    assert result['state']=='LOW_DISK_SPACE_GUARD' and not path.exists()

def test_existing_probe_directory_cannot_be_reused(tmp_path):
    with pytest.raises(ValueError,match='NEW'):
        asyncio.run(api().run_probe(tmp_path,authorized=True,free_bytes=lambda:10*2**30))
