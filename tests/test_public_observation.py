import asyncio
import importlib

from tests.test_stage7_replay import START
from tests.research_fixtures import cohort_events
from btc_v3.replay.readers import read_jsonl


def api():
    assert importlib.util.find_spec('btc_v3.operations.observation'), 'Missing unfunded bounded observation path'
    return importlib.import_module('btc_v3.operations.observation')


def test_observation_requires_authorization_and_storage_before_public_io(tmp_path):
    class Feed:
        async def poll(self):raise AssertionError('must not contact network')
    out=tmp_path/'capture.jsonl'
    result=asyncio.run(api().capture_public(Feed(),out,seconds=5,authorized=False))
    assert result['state']=='DISABLED' and not out.exists()
    result=asyncio.run(api().capture_public(Feed(),out,seconds=5,authorized=True,free_bytes=lambda:1))
    assert result['state']=='LOW_DISK_SPACE_GUARD' and not out.exists()


def test_unfunded_capture_writes_immutable_causal_replay_input(tmp_path):
    clock=[START];mono=[0.0]
    class Feed:
        rows=cohort_events()
        async def poll(self):
            clock[0]+=1_000_000;mono[0]+=1
            rows,self.rows=self.rows,[]
            return rows
    async def sleep(seconds):clock[0]+=int(seconds*1e6);mono[0]+=seconds
    out=tmp_path/'capture.jsonl'
    result=asyncio.run(api().capture_public(Feed(),out,seconds=5,authorized=True,clock_us=lambda:clock[0],
        monotonic=lambda:mono[0],free_bytes=lambda:10*2**30,sleep=sleep))
    manifest,rows=read_jsonl(out)
    assert rows and rows[-1].processed_at_us>=rows[-1].received_at_us
    assert result['funded_account_activated'] is False
    assert sorted(p.name for p in tmp_path.iterdir())==['capture.jsonl']
