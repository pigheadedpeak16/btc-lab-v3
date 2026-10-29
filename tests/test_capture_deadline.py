"""Scheduled cancellation is not a public-network failure; early timeouts are."""
import asyncio
from pathlib import Path

import pytest

from btc_v3.operations import observation
from btc_v3.replay.readers import read_jsonl
from tests.test_stage7_replay import START


def capture_with_timeout(tmp_path,monkeypatch,*,advance_us):
    clock=[START];mono=[0.0]
    class Feed:
        async def poll(self):raise AssertionError('poll body replaced by deterministic wait boundary')
    async def timed_out(coroutine,*,timeout):
        if isinstance(coroutine,asyncio.Task):
            coroutine.cancel()
            try:await coroutine
            except asyncio.CancelledError:pass
        else:coroutine.close()
        clock[0]+=advance_us;mono[0]+=advance_us/1e6
        raise asyncio.TimeoutError
    async def sleep(seconds):clock[0]+=int(seconds*1e6);mono[0]+=seconds
    monkeypatch.setattr(observation.asyncio,'wait_for',timed_out)
    output=tmp_path/'capture.jsonl'
    result=asyncio.run(observation.capture_public(Feed(),output,seconds=5,authorized=True,
        clock_us=lambda:clock[0],monotonic=lambda:mono[0],free_bytes=lambda:10*2**30,sleep=sleep))
    return result,read_jsonl(output)


def test_timeout_at_scheduled_cutoff_records_censored_poll_not_network_failure(tmp_path,monkeypatch):
    result,(manifest,rows)=capture_with_timeout(tmp_path,monkeypatch,advance_us=5_000_000)
    assert result['state']=='COMPLETE_OBSERVATION'
    assert result['public_input_errors']==0
    assert result['inflight_poll_cancelled_at_deadline']==1
    assert manifest.input_completeness=='BOUNDED_CAPTURE_COMPLETE'
    assert result['observation_policy_revision']=='stage9_unfunded_public_observation_v2'


def test_timeout_before_cutoff_still_marks_capture_incomplete(tmp_path,monkeypatch):
    result,(manifest,rows)=capture_with_timeout(tmp_path,monkeypatch,advance_us=1_000_000)
    assert result['state']=='INCOMPLETE_PUBLIC_INPUTS'
    assert result['public_input_errors']>0
    assert manifest.input_completeness=='INCOMPLETE_CAPTURE'


def test_inner_network_timeout_at_cutoff_is_not_hidden_as_scheduled_cancellation(tmp_path,monkeypatch):
    clock=[START];mono=[0.0]
    class Feed:
        async def poll(self):
            clock[0]+=5_000_000;mono[0]+=5
            raise asyncio.TimeoutError
    async def direct(awaitable,*,timeout):return await awaitable
    monkeypatch.setattr(observation.asyncio,'wait_for',direct)
    output=tmp_path/'capture.jsonl'
    result=asyncio.run(observation.capture_public(Feed(),output,seconds=5,authorized=True,
        clock_us=lambda:clock[0],monotonic=lambda:mono[0],free_bytes=lambda:10*2**30))
    assert result['state']=='INCOMPLETE_PUBLIC_INPUTS'
    assert result['public_input_errors']==1


def test_real_asyncio_deadline_cancels_pending_poll_without_network(tmp_path):
    class Feed:
        async def poll(self):await asyncio.sleep(60)
    output=tmp_path/'real-deadline.jsonl'
    result=asyncio.run(observation.capture_public(Feed(),output,seconds=5,authorized=True,
        free_bytes=lambda:10*2**30))
    assert result['public_input_errors']==0
    assert result['inflight_poll_cancelled_at_deadline']==1
    assert result['state']=='COMPLETE_OBSERVATION'
