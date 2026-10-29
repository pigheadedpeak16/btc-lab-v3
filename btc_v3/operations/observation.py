"""Finite unfunded public capture for research bootstrap. No ledger or account IO."""
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import shutil
import time
import uuid

MIN_FREE_BYTES = 8 * 2**30
from ..feeds.clock_health import RuntimeClockHealth
from ..timeutil import now_epoch_us
from ..replay.contracts import ReplayEvent,ReplayManifest,fingerprint

OBSERVATION_POLICY_REVISION='stage9_unfunded_public_observation_v2'


async def capture_public(feed,output,*,seconds=300,authorized=False,clock_us=now_epoch_us,
                         monotonic=time.monotonic,free_bytes=None,sleep=asyncio.sleep):
    if not authorized:return dict(state='DISABLED',funded_account_activated=False)
    output=Path(output)
    if type(seconds) is not int or not 5<=seconds<=300:raise ValueError('OBSERVATION_DURATION_LIMIT')
    if output.exists():raise ValueError('OBSERVATION_OUTPUT_MUST_BE_NEW')
    free_bytes=free_bytes or (lambda:shutil.disk_usage(output.parent).free)
    if free_bytes()<MIN_FREE_BYTES:return dict(state='LOW_DISK_SPACE_GUARD',funded_account_activated=False)
    start=clock_us();end=start+seconds*1_000_000
    guard=RuntimeClockHealth(utc_us=start,monotonic_seconds=monotonic())
    rows=[];used=0;errors=0;state='COMPLETE_OBSERVATION';late=0;deadline_cancelled=0
    while clock_us()<end:
        if free_bytes()<MIN_FREE_BYTES:state='INCOMPLETE_STORAGE_GUARD';break
        guard.validate(clock_us(),monotonic())
        remaining=(end-clock_us())/1e6
        poll=asyncio.create_task(feed.poll())
        try:
            batch=await asyncio.wait_for(poll,timeout=min(15,remaining))
        except asyncio.TimeoutError:
            if remaining<=15 and clock_us()>=end and poll.cancelled():
                # A scheduled stop censors this in-flight request; it is not an outage.
                deadline_cancelled+=1;batch=[]
            else:errors+=1;batch=[];state='INCOMPLETE_PUBLIC_INPUTS'
        except Exception:
            errors+=1;batch=[];state='INCOMPLETE_PUBLIC_INPUTS'
        guard.validate(clock_us(),monotonic())
        for supplied in batch:
            at=clock_us()
            if at>=end:late+=1;continue
            e=ReplayEvent.from_dict({**supplied,'processed_at_us':at,'sequence':len(rows)+1})
            encoded=json.dumps(e.to_dict(),sort_keys=True,separators=(',',':'))
            if len(encoded.encode())>262144 or used+len(encoded.encode())>15_000_000 or len(rows)>=50000:
                state='INCOMPLETE_CAPTURE_LIMIT';break
            rows.append(e);used+=len(encoded.encode())
        if state=='INCOMPLETE_CAPTURE_LIMIT':break
        if clock_us()<end:await sleep(min(1,(end-clock_us())/1e6))
    if getattr(feed,'diagnostics',{}).get('PUBLIC_INPUT_UNAVAILABLE'):
        state='INCOMPLETE_PUBLIC_INPUTS'
    manifest=ReplayManifest.from_dict(dict(dataset_id='public-observation:'+uuid.uuid4().hex,
        source_sha256=fingerprint([e.to_dict() for e in rows]),start_us=start,end_us=end,
        training_end_us=start+(end-start)//3,validation_end_us=start+2*(end-start)//3,
        input_completeness='BOUNDED_CAPTURE_COMPLETE' if state=='COMPLETE_OBSERVATION' else 'INCOMPLETE_CAPTURE',
        capture_policy_revision=OBSERVATION_POLICY_REVISION))
    # Write only the new small artifact; never initialize project databases.
    with output.open('x',encoding='utf-8') as stream:
        stream.write(json.dumps(asdict(manifest),sort_keys=True)+'\n')
        for row in rows:stream.write(json.dumps(row.to_dict(),sort_keys=True,separators=(',',':'))+'\n')
    return dict(state=state,observation_policy_revision=OBSERVATION_POLICY_REVISION,output=str(output),events=len(rows),
        public_input_errors=errors,late_observations_excluded=late,inflight_poll_cancelled_at_deadline=deadline_cancelled,funded_account_activated=False,
        source_sha256=manifest.source_sha256,
        limitations=['Replay manifest partitions do not establish fitted training/validation evidence.',
            'Observed collection dispatch times do not prove a hypothetical order would fill.',
            'No account, orders or strategy promotion occurred; official outcome labels remain separate.'])
