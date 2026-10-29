from __future__ import annotations

from dataclasses import dataclass, asdict
import hashlib
import json
import re
from types import MappingProxyType
from typing import Mapping

REPLAY_POLICY_REVISION='stage7_observable_replay_v1'
KINDS=frozenset({'market','fee','book','delta','gap','wallet','tick','oracle','prediction','resolution','payout'})


def plain(value):
    if isinstance(value,Mapping):return {str(k):plain(v) for k,v in value.items()}
    if isinstance(value,(tuple,list)):return [plain(v) for v in value]
    return value


def fingerprint(value):
    return hashlib.sha256(json.dumps(plain(value),sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def freeze(value):
    if isinstance(value,dict):return MappingProxyType({str(k):freeze(v) for k,v in value.items()})
    if isinstance(value,list):return tuple(freeze(v) for v in value)
    if value is None or type(value) in (str,int,bool,float):
        json.dumps(value,allow_nan=False)
        return value
    raise ValueError('REPLAY_NON_JSON_DATA')


def integer(value):
    if type(value) is not int or value<0:raise ValueError('REPLAY_INVALID_CLOCK')
    return value


@dataclass(frozen=True)
class ReplayManifest:
    dataset_id:str
    source_sha256:str
    start_us:int
    end_us:int
    training_end_us:int
    validation_end_us:int
    strategy_id:str='S9_BOOK_CONFIRM_75'
    replay_policy_revision:str=REPLAY_POLICY_REVISION
    input_completeness:str='CALLER_ASSERTED'
    capture_policy_revision:str|None=None

    @classmethod
    def from_dict(cls,row):
        try:
            m=cls(**row)
            for t in (m.start_us,m.end_us,m.training_end_us,m.validation_end_us):integer(t)
            if (not m.dataset_id or not re.fullmatch('[0-9a-f]{64}',m.source_sha256)
                or not m.start_us<m.training_end_us<m.validation_end_us<m.end_us
                or m.end_us-m.start_us>3_600_000_000
                or m.strategy_id not in {'S9_BOOK_CONFIRM_75','S9_PRICE60_80','CONFIGURED_RESEARCH'}
                or m.replay_policy_revision!=REPLAY_POLICY_REVISION):raise ValueError('REPLAY_INVALID_MANIFEST')
            if m.input_completeness not in {'CALLER_ASSERTED','BOUNDED_CAPTURE_COMPLETE','INCOMPLETE_CAPTURE'}:raise ValueError('REPLAY_INVALID_CAPTURE_STATE')
            return m
        except (TypeError,KeyError) as exc:raise ValueError('REPLAY_INVALID_MANIFEST') from exc

    @property
    def fingerprint(self):return fingerprint(asdict(self))


@dataclass(frozen=True)
class ReplayEvent:
    event_id:str
    kind:str
    slug:str
    source_ts_us:int|None
    received_at_us:int
    processed_at_us:int
    sequence:int
    data:Mapping

    @classmethod
    def from_dict(cls,row):
        try:
            e=cls(**{**row,'data':freeze(row['data'])})
            for t in (e.received_at_us,e.processed_at_us,e.sequence):integer(t)
            if e.source_ts_us is not None:integer(e.source_ts_us)
            if (not e.event_id or e.kind not in KINDS or not e.slug.startswith('btc-updown-5m-')
                or e.processed_at_us<e.received_at_us
                or (e.source_ts_us is not None and e.source_ts_us>e.received_at_us)
                or not isinstance(e.data,Mapping)):raise ValueError('REPLAY_INVALID_EVENT')
            return e
        except (TypeError,KeyError,AttributeError) as exc:raise ValueError('REPLAY_INVALID_EVENT') from exc

    def to_dict(self):return {name:plain(getattr(self,name)) for name in self.__dataclass_fields__}


def validate_events(manifest,rows):
    events=[];previous=None;ids=set()
    if len(rows)>50000:raise ValueError('REPLAY_EVENT_LIMIT')
    for row in rows:
        e=row if isinstance(row,ReplayEvent) else ReplayEvent.from_dict(row)
        key=(e.processed_at_us,e.sequence)
        if previous is not None and key<=previous:raise ValueError('REPLAY_EVENT_ORDER')
        if not manifest.start_us<=e.processed_at_us<=manifest.end_us:raise ValueError('REPLAY_OUTSIDE_WINDOW')
        if e.event_id in ids:raise ValueError('REPLAY_DUPLICATE_EVENT_ID')
        ids.add(e.event_id);events.append(e);previous=key
    return tuple(events)
