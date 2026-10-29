import importlib
from copy import deepcopy
from decimal import Decimal

import pytest

from tests.test_stage7_replay import START,END
from tests.research_fixtures import states,ticks
from tests.research_fixtures import market


def api():
    assert importlib.util.find_spec('btc_v3.state.readiness'),'Missing staged readiness and source diagnostics'
    return importlib.import_module('btc_v3.state.readiness')


def test_source_staleness_is_separate_from_fresh_receipt_and_startup_warmup():
    rows=ticks()
    for row in rows:
        if row['source']=='kraken':row['source_us']=START-10_000_000
    view=api().reference_readiness(rows,at_us=START+1_000_000)
    assert view['sources']['coinbase']['state']=='READY'
    assert view['sources']['kraken']['state']=='SOURCE_STALE'
    assert view['sources']['kraken']['receive_age_seconds']<.01
    assert not view['ready']
    initial=api().reference_readiness(ticks()[2:],at_us=START+1_000_000)
    assert initial['sources']['coinbase']['state']=='WARMING_HISTORY'


def test_missing_down_and_one_sided_depth_have_distinct_explanations():
    books=states();books['down'].mark_unsynchronized()
    view=api().assess_readiness(market(),books,at_us=START+1_000_000,ticks=ticks())
    assert view['up_book']['state']=='BOOK_READY' and view['down_book']['reason']=='DEPTH_UNSYNCHRONIZED'
    books=states();books['down'].bids={}
    view=api().assess_readiness(market(),books,at_us=START+1_000_000,ticks=ticks())
    assert view['down_book']['state']=='SYNCED_ONE_SIDED'
    assert not view['execution_book_inputs_ready']


def test_book_only_signal_does_not_claim_official_reference_or_model_is_required():
    view=api().assess_readiness(market(),states(),at_us=START+1_000_000,ticks=[],strategy='S9_BOOK_CONFIRM_75')
    assert view['up_book']['state']=='BOOK_READY'
    assert view['official_reference']['state']=='UNAVAILABLE'
    assert view['model']['state']=='NOT_REQUIRED_BY_SIGNAL'
    assert view['strategy']['state']=='NOT_EVALUATED'
    assert view['execution']['state']=='NOT_EVALUATED'


def test_timeline_records_source_receipt_processing_and_monotonic_latency():
    mono=[0.0];t=api().ReadinessTimeline(started_us=START,started_mono=0,monotonic=lambda:mono[0])
    mono[0]=1;view=t.observe(market(),states(),at_us=START+1_000_000)
    assert view['first_up_snapshot']['source_us']==START+999000
    assert view['first_up_snapshot']['received_us']==START+999000
    assert view['first_up_snapshot']['processed_us']==START+1_000_000
    assert view['first_paired_book_latency_ms']==1000
    mono[0]=2;t.observe(market(),states(),at_us=START+2_000_000)
    assert t.snapshot()['first_paired_book_us']==START+1_000_000


def test_expired_or_future_depth_never_becomes_execution_ready():
    assert not api().assess_readiness(market(),states(),at_us=END+1,ticks=[])['execution_book_inputs_ready']
    books=states();books['up'].last_server_ts_us=END
    assert not api().assess_readiness(market(),books,at_us=START+1_000_000,ticks=[])['execution_book_inputs_ready']
