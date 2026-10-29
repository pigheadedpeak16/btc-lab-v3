import json
from btc_v3.forensics import dashboard_v4 as v4


def test_reference_display_does_not_turn_waiting_model_into_execution_ready():
    sample=dict(state='WAITING',unavailable_reason='STALE_KRAKEN_HISTORY',sample_ts_us=100,
        round_start_us=0,round_end_us=1000,ptb=86020,
        lab_json=json.dumps({'market_reference':{'state':'VERIFIED_RETAINED','price_to_beat':'86020','execution_eligible':False}}),book_json='{}')
    assert hasattr(v4,'sample_diagnostics'),'Missing structured PTB/model/source diagnostic projection'
    d=v4.sample_diagnostics(sample,now_us=110,runner_state='LIVE')
    assert d['reference']['state']=='VERIFIED_RETAINED'
    assert d['model']['state']=='WAITING'
    assert d['execution']['state']=='NOT_EVALUATED'
    assert d['execution']['eligible'] is False
    historical=v4.sample_diagnostics(sample,now_us=110,runner_state='STOPPED')
    assert historical['observation_state']=='HISTORICAL_STOPPED'


def test_dashboard_preserves_clock_and_resync_diagnostics_without_unknown_keys():
    d=v4.public_feed_details(json.dumps({'source_ahead_of_receive_events':10,'source_ahead_peak_us':40000,
        'bba_mismatches':7,'resync_superseded':2,'credential':'MUST_NOT_DISPLAY'}))
    assert d.get('source_ahead_of_receive_events')==10 and d.get('bba_mismatches')==7
    assert 'credential' not in d


def test_results_include_all_arms_without_hidden_shortlist_or_promotion():
    assert 'Latest session · all strategy arms' in v4.HTML
    assert 'table(d.strategies,' in v4.HTML
    assert 'do not establish an edge' in v4.HTML


def test_raw_book_diagnostics_separate_up_down_integrity_and_clocks(feed):
    from btc_v3.forensics.live import _raw_collector_book_payload
    from tests.test_stage3_feed_integrity import state
    feed.states['up']=state();feed.states['up'].slug=feed.market.slug
    d=_raw_collector_book_payload(feed,feed.market.slug)
    assert d['up']['depth_reliable'] is True
    assert d['down']['depth_reliable'] is False
    assert d['up']['source_ts_us']==90_000_000

from tests.test_stage3_feed_integrity import feed
