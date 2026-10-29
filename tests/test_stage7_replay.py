"""Offline replay contracts and behavior, always on small synthetic inputs."""
import importlib
import importlib.util
import json
from copy import deepcopy
from decimal import Decimal as D

import pytest

START=1_800_000_000_000_000
END=START+300_000_000


def api(name):
    assert importlib.util.find_spec('btc_v3.replay') is not None,'Missing offline replay package'
    spec=importlib.util.find_spec('btc_v3.replay.'+name)
    assert spec is not None,'Missing replay '+name+' capability'
    return importlib.import_module('btc_v3.replay.'+name)


def manifest(**changes):
    value=dict(dataset_id='synthetic-stage7',source_sha256='a'*64,start_us=START,end_us=END+20_000_000,
        training_end_us=START+100_000_000,validation_end_us=START+200_000_000,
        strategy_id='S9_BOOK_CONFIRM_75')
    value.update(changes);return value


def event(kind,offset,sequence,data,**changes):
    value=dict(event_id=f'e{sequence}',kind=kind,slug=f'btc-updown-5m-{START//1_000_000}',
        source_ts_us=START+offset,received_at_us=START+offset,processed_at_us=START+offset,
        sequence=sequence,data=data)
    value.update(changes);return value


def events():
    market=dict(market_id='m',condition_id='c',round_start_us=START,round_end_us=END,
        up_token_id='up',down_token_id='down')
    book=lambda token,bid_size,ask_size:dict(token_id=token,bids=[{'price':'.49','size':bid_size}],
        asks=[{'price':'.50','size':ask_size}],tick_size='.01',min_order_size='5')
    return [event('market',0,1,market),event('fee',1,2,dict(rate='.07',exponent=1)),
        event('book',2,3,book('up','100','10')),event('book',3,4,book('down','1','100')),
        event('wallet',1_000_000,5,dict(economic_key='economic-1',token_id='up',outcome='UP',
            side='BUY',price='.50',shares='10'))]


def run(rows=None,**kwargs):
    return api('engine').replay(manifest(),events() if rows is None else rows,**kwargs)


def test_manifest_is_frozen_and_fingerprinted():
    cls=api('contracts').ReplayManifest;m=cls.from_dict(manifest())
    assert m.fingerprint==cls.from_dict(dict(reversed(list(manifest().items())))).fingerprint
    with pytest.raises((AttributeError,TypeError)):m.dataset_id='edited'


@pytest.mark.parametrize('change',[{'end_us':START},{'source_sha256':'unknown'},
    {'training_end_us':END+1},{'strategy_id':'REAL_MONEY'},{'end_us':START+4_000_000_000}])
def test_invalid_or_unbounded_manifest_rejects(change):
    with pytest.raises(ValueError):api('contracts').ReplayManifest.from_dict(manifest(**change))


@pytest.mark.parametrize('change',[{'processed_at_us':START-1},{'received_at_us':START-1},
    {'source_ts_us':END+1},{'kind':'invented'},{'sequence':-1}])
def test_invalid_event_clocks_or_kind_rejects(change):
    row=event('wallet',0,1,{});row.update(change)
    with pytest.raises(ValueError):api('contracts').ReplayEvent.from_dict(row)


def test_event_payload_is_deeply_immutable():
    row=events()[2];obj=api('contracts').ReplayEvent.from_dict(row)
    row['data']['bids'][0]['size']='999'
    assert obj.data['bids'][0]['size']=='100'
    with pytest.raises(TypeError):obj.data['bids'][0]['size']='1'


def test_reader_is_bounded_and_checks_chronology(tmp_path):
    reader=api('readers');p=tmp_path/'input.jsonl'
    p.write_text('\n'.join(json.dumps(v) for v in [manifest(),*events()]))
    m,rows=reader.read_jsonl(p)
    assert len(rows)==5 and m.dataset_id=='synthetic-stage7'
    with pytest.raises(ValueError,match='LIMIT'):reader.read_jsonl(p,max_bytes=2)
    with pytest.raises(ValueError,match='LIMIT'):reader.read_jsonl(p,max_events=2)
    p.write_text('\n'.join(json.dumps(v) for v in [manifest(),*reversed(events())]))
    with pytest.raises(ValueError,match='ORDER'):reader.read_jsonl(p)


def test_frozen_book_timing_and_full_cost_accounting():
    result=run();fills=result['fills']
    assert result['status']=='SUPPORTED_SYNTHETIC_OR_CURATED'
    assert len(fills)==1
    assert fills[0]['at_us']==START+1_100_000
    assert D(fills[0]['shares'])==10 and D(fills[0]['notional'])==5
    assert D(fills[0]['notional'])+D(fills[0]['fee'])==D('5.175')
    assert 'account' not in result


def test_duplicate_wallet_observations_cannot_fill_twice():
    rows=events();other=deepcopy(rows[-1]);other.update(event_id='other-endpoint',sequence=6,
        processed_at_us=START+1_010_000,received_at_us=START+1_010_000)
    rows.append(other);result=run(rows)
    assert len(result['fills'])==1
    assert any(d['reason']=='DUPLICATE_WALLET_OBSERVATION' for d in result['decisions'])


@pytest.mark.parametrize('fault,reason', [('missing','BOOK_CONFIRM_UNAVAILABLE'),('gap','BOOK_CONFIRM_UNAVAILABLE'),
    ('token','SIGNAL_MARKET_MISMATCH'),('fee','FEE_UNAVAILABLE'),('stale','BOOK_CONFIRM_STALE')])
def test_missing_invalid_and_stale_inputs_do_not_execute(fault,reason):
    rows=events()
    if fault=='missing':rows=[r for r in rows if r['kind']!='book']
    if fault=='fee':rows=[r for r in rows if r['kind']!='fee']
    if fault=='gap':rows.insert(4,event('gap',500000,8,{'reason':'TRANSPORT_LOSS'}))
    if fault=='token':rows[-1]['data']['token_id']='alien'
    if fault=='stale':rows[-1].update(source_ts_us=START+4_000_000,received_at_us=START+4_000_000,processed_at_us=START+4_000_000)
    result=run(rows)
    assert not result['fills']
    assert reason in str(result['decisions'])


def test_partial_fill_keeps_submission_minimum_and_cancels_remainder():
    rows=events();rows[2]['data']['asks'][0]['size']='2'
    result=run(rows)
    assert D(result['fills'][0]['shares'])==2
    assert D(result['orders'][0]['submitted_shares'])>=5


def test_processing_delay_across_expiry_never_backdates_fill():
    rows=events();rows[-1].update(source_ts_us=END-1000,received_at_us=END-1000,processed_at_us=END+1)
    result=run(rows)
    assert not result['fills'] and 'MARKET_EXPIRED' in str(result['decisions'])


def test_future_events_do_not_change_past_decisions():
    first=run();rows=events()+[event('gap',5_000_000,6,{'reason':'later'})]
    second=run(rows)
    assert first['fills']==second['fills']
    assert first['decisions']==second['decisions'][:len(first['decisions'])]


def test_replay_restart_is_deterministic_and_rejects_edited_prefix():
    first=run();second=run(checkpoint=first['checkpoint'])
    assert first==second
    rows=events();rows[-1]['data']['shares']='20'
    with pytest.raises(ValueError,match='CHECKPOINT'):run(rows,checkpoint=first['checkpoint'])


def test_export_without_original_l2_is_unsupported(tmp_path):
    import zipfile
    p=tmp_path/'export.zip'
    with zipfile.ZipFile(p,'w') as z:z.writestr('05_fills.csv','fill_id,shares\nf,10\n')
    result=api('readers').inspect_export(p)
    assert result['replay_supported'] is False
    assert 'ORIGINAL_EVENT_STREAM_MISSING' in result['reasons']


def test_missing_baseline_is_reported_as_unsupported_not_a_losing_opportunity():
    result=run([r for r in events() if r['kind']!='book'])
    assert result['status']=='UNSUPPORTED_INPUTS'
    assert 'BOOK_BASELINE_MISSING' in result['unsupported_reasons']


def test_out_of_order_depth_quarantines_until_fresh_baseline():
    rows=events();rows.insert(4,event('delta',500000,8,dict(token_id='up',side='BUY',price='.49',size='100'),source_ts_us=START+1))
    assert not run(rows)['fills']
    # A new baseline is actual evidence of recovery, not elapsed timeout.
    recovered=deepcopy(rows[2]);recovered.update(event_id='recovered',sequence=9,
        source_ts_us=START+600000,received_at_us=START+600000,processed_at_us=START+600000)
    rows.insert(5,recovered)
    assert len(run(rows)['fills'])==1


def test_independent_shadow_fills_are_never_pooled_into_an_account():
    rows=events();rows[2]['data']['asks'][0]['size']='100'
    other=deepcopy(rows[-1]);other['data']['economic_key']='other';other.update(event_id='other',sequence=6)
    result=run(rows+[other])
    assert len(result['fills'])==2  # independent shadow fills, never pooled as account cash
    assert all(D(fill['notional'])+D(fill['fee'])==D('5.175') for fill in result['fills'])
    assert 'account' not in result


def test_validated_resolution_settles_shadow_pnl_and_payout_cannot_credit_it_again():
    from datetime import datetime,timezone
    rows=events()
    resolved=dict(id='m',slug=rows[0]['slug'],conditionId='c',closed=True,
        endDate=datetime.fromtimestamp(END/1_000_000,timezone.utc).isoformat(),
        outcomes=['UP','DOWN'],clobTokenIds=['up','down'],outcomePrices=['1','0'],umaResolutionStatus='resolved')
    rows.append(event('resolution',301_000_000,6,resolved))
    unpaid=run(rows)
    assert D(unpaid['research_rounds'][0]['pnl'])==D('4.825')
    rows.append(event('payout',302_000_000,7,dict(amount='10',receipt_ref='fixture-evidenced-payment')))
    paid=run(rows)
    assert paid['research_rounds']==unpaid['research_rounds']
    assert 'PAYOUT_NOT_APPLICABLE_TO_SHADOW_RESEARCH' in str(paid['decisions'])


def test_prediction_received_late_cannot_rewrite_book_benchmark():
    rows=events()+[event('prediction',2_000_000,6,dict(p_up=.99,decision_us=START+1_000_000))]
    assert run(rows)['fills']==run()['fills']


def test_cli_produces_bounded_report_without_touching_input(tmp_path):
    from btc_v3 import cli
    function=getattr(cli,'cmd_offline_replay',None)
    assert function is not None,'Missing bounded replay CLI'
    p=tmp_path/'input.jsonl';p.write_text('\n'.join(json.dumps(v) for v in [manifest(),*events()]))
    original=p.read_bytes();out=tmp_path/'result.json'
    args=cli.build_parser().parse_args(['replay-jsonl','--input',str(p),'--output',str(out)])
    assert function(args)==0 and json.loads(out.read_text())['fills']
    assert p.read_bytes()==original
    assert function(args)==3  # existing artifacts are never silently overwritten


def test_replay_report_and_checkpoint_identify_actual_runtime_policies():
    result=run()
    assert result['policies']['execution']=='stage4_execution_realism_v1'
    assert result['policies']['accounting']=='research_shadow_ledger_v1'
    assert result['policies']['model']=='stage6_causal_twap_v1'
    assert result['checkpoint']['runtime_policy_sha256']


def test_delayed_older_snapshot_cannot_restore_stale_cheap_depth():
    rows=events();older=deepcopy(rows[2]);older.update(event_id='older-snapshot',sequence=8,
        source_ts_us=START+1,received_at_us=START+500000,processed_at_us=START+500000)
    rows.insert(4,older)
    result=run(rows)
    assert not result['fills']
    assert 'OUT_OF_ORDER_SNAPSHOT' in str(result['decisions'])
