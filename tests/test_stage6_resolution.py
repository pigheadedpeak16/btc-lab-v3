"""Official resolution adapter uses mocked public GETs and temporary PAPER ledgers."""
import asyncio
from dataclasses import replace
import json
from decimal import Decimal as D

import httpx
import pytest

from btc_v3.feeds import settlement_reference as r
from btc_v3.forensics.forward_store import ForwardWalletStore
from btc_v3.storage.db import initialize_project_databases
from test_phase4_features import market, MICROS

START=1_800_000_000*MICROS


def contract():
    return replace(market(),slug=f'btc-updown-5m-{START//MICROS}',round_start_us=START,
        round_end_us=START+300*MICROS,market_id='m',condition_id='condition')


def payload(**changes):
    m=contract()
    data=dict(id='m',slug=m.slug,conditionId=m.condition_id,closed=True,
        endDate='2027-01-15T08:05:00Z',outcomes='["Down","Up"]',
        clobTokenIds='["down","up"]',outcomePrices='["0","1"]',
        umaResolutionStatus='resolved')
    # Derive the ISO date from the fixture rather than assume a calendar date.
    from datetime import datetime,timezone
    data['endDate']=datetime.fromtimestamp(m.round_end_us/MICROS,timezone.utc).isoformat()
    data.update(changes); return data


def reader_type():
    cls=getattr(r,'OfficialResolutionReader',None)
    assert cls is not None,'Stage 5 has only manual resolution attestations, no public verified resolution adapter'
    return cls


def read(data,*,status=200,at=None,decision=None):
    m=contract();at=m.round_end_us+10*MICROS if at is None else at
    calls=[]
    def handler(request):
        calls.append(request)
        assert request.method=='GET'
        assert str(request.url)==f'https://gamma-api.polymarket.com/markets/slug/{m.slug}'
        assert 'authorization' not in request.headers
        return httpx.Response(status,json=data)
    async def run():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            return await reader_type()(client=client,clock_us=lambda:at).read(
                m,decision_time_us=at if decision is None else decision)
    return asyncio.run(run()),calls


def test_read_official_binary_outcome_preserves_mapping_and_evidence():
    result,calls=read(payload())
    assert result.outcome=='UP' and result.condition_id=='condition'
    assert result.observed_at_us==contract().round_end_us+10*MICROS
    assert result.evidence_ref.startswith('https://gamma-api.polymarket.com/markets/slug/')
    assert '#sha256=' in result.evidence_ref and len(calls)==1


@pytest.mark.parametrize('change',[
    {'closed':False},{'closed':'true'},{'umaResolutionStatus':'proposed'},
    {'umaResolutionStatus':'disputed'}, {'outcomePrices':'["0.01","0.99"]'},
    {'outcomePrices':'["0.5","0.5"]'}, {'outcomePrices':'["NaN","1"]'},
    {'clobTokenIds':'["up","down"]'}, {'outcomes':'["Up","Up"]'},
    {'slug':'wrong'},{'conditionId':'wrong'},{'id':'wrong'},
    {'endDate':'2030-01-01T00:00:00Z'},{'outcomePrices':'garbage'},
    {'resolutionStatus':'active'}, {'resolutionStatus':'unknown'},
])
def test_incomplete_conflicting_or_mismatched_resolution_is_not_accepted(change):
    with pytest.raises(ValueError,match='RESOLUTION_'):
        read(payload(**change))


def test_v2_native_resolution_does_not_require_legacy_uma_status():
    data=payload(resolutionStatus='resolved',version='v2');data.pop('umaResolutionStatus')
    result,_=read(data);assert result.outcome=='UP'


@pytest.mark.parametrize('status',[404,429,503])
def test_http_failure_does_not_become_a_resolution(status):
    with pytest.raises((ValueError,httpx.HTTPStatusError)):
        read({},status=status)


def test_response_before_requested_decision_is_rejected():
    with pytest.raises(ValueError,match='RESOLUTION_CLOCK'):
        read(payload(),at=contract().round_end_us+MICROS,decision=contract().round_end_us+2*MICROS)
