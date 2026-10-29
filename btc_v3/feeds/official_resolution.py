"""Public GET-only resolution verification. Never redeems tokens or credits cash.

Gamma snapshots are API evidence, not independent on-chain finality proofs. Unknown,
disputed, fractional, inconsistent or non-terminal outcomes remain unresolved.
No reader is launched automatically; callers must explicitly supply a public client.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import hashlib
import json
from urllib.parse import quote

import httpx

from .polymarket import DiscoveredMarket, GAMMA_BASE_URL
from ..timeutil import now_epoch_us, parse_source_timestamp_us

RESOLUTION_POLICY_REVISION = 'stage6_public_binary_resolution_v1'
PUBLIC_CONFIRMATION_SOURCE = 'PUBLIC_GAMMA_VERIFIED_BINARY'


@dataclass(frozen=True)
class OfficialResolution:
    slug: str
    condition_id: str
    outcome: str
    observed_at_us: int
    evidence_ref: str
    raw_json: str


def _array(value):
    result = json.loads(value) if isinstance(value, str) else value
    if not isinstance(result, list) or len(result) != 2:
        raise ValueError('RESOLUTION_INVALID_BINARY_ARRAY')
    return result


def _url(market):
    return f'{GAMMA_BASE_URL}/markets/slug/{quote(market.slug, safe="")}'


def validate_resolution(payload, market: DiscoveredMarket, *, observed_at_us: int) -> OfficialResolution:
    """Validate a supplied snapshot; only OfficialResolutionReader establishes GET provenance."""
    try:
        if (market.round_end_us - market.round_start_us != 300_000_000
                or market.round_start_us % 300_000_000
                or market.slug != f'btc-updown-5m-{market.round_start_us // 1_000_000}'
                or market.up_token_id == market.down_token_id
                or not market.condition_id or not market.market_id):
            raise ValueError('RESOLUTION_EXPECTED_MARKET_INVALID')
        if observed_at_us < market.round_end_us:
            raise ValueError('RESOLUTION_MARKET_NOT_EXPIRED')
        if (not isinstance(payload, dict) or payload.get('slug') != market.slug
                or str(payload.get('id')) != market.market_id
                or payload.get('conditionId') != market.condition_id
                or parse_source_timestamp_us(payload.get('endDate')) != market.round_end_us):
            raise ValueError('RESOLUTION_MARKET_IDENTITY_MISMATCH')
        if payload.get('closed') is not True:
            raise ValueError('RESOLUTION_NOT_CLOSED')
        native, legacy = payload.get('resolutionStatus'), payload.get('umaResolutionStatus')
        version = payload.get('version')
        if version not in (None, 'v1', 'v2'):
            raise ValueError('RESOLUTION_UNKNOWN_PROTOCOL')
        if ((native is not None and native != 'resolved')
                or (legacy is not None and legacy not in {'resolved', 'settled'})
                or (native is None and legacy is None)
                or (version == 'v2' and native != 'resolved')):
            raise ValueError('RESOLUTION_NOT_TERMINAL_OR_CONFLICTING_STATUS')
        labels = [str(v).upper() for v in _array(payload.get('outcomes'))]
        tokens = [str(v) for v in _array(payload.get('clobTokenIds'))]
        prices = [Decimal(str(v)) for v in _array(payload.get('outcomePrices'))]
        expected = {'UP': market.up_token_id, 'DOWN': market.down_token_id}
        if set(labels) != set(expected) or any(expected[label] != token for label, token in zip(labels, tokens)):
            raise ValueError('RESOLUTION_TOKEN_OUTCOME_MISMATCH')
        if any(not p.is_finite() for p in prices) or sorted(prices) != [Decimal(0), Decimal(1)]:
            raise ValueError('RESOLUTION_NON_BINARY_OR_INCOMPLETE_PAYOUT')
        raw = json.dumps(payload, sort_keys=True, separators=(',', ':'), allow_nan=False)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        return OfficialResolution(market.slug, market.condition_id, labels[prices.index(Decimal(1))],
            observed_at_us, _url(market) + '#sha256=' + digest, raw)
    except (TypeError, KeyError, InvalidOperation, json.JSONDecodeError) as exc:
        raise ValueError('RESOLUTION_MALFORMED_RESPONSE') from exc


class OfficialResolutionReader:
    """One bounded unauthenticated GET; transport failures leave commitments intact."""
    def __init__(self, *, client: httpx.AsyncClient, clock_us=now_epoch_us):
        self.client = client
        self.clock_us = clock_us

    async def read(self, market: DiscoveredMarket, *, decision_time_us: int) -> OfficialResolution:
        # auth=None does not remove explicitly configured Authorization/Cookie
        # headers. Inspect names/presence only; never read or log credential values.
        names = {name.lower() for name in self.client.headers}
        if (names & {'authorization', 'proxy-authorization', 'cookie', 'x-api-key', 'api-key'}
                or any(name.startswith('poly_') for name in names) or self.client.cookies):
            raise ValueError('RESOLUTION_AUTHENTICATED_CLIENT_FORBIDDEN')
        if decision_time_us < market.round_end_us:
            raise ValueError('RESOLUTION_MARKET_NOT_EXPIRED')
        response = await self.client.get(_url(market), timeout=5.0, follow_redirects=False, auth=None)
        observed_at_us = self.clock_us()  # Fresh receive time AFTER the response.
        if observed_at_us < decision_time_us:
            raise ValueError('RESOLUTION_CLOCK_REGRESSION')
        response.raise_for_status()
        if response.status_code != 200 or len(response.content) > 1_000_000:
            raise ValueError('RESOLUTION_INVALID_RESPONSE')
        return validate_resolution(response.json(), market, observed_at_us=observed_at_us)
