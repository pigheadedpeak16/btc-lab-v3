"""Bounded unauthenticated REST observations; no orders, credentials or signing.

This is a separately versioned sampled-public-feed cohort, not a replay of the
legacy WebSocket experiment. Missing source timestamps never become fresh ticks.
"""
import asyncio
from datetime import datetime,timezone
from decimal import Decimal
import json
import re
import uuid

import httpx

from ..timeutil import now_epoch_us,parse_source_timestamp_us
from ..forensics.models import WalletTrade
from ..forensics.forward_lab import economic_key

PUBLIC_CAPTURE_REVISION='stage9_sampled_public_rest_v1'


class PublicSnapshotFeed:
    def __init__(self,*,rest,client,watch_wallet,clock_us=now_epoch_us):
        names={n.lower() for n in client.headers}
        if (names & {'authorization','proxy-authorization','cookie','x-api-key','api-key'}
            or any(n.startswith('poly_') for n in names) or client.cookies):
            raise ValueError('PUBLIC_FEED_AUTHENTICATED_CLIENT_FORBIDDEN')
        if not re.fullmatch('0x[0-9a-fA-F]{40}',watch_wallet):raise ValueError('PUBLIC_WATCH_WALLET_INVALID')
        self.rest=rest;self.client=client;self.wallet=watch_wallet;self.clock=clock_us
        self.session=uuid.uuid4().hex;self.sequence=0;self.last_slug=None;self.fee_slug=None
        self.diagnostics={};self.next_poll_us=0

    def _event(self,kind,market,data,*,source=None,received=None):
        self.sequence+=1;at=self.clock();received=at if received is None else received
        if source is not None and source>received:raise ValueError('PUBLIC_SOURCE_CLOCK_FUTURE')
        return dict(event_id=self.session+':'+str(self.sequence),kind=kind,slug=market.slug,source_ts_us=source,
            received_at_us=received,processed_at_us=at,sequence=self.sequence,data=data)

    async def _json(self,url,params=None):
        raw=bytearray()
        async with self.client.stream('GET',url,params=params,timeout=3.0,follow_redirects=False,auth=None) as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                if len(raw)+len(chunk)>1_000_000:raise ValueError('PUBLIC_RESPONSE_LIMIT')
                raw.extend(chunk)
        received=self.clock()
        return json.loads(raw,parse_float=Decimal),received

    async def poll(self):
        if self.clock()<self.next_poll_us:return []
        rows=[];market=None
        try:
            market=await asyncio.wait_for(self.rest.discover_btc_5m(
                now=datetime.fromtimestamp(self.clock()/1e6,timezone.utc),candidate_offsets=(0,)),timeout=3)
            at=self.clock()
            if market.slug!=self.last_slug:
                rows.append(self._event('market',market,dict(market_id=market.market_id,condition_id=market.condition_id,
                    round_start_us=market.round_start_us,round_end_us=market.round_end_us,up_token_id=market.up_token_id,down_token_id=market.down_token_id)))
                self.last_slug=market.slug
            if not market.round_start_us<=at<market.round_end_us or market.closed:
                return rows+[self._event('gap',market,{'reason':'DISCOVERY_MARKET_NOT_LIVE'})]
            if self.fee_slug!=market.slug:
                info=await asyncio.wait_for(self.rest.get_clob_market_info(market.condition_id),timeout=3)
                received=self.clock();fd=info.get('fd')
                if not isinstance(fd,dict) or fd.get('r') is None:raise ValueError('PUBLIC_FEE_UNAVAILABLE')
                rate=Decimal(str(fd['r']));exponent=fd.get('e',1)
                if not rate.is_finite() or rate<0 or type(exponent) is not int or exponent<0:raise ValueError('PUBLIC_FEE_INVALID')
                rows.append(self._event('fee',market,dict(rate=str(rate),exponent=exponent),received=received))
                self.fee_slug=market.slug
            for token in market.token_ids:
                payload=await asyncio.wait_for(self.rest.get_order_book(token),timeout=3)
                received=self.clock()
                if len(json.dumps(payload))>262144:raise ValueError('PUBLIC_BOOK_LIMIT')
                asset=str(payload.get('asset_id') or payload.get('token_id') or payload.get('assetId') or '')
                if asset!=token:raise ValueError('PUBLIC_BOOK_MARKET_MISMATCH')
                source=parse_source_timestamp_us(payload.get('timestamp'))
                if source is None:raise ValueError('PUBLIC_BOOK_SOURCE_TIME_MISSING')
                rows.append(self._event('book',market,dict(token_id=token,bids=payload['bids'],asks=payload['asks'],
                    tick_size=payload.get('tick_size'),min_order_size=payload.get('min_order_size'),book_hash=payload.get('hash')),
                    source=source,received=received))
            payload,received=await self._json('https://api.exchange.coinbase.com/products/BTC-USD/ticker')
            source=parse_source_timestamp_us(payload.get('time'))
            if source is None:raise ValueError('PUBLIC_COINBASE_SOURCE_TIME_MISSING')
            rows.append(self._event('tick',market,dict(source='coinbase',price=str(payload['price'])),source=source,received=received))
            payload,received=await self._json('https://api.kraken.com/0/public/Trades',{'pair':'XBTUSD','count':2})
            if payload.get('error'):raise ValueError('PUBLIC_KRAKEN_UNAVAILABLE')
            trades=[v for k,v in payload['result'].items() if k!='last']
            if len(trades)!=1 or not trades[0]:raise ValueError('PUBLIC_KRAKEN_SOURCE_TIME_MISSING')
            trade=trades[0][-1];source=int(Decimal(str(trade[2]))*1_000_000)
            rows.append(self._event('tick',market,dict(source='kraken',price=str(trade[0])),source=source,received=received))
            payload,received=await self._json('https://data-api.polymarket.com/v2/trades',dict(user=self.wallet,
                start=market.round_start_us//1_000_000,limit=100,taker_only='false'))
            raw=payload.get('data') if isinstance(payload,dict) else None
            if not isinstance(raw,list) or len(raw)>=100 or payload.get('pagination',{}).get('has_more'):
                raise ValueError('PUBLIC_WALLET_COVERAGE_INCOMPLETE')
            for item in raw:
                row=WalletTrade.from_api(self.wallet,item)
                if (row.slug or row.event_slug)!=market.slug:
                    self.diagnostics['SIGNAL_OTHER_MARKET_REJECTED']=self.diagnostics.get('SIGNAL_OTHER_MARKET_REJECTED',0)+1;continue
                if (row.token_id not in market.token_ids or market.outcome_for_token(row.token_id)!=row.outcome
                    or row.side!='BUY' or row.price is None or row.size is None or not row.price.is_finite()
                    or not row.size.is_finite() or not 0<row.price<1 or row.size<=0):continue
                rows.append(self._event('wallet',market,dict(economic_key=economic_key(row),token_id=row.token_id,outcome=row.outcome,
                    side='BUY',price=str(row.price),shares=str(row.size)),source=row.timestamp_us,received=received))
            if not market.round_start_us<=self.clock()<market.round_end_us:
                rows=[r for r in rows if r['kind']!='wallet']+[self._event('gap',market,{'reason':'ROLLOVER_DURING_PUBLIC_RESPONSE'})]
        except (httpx.HTTPError,ValueError,KeyError,TypeError,RuntimeError,asyncio.TimeoutError):
            # Fixed codes only: no SDK exception/body/cookie values enter logs.
            self.diagnostics['PUBLIC_INPUT_UNAVAILABLE']=self.diagnostics.get('PUBLIC_INPUT_UNAVAILABLE',0)+1
            self.next_poll_us=self.clock()+5_000_000
            if market is not None:rows=[r for r in rows if r['kind']!='wallet']+[self._event('gap',market,{'reason':'PUBLIC_INPUT_UNAVAILABLE'})]
        return rows
