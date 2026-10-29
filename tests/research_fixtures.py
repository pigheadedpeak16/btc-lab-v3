"""Small synthetic public observations, independent of account/selector features."""
from decimal import Decimal as D
from btc_v3.feeds.polymarket import DiscoveredMarket
from btc_v3.state.book_state import OrderBookState
from tests.test_stage7_replay import START,END,events,event

def market():
    return DiscoveredMarket(slug=events()[0]['slug'],event_id=None,market_id='m',condition_id='c',question=None,rules_text=None,
        round_start_us=START,round_end_us=END,up_token_id='up',down_token_id='down',active=True,closed=False,raw_event={},raw_market={})

def book():
    state=OrderBookState(slug=events()[0]['slug'],token_id='up',outcome='UP')
    state.apply_snapshot(bids=[{'price':'.49','size':'100'}],asks=[{'price':'.5','size':'100'}],
        received_at_us=START+999000,server_ts_us=START+999000,book_hash='fixture',tick_size='.01',min_order_size='5')
    return state

def ticks():
    return [dict(source=source,source_us=START-1_000_000,received_us=START-900000,price='100') for source in ('coinbase','kraken')]+[
        dict(source=source,source_us=START+999000,received_us=START+999500,price='100.01') for source in ('coinbase','kraken')]

def states():
    up=book();down=book();down.token_id='down';down.outcome='DOWN'
    return {'up':up,'down':down}

def tournament_events():
    rows=events()[:2];seq=3
    for second in range(122):
        for kind,data in [('tick',dict(source='coinbase',price=str(100+second*.001))),
                          ('tick',dict(source='kraken',price=str(100+second*.001))),
                          ('oracle',dict(value=str(100+max(0,second-30)*.001)))]:
            rows.append(event(kind,second*1_000_000,seq,data));seq+=1
        if second in (109,110,119,120,121):
            for token in ('up','down'):
                bid='.49' if second<119 or token=='down' else '.51'
                ask='.50' if second<119 or token=='down' else '.52'
                rows.append(event('book',second*1_000_000,seq,dict(token_id=token,
                    bids=[dict(price=bid,size='100' if token=='up' else '5')],
                    asks=[dict(price=ask,size='5' if token=='up' else '100')],tick_size='.01',min_order_size='5')));seq+=1
    return sorted(rows,key=lambda r:(r['processed_at_us'],r['sequence']))

def cohort_events():
    rows=events()
    rows[-1]['data']['price']='.65'
    refs=[event('tick',r['received_us']-START,10+i,dict(source=r['source'],price=r['price']),source_ts_us=r['source_us']) for i,r in enumerate(ticks())]
    # Prelude references belong to the bounded input window, before market open.
    refs=[r for r in refs if r['received_at_us']>=START]
    for row in refs:row.update(source_ts_us=START+1_000_000,received_at_us=START+1_000_000,processed_at_us=START+1_000_000)
    for source in ('coinbase','kraken'):
        refs.insert(0,event('tick',0,20+len(refs),dict(source=source,price='100')))
    return sorted(rows+refs,key=lambda r:(r['processed_at_us'],r['sequence']))
