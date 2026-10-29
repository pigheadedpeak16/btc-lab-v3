import asyncio,json
from tests.test_stage3_feed_integrity import feed


def test_frame_quote_projection_preserves_every_delta_and_terminal_receive_time(feed):
    async def scenario():
        await feed._process_event(dict(event_type='book',asset_id='up',timestamp='100',
            bids=[{'price':'.4','size':'5'}],asks=[{'price':'.6','size':'5'}]),100_001_000)
        changes=[dict(asset_id='up',side='BUY',price=str(p),size='7',hash=str(p)) for p in ('.31','.32','.33')]
        await feed._process_event(dict(event_type='price_change',timestamp='101',price_changes=changes),101_001_000)
    asyncio.run(scenario())
    rows=feed.store.conn.execute('SELECT delta_json,received_at_us FROM book_deltas ORDER BY id').fetchall()
    assert len(rows)==3 and all(r[1]==101_001_000 for r in rows)
    assert [json.loads(r[0])['price'] for r in rows]==['.31','.32','.33']
    quotes=feed.store.conn.execute('SELECT best_bid,received_at_us FROM best_quote_observations ORDER BY id').fetchall()
    assert len(quotes)==2,'Derived quotes belong to completed events, not unobservable intermediate levels'
    assert quotes[-1]==('0.4',101_001_000)


def test_depth_frame_batches_commits_without_losing_normalized_evidence(feed):
    statements=[]
    async def exercise():
        await feed._process_event(dict(event_type='book',asset_id='up',timestamp='100',
            bids=[dict(price='.4',size='5')],asks=[dict(price='.6',size='5')]),100_001_000)
        feed.store.conn.set_trace_callback(statements.append)
        await feed._process_event(dict(event_type='price_change',timestamp='101',price_changes=[
            dict(asset_id='up',side='BUY',price='.39',size=str(i+1),hash=str(i)) for i in range(100)]),101_001_000)
    asyncio.run(exercise())
    commits=[s for s in statements if s.strip().upper()=='COMMIT']
    assert len(commits)==1
    assert feed.store.conn.execute('SELECT COUNT(*) FROM book_deltas').fetchone()[0]==100


def test_failed_frame_commit_rolls_back_rows_and_quarantines_memory(feed):
    import sqlite3,pytest
    async def exercise():
        await feed._process_event(dict(event_type='book',asset_id='up',timestamp='100',
            bids=[dict(price='.4',size='5')],asks=[dict(price='.6',size='5')]),100_001_000)
        feed.store.conn.execute("CREATE TRIGGER reject_second_delta BEFORE INSERT ON book_deltas WHEN json_extract(NEW.delta_json,'$.hash')='reject' BEGIN SELECT RAISE(ABORT,'injected failure'); END")
        with pytest.raises(sqlite3.IntegrityError):
            await feed._process_event(dict(event_type='price_change',timestamp='101',price_changes=[
                dict(asset_id='up',side='BUY',price='.39',size='9',hash=h) for h in ('ok','reject')]),101_001_000)
    asyncio.run(exercise())
    assert feed.store.conn.execute('SELECT COUNT(*) FROM book_deltas').fetchone()[0]==0
    assert feed.store.conn.execute('SELECT COUNT(*) FROM best_quote_observations').fetchone()[0]==1
    assert not feed.states['up'].depth_reliable
