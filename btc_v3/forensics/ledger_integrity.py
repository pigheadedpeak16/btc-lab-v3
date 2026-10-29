"""Nonmutating, session-scoped reconciliation of recorded paper fills and positions."""
from decimal import Decimal, InvalidOperation


def reconcile_session(conn, session_id: str) -> dict:
    issues = []

    def rows(sql):
        cursor = conn.execute(sql, (session_id,))
        names = [column[0] for column in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def issue(reason, **context):
        issues.append({"reason": reason, **context})

    def number(value):
        result = Decimal(str(value))
        if not result.is_finite():
            raise ValueError("nonfinite")
        return result

    if not conn.execute("SELECT 1 FROM wallet_forward_sessions WHERE session_id=?", (session_id,)).fetchone():
        issue("SESSION_NOT_FOUND")
    orders = {r['order_id']: r for r in rows('SELECT * FROM wallet_forward_orders WHERE session_id=?')}
    fills = rows('SELECT * FROM wallet_forward_fills WHERE session_id=? ORDER BY fill_ts_us,fill_id')
    rounds = {(r['strategy'], r['slug']): r for r in rows('SELECT * FROM wallet_forward_rounds WHERE session_id=?')}
    totals, by_order = {}, {}
    tolerance = Decimal('0.00000001')  # Decimal VWAP serialization may round tiny fractions.
    for fill in fills:
        key = (fill['strategy'], fill['slug'])
        oid = fill['order_id']
        order = orders.get(oid)
        if order is None:
            issue('FILL_ORDER_MISSING', fill_id=fill['fill_id'])
        elif any(fill[k] != order[k] for k in ('session_id', 'strategy', 'slug', 'token_id', 'outcome', 'side')):
            issue('FILL_ORDER_IDENTITY_MISMATCH', fill_id=fill['fill_id'])
        try:
            shares, price, cost, fee = [number(fill[k]) for k in ('shares', 'price', 'notional', 'fee')]
            if shares <= 0 or not 0 < price < 1 or cost < 0 or fee < 0:
                raise ValueError('invalid fill')
            if abs(shares * price - cost) > tolerance:
                issue('FILL_NOTIONAL_MISMATCH', fill_id=fill['fill_id'])
            if fill['outcome'] not in {'UP', 'DOWN'} or fill['side'] not in {'BUY', 'SELL'}:
                raise ValueError('invalid direction')
            total = totals.setdefault(key, dict(up_shares=Decimal(0), down_shares=Decimal(0),
                up_cost=Decimal(0), down_cost=Decimal(0), sell_proceeds=Decimal(0), fees=Decimal(0)))
            side = fill['outcome'].lower()
            total[side + '_shares'] += shares if fill['side'] == 'BUY' else -shares
            if fill['side'] == 'BUY':
                total[side + '_cost'] += cost
            else:
                total['sell_proceeds'] += cost
            total['fees'] += fee
            qty, count = by_order.get(oid, (Decimal(0), 0))
            by_order[oid] = (qty + shares, count + 1)
        except (InvalidOperation, ValueError):
            issue('INVALID_FILL', fill_id=fill['fill_id'])
        round_row = rounds.get(key)
        if round_row is None:
            issue('FILL_POSITION_MISSING', fill_id=fill['fill_id'])
        elif not round_row['round_start_us'] <= fill['fill_ts_us'] < round_row['round_end_us']:
            issue('FILL_OUTSIDE_MARKET_WINDOW', fill_id=fill['fill_id'])

    for oid, order in orders.items():
        qty, count = by_order.get(oid, (Decimal(0), 0))
        try:
            if qty > number(order['shares']) + tolerance:
                issue('ORDER_OVERFILLED', order_id=oid)
        except (InvalidOperation, ValueError):
            issue('INVALID_ORDER_QUANTITY', order_id=oid)
        if order['style'] == 'FAK_PAPER' and count > 1:
            issue('DUPLICATE_TAKER_EXECUTION', order_id=oid)
        if count and order['status'] not in {'FILLED', 'PARTIAL', 'CANCELLED'}:
            issue('FILL_ORDER_STATUS_MISMATCH', order_id=oid)
        if not count and order['status'] in {'FILLED', 'PARTIAL'}:
            issue('EXECUTED_ORDER_WITHOUT_FILL', order_id=oid)

    for key, round_row in rounds.items():
        total = totals.get(key, dict(up_shares=Decimal(0), down_shares=Decimal(0),
            up_cost=Decimal(0), down_cost=Decimal(0), sell_proceeds=Decimal(0), fees=Decimal(0)))
        try:
            if any(abs(number(round_row[k]) - value) > tolerance for k, value in total.items()):
                issue('POSITION_LEDGER_MISMATCH', strategy=key[0], slug=key[1])
            if total['up_shares'] < 0 or total['down_shares'] < 0:
                issue('NEGATIVE_INVENTORY', strategy=key[0], slug=key[1])
            if round_row['pnl'] is not None:
                if round_row['settled_side'] not in {'UP', 'DOWN'}:
                    raise ValueError('invalid settlement')
                payout = total[round_row['settled_side'].lower() + '_shares']
                expected = total['sell_proceeds'] - total['up_cost'] - total['down_cost'] - total['fees'] + payout
                if abs(number(round_row['pnl']) - expected) > tolerance:
                    issue('SETTLEMENT_PNL_MISMATCH', strategy=key[0], slug=key[1])
        except (InvalidOperation, ValueError):
            issue('INVALID_POSITION', strategy=key[0], slug=key[1])
    return {'ok': not issues, 'orders_checked': len(orders), 'fills_checked': len(fills),
            'positions_checked': len(rounds), 'issues': issues}
