from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal

import websockets

from .polymarket import CLOB_MARKET_WS_URL, PolymarketPublicRest
from ..timeutil import utc_now


def _best(levels: list[dict], *, bid: bool) -> str:
    vals: list[Decimal] = []
    for row in levels or []:
        try:
            vals.append(Decimal(str(row.get("price"))))
        except Exception:
            pass
    if not vals:
        return "-"
    return format(max(vals) if bid else min(vals), "f")


async def verify() -> int:
    print("BTC Lab V3 - public CLOB live preflight")
    print("No authentication. No orders. Read-only market-data verification.")
    async with PolymarketPublicRest(timeout_seconds=3.0) as rest:
        t0 = time.perf_counter()
        try:
            market = await asyncio.wait_for(
                rest.discover_btc_5m(now=utc_now(), candidate_offsets=(0, 1, -1)),
                timeout=6.0,
            )
        except Exception as exc:
            print(f"FAIL discovery: {type(exc).__name__}: {exc}")
            return 2
        now_us = int(time.time() * 1_000_000)
        remaining = (market.round_end_us - now_us) / 1_000_000
        if remaining <= 0:
            print(f"FAIL discovery returned expired market: {market.slug} remaining={remaining:.1f}s")
            return 3
        print(f"market   {market.slug}  remaining={remaining:.1f}s  discovery={(time.perf_counter()-t0)*1000:.1f}ms")

        for outcome, token in (("UP", market.up_token_id), ("DOWN", market.down_token_id)):
            start = time.perf_counter()
            try:
                payload = await asyncio.wait_for(rest.get_order_book(token), timeout=3.0)
            except Exception as exc:
                print(f"FAIL REST {outcome}: {type(exc).__name__}: {exc}")
                return 4
            bids = payload.get("bids") or []
            asks = payload.get("asks") or []
            print(
                f"REST {outcome:<4} {((time.perf_counter()-start)*1000):7.1f}ms "
                f"levels={len(bids)}/{len(asks)} best={_best(bids,bid=True)}/{_best(asks,bid=False)}"
            )

    snapshots: dict[str, dict] = {}
    token_to_outcome = {market.up_token_id: "UP", market.down_token_id: "DOWN"}
    try:
        async with websockets.connect(
            CLOB_MARKET_WS_URL,
            ping_interval=None,
            close_timeout=2,
            max_size=None,
            open_timeout=4,
        ) as ws:
            await ws.send(json.dumps({
                "assets_ids": list(market.token_ids),
                "type": "market",
                "custom_feature_enabled": True,
            }, separators=(",", ":")))
            deadline = asyncio.get_running_loop().time() + 5.0
            while len(snapshots) < 2:
                left = deadline - asyncio.get_running_loop().time()
                if left <= 0:
                    break
                raw = await asyncio.wait_for(ws.recv(), timeout=left)
                text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
                if text.upper() == "PONG":
                    continue
                decoded = json.loads(text)
                events = decoded if isinstance(decoded, list) else [decoded]
                for event in events:
                    if not isinstance(event, dict) or (event.get("event_type") or event.get("type")) != "book":
                        continue
                    token = str(event.get("asset_id") or event.get("token_id") or event.get("tokenId") or "")
                    if token in token_to_outcome:
                        snapshots[token] = event
    except Exception as exc:
        print(f"FAIL websocket: {type(exc).__name__}: {exc}")
        return 5

    missing = [token_to_outcome[t] for t in market.token_ids if t not in snapshots]
    if missing:
        print("FAIL websocket snapshots missing: " + ",".join(missing))
        return 6

    for token in market.token_ids:
        event = snapshots[token]
        bids = event.get("bids") or []
        asks = event.get("asks") or []
        outcome = token_to_outcome[token]
        completeness = "FULL" if bids and asks else "PARTIAL"
        print(
            f"WS   {outcome:<4} {completeness:<7} levels={len(bids)}/{len(asks)} "
            f"best={_best(bids,bid=True)}/{_best(asks,bid=False)}"
        )

    print("result  PASS - REST reachable and both subscribed token snapshots received")
    return 0


def main() -> None:
    raise SystemExit(asyncio.run(verify()))


if __name__ == "__main__":
    main()
