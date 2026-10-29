from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from btc_v3.forensics.latency import OverlapCollectorPool, _CollectorSlot


class _SlowCollector:
    async def request_stop(self) -> None:
        await asyncio.sleep(10)


class _Store:
    pass


class _Rest:
    pass


def test_prune_schedules_slow_retirement_without_blocking_hot_loop(tmp_path):
    async def scenario() -> None:
        pool = OverlapCollectorPool(root=tmp_path, rest=_Rest(), store=_Store(), grace_seconds=20)
        sleeper = asyncio.create_task(asyncio.sleep(10))
        market = SimpleNamespace(slug="btc-updown-5m-test")
        slot = _CollectorSlot(market, _SlowCollector(), sleeper, retire_after_mono=asyncio.get_running_loop().time() - 1)
        pool.slots[market.slug] = slot

        started = time.perf_counter()
        await pool.prune()
        elapsed = time.perf_counter() - started

        assert elapsed < 0.25
        assert market.slug not in pool.slots
        assert market.slug in pool._retire_tasks
        await pool.stop()

    asyncio.run(scenario())


def test_active_collector_is_not_retired_early(tmp_path):
    async def scenario() -> None:
        pool = OverlapCollectorPool(root=tmp_path, rest=_Rest(), store=_Store(), grace_seconds=20)
        sleeper = asyncio.create_task(asyncio.sleep(10))
        market = SimpleNamespace(slug="btc-updown-5m-current")
        slot = _CollectorSlot(market, _SlowCollector(), sleeper, retire_after_mono=asyncio.get_running_loop().time() + 60)
        pool.slots[market.slug] = slot
        await pool.prune()
        assert market.slug in pool.slots
        assert market.slug not in pool._retire_tasks
        sleeper.cancel()
        await asyncio.gather(sleeper, return_exceptions=True)

    asyncio.run(scenario())
