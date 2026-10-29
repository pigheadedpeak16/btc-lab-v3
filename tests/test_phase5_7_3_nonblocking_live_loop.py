import asyncio

import pytest

from btc_v3.forensics.forward_lab import _bounded_btc_discovery


class _FakeRest:
    def __init__(self, delay: float, value=object()):
        self.delay = delay
        self.value = value
        self.offsets = None

    async def discover_btc_5m(self, *, candidate_offsets):
        self.offsets = tuple(candidate_offsets)
        await asyncio.sleep(self.delay)
        return self.value


def test_bounded_discovery_uses_only_nearby_live_candidates():
    async def scenario():
        rest = _FakeRest(0.0, value="market")
        result = await _bounded_btc_discovery(rest, timeout_seconds=0.1)
        assert result == "market"
        assert rest.offsets == (0, 1, -1)

    asyncio.run(scenario())


def test_bounded_discovery_times_out_instead_of_hanging():
    async def scenario():
        rest = _FakeRest(0.05)
        with pytest.raises(asyncio.TimeoutError):
            await _bounded_btc_discovery(rest, timeout_seconds=0.005)

    asyncio.run(scenario())
