# Replay input and strategy definitions

[Usage](USAGE.md) · [Complete synthetic input](../examples/synthetic-btc-window.jsonl) · [Exported profile](../examples/book-imbalance.strategy.json)

The included example is synthetic. It contains no real trading history or credentials. Its prices, fees, book and outcome are test observations.

## JSONL structure

Line one is a manifest. Each later line is one JSON event; there are no comments or enclosing arrays. The full example is runnable. This abbreviated manifest is descriptive:

```json
{
  "dataset_id": "your-explicit-dataset-id",
  "source_sha256": "64 lowercase hexadecimal characters",
  "start_us": 1800000000000000,
  "end_us": 1800000320000000,
  "training_end_us": 1800000100000000,
  "validation_end_us": 1800000200000000,
  "strategy_id": "CONFIGURED_RESEARCH",
  "replay_policy_revision": "stage7_observable_replay_v1",
  "input_completeness": "CALLER_ASSERTED"
}
```

The digest is a provenance claim, not independent source authentication. The example uses the library's canonical event fingerprint. Frozen manifest/runtime hashes bind replay/checkpoint identity. Training/validation boundaries are ordered metadata; they do not themselves perform training or establish out-of-sample quality.

An event has these fields:

```json
{
  "event_id": "unique-observation-id",
  "kind": "tick",
  "slug": "btc-updown-5m-1800000000",
  "source_ts_us": 1800000001000000,
  "received_at_us": 1800000001001000,
  "processed_at_us": 1800000001002000,
  "sequence": 42,
  "data": {"source": "coinbase", "price": "100.01"}
}
```

Clocks are integer UTC microseconds. Known source time must not exceed receipt; receipt must not exceed processing. Events follow deterministic processing/sequence order. Never substitute receipt for unknown historical processing time just to make input pass.

## Event kinds

| Kind | Principal payload / role |
| --- | --- |
| `market` | Exact five-minute start/end, slug, market/condition identity and distinct UP/DOWN tokens |
| `fee` | Observed taker `rate` and integer `exponent` |
| `book` | Token, bid/ask levels, source time, tick size and minimum size |
| `delta` | Token, side, price and size; requires valid baseline depth |
| `gap` | Invalidates depth explicitly, with a reason |
| `wallet` | BUY observation with economic identity, token, outcome, price and shares for frozen wallet benchmarks |
| `tick` | Coinbase/Kraken source, price and causal clocks |
| `oracle` | Reference value with source/receipt clocks |
| `resolution` | Binary payload matching the market's identity, end and token/outcome mapping |
| `prediction` | Recorded unused prediction information; cannot replace causal model inputs |
| `payout` | Legacy event marked inapplicable to shadow accounting; cannot credit PnL |

The executable contracts are [`contracts.py`](../btc_v3/replay/contracts.py) and [`engine.py`](../btc_v3/replay/engine.py). Summary CSVs or ZIPs missing original L2/processing chronology are insufficient for executable replay.

## Profiles and reports

Exported profiles contain typed configuration, immutable revision identity and configuration hash, validated by `lab_profiles.py`. Edit through the workbench rather than changing frozen exports without updating provenance.

Use `replay-jsonl --strategy-profile-json ...` for the CLI; the browser passes its saved revision directly. Both use the causal model/execution engine. `SUPPORTED_SYNTHETIC_OR_CURATED` means implemented input requirements were met, not that data is authenticated or a strategy profitable.

Reports retain fills, submitted shares, fees, order rejection reasons, per-market coverage, settlement evidence and policies. Unsettled rounds have `pnl: null`. A valid window can produce no trade for some families. Never fill missing evidence with fabricated trades or outcomes.
