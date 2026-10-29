# Research question, method and evidence

## Question

When a BTC five-minute signal becomes observable, could a strategy have executed against the market state available at that time? How do source delay, market rollover, stale books and depth constraints change that answer?

The project focuses on the **validity of the experiment before the attractiveness of its return**. It separates data availability, signal formation and fill eligibility. A favorable price in a later sample is not evidence that an earlier order could have filled.

## Method

1. Preserve source, receipt and processing timestamps rather than collapsing them into one event time.
2. Build features from observations available at the decision; reject future source or receipt values.
3. Bind the signal, model and book to the same verified five-minute market.
4. Delay execution explicitly and recheck expiry, both relevant books, fees and submission constraints at actual execution.
5. Walk only observed liquidity; record partial fills and rejected orders.
6. Finalize shadow PnL only with a validated supplied resolution, and keep settlement immutable.
7. Save the input identity, configuration revision and policy hashes so the result can be inspected later.

Independent strategy arms are counterfactuals, not one portfolio. The unit for win/loss summaries is a settled market, not a fill. Reports include fees, unresolved counts and concentration/drawdown diagnostics; descriptive summaries are not a statistical edge claim.

## Reproducible public example

The public dataset is deliberately synthetic and contains artificial price/reference history, source/receipt/processing clocks, books, fees and an UP resolution. It is a software verification window rather than a strategy sample.

The regression `tests/test_public_example.py` runs that same input through all seven built-in configurations. Under those frozen definitions, favourite, fair value, book imbalance and microstructure produce one shadow fill each; momentum, mean reversion and cross-exchange produce none. The test checks exact shadow accounting as resolved quantity minus executed notional and fees. Abstention is an expected outcome, not a failure to force a trade.

To reproduce: install `.[dev]`, then run `python -B -m pytest tests/test_public_example.py -q` with the virtual environment. The README also provides the browser and CLI path for the book-imbalance example. The source manifest and configuration hashes travel with each replay report.

## Engineering findings represented by the tests

- Fresh receipt of a stale source observation cannot refresh execution eligibility.
- A scheduled action can cross the market deadline before it actually executes.
- Rollover cannot substitute a new market's model for an earlier signal's market.
- A delayed REST snapshot can be superseded by newer WebSocket depth or constraints.
- Minimum submitted quantity and partially filled quantity are different concepts.
- Settled state must remain immutable after cache eviction, duplicate callbacks and delayed fills.
- An input-limited window must be reported as unsupported instead of assigned an invented trade or loss.

These are research-integrity and engineering findings. They do not establish any particular strategy's profitability.

## What remains unproven

The synthetic example cannot establish external-data authenticity, historical edge, market impact, sustained API coverage, calibration or multi-night resource stability. Meaningful strategy assessment requires sufficient chronological, prospectively observed data with independently verified labels and explicit policy cohorts. An adaptive selector and persistent funded account are not part of this release.

For a project discussion, the defensible contributions are the causal replay design, execution safeguards, observable failure states, regression evidence and reproducible research workflow—not a claim of live profitability. See [architecture](ARCHITECTURE.md) and [limits](LIMITATIONS.md).
