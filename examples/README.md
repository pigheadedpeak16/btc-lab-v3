# Synthetic offline example

`synthetic-btc-window.jsonl` is generated from controlled test observations, **not real market data**. It contains a single five-minute identity, artificial source/receipt/processing clocks, Coinbase/Kraken ticks, reference observations, sparse L2 snapshots, fees and an artificial resolution.

`book-imbalance.strategy.json` is a frozen exported preset. Load the equivalent **Fair value book imbalance** preset in Strategies and upload the JSONL, or pass both files to the CLI as shown in the root README.

The example is deliberately small and reproducible. Any simulated positive/negative PnL is test output, not a performance or edge claim. Do not replace the example with private archives or credentials when submitting an issue.
