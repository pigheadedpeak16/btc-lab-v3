# Using BTC LAB

[Project website](https://pigheadedpeak16.github.io/btc-lab-v3/) · [README](../README.md) · [Replay format](REPLAY_FORMAT.md)

## First run

Clone the repository, create a Python 3.11+ virtual environment and install `.[dev]` as shown in the README. Use that environment's Python for every command:

```powershell
.\.venv\Scripts\python.exe -B -m btc_v3.forensics.browser_dashboard --root . --open
```

On macOS/Linux, substitute `.venv/bin/python`. The server listens only on `127.0.0.1`. It is for local use; do not expose it through a public proxy. To choose another port, add `--port 8593`. Stop its terminal with `Ctrl+C` before relaunching the same port.

The offline demo does not require `init`. Reading an empty catalog creates no database. Saving a strategy initializes only its dedicated local catalog; web replay executes in isolated temporary stores and saves a new report under `research/strategy_runs/`.

## The four tabs

### Live Data

Shows the latest recorded session, market, price to beat, sample age and separate reference/model/execution states. An empty checkout correctly shows STOPPED or unavailable values. Historical samples are not current observations. Bounded database projections are not proof that an operating-system process is alive.

A displayed model readiness state does not authorize a strategy or an order. A retained official reference can remain valid while a model waits for fresh causal history.

### Strategies

Select a frozen preset or a custom strategy. Editable settings include decision timing, shares, delay, book age, edge, spread and family-specific signal parameters. Fixed engine settings are disabled and labeled. The UI accepts typed configurations, not arbitrary Python code.

Saving a preset makes a custom strategy. Saving a custom strategy makes a new immutable revision. A stale browser revision cannot overwrite a newer one. Save edits before export or replay. Archiving removes a custom strategy from the active library but preserves its versions and results. Frozen presets cannot be archived.

For the demo, choose **Fair value book imbalance**, upload `examples/synthetic-btc-window.jsonl`, and select **Run offline replay**. The synthetic outcome is not evidence of edge.

Web limits: **2 MB, 5,000 events, 30 minutes**, one active replay, up to 100 saved reports. Both project and temporary drives need **8 GiB free**. Missing market, fee, depth or model evidence produces explicit rejections or unsupported input, not fabricated losses.

### Results

Lists the exact reports saved by Strategies. Inspect opens a saved run without executing it again. Download includes manifest, definition, policies, fills, fees, orders, coverage and settlement evidence. Archiving a strategy does not hide its runs. History refresh is explicit and capped at 20 reports / 4 MB.

The latest-session table includes all independent shadow arms, bounded to 2,000 market rows. Wins/losses are settled-market outcomes, not fills. Arms are separate counterfactuals, not a shared wallet. Compare compatible policy cohorts only. Small samples and concentration diagnostics do not establish edge.

CLI reports and older `replay_runs/` files are local artifacts, not automatically imported into this list.

### Diagnostics

Inspect source-trade versus network-receipt ages, UP/DOWN integrity, clock/resync counters, retained reference, model requirements, rejection reasons, policies and storage warnings. Fresh transport activity does not make old source data fresh. Pausing/resuming refresh never starts collection.

## Optional live-data workflows

**These commands contact public services and create observations. They are unnecessary for the demo and were not run during publication.** Check current upstream access conditions and availability. This implementation targets international Polymarket interfaces, not a US venue integration.

Use a separate new root for collection, consistently across commands. Do not initialize an existing historical experiment:

```powershell
# Initialize a NEW local research root
.\.venv\Scripts\python.exe -B -m btc_v3.cli --root .local-lab init

# Explicit bounded public collection, in separate terminals
.\.venv\Scripts\python.exe -B -m btc_v3.cli --root .local-lab collect-polymarket --seconds 60
.\.venv\Scripts\python.exe -B -m btc_v3.cli --root .local-lab collect-external --seconds 60

# Inspect that same root
.\.venv\Scripts\python.exe -B -m btc_v3.forensics.browser_dashboard --root .local-lab --open
```

Public collectors update feed diagnostics. The overview and per-arm results depend on forward-session samples; standalone collectors do not populate a complete strategy session. Neither the website nor the dashboard launches them automatically.

`forward-wallet-lab` runs independent shadow strategies and requires verified host synchronization plus authenticated read-only official-reference access. Inspect its `--help` before a deliberate bounded run. It does not place exchange orders. S8 maker research is disabled by default.

Optional TWAP authentication uses the SDK and an environment variable named `POLYMARKET_PRIVATE_KEY`. This is an advanced read-only subscription path, not a beginner requirement. Keep authorized credentials local to a secure process, never in source, shell history, screenshots, issues, chats or replay input. The optional on-chain check separately uses `ETHERSCAN_API_KEY`. No step in the demo asks you to create or fund a wallet.

Windows time-service and Linux `timedatectl` reports are supported by the clock check. Unsupported/unverified synchronization blocks guarded forward runs. macOS offline support does not imply live preflight support. Never relax a clock check to force a READY label.

## Command-line replay

```powershell
.\.venv\Scripts\python.exe -B -m btc_v3.cli replay-jsonl --input examples/synthetic-btc-window.jsonl --strategy-profile-json examples/book-imbalance.strategy.json --output replay-demo-report.json
```

Use a new output filename each time. A supplied profile selects `CONFIGURED_RESEARCH`. Frozen BOOK_CONFIRM_75 and PRICE60_80 also have supported shadow paths. Removed account/adaptive cohorts are rejected. CLI limits are 16 MB / 50,000 events / one hour; the browser is deliberately stricter. CLI outputs are not registered as web runs.

## Troubleshooting

| Symptom | Meaning / action |
| --- | --- |
| Cannot connect | Keep the server terminal open and check its printed port. Try `--port 8593`. |
| Empty / STOPPED | Normal on a fresh clone. Run the included offline example. |
| `LAB_LOW_DISK_GUARD` | Free space outside retained datasets until project and TEMP each have 8 GiB. |
| `LAB_REVISION_CONFLICT` | Reload the newest saved revision before editing. |
| No fills / unsupported input | Inspect coverage/rejections; do not invent missing clocks, depth or outcomes. |
| `CLOCK_SYNC_UNVERIFIED` | Verify the operating system's time synchronization; the app does not change it. |
| Missing official reference | Spot ticks cannot replace the official reference. The example already contains synthetic reference observations. |
| Existing output file | Choose a new filename; overwriting is refused. |
| Report cap reached | Export/preserve reports and manage local storage deliberately; results are not automatically deleted. |
| PowerShell activation blocked | Invoke `.\.venv\Scripts\python.exe` directly; activation is unnecessary. |

Raw archives and retained temporary replay stores can accumulate. Guards do not establish flat multi-day resource use. Keep experiments supervised and preserve provenance when moving data.
