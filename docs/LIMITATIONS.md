# Research and operational limits

- **Paper only.** No real-money order submission is implemented. There is no persistent funded account or adaptive selector in this release.
- **Not a profit claim.** No strategy is presented as having validated positive edge or consistent returns. Adaptive switching is not a feature or an assumed source of edge.
- **Synthetic demo.** The example and public screenshots are fabricated test observations, clearly labeled. Their PnL is not historical performance.
- **Provenance depends on input.** Format and causal validation cannot independently authenticate a caller's source or resolution payload.
- **Missing history stays missing.** Summary exports without original depth and processing chronology cannot be upgraded into execution-realistic replay by inference.
- **Feed reliability is conditional.** Upstream coverage, gaps, delayed books, rate limits and stale trades can prevent execution. Current multi-hour availability is not established by regression tests.
- **Independent clocks.** Host time-service state, UTC/monotonic continuity and source/receipt causality are separate requirements. A synchronized service report is not an independent measurement of absolute clock offset.
- **Reference access is separate.** Model-based signals can require official TWAP/PTB observations. Public spot data cannot silently replace them. Authenticated reference support is an optional advanced read-only path.
- **Platform scope.** Offline workbench/replay use standard Python. Guarded live preflight can verify Windows time service and Linux `timedatectl`; unsupported environments fail closed. An offline install on macOS does not establish live preflight support.
- **Local web app.** The loopback dashboard is not a multi-user hosted service. Do not expose it through a public proxy. GitHub Pages hosts documentation only.
- **Bounded views.** Latest-session strategy results are capped; run history is capped and refreshed explicitly. A dashboard table is not a complete historical audit.
- **Resource use.** Replay retains small temporary execution stores; repeated runs and live raw archives can accumulate. The 8-GiB guard is not a substitute for storage planning or observed multi-night stability.
- **Cohort comparability.** Execution/model/feed changes alter the research question. Preserve policy hashes and never rewrite or pool incompatible historical returns.
- **Venue scope.** The implementation targets international Polymarket APIs, not Polymarket US. Endpoint existence does not establish access eligibility; users are responsible for current service conditions.

The public repository contains code and controlled fixtures. The developer's local experiment databases, archives, private configuration, credentials and internal handoff/evidence files are excluded.
