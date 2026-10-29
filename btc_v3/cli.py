from __future__ import annotations

import argparse
import asyncio
import json
import os
import sqlite3
import statistics
import time
from dataclasses import asdict
from pathlib import Path
from .forensics.forward_store import ForwardWalletStore

from .feeds.coinbase import CoinbaseCollector
from .feeds.kraken import KrakenCollector
from .feeds.polymarket import ROUND_SECONDS, PolymarketPublicRest
from .feeds.polymarket_collector import PolymarketBookCollector
from .feeds.settlement_reference import (
    AUTH_ENABLED_STATE,
    COMPONENT as REFERENCE_COMPONENT,
    MISSING_CREDENTIALS_STATE,
    PRIVATE_KEY_ENV,
    SKIP_REASON,
    AuthenticatedTwapCollector,
    PriceReferenceMapper,
    SettlementReferencePolicy,
    set_reference_policy_status,
)
from .features.btc import CausalBtcFeatureEngine, FeatureUnavailable
from .forensics.commands import (
    TARGET_WALLET_98EUF98A,
    cmd_analyze_wallet,
    cmd_wallet_add,
    cmd_wallet_onchain_check,
    cmd_wallet_set_enabled,
    cmd_wallet_tournament,
    cmd_wallet_watchlist,
)
from .forensics.live import LiveForensicsConfig, run_live_wallet_forensics, run_live_wallet_tournament
from .forensics.latency import LatencyLabConfig, latency_report, run_wallet_latency_lab
from .forensics.forward_lab import (
    ForwardLabConfig,
    run_forward_wallet_lab,
    verify_forward_lab_config,
    verify_full_tournament_config,
    verify_phase590_config,
)
from .forensics.watchlist import WalletWatchlist
from .config import StrategyLabConfig
from .experiments.hashing import config_hash
from .experiments.search_space import baseline_search_spaces
from .experiments.tournament import comparison_matrix
from .models.settlement import SettlementModelConfig, TwapSettlementModel
from .storage.db import initialize_project_databases
from .storage.external_store import ExternalStore
from .storage.market_store import MarketStore
from .timeutil import epoch_us_to_iso, now_epoch_us


def cmd_init(root: Path) -> int:
    paths = initialize_project_databases(root)
    print("BTC Lab V3 initialized (research + paper-trading only).")
    for name, path in paths.items():
        print(f"  {name:8s} {path}")
    print(f"  raw      {root / 'data' / 'raw'}")
    print(f"  datasets {root / 'research' / 'datasets'}")
    print(f"  logs     {root / 'logs'}")
    with ExternalStore.open_project(root) as store:
        set_reference_policy_status(store)
    return 0


def cmd_verify_live_hotpath(root: Path) -> int:
    """Verify the large-history SQLite path used by live causal features.

    This check intentionally runs against the user's real external database after
    schema initialization.  It does not require live network feeds.
    """
    required = {"idx_coinbase_causal", "idx_kraken_causal"}
    failures: list[str] = []
    print("BTC Lab V3 live hot-path verification")
    with ExternalStore.open_project(root) as store:
        names = {
            str(row[0])
            for row in store.conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
        missing = sorted(required - names)
        if missing:
            failures.append("missing indexes: " + ", ".join(missing))

        for source in ("coinbase", "kraken"):
            table, index = store._tick_table_and_index(source)
            if index not in names:
                print(f"  {source:8s} FAIL missing {index}")
                continue
            latest_row = store.conn.execute(
                f"""
                SELECT COALESCE(source_ts_us, received_at_us), received_at_us
                FROM {table} INDEXED BY {index}
                ORDER BY COALESCE(source_ts_us, received_at_us) DESC, received_at_us DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            if latest_row is None:
                print(f"  {source:8s} PASS index present (table currently empty)")
                continue
            target_us = int(latest_row[0])
            decision_us = max(target_us, int(latest_row[1]))
            plan = store.conn.execute(
                f"""
                EXPLAIN QUERY PLAN
                SELECT COALESCE(source_ts_us, received_at_us), received_at_us, price
                FROM {table} INDEXED BY {index}
                WHERE COALESCE(source_ts_us, received_at_us)<=?
                  AND received_at_us<=?
                ORDER BY COALESCE(source_ts_us, received_at_us) DESC, received_at_us DESC, id DESC
                LIMIT 1
                """,
                (target_us, decision_us),
            ).fetchall()
            plan_text = " ".join(str(x) for row in plan for x in row).upper()
            uses_index = index.upper() in plan_text and "TEMP B-TREE" not in plan_text
            samples_ms: list[float] = []
            for _ in range(30):
                t0 = time.perf_counter()
                store.latest_tick(
                    source=source,
                    target_source_ts_us=target_us,
                    decision_time_us=decision_us,
                )
                samples_ms.append((time.perf_counter() - t0) * 1000.0)
            ordered = sorted(samples_ms)
            p50 = statistics.median(ordered)
            p95 = ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]
            ok = uses_index and p95 < 250.0
            print(
                f"  {source:8s} {'PASS' if ok else 'FAIL'} "
                f"index={index} p50={p50:.3f}ms p95={p95:.3f}ms "
                f"temp_sort={'NO' if 'TEMP B-TREE' not in plan_text else 'YES'}"
            )
            if not ok:
                failures.append(f"{source} hot path failed: p95={p95:.3f}ms plan={plan_text}")
        store.conn.execute("PRAGMA optimize")

    if failures:
        print("  result  FAIL")
        for item in failures:
            print("  reason ", item)
        return 3
    print("  result  PASS")
    print("  note    live feature history lookups are indexed; no full-history sort is required")
    return 0


def _fmt_age(ts_us: int | None) -> str:
    if ts_us is None:
        return "-"
    age = max(0.0, (now_epoch_us() - ts_us) / 1_000_000.0)
    return f"{age:.2f}s"


def _print_external_health(root: Path) -> None:
    external_path = root / "data" / "v3_external.sqlite"
    if not external_path.exists():
        return
    with ExternalStore(external_path) as store:
        health = store.current_health()
    print("\nExternal Phase-3 health")
    print(f"  Coinbase ticks     {health.coinbase_count}")
    print(f"  Coinbase age       {_fmt_age(health.latest_coinbase_at_us)}")
    print(f"  Kraken ticks       {health.kraken_count}")
    print(f"  Kraken age         {_fmt_age(health.latest_kraken_at_us)}")
    print(f"  TWAP observations  {health.settlement_observation_count}")
    print(f"  causal PTB rows    {health.price_reference_count}")
    for name in ("coinbase_btc_usd", "kraken_btc_usd", REFERENCE_COMPONENT):
        component = health.components.get(name)
        if component is None:
            print(f"  {name:20s} -")
            continue
        print(
            f"  {name:20s} {component.state or '-'}  "
            f"msg_age={_fmt_age(component.last_message_at_us)} "
            f"tick_age={_fmt_age(component.last_tick_at_us)} "
            f"reconnects={component.reconnect_count} errors={component.error_count}"
        )
        if name == REFERENCE_COMPONENT:
            if component.details.get("skip_reason"):
                print(f"    skip reason       {component.details['skip_reason']}")
            print(f"    auth              {component.details.get('authentication', component.details.get('reason', '-'))}")
            print(f"    mapped PTBs       {component.details.get('mapped_ptb_count', '-')}")
            print(f"    real trading      {component.details.get('real_trading', 'DISABLED')}")


def cmd_status(root: Path) -> int:
    expected = {
        "market": root / "data" / "v3_market.sqlite",
        "external": root / "data" / "v3_external.sqlite",
        "forward": root / "research" / "v3_forward.sqlite",
        "research": root / "research" / "v3_research.sqlite",
    }
    print("BTC Lab V3 status")
    ok = True
    for name, path in expected.items():
        exists = path.exists()
        ok = ok and exists
        print(f"  {name:8s} {'OK' if exists else 'MISSING'}  {path}")

    market_path = expected["market"]
    if market_path.exists():
        with MarketStore(market_path) as store:
            health = store.current_health()
        print("\nPolymarket Phase-2 health")
        print(f"  market slug       {health.slug or '-'}")
        print(f"  round start UTC   {epoch_us_to_iso(health.round_start_us)}")
        print(f"  round end UTC     {epoch_us_to_iso(health.round_end_us)}")
        print(f"  collector state   {health.collector_state or '-'}")
        print(f"  status age        {_fmt_age(health.collector_updated_at_us)}")
        print(f"  latest book age   {_fmt_age(health.last_book_received_at_us)}")
        print(f"  book checkpoints  {health.book_checkpoint_count}")
        print(f"  book deltas       {health.book_delta_count}")
        print(f"  reconnects        {health.reconnect_count}")
        print(f"  collector errors  {health.error_count}")
        tokens = health.collector_details.get("tokens", {}) if health.collector_details else {}
        for outcome in ("UP", "DOWN"):
            details = tokens.get(outcome) or {}
            print(
                f"  {outcome:4s} sync/book      "
                f"{details.get('synchronized', '-')}  "
                f"bid={details.get('best_bid', '-')} ask={details.get('best_ask', '-')}  "
                f"levels={details.get('bid_levels', '-')}/{details.get('ask_levels', '-')}"
            )
    _print_external_health(root)
    return 0 if ok else 2


async def _discover(root: Path, *, persist: bool) -> object:
    initialize_project_databases(root)
    async with PolymarketPublicRest() as rest:
        market = await rest.discover_btc_5m()
    if persist:
        with MarketStore.open_project(root) as store:
            store.persist_market(market, now_epoch_us())
    return market


def _print_market(market) -> None:
    now_us = now_epoch_us()
    print("BTC Up/Down 5m market")
    print(f"  slug         {market.slug}")
    print(f"  phase        {market.phase_at(now_us)}")
    print(f"  start UTC    {epoch_us_to_iso(market.round_start_us)}")
    print(f"  end UTC      {epoch_us_to_iso(market.round_end_us)}")
    print(f"  condition    {market.condition_id}")
    print(f"  Up token     {market.up_token_id}")
    print(f"  Down token   {market.down_token_id}")
    print(f"  active       {market.active}")
    print(f"  closed       {market.closed}")
    if market.question:
        print(f"  question     {market.question}")


def cmd_discover_market(root: Path) -> int:
    market = asyncio.run(_discover(root, persist=True))
    _print_market(market)
    return 0


async def _collect_polymarket(root: Path, seconds: float | None) -> None:
    initialize_project_databases(root)
    async with PolymarketPublicRest() as rest:
        market = await rest.discover_btc_5m()
        _print_market(market)
        print("\nStarting public/read-only Polymarket CLOB collection.")
        if seconds is not None:
            print(f"  duration     {seconds:g}s")
        else:
            print("  duration     until Ctrl+C")
        print("  real orders  DISABLED (no execution code in this phase)")
        with MarketStore.open_project(root) as store:
            collector = PolymarketBookCollector(root=root, market=market, store=store, rest=rest)
            if seconds is None:
                await collector.run()
            else:
                await collector.run_for(seconds)


def cmd_collect_polymarket(root: Path, seconds: float | None) -> int:
    try:
        asyncio.run(_collect_polymarket(root, seconds))
    except KeyboardInterrupt:
        print("\nCollector stopped by Ctrl+C.")
    return 0


async def _collect_external(root: Path, seconds: float) -> None:
    initialize_project_databases(root)
    print("Starting public/read-only external BTC predictor collectors.")
    print(f"  duration      {seconds:g}s")
    print("  Coinbase      BTC-USD public ticker + heartbeat")
    print("  Kraken        BTC/USD public trade stream")
    print("  official TWAP separate authenticated read-only collector")
    print("  real trading  DISABLED")
    with ExternalStore.open_project(root) as coinbase_store, ExternalStore.open_project(root) as kraken_store:
        coinbase = CoinbaseCollector(root=root, store=coinbase_store)
        kraken = KrakenCollector(root=root, store=kraken_store)
        await asyncio.gather(coinbase.run_for(seconds), kraken.run_for(seconds))


def cmd_collect_external(root: Path, seconds: float) -> int:
    try:
        asyncio.run(_collect_external(root, seconds))
    except KeyboardInterrupt:
        print("\nExternal collectors stopped by Ctrl+C.")
    return 0


async def _collect_reference(root: Path, seconds: float) -> tuple[object, int]:
    initialize_project_databases(root)
    market = await _discover(root, persist=True)
    _print_market(market)
    print("\nStarting authenticated READ-ONLY official TWAP collection.")
    print(f"  duration      {seconds:g}s")
    print("  source        Chainlink BTC/USD 60-second TWAP via Polymarket")
    print(f"  credentials   {PRIVATE_KEY_ENV} from this PowerShell process only")
    print("  wallet mode   signer EOA explicitly selected; no Deposit Wallet deployment")
    print("  orders        DISABLED")
    print("  cancellations DISABLED")
    print("  transactions  DISABLED")
    with ExternalStore.open_project(root) as store:
        collector = AuthenticatedTwapCollector(root=root, store=store, market=market)
        await collector.run_for(seconds)
        reference = store.causal_price_reference(market.slug, now_epoch_us())
        if reference is None:
            print("\nNo causal PTB mapped for the discovered round during this probe.")
            print(f"  reason        {SKIP_REASON}")
            return market, 3
        print("\nAuthoritative causal Price To Beat mapped")
        print(f"  slug          {reference.slug}")
        print(f"  PTB           {reference.price_to_beat}")
        print(f"  reference UTC {epoch_us_to_iso(reference.reference_ts_us)}")
        print(f"  observed UTC  {epoch_us_to_iso(reference.observed_at_us)}")
        print(f"  source        {reference.source}")
        print(f"  mapping       {reference.mapping_method}")
        return market, 0


def cmd_collect_reference(root: Path, seconds: float) -> int:
    if not os.environ.get(PRIVATE_KEY_ENV):
        print(f"ERROR: {PRIVATE_KEY_ENV} is not set in this PowerShell session.")
        print("Use the secure PowerShell prompt supplied with the Phase-3 auth patch; do not paste the key into chat.")
        with ExternalStore.open_project(root) as store:
            set_reference_policy_status(store)
            store.set_status(
                component=REFERENCE_COMPONENT,
                session_id=None,
                state=MISSING_CREDENTIALS_STATE,
                updated_at_us=now_epoch_us(),
                last_message_at_us=None,
                last_tick_at_us=None,
                reconnect_count=0,
                error_count=0,
                details={"skip_reason": SKIP_REASON, "real_trading": "DISABLED"},
            )
        return 4
    try:
        _, code = asyncio.run(_collect_reference(root, seconds))
        return code
    except KeyboardInterrupt:
        print("\nReference collector stopped by Ctrl+C.")
        return 0




def _boundary_aware_probe_seconds(now_us: int, requested_seconds: float) -> float:
    """Allow a finite verification probe to survive a missing current-round PTB.

    The authenticated TWAP subscription provides recent history, but its documented contract
    does not guarantee that the current 5-minute round-start observation is always included.
    If that opening point is unavailable, a correct live verifier must stay connected through
    the next 5-minute boundary and retain enough post-connect history for the 60-second
    volatility model. The probe still exits early as soon as a live round becomes evaluable.
    """
    round_us = ROUND_SECONDS * 1_000_000
    next_boundary_us = ((now_us // round_us) + 1) * round_us
    seconds_to_next_boundary = max(0.0, (next_boundary_us - now_us) / 1_000_000.0)
    boundary_fallback = seconds_to_next_boundary + 75.0
    return max(float(requested_seconds), boundary_fallback)
async def _probe_settlement_model(root: Path, seconds: float) -> int:
    initialize_project_databases(root)
    initial_market = await _discover(root, persist=True)
    started_us = now_epoch_us()
    max_seconds = _boundary_aware_probe_seconds(started_us, seconds)
    print("Collecting synchronized Phase-4 model inputs.")
    print(f"  minimum probe {seconds:g}s")
    print(f"  boundary cap  {max_seconds:.1f}s (only used if current-round PTB is unavailable)")
    print("  Coinbase      public BTC-USD")
    print("  Kraken        public BTC/USD")
    print("  official TWAP authenticated read-only BTC/USD 60s")
    print("  model         deterministic TWAP-aware Monte Carlo")
    print("  real trading  DISABLED")

    selected_market = None
    selected_features = None
    selected_result = None
    selected_decision_us = None
    last_reason: str | None = None

    with (
        ExternalStore.open_project(root) as coinbase_store,
        ExternalStore.open_project(root) as kraken_store,
        ExternalStore.open_project(root) as reference_store,
    ):
        coinbase = CoinbaseCollector(root=root, store=coinbase_store)
        kraken = KrakenCollector(root=root, store=kraken_store)
        reference = AuthenticatedTwapCollector(root=root, store=reference_store, market=initial_market)
        collectors = (coinbase, kraken, reference)
        tasks = [asyncio.create_task(collector.run()) for collector in collectors]
        deadline_us = started_us + int(max_seconds * 1_000_000)
        try:
            while now_epoch_us() < deadline_us:
                for task in tasks:
                    if task.done():
                        exc = task.exception()
                        if exc is not None:
                            raise exc
                        raise RuntimeError("a live input collector stopped unexpectedly during the model probe")

                market = await _discover(root, persist=True)
                decision_us = now_epoch_us()
                if market.round_start_us <= decision_us < market.round_end_us:
                    mapper = PriceReferenceMapper(reference_store)
                    mapper.map_existing_exact(market, decision_us)
                    engine = CausalBtcFeatureEngine(reference_store)
                    try:
                        features = engine.build(market=market, decision_time_us=decision_us)
                        result = TwapSettlementModel(
                            engine,
                            SettlementModelConfig(paths=10_000, volatility_lookback_seconds=60),
                        ).evaluate(market=market, decision_time_us=decision_us, features=features)
                    except FeatureUnavailable as exc:
                        reason = str(exc)
                        if reason != last_reason:
                            print(f"  waiting        {reason}")
                            if reason == SKIP_REASON:
                                print("                 current round-start TWAP is not yet causally available; staying connected")
                        last_reason = reason
                    else:
                        selected_market = market
                        selected_features = features
                        selected_result = result
                        selected_decision_us = decision_us
                        break
                await asyncio.sleep(2.0)
        finally:
            await asyncio.gather(*(collector.request_stop() for collector in collectors), return_exceptions=True)
            await asyncio.gather(*tasks, return_exceptions=True)

    if selected_result is None or selected_features is None or selected_market is None or selected_decision_us is None:
        print("Phase-4 model probe skipped.")
        print(f"  reason        {last_reason or SKIP_REASON}")
        print("  diagnostic    no currently-live round became evaluable before the boundary-aware probe cap")
        return 6

    market = selected_market
    features = selected_features
    result = selected_result
    decision_us = selected_decision_us
    print("\nPhase-4 live settlement probability probe")
    print(f"  slug          {market.slug}")
    print(f"  decision UTC  {epoch_us_to_iso(decision_us)}")
    print(f"  seconds left  {features.seconds_remaining:.3f}")
    print(f"  PTB           {result.price_to_beat:.12f}")
    print(f"  predictor BTC {result.predictor_price:.6f}")
    print(f"  official TWAP {result.official_twap:.12f}")
    print(f"  sigma / sqrt(s) {result.volatility_per_sqrt_second:.8f}")
    print(f"  P(UP) MC      {result.p_up:.6f}")
    print(f"  P(DOWN) MC    {result.p_down:.6f}")
    print(f"  P(UP) analytic {result.analytical_p_up:.6f}")
    print(f"  TWAP mean     {result.simulated_twap_mean:.6f}")
    print(f"  TWAP std      {result.simulated_twap_std:.6f}")
    print(f"  known/future  {result.known_final_window_samples}/{result.future_final_window_samples}")
    print(f"  deterministic seed {result.seed}")
    print(f"  approximation {result.approximation}")
    print("  action        NONE (Phase 4 estimates settlement probability only)")
    return 0


def cmd_probe_settlement_model(root: Path, seconds: float) -> int:
    if not os.environ.get(PRIVATE_KEY_ENV):
        print(f"ERROR: {PRIVATE_KEY_ENV} is not set in this PowerShell session.")
        return 4
    try:
        return asyncio.run(_probe_settlement_model(root, seconds))
    except KeyboardInterrupt:
        print("\nPhase-4 model probe stopped by Ctrl+C.")
        return 0


def cmd_reference_policy(root: Path) -> int:
    initialize_project_databases(root)
    policy = SettlementReferencePolicy()
    with ExternalStore.open_project(root) as store:
        set_reference_policy_status(store, policy)
    print("Official settlement-reference access policy")
    print(f"  source        {policy.source}")
    print(f"  stream        {policy.stream}")
    print(f"  state         {policy.state}")
    print(f"  auth allowed  {policy.authenticated_read_only_allowed}")
    print(f"  key available {bool(os.environ.get(PRIVATE_KEY_ENV))}")
    print(f"  reason        {policy.reason}")
    print("  real trading  DISABLED")
    print(f"  missing PTB   {SKIP_REASON}")
    return 0


def cmd_verify_phase2(root: Path) -> int:
    market_path = root / "data" / "v3_market.sqlite"
    if not market_path.exists():
        print("PHASE 2 VERIFY: FAIL - market database is missing")
        return 3
    with MarketStore(market_path) as store:
        health = store.current_health()
    details = health.collector_details or {}
    tokens = details.get("tokens", {})
    up = tokens.get("UP") or {}
    down = tokens.get("DOWN") or {}
    stream_snapshots = int(details.get("book_snapshots", 0) or 0)
    checks = {
        "market discovered": bool(health.slug),
        "Up token synchronized": up.get("synchronized") is True,
        "Down token synchronized": down.get("synchronized") is True,
        "stream full-book snapshots >= 2": stream_snapshots >= 2,
        "persisted full-book checkpoints >= 2": health.book_checkpoint_count >= 2,
    }
    print("BTC Lab V3 Phase-2 verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    passed_all = all(checks.values())
    print(f"  result  {'PASS' if passed_all else 'FAIL'}")
    return 0 if passed_all else 3


def cmd_verify_phase3(root: Path) -> int:
    external_path = root / "data" / "v3_external.sqlite"
    if not external_path.exists():
        print("PHASE 3 VERIFY: FAIL - external database is missing")
        return 3
    with ExternalStore(external_path) as store:
        health = store.current_health()
    cb = health.components.get("coinbase_btc_usd")
    kr = health.components.get("kraken_btc_usd")
    ref = health.components.get(REFERENCE_COMPONENT)
    checks = {
        "Coinbase public ticks collected": health.coinbase_count > 0,
        "Kraken public ticks collected": health.kraken_count > 0,
        "Coinbase collector not fatal": cb is not None and cb.state != "FATAL",
        "Kraken collector not fatal": kr is not None and kr.state != "FATAL",
        "official TWAP observations collected": health.settlement_observation_count > 0,
        "causal Price To Beat mapped": health.price_reference_count > 0,
        "reference collector not fatal": ref is not None and ref.state != "FATAL",
        "real trading remains disabled": ref is not None and ref.details.get("real_trading") == "DISABLED",
    }
    print("BTC Lab V3 Phase-3 verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    passed_all = all(checks.values())
    print(f"  result  {'PASS' if passed_all else 'FAIL'}")
    if passed_all:
        print("  note    Predictor feeds + authoritative causal TWAP/PTB are now available for Phase 4.")
    return 0 if passed_all else 3




def cmd_strategy_lab_plan() -> int:
    lab = StrategyLabConfig()
    cohorts = comparison_matrix(lab=lab)
    print("BTC Lab V3 Phase-5 Strategy & Feature Lab")
    print("  baseline strategies 7")
    print(f"  decision horizons  {list(lab.decision_horizons_seconds)}")
    print("  rule               compare strategies only within the same horizon")
    print("  parameters         STARTING VALUES; not claimed optimal")
    print("  regime features    diagnostic/research-only until Phase 9 validation")
    print("  ensemble           interface available; weights inactive until trained")
    print("  execution          NONE (Phase 6)")
    print("  real trading       DISABLED")
    print("\nFrozen baseline cohorts")
    for cohort in cohorts:
        horizon = cohort.strategies[0].common.decision_seconds_before_close
        print(f"  horizon={horizon:3d}s cohort_hash={config_hash(cohort)[:16]} strategies={len(cohort.strategies)}")
        for strategy in cohort.strategies:
            print(f"    {strategy.strategy_family.value:30s} {config_hash(strategy)[:16]}")
    print("\nPhase-9 candidate search spaces")
    for space in baseline_search_spaces():
        print(f"  {space.family.value:30s} dimensions={len(space.dimensions)}")
    return 0


def cmd_verify_phase5() -> int:
    lab = StrategyLabConfig()
    cohorts = comparison_matrix(lab=lab)
    checks = {
        "six separate decision-horizon cohorts": len(cohorts) == 6,
        "seven strategies per cohort": all(len(c.strategies) == 7 for c in cohorts),
        "same horizon within each comparison": all(
            len({s.common.decision_seconds_before_close for s in c.strategies}) == 1 for c in cohorts
        ),
        "distinct frozen strategy hashes within each cohort": all(
            len({config_hash(s) for s in c.strategies}) == 7 for c in cohorts
        ),
        "search spaces cover seven baseline families": len(baseline_search_spaces()) == 7,
    }
    print("BTC Lab V3 Phase-5 verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    Strategy/feature lab is research-ready; execution and real trading remain disabled.")
    return 0 if ok else 3



def cmd_watch_wallet(
    root: Path,
    *,
    wallet: str,
    seconds: float | None,
    poll_seconds: float,
    sample_seconds: float,
    prebuffer_seconds: int,
    model_paths: int,
) -> int:
    if not os.environ.get(PRIVATE_KEY_ENV):
        print(f"ERROR: {PRIVATE_KEY_ENV} is not set in this PowerShell session.")
        print("Use the provided secure PowerShell launcher; do not paste the key into chat.")
        return 4
    config = LiveForensicsConfig(
        poll_seconds=poll_seconds,
        sample_seconds=sample_seconds,
        prebuffer_seconds=prebuffer_seconds,
        model_paths=model_paths,
    )
    try:
        initialize_project_databases(root)
        return asyncio.run(
            run_live_wallet_forensics(
                root, requested_wallet=wallet, seconds=seconds, config=config
            )
        )
    except KeyboardInterrupt:
        print("\nLive wallet forensics stopped by Ctrl+C.")
        return 0
    except ValueError as exc:
        print(f"Live wallet forensics configuration error: {exc}")
        return 3



def cmd_verify_phase5_6() -> int:
    checks = verify_forward_lab_config()
    print("BTC Lab V3 Phase-5.6 verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    Strategies 8, 9A, and 9B are paper-forward only; real trading remains disabled.")
    return 0 if ok else 3


def cmd_verify_phase5_7() -> int:
    checks = verify_full_tournament_config()
    print("BTC Lab V3 Phase-5.7 full forward tournament verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    84 pre-registered S1-S7/control variants plus S8/9A/9B; paper only.")
    return 0 if ok else 3


def cmd_verify_phase5_9() -> int:
    checks = verify_phase590_config()
    print("BTC Lab V3 Phase-5.9 wallet-alpha verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    New wallet-copy arms are frozen prospectively; execution remains paper-only.")
    return 0 if ok else 3



def cmd_verify_phase5_9_1(root: Path) -> int:
    checks = dict(verify_phase590_config())
    try:
        with MarketStore.open_project(root) as store:
            profile = store.storage_profile()
            table_names = {
                str(row[0])
                for row in store.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            checks["market DB uses compact raw-reference storage"] = profile == "compact_raw_ref_v1"
            checks["storage health telemetry table present"] = "storage_health_snapshots" in table_names
            snapshot = store.capture_storage_health(root=root)
            checks["storage health snapshot writes successfully"] = snapshot.db_bytes >= 0 and snapshot.free_bytes > 0
            checks["disk safety reserve currently >=8 GiB"] = snapshot.free_bytes >= 8 * (1024 ** 3)
    except Exception as exc:
        checks[f"market storage verifier exception: {type(exc).__name__}"] = False

    print("BTC Lab V3 Phase-5.9.1 storage-hardened wallet-alpha verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    Full raw CLOB payloads are gzip archived; live SQLite keeps compact raw references.")
        print("  note    Wallet-alpha execution remains PAPER ONLY.")
    return 0 if ok else 3

def cmd_verify_phase5_9_2(root: Path) -> int:
    checks = dict(verify_phase590_config())
    try:
        with MarketStore.open_project(root) as store:
            profile = store.storage_profile()
            table_names = {
                str(row[0])
                for row in store.conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            checks["market DB uses compact raw-reference storage"] = profile == "compact_raw_ref_v1"
            checks["storage health telemetry table present"] = "storage_health_snapshots" in table_names
            snapshot = store.capture_storage_health(root=root)
            checks["storage health snapshot writes successfully"] = snapshot.db_bytes >= 0 and snapshot.free_bytes > 0
            checks["disk safety reserve currently >=8 GiB"] = snapshot.free_bytes >= 8 * (1024 ** 3)
    except Exception as exc:
        checks[f"market storage verifier exception: {type(exc).__name__}"] = False

    print("BTC Lab V3 Phase-5.9.2 evidence-routed wallet-alpha verification")
    for name, passed in checks.items():
        print(f"  {'PASS' if passed else 'FAIL':4s}  {name}")
    ok = all(checks.values())
    print(f"  result  {'PASS' if ok else 'FAIL'}")
    if ok:
        print("  note    Phase 5.9.1 controls retained; 5.9.2 challengers are frozen prospectively.")
        print("  note    Full raw CLOB payloads remain gzip archived; execution remains PAPER ONLY.")
    return 0 if ok else 3


def cmd_forward_wallet_lab(
    root: Path,
    *,
    wallet: str,
    seconds: float | None,
    trades_poll_ms: int,
    activity_poll_ms: int,
    copy_delay_ms: int,
    model_paths: int,
    full_tournament: bool = False,
    tournament_delay_ms: int = 100,
    strict_market_minimum: bool = True,
    s8_research_policy: str = 'DISABLED_V1',
) -> int:
    from .feeds.clock_health import require_host_sync
    try:require_host_sync()
    except RuntimeError as exc:
        print(str(exc));return 3
    if not os.environ.get(PRIVATE_KEY_ENV):
        print(f"ERROR: {PRIVATE_KEY_ENV} is not set in this PowerShell session.")
        print("Use the supplied secure launcher; the key is needed only for read-only official BTC TWAP.")
        return 4
    config = ForwardLabConfig(
        trades_poll_ms=trades_poll_ms,
        activity_poll_ms=activity_poll_ms,
        copy_execution_delay_ms=copy_delay_ms,
        model_paths=model_paths,
        enable_full_tournament=full_tournament,
        tournament_execution_delay_ms=tournament_delay_ms,
        strict_market_minimum=strict_market_minimum,
        s8_research_policy=s8_research_policy,
    )
    try:
        initialize_project_databases(root)
        return asyncio.run(run_forward_wallet_lab(root, requested_wallet=wallet, seconds=seconds, config=config))
    except KeyboardInterrupt:
        print("\nPhase 5.9.2 evidence-routed wallet-alpha forward lab stopped by Ctrl+C.")
        return 0
    except (ValueError, RuntimeError) as exc:
        print(f"Phase 5.9.2 evidence-routed wallet-alpha lab configuration/runtime error: {exc}")
        return 3











def cmd_research_tools(args) -> int:
    """Bounded immutable coverage inspection only; never infers missing fills."""
    from .replay.readers import inspect_native_window,read_forward_window
    try:
        if args.output.exists():raise ValueError('RESEARCH_OUTPUT_MUST_BE_NEW')
        result=inspect_native_window(args.database,start_us=args.start_us,end_us=args.end_us)
        if args.forward_zip is not None:
            result['forward_comparison_evidence']=read_forward_window(args.forward_zip,start_us=args.start_us,end_us=args.end_us)
        with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,sort_keys=True,indent=2,allow_nan=False)
        print('Offline coverage artifact: '+str(args.output));return 0
    except (ValueError,OSError,KeyError,TypeError,sqlite3.Error) as exc:
        print('OFFLINE_RESEARCH_ERROR: '+str(exc));return 3


def cmd_public_observation(args) -> int:
    """Future authorized five-minute observation; never opens any account/database."""
    if not args.authorize_paper_observation:
        print('EXPLICIT_PUBLIC_OBSERVATION_AUTHORIZATION_REQUIRED');return 3
    from .operations.observation import capture_public
    from .operations.public_feed import PublicSnapshotFeed
    async def launch():
        import httpx
        async with httpx.AsyncClient(timeout=3,follow_redirects=False) as client:
            feed=PublicSnapshotFeed(rest=PolymarketPublicRest(client=client),client=client,watch_wallet=args.watch_wallet)
            return await capture_public(feed,args.output,seconds=args.seconds,authorized=True)
    try:
        result=asyncio.run(launch());print(json.dumps(result,sort_keys=True,indent=2));return 0
    except (ValueError,OSError,KeyError,RuntimeError):
        print('PUBLIC_OBSERVATION_STOPPED: clock, storage or public input requires review');return 3




def cmd_offline_replay(args) -> int:
    """Explicit bounded immutable input, new output artifact, isolated scratch DBs."""
    from .replay.readers import read_jsonl
    from .replay.engine import replay
    try:
        if args.output.exists() or args.input.resolve()==args.output.resolve():
            raise ValueError('REPLAY_OUTPUT_MUST_BE_NEW')
        manifest,events=read_jsonl(args.input)
        definition=None
        if args.strategy_profile_json is not None:
            from dataclasses import replace
            if args.strategy_profile_json.stat().st_size>65536:raise ValueError('LAB_DEFINITION_LIMIT')
            definition=json.loads(args.strategy_profile_json.read_text(encoding='utf-8-sig'))
            manifest=replace(manifest,strategy_id='CONFIGURED_RESEARCH')
        result=replay(manifest,events,strategy_definition=definition)
        with args.output.open('x',encoding='utf-8') as stream:
            json.dump(result,stream,sort_keys=True,indent=2,allow_nan=False)
        print('PAPER replay: '+result['status']+'; '+str(len(result['fills']))+' shadow fills; '+str(args.output))
        return 0
    except (ValueError,OSError,KeyError,sqlite3.Error) as exc:
        print('OFFLINE_REPLAY_ERROR: '+str(exc));return 3


def cmd_model_calibration_report(args) -> int:
    """Offline caller-supplied JSON only; never opens a project database or feed."""
    from .models.calibration_diagnostics import calibration_report
    try:
        if args.input_json.stat().st_size > 10_000_000:
            raise ValueError('CALIBRATION_INPUT_TOO_LARGE')
        rows = json.loads(args.input_json.read_text(encoding='utf-8'))
        if not isinstance(rows, list):
            raise ValueError('CALIBRATION_SAMPLE_ARRAY_REQUIRED')
        result = calibration_report(rows, training_cutoff_us=args.training_cutoff_us,
            evaluation_end_us=args.evaluation_end_us, labels_cutoff_us=args.labels_cutoff_us)
        print(json.dumps(result, sort_keys=True, indent=2, allow_nan=False))
        return 0
    except (OSError, ValueError) as exc:
        print('MODEL_CALIBRATION_ERROR: ' + str(exc))
        return 3


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m btc_v3.cli",
        description="BTC/Polymarket V3 research and paper-trading laboratory",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path.cwd(),
        help="project root (default: current directory)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    observe=sub.add_parser('paper-observe-public',help='explicit bounded public capture; no funded account, signing or orders')
    observe.add_argument('--watch-wallet',required=True);observe.add_argument('--output',type=Path,required=True)
    observe.add_argument('--seconds',type=int,default=300);observe.add_argument('--authorize-paper-observation',action='store_true')
    native=sub.add_parser('replay-native-inspect',help='stopped, immutable, indexed five-minute source coverage; no inferred fills')
    native.add_argument('--database',type=Path,required=True);native.add_argument('--forward-zip',type=Path)
    for name in ('start-us','end-us'):native.add_argument('--'+name,type=int,required=True)
    native.add_argument('--output',type=Path,required=True)
    replay_parser=sub.add_parser('replay-jsonl',help='bounded causal PAPER replay; no network or production database writes')
    replay_parser.add_argument('--input',type=Path,required=True)
    replay_parser.add_argument('--output',type=Path,required=True)
    replay_parser.add_argument('--strategy-profile-json',type=Path,help='frozen exported strategy definition; isolated shadow research only')
    calibration = sub.add_parser('model-calibration-report', help='offline chronological market-level probability diagnostics; no promotion')
    calibration.add_argument('--input-json', type=Path, required=True)
    calibration.add_argument('--training-cutoff-us', type=int, required=True)
    calibration.add_argument('--evaluation-end-us', type=int, required=True)
    calibration.add_argument('--labels-cutoff-us', type=int, required=True)
    sub.add_parser("init", help="initialize/update project directories and SQLite schemas")
    sub.add_parser("status", help="show database, Polymarket, and external-feed health")
    sub.add_parser("verify-live-hotpath", help="verify indexed large-history live feature queries")
    sub.add_parser("discover-market", help="discover the current/nearest BTC Up/Down 5m market")
    sub.add_parser("reference-policy", help="show authenticated read-only TWAP policy")
    sub.add_parser("verify-phase2", help="verify that live full-L2 Phase-2 collection succeeded")
    sub.add_parser("verify-phase3", help="verify predictor feeds plus authoritative TWAP/PTB")
    sub.add_parser("strategy-lab-plan", help="show Phase-5 horizons, frozen cohorts, and optimization search spaces")
    sub.add_parser("verify-phase5", help="verify the Phase-5 strategy and feature laboratory")
    sub.add_parser("verify-phase5-6", help="verify the Phase-5.6 wallet-inferred/copy forward lab")
    sub.add_parser("verify-phase5-7", help="verify the Phase-5.7 full live forward tournament")
    sub.add_parser("verify-phase5-9", help="verify Phase-5.9 wallet-alpha strategies and live-tester invariants")
    sub.add_parser("verify-phase5-9-1", help="verify Phase-5.9.1 compact market storage + wallet-alpha invariants")
    sub.add_parser("verify-phase5-9-2", help="verify Phase-5.9.2 evidence-routed wallet-alpha + storage invariants")
    sub.add_parser("wallet-watchlist", help="show the multi-wallet research watchlist")
    wallet_add = sub.add_parser("wallet-add", help="add/enable a wallet in the forensic tournament watchlist")
    wallet_add.add_argument("--wallet", required=True)
    wallet_add.add_argument("--label", default=None)
    wallet_disable = sub.add_parser("wallet-disable", help="disable a wallet without deleting its stored research data")
    wallet_disable.add_argument("--wallet", required=True)
    wallet_enable = sub.add_parser("wallet-enable", help="re-enable an existing watchlist wallet")
    wallet_enable.add_argument("--wallet", required=True)
    tournament = sub.add_parser("wallet-tournament", help="sync enabled wallets and rank raw PnL, copyability, and inference usefulness separately")
    tournament.add_argument("--max-pages", type=int, default=4, help="500-row pages per feed per wallet; use 0 for full available history")
    watch_many = sub.add_parser("watch-wallets", help="watch every enabled wallet against one shared live BTC-5m telemetry stream")
    watch_many.add_argument("--seconds", type=float, default=None, help="stop after N seconds; omit to run until Ctrl+C")
    watch_many.add_argument("--poll-seconds", type=float, default=2.0, help="public wallet tournament poll-cycle interval")
    watch_many.add_argument("--sample-seconds", type=float, default=1.0, help="shared causal telemetry sample interval")
    watch_many.add_argument("--prebuffer-seconds", type=int, default=120)
    watch_many.add_argument("--model-paths", type=int, default=2000)

    wallet = sub.add_parser(
        "analyze-wallet",
        help="download public Polymarket wallet history and build BTC-5m behavioral forensics",
    )
    wallet.add_argument(
        "--wallet",
        default=TARGET_WALLET_98EUF98A,
        help="public wallet/proxy address (defaults to the 98euf98a research target)",
    )
    wallet.add_argument("--label", default="98euf98a", help="research label stored with the wallet")
    wallet.add_argument(
        "--max-pages",
        type=int,
        default=8,
        help="max 500-row pages per feed; use 0 for full available history",
    )

    watch = sub.add_parser(
        "watch-wallet",
        help="continuously capture live market telemetry around a public target wallet's BTC-5m trades",
    )
    watch.add_argument("--wallet", default=TARGET_WALLET_98EUF98A)
    watch.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="stop after N seconds; omit to run until Ctrl+C",
    )
    watch.add_argument("--poll-seconds", type=float, default=1.0, help="public wallet trade poll interval")
    watch.add_argument("--sample-seconds", type=float, default=1.0, help="causal telemetry sample interval")
    watch.add_argument("--prebuffer-seconds", type=int, default=120, help="rolling pre-trade sample buffer")
    watch.add_argument("--model-paths", type=int, default=2000, help="Monte Carlo paths per live telemetry sample")

    latency = sub.add_parser(
        "wallet-latency-lab",
        help="measure BTC/ETH 5m target-wallet public first-seen latency against locally received CLOB trade events",
    )
    latency.add_argument("--wallet", default=TARGET_WALLET_98EUF98A)
    latency.add_argument("--seconds", type=float, default=None, help="stop after N seconds; omit to run until Ctrl+C")
    latency.add_argument("--poll-ms", type=int, default=100, help="requested public wallet poll cadence in milliseconds")
    latency.add_argument("--telemetry-ms", type=int, default=1000, help="derived telemetry snapshot cadence; raw feeds remain event-level")
    latency.add_argument("--model-paths", type=int, default=500)
    latency.add_argument("--min-verdict-samples", type=int, default=20)

    latency_report_parser = sub.add_parser(
        "wallet-latency-report",
        help="summarize accumulated correlated target-wallet publication latency",
    )
    latency_report_parser.add_argument("--wallet", default=TARGET_WALLET_98EUF98A)
    latency_report_parser.add_argument("--min-samples", type=int, default=20)

    forward = sub.add_parser(
        "forward-wallet-lab",
        help="paper-forward Strategy 8 paired-grid plus Strategy 9A/9B target-wallet mirror",
    )
    forward.add_argument("--wallet", default=TARGET_WALLET_98EUF98A)
    forward.add_argument("--seconds", type=float, default=None, help="stop after N seconds; omit to run until Ctrl+C")
    forward.add_argument("--trades-poll-ms", type=int, default=100, help="/v2/trades attribution poll cadence")
    forward.add_argument("--activity-poll-ms", type=int, default=250, help="/v2/activity attribution poll cadence")
    forward.add_argument("--copy-delay-ms", type=int, default=25, help="local paper reaction delay after first public attribution")
    forward.add_argument("--model-paths", type=int, default=500)
    forward.add_argument("--full-tournament", action="store_true", help="also run S1-S7 BASE + wallet-informed variants + controls")
    forward.add_argument("--tournament-delay-ms", type=int, default=100, help="local paper execution delay for S1-S7 tournament variants")
    forward.add_argument("--strict-market-minimum", action="store_true", default=True,
                         help="enforce market minimums and ticks (always enabled for forward paper runs)")
    forward.add_argument('--s8-research-policy',choices=['DISABLED_V1','LEGACY_GRID_V1'],default='DISABLED_V1',
        help='S8 is excluded by default; LEGACY_GRID_V1 explicitly opts into unchanged independent maker research')

    chain = sub.add_parser(
        "wallet-onchain-check",
        help="optional Polygon chain cross-check via Etherscan API V2 (requires ETHERSCAN_API_KEY)",
    )
    chain.add_argument("--wallet", default=TARGET_WALLET_98EUF98A)
    chain.add_argument("--pages", type=int, default=1, help="10,000-row pages per on-chain endpoint")
    collect = sub.add_parser(
        "collect-polymarket",
        help="collect public Polymarket full-L2 data for the discovered BTC 5m market",
    )
    collect.add_argument(
        "--seconds",
        type=float,
        default=None,
        help="stop after N seconds; omit to run until Ctrl+C",
    )
    external = sub.add_parser(
        "collect-external",
        help="collect public Coinbase/Kraken BTC predictor feeds",
    )
    external.add_argument("--seconds", type=float, default=25.0, help="probe duration in seconds")
    reference = sub.add_parser(
        "collect-reference",
        help="collect authenticated read-only official BTC/USD 60-second TWAP and map causal PTB",
    )
    reference.add_argument("--seconds", type=float, default=25.0, help="probe duration in seconds")
    model_probe = sub.add_parser(
        "probe-settlement-model",
        help="collect synchronized live inputs and run the Phase-4 TWAP-aware settlement model",
    )
    model_probe.add_argument("--seconds", type=float, default=75.0, help="input collection duration in seconds")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    root = args.root.resolve()
    if args.command=='paper-observe-public':return cmd_public_observation(args)
    if args.command=='replay-native-inspect':return cmd_research_tools(args)
    if args.command == 'replay-jsonl':
        return cmd_offline_replay(args)
    if args.command == 'model-calibration-report':
        return cmd_model_calibration_report(args)
    if args.command == "init":
        return cmd_init(root)
    if args.command == "status":
        return cmd_status(root)
    if args.command == "verify-live-hotpath":
        return cmd_verify_live_hotpath(root)
    if args.command == "discover-market":
        return cmd_discover_market(root)
    if args.command == "reference-policy":
        return cmd_reference_policy(root)
    if args.command == "verify-phase2":
        return cmd_verify_phase2(root)
    if args.command == "verify-phase3":
        return cmd_verify_phase3(root)
    if args.command == "strategy-lab-plan":
        return cmd_strategy_lab_plan()
    if args.command == "verify-phase5":
        return cmd_verify_phase5()
    if args.command == "verify-phase5-6":
        return cmd_verify_phase5_6()
    if args.command == "verify-phase5-7":
        return cmd_verify_phase5_7()
    if args.command == "verify-phase5-9":
        return cmd_verify_phase5_9()
    if args.command == "verify-phase5-9-1":
        return cmd_verify_phase5_9_1(root)
    if args.command == "verify-phase5-9-2":
        return cmd_verify_phase5_9_2(root)
    if args.command == "wallet-watchlist":
        return cmd_wallet_watchlist(root)
    if args.command == "wallet-add":
        return cmd_wallet_add(root, wallet=args.wallet, label=args.label)
    if args.command == "wallet-disable":
        return cmd_wallet_set_enabled(root, wallet=args.wallet, enabled=False)
    if args.command == "wallet-enable":
        return cmd_wallet_set_enabled(root, wallet=args.wallet, enabled=True)
    if args.command == "wallet-tournament":
        if args.max_pages < 0:
            parser.error("--max-pages must be >= 0")
        return cmd_wallet_tournament(root, max_pages=None if args.max_pages == 0 else args.max_pages)
    if args.command == "watch-wallets":
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive when provided")
        entries = [row for row in WalletWatchlist.open_project(root).load() if row.enabled]
        if not entries:
            print("No enabled wallets in research/wallet_watchlist.json")
            return 3
        config = LiveForensicsConfig(
            poll_seconds=args.poll_seconds,
            sample_seconds=args.sample_seconds,
            prebuffer_seconds=args.prebuffer_seconds,
            model_paths=args.model_paths,
        )
        try:
            return asyncio.run(
                run_live_wallet_tournament(
                    root,
                    requested_wallets=[row.wallet for row in entries],
                    seconds=args.seconds,
                    config=config,
                )
            )
        except KeyboardInterrupt:
            print("\nLive multi-wallet forensics stopped by Ctrl+C.")
            return 0
        except (ValueError, RuntimeError) as exc:
            print(f"Live multi-wallet forensics configuration/runtime error: {exc}")
            return 3

    if args.command == "analyze-wallet":
        if args.max_pages < 0:
            parser.error("--max-pages must be >= 0")
        return cmd_analyze_wallet(
            root,
            wallet=args.wallet,
            label=args.label,
            max_pages=None if args.max_pages == 0 else args.max_pages,
        )

    if args.command == "watch-wallet":
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive when provided")
        return cmd_watch_wallet(
            root,
            wallet=args.wallet,
            seconds=args.seconds,
            poll_seconds=args.poll_seconds,
            sample_seconds=args.sample_seconds,
            prebuffer_seconds=args.prebuffer_seconds,
            model_paths=args.model_paths,
        )

    if args.command == "wallet-latency-lab":
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive when provided")
        config = LatencyLabConfig(
            poll_ms=args.poll_ms,
            telemetry_ms=args.telemetry_ms,
            model_paths=args.model_paths,
            min_verdict_samples=args.min_verdict_samples,
        )
        try:
            return asyncio.run(
                run_wallet_latency_lab(
                    root,
                    requested_wallet=args.wallet,
                    seconds=args.seconds,
                    config=config,
                )
            )
        except KeyboardInterrupt:
            print("\nTarget-wallet latency lab stopped by Ctrl+C.")
            return 0
        except (ValueError, RuntimeError) as exc:
            print(f"Wallet latency lab configuration/runtime error: {exc}")
            return 3

    if args.command == "forward-wallet-lab":
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive when provided")
        return cmd_forward_wallet_lab(
            root,
            wallet=args.wallet,
            seconds=args.seconds,
            trades_poll_ms=args.trades_poll_ms,
            activity_poll_ms=args.activity_poll_ms,
            copy_delay_ms=args.copy_delay_ms,
            model_paths=args.model_paths,
            full_tournament=args.full_tournament,
            tournament_delay_ms=args.tournament_delay_ms,
            strict_market_minimum=args.strict_market_minimum,
            s8_research_policy=args.s8_research_policy,
        )

    if args.command == "wallet-latency-report":
        if args.min_samples <= 0:
            parser.error("--min-samples must be positive")
        verdict = latency_report(root, args.wallet, min_samples=args.min_samples)
        print("BTC Lab V3 Target-Wallet Latency Report")
        print(f"  wallet             {args.wallet.lower()}")
        print(f"  correlated events  {verdict.sample_count}")
        print(f"  market->wallet p50 {('-' if verdict.p50_ms is None else f'{verdict.p50_ms:.1f} ms')}")
        print(f"  market->wallet p90 {('-' if verdict.p90_ms is None else f'{verdict.p90_ms:.1f} ms')}")
        print(f"  market->wallet p95 {('-' if verdict.p95_ms is None else f'{verdict.p95_ms:.1f} ms')}")
        print(f"  <=200ms fraction   {('-' if verdict.under_200ms_fraction is None else f'{100*verdict.under_200ms_fraction:.1f}%')}")
        print(f"  verdict            {verdict.verdict}")
        print("  rule               CONSISTENT_SUB_200MS requires p90 <= 200 ms and enough samples")
        return 0

    if args.command == "wallet-onchain-check":
        if args.pages <= 0:
            parser.error("--pages must be positive")
        return cmd_wallet_onchain_check(root, wallet=args.wallet, pages=args.pages)
    if args.command == "collect-polymarket":
        if args.seconds is not None and args.seconds <= 0:
            parser.error("--seconds must be positive")
        return cmd_collect_polymarket(root, args.seconds)
    if args.command == "collect-external":
        if args.seconds <= 0:
            parser.error("--seconds must be positive")
        return cmd_collect_external(root, args.seconds)
    if args.command == "collect-reference":
        if args.seconds <= 0:
            parser.error("--seconds must be positive")
        return cmd_collect_reference(root, args.seconds)
    if args.command == "probe-settlement-model":
        if args.seconds <= 0:
            parser.error("--seconds must be positive")
        return cmd_probe_settlement_model(root, args.seconds)
    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
