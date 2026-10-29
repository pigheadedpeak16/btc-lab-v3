from __future__ import annotations

import asyncio
import os
from pathlib import Path

from ..storage.db import initialize_project_databases
from .analysis import analyze_wallet_rows
from .client import PolymarketDataClient, PublicDataApiError
from .onchain import PolygonOnchainClient, OnchainApiError
from .store import ForensicsStore

TARGET_WALLET_98EUF98A = "0xdF4c6a942bd95Bf903D6066B4BA7051E6F914F22"
ETHERSCAN_KEY_ENV = "ETHERSCAN_API_KEY"


def _pct(value: float) -> str:
    return f"{value * 100.0:.1f}%"


async def _sync_wallet(
    root: Path,
    *,
    requested_address: str,
    label: str | None,
    max_pages: int | None,
) -> tuple[str, object, Path]:
    initialize_project_databases(root)
    async with PolymarketDataClient() as client:
        proxy_wallet, gamma_profile = await client.resolve_proxy_wallet(requested_address)
        print("Public wallet source")
        print(f"  requested      {requested_address.lower()}")
        print(f"  proxy wallet   {proxy_wallet}")
        print("  Polymarket API public/read-only; no wallet credentials required")
        print(f"  history pages  {'FULL' if max_pages is None else max_pages}")

        stats_task = asyncio.create_task(client.user_stats(proxy_wallet))
        volume_task = asyncio.create_task(client.user_volume(proxy_wallet, start=1, end=0))
        pnl_task = asyncio.create_task(client.user_pnl(proxy_wallet))
        activity_task = asyncio.create_task(
            client.activities(proxy_wallet, start=1, end=0, limit=500, max_pages=max_pages)
        )
        trades_task = asyncio.create_task(
            client.trades(
                proxy_wallet,
                start=1,
                end=0,
                limit=500,
                max_pages=max_pages,
                include_maker_rows=True,
            )
        )
        profile_stats, volume_stats, pnl_stats, activities, trades = await asyncio.gather(
            stats_task, volume_task, pnl_task, activity_task, trades_task
        )

    with ForensicsStore.open_project(root) as store:
        store.save_profile(
            proxy_wallet,
            label=label,
            requested_address=requested_address,
            profile_stats=profile_stats,
            volume_stats=volume_stats,
            pnl_stats=pnl_stats,
            gamma_profile=gamma_profile,
        )
        new_activities = store.save_activities(activities)
        new_trades = store.save_trades(trades)
        all_activities = store.load_activities(proxy_wallet)
        all_trades = store.load_trades(proxy_wallet)
        report = analyze_wallet_rows(proxy_wallet, all_activities, all_trades)
        report_id = store.save_report(report)
        output_dir = store.export(root, report)

    print("\nIngestion")
    print(f"  activity rows fetched  {len(activities)}")
    print(f"  activity rows new      {new_activities}")
    print(f"  trade rows fetched     {len(trades)}")
    print(f"  trade rows new         {new_trades}")
    print(f"  report id              {report_id}")
    return proxy_wallet, report, output_dir


def _print_report(report, output_dir: Path) -> None:
    rounds = report.btc5m_rounds
    both_ratio = report.both_side_rounds / rounds if rounds else 0.0
    one_sided = report.up_only_rounds + report.down_only_rounds
    one_ratio = one_sided / rounds if rounds else 0.0
    final60_ratio = report.final_60s_trade_actions / report.btc5m_trade_actions if report.btc5m_trade_actions else 0.0
    sell_ratio = report.rounds_with_sell / rounds if rounds else 0.0

    print("\nBTC 5-minute wallet forensics")
    print(f"  wallet                  {report.wallet}")
    print(f"  stored activity rows    {report.source_activity_rows}")
    print(f"  stored trade rows       {report.source_trade_rows}")
    print(f"  BTC5m activity rows     {report.btc5m_activity_rows}")
    print(f"  BTC5m trade actions     {report.btc5m_trade_actions}")
    print(f"  BTC5m rounds mapped     {rounds}")
    print(f"  UP-only rounds          {report.up_only_rounds}")
    print(f"  DOWN-only rounds        {report.down_only_rounds}")
    print(f"  both-side rounds        {report.both_side_rounds} ({_pct(both_ratio)})")
    print(f"  one-sided rounds        {one_sided} ({_pct(one_ratio)})")
    print(f"  rounds with SELL        {report.rounds_with_sell} ({_pct(sell_ratio)})")
    print(f"  final-60s trade actions {report.final_60s_trade_actions} ({_pct(final60_ratio)})")
    if report.median_trade_price is not None:
        print(f"  median trade price      {report.median_trade_price:.4f}")
    if report.median_trade_size is not None:
        print(f"  median trade size       {report.median_trade_size:.4f} shares")

    print("\nHypothesis diagnostics (descriptive, not proof of strategy)")
    for name, evidence in report.hypothesis_evidence.items():
        value = evidence.get("value")
        value_text = "-" if value is None else f"{float(value):.4f}"
        print(f"  {name:26s} {evidence.get('strength','-'):12s} metric={value_text}")

    if report.timing_bands:
        print("\nTrade timing")
        for name, count in report.timing_bands.items():
            print(f"  {name:12s} {count}")
    if report.price_bands:
        print("\nTrade price bands")
        for name, count in report.price_bands.items():
            print(f"  {name:12s} {count}")

    print("\nLimits of this inference")
    for line in report.limitations:
        print(f"  - {line}")
    print(f"\nExported report: {output_dir}")


def cmd_analyze_wallet(
    root: Path,
    *,
    wallet: str,
    label: str | None,
    max_pages: int | None,
) -> int:
    try:
        _, report, output_dir = asyncio.run(
            _sync_wallet(root, requested_address=wallet, label=label, max_pages=max_pages)
        )
    except (PublicDataApiError, ValueError) as exc:
        print(f"Wallet forensics failed: {exc}")
        return 3
    _print_report(report, output_dir)
    return 0


async def _onchain_probe(root: Path, wallet: str, pages: int) -> tuple[int, int, int]:
    key = os.environ.get(ETHERSCAN_KEY_ENV, "").strip()
    if not key:
        raise ValueError(
            f"{ETHERSCAN_KEY_ENV} is not set. The PolygonScan/Etherscan cross-check is optional; "
            "the Polymarket wallet analysis does not need it."
        )
    initialize_project_databases(root)
    normal: list[dict] = []
    erc20: list[dict] = []
    erc1155: list[dict] = []
    async with PolygonOnchainClient(key) as client:
        for page in range(1, pages + 1):
            normal.extend(await client.normal_transactions(wallet, page=page))
            erc20.extend(await client.erc20_transfers(wallet, page=page))
            erc1155.extend(await client.erc1155_transfers(wallet, page=page))
            if len(normal) < page * 10_000 and len(erc20) < page * 10_000 and len(erc1155) < page * 10_000:
                break
    with ForensicsStore.open_project(root) as store:
        store.save_onchain_events(wallet, "txlist", normal)
        store.save_onchain_events(wallet, "tokentx", erc20)
        store.save_onchain_events(wallet, "token1155tx", erc1155)
    return len(normal), len(erc20), len(erc1155)


def cmd_wallet_onchain_check(root: Path, *, wallet: str, pages: int) -> int:
    try:
        normal, erc20, erc1155 = asyncio.run(_onchain_probe(root, wallet, pages))
    except (ValueError, OnchainApiError, PublicDataApiError) as exc:
        print(f"Polygon on-chain cross-check unavailable: {exc}")
        return 3
    print("Polygon on-chain cross-check")
    print(f"  wallet              {wallet.lower()}")
    print(f"  normal tx rows      {normal}")
    print(f"  ERC-20 transfer rows {erc20}")
    print(f"  ERC-1155 rows       {erc1155}")
    print("  source              Etherscan API V2, chainid=137 (Polygon)")
    print("  purpose             verification only; not sufficient to infer hidden orders")
    return 0


def cmd_wallet_watchlist(root: Path) -> int:
    from .watchlist import WalletWatchlist

    watchlist = WalletWatchlist.open_project(root)
    entries = watchlist.load()
    print("BTC Lab V3 wallet watchlist")
    print(f"  file {watchlist.path}")
    if not entries:
        print("  (empty)")
        return 0
    for row in entries:
        print(f"  {'ON ' if row.enabled else 'OFF'} {row.wallet}  label={row.label}")
    return 0


def cmd_wallet_add(root: Path, *, wallet: str, label: str | None) -> int:
    from .watchlist import WalletWatchlist

    try:
        entry = WalletWatchlist.open_project(root).add(wallet, label)
    except ValueError as exc:
        print(f"Watchlist update failed: {exc}")
        return 3
    print("Wallet added/enabled")
    print(f"  wallet {entry.wallet}")
    print(f"  label  {entry.label}")
    return 0


def cmd_wallet_set_enabled(root: Path, *, wallet: str, enabled: bool) -> int:
    from .watchlist import WalletWatchlist

    try:
        entry = WalletWatchlist.open_project(root).set_enabled(wallet, enabled)
    except ValueError as exc:
        print(f"Watchlist update failed: {exc}")
        return 3
    print("Wallet watch state updated")
    print(f"  wallet  {entry.wallet}")
    print(f"  enabled {entry.enabled}")
    return 0


async def _run_wallet_tournament(root: Path, *, max_pages: int | None) -> tuple[dict, Path]:
    import uuid

    from .tournament import build_tournament_row, tournament_manifest
    from .watchlist import WalletWatchlist

    watchlist = WalletWatchlist.open_project(root)
    entries = [row for row in watchlist.load() if row.enabled]
    if not entries:
        raise ValueError("wallet watchlist has no enabled wallets")

    print("BTC Lab V3 Wallet Tournament")
    print(f"  enabled wallets {len(entries)}")
    print(f"  history pages   {'FULL' if max_pages is None else max_pages}")
    print("  source          Polymarket Data API v2, public/read-only")
    print("  scores          transparent research heuristics; not profit guarantees")

    rows = []
    for index, entry in enumerate(entries, start=1):
        print(f"\n[{index}/{len(entries)}] syncing {entry.label} {entry.wallet}")
        try:
            proxy_wallet, report, _out = await _sync_wallet(
                root,
                requested_address=entry.wallet,
                label=entry.label,
                max_pages=max_pages,
            )
        except (PublicDataApiError, ValueError) as exc:
            print(f"  SKIP wallet sync failed: {exc}")
            continue
        with ForensicsStore.open_project(root) as store:
            stored_label, profile_stats, volume_stats = store.load_wallet_metadata(proxy_wallet)
            live_events = store.live_events_for_wallet(proxy_wallet)
            copy_curve = store.copy_curve_for_wallet(proxy_wallet)
        rows.append(
            build_tournament_row(
                wallet=proxy_wallet,
                label=stored_label or entry.label,
                report=report,
                profile_stats=profile_stats,
                volume_stats=volume_stats,
                live_events=live_events,
                copy_curve=copy_curve,
            )
        )

    if not rows:
        raise ValueError("no wallet could be synchronized")
    manifest = tournament_manifest(rows)
    tournament_id = f"wallet_tournament_{str(uuid.uuid4())[:8]}"
    with ForensicsStore.open_project(root) as store:
        store.save_tournament(
            tournament_id=tournament_id,
            config={"max_pages": max_pages, "wallet_count": len(entries)},
            manifest=manifest,
        )
        output = store.export_tournament(root, tournament_id=tournament_id, manifest=manifest)
    return manifest, output


def _fmt_num(value: float | None, decimals: int = 2) -> str:
    return "-" if value is None else f"{value:.{decimals}f}"


def cmd_wallet_tournament(root: Path, *, max_pages: int | None) -> int:
    try:
        manifest, output = asyncio.run(_run_wallet_tournament(root, max_pages=max_pages))
    except (ValueError, PublicDataApiError) as exc:
        print(f"Wallet tournament failed: {exc}")
        return 3

    rows = manifest["wallets"]
    print("\nWallet tournament summary")
    print("  raw PnL, copyability, and inference usefulness are separate rankings")
    header = f"{'label':16s} {'rawPnL':>12s} {'BTC5m':>7s} {'liveN':>6s} {'copy':>7s} {'infer':>7s} {'lag50':>8s} {'drift50':>8s}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for row in rows:
        print(
            f"  {str(row['label'])[:16]:16s} "
            f"{_fmt_num(row['all_time_pnl'], 0):>12s} "
            f"{int(row['btc5m_rounds']):7d} "
            f"{int(row['live_target_events']):6d} "
            f"{_fmt_num(row['copyability_score'], 1):>7s} "
            f"{_fmt_num(row['inference_usefulness_score'], 1):>7s} "
            f"{_fmt_num(row['median_observable_lag_seconds'], 2):>8s} "
            f"{_fmt_num(row['median_abs_copy_price_delta'], 3):>8s}"
        )
        curve = row.get("copy_curve") or {}
        if curve:
            parts = []
            for delay, values in sorted(curve.items(), key=lambda item: int(item[0])):
                parts.append(
                    f"{delay}ms:n={values.get('observations',0)},d50={_fmt_num(values.get('median_abs_price_delta'),3)}"
                )
            print("    copy curve " + " | ".join(parts))

    print("\nRankings")
    by_wallet = {row["wallet"]: row["label"] for row in rows}
    for name, wallets in manifest["rankings"].items():
        labels = [by_wallet.get(wallet, wallet[:10]) for wallet in wallets]
        print(f"  {name:30s} {' > '.join(labels) if labels else '-'}")
    print(f"\nExported tournament: {output}")
    print("NOTE: copyability remains NO_LIVE_EVENTS/PROVISIONAL until the live watcher captures enough target trades.")
    return 0
