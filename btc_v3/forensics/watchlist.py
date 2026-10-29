from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path

ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
DEFAULT_TARGET_WALLET = "0xdF4c6a942bd95Bf903D6066B4BA7051E6F914F22"


@dataclass(frozen=True)
class WalletWatchEntry:
    wallet: str
    label: str
    enabled: bool = True

    def normalized(self) -> "WalletWatchEntry":
        wallet = self.wallet.strip().lower()
        if not ADDRESS_RE.fullmatch(wallet):
            raise ValueError(f"invalid EVM wallet address: {self.wallet}")
        label = self.label.strip() or wallet[:10]
        return WalletWatchEntry(wallet=wallet, label=label, enabled=bool(self.enabled))


class WalletWatchlist:
    def __init__(self, path: Path) -> None:
        self.path = path

    @classmethod
    def open_project(cls, root: Path) -> "WalletWatchlist":
        return cls(root / "research" / "wallet_watchlist.json")

    def ensure_default(self) -> None:
        if self.path.exists():
            return
        self.save([WalletWatchEntry(DEFAULT_TARGET_WALLET, "98euf98a", True)])

    def load(self) -> list[WalletWatchEntry]:
        self.ensure_default()
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        rows = payload.get("wallets", payload if isinstance(payload, list) else [])
        result: list[WalletWatchEntry] = []
        seen: set[str] = set()
        for row in rows:
            entry = WalletWatchEntry(
                wallet=str(row["wallet"]),
                label=str(row.get("label") or str(row["wallet"])[:10]),
                enabled=bool(row.get("enabled", True)),
            ).normalized()
            if entry.wallet in seen:
                continue
            seen.add(entry.wallet)
            result.append(entry)
        return result

    def save(self, entries: list[WalletWatchEntry]) -> None:
        normalized: list[WalletWatchEntry] = []
        seen: set[str] = set()
        for row in entries:
            entry = row.normalized()
            if entry.wallet in seen:
                continue
            seen.add(entry.wallet)
            normalized.append(entry)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": 1,
            "wallets": [asdict(row) for row in normalized],
        }
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def add(self, wallet: str, label: str | None = None) -> WalletWatchEntry:
        entry = WalletWatchEntry(wallet, label or wallet[:10], True).normalized()
        entries = self.load()
        replaced = False
        output: list[WalletWatchEntry] = []
        for row in entries:
            if row.wallet == entry.wallet:
                output.append(entry)
                replaced = True
            else:
                output.append(row)
        if not replaced:
            output.append(entry)
        self.save(output)
        return entry

    def set_enabled(self, wallet: str, enabled: bool) -> WalletWatchEntry:
        target = WalletWatchEntry(wallet, wallet[:10], enabled).normalized().wallet
        entries = self.load()
        output: list[WalletWatchEntry] = []
        found: WalletWatchEntry | None = None
        for row in entries:
            if row.wallet == target:
                found = WalletWatchEntry(row.wallet, row.label, enabled)
                output.append(found)
            else:
                output.append(row)
        if found is None:
            raise ValueError(f"wallet is not in watchlist: {target}")
        self.save(output)
        return found
