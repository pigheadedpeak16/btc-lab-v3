# Security and privacy

BTC LAB is a **local research application**. Keep its dashboard on loopback. The public GitHub Pages site serves static documentation and screenshots; it has no access to your local files, collectors, strategy database or wallet.

Never commit `.env` files, private keys, API tokens, authenticated logs, personal datasets or raw archives. The offline demo requires none of them. Optional reference authentication is read-only and separate from the demo; do not paste credentials into a browser form, issue or chat. Keep credentials scoped to an authorized process. Real-money order execution is not implemented.

Report suspected vulnerabilities through the repository's private security-reporting feature if available. If it is unavailable, open an issue requesting a private reporting channel without including sensitive details or exploit payloads. Do not publish credentials or private data as proof. Rotate any exposed credential through its provider.

Security checks and tests are bounded evidence, not a guarantee that every upstream library or deployment is secure. Do not expose the local server to untrusted users or assume a replay file is authenticated merely because it passes format validation.
