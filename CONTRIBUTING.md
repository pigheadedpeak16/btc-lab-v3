# Contributing and development

This is a public research project **without a reuse license**. Discuss proposed contributions in an issue before sending code; public visibility does not grant a license. Do not include private datasets or credentials in an issue or pull request.

Install with `python -m pip install -e ".[dev]"` in a Python 3.11+ virtual environment. Run `python -B -m pytest tests -q -o addopts='' -p no:cacheprovider`. The test suite uses isolated fixtures; a change must not start collectors, initialize a funded account or touch historical data during tests.

Keep fixes focused and include a failing regression for behavioral defects. Preserve frozen BOOK_CONFIRM_75 definitions and existing historical results. Record an execution-policy revision when changing fill eligibility or timing. Validate source and receipt cutoffs, expiry at actual execution, depth/fee/minimum rules and settlement immutability when touching execution.

Use the synthetic example for reproducible reports. State the OS, Python version, command, expected behavior and sanitized error code. Missing live-world evidence must be distinguished from an engineering failure. A fixture or simulated win does not demonstrate an edge.

Website files live in `docs/`; all assets are local and paths are relative for the GitHub Pages project URL. Check mobile layout, keyboard focus, local links, copy buttons and operating-system quick-start tabs after editing the site. Product screenshots must be labeled synthetic unless their actual provenance has been approved for publication.
