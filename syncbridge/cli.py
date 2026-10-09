"""CLI: ``syncbridge contacts --src-url … --dst-url … --state sync.db``

Secrets come from environment variables (SRC_TOKEN, DST_TOKEN) — never from
the command line, so they do not end up in shell history or process lists.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from . import __version__
from .http import ApiClient
from .state import State
from .sync import sync_contacts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="syncbridge", description="Sync records between two REST APIs")
    p.add_argument("--version", action="version", version=f"syncbridge {__version__}")
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("contacts", help="CRM customers -> mailing-list subscribers")
    c.add_argument("--src-url", required=True)
    c.add_argument("--dst-url", required=True)
    c.add_argument("--state", type=Path, default=Path("sync-state.db"))
    c.add_argument("--rate", type=float, default=5.0, help="max requests/second per API")
    c.add_argument("--dry-run", action="store_true")
    c.add_argument("--json", action="store_true", help="print summary as JSON")
    c.add_argument("-v", "--verbose", action="store_true")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s")

    src = ApiClient(args.src_url, {"Authorization": f"Bearer {os.getenv('SRC_TOKEN', '')}"}, rate_per_sec=args.rate)
    dst = ApiClient(args.dst_url, {"Authorization": f"Bearer {os.getenv('DST_TOKEN', '')}"}, rate_per_sec=args.rate)
    state = State(args.state)
    try:
        summary = sync_contacts(src, dst, state, dry_run=args.dry_run)
    finally:
        src.close(); dst.close(); state.close()

    if args.json:
        print(json.dumps(summary.as_dict(), indent=2))
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
