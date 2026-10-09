"""The sync engine.  It is deliberately generic: a *source* yields records,
a *mapper* turns a source record into the target payload, and a *target*
knows how to create / update.  The engine handles idempotency, change
detection, incremental runs (``since`` cursor), error isolation (one bad
record never stops the batch) and a summary.

The ``contacts`` example at the bottom syncs customers from a CRM-style API
to a mailing-list-style API — swap the two small adapter classes for any
pair of REST services.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol

from .http import ApiClient
from .state import State, fingerprint

log = logging.getLogger("syncbridge")


class Target(Protocol):
    def create(self, payload: dict) -> str: ...        # returns target id
    def update(self, target_id: str, payload: dict) -> None: ...


@dataclass
class Summary:
    created: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return self.__dict__


def run_sync(
    records: Iterable[dict],
    *,
    source_id: Callable[[dict], str],
    mapper: Callable[[dict], dict],
    target: Target,
    state: State,
    dry_run: bool = False,
    max_failures: int = 25,
) -> Summary:
    s = Summary()
    for rec in records:
        sid = source_id(rec)
        try:
            payload = mapper(rec)
            fp = fingerprint(payload)
            known = state.lookup(sid)
            if known and known[1] == fp:
                s.skipped += 1
                continue
            if dry_run:
                log.info("[dry-run] would %s %s", "update" if known else "create", sid)
                s.updated += 1 if known else 0
                s.created += 0 if known else 1
                continue
            if known:
                target.update(known[0], payload)
                state.record(sid, known[0], fp)
                s.updated += 1
            else:
                tid = target.create(payload)
                state.record(sid, tid, fp)
                s.created += 1
        except Exception as exc:  # noqa: BLE001 — isolate per-record failures
            s.failed += 1
            s.errors.append({"source_id": sid, "error": str(exc)})
            log.error("record %s failed: %s", sid, exc)
            if s.failed >= max_failures:
                log.critical("aborting: %d failures reached", s.failed)
                break
    log.info("sync done: %d created, %d updated, %d skipped, %d failed",
             s.created, s.updated, s.skipped, s.failed)
    return s


# --------------------------------------------------------------------------- #
# Example adapters: CRM customers -> mailing list subscribers
# --------------------------------------------------------------------------- #

class CrmSource:
    """Reads /customers?updated_since=... with {data, next} pagination."""

    def __init__(self, client: ApiClient):
        self.client = client

    def customers(self, since: str | None) -> Iterable[dict]:
        params = {"updated_since": since} if since else None
        yield from self.client.paginate("/customers", params=params)


class MailingTarget:
    """Writes to /subscribers (POST) and /subscribers/{id} (PUT)."""

    def __init__(self, client: ApiClient):
        self.client = client

    def create(self, payload: dict) -> str:
        return str(self.client.post("/subscribers", json=payload)["id"])

    def update(self, target_id: str, payload: dict) -> None:
        self.client.put(f"/subscribers/{target_id}", json=payload)


def customer_to_subscriber(c: dict) -> dict:
    """Field mapping + normalisation. Keep all business rules in one place."""
    if not c.get("email"):
        raise ValueError("customer has no e-mail address")
    first, _, last = (c.get("name") or "").partition(" ")
    return {
        "email": c["email"].strip().lower(),
        "first_name": first,
        "last_name": last,
        "tags": sorted({"crm", *(c.get("segments") or [])}),
        "subscribed": not c.get("opted_out", False),
    }


def sync_contacts(src: ApiClient, dst: ApiClient, state: State, *, dry_run: bool = False) -> Summary:
    since = state.get_meta("cursor")
    started = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    log.info("syncing customers updated since %s", since or "the beginning")
    summary = run_sync(
        CrmSource(src).customers(since),
        source_id=lambda c: str(c["id"]),
        mapper=customer_to_subscriber,
        target=MailingTarget(dst),
        state=state,
        dry_run=dry_run,
    )
    if not dry_run and summary.failed == 0:
        state.set_meta("cursor", started)   # next run is incremental
    return summary
