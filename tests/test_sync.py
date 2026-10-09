"""Tests run fully offline: both APIs are simulated with httpx.MockTransport,
including pagination, rate limiting (429 + Retry-After) and flaky 503s."""

from __future__ import annotations

import json

import httpx
import pytest

from syncbridge.http import ApiClient
from syncbridge.state import State, fingerprint
from syncbridge.sync import customer_to_subscriber, sync_contacts


class FakeApis:
    """In-memory CRM (source) + mailing list (target) behind one transport."""

    def __init__(self, customers: list[dict], *, flaky_503: int = 0, throttle_once: bool = False):
        self.customers = customers
        self.subscribers: dict[int, dict] = {}
        self.calls: list[str] = []
        self.flaky_503 = flaky_503
        self.throttle_once = throttle_once
        self._next_id = 100

    def handler(self, req: httpx.Request) -> httpx.Response:
        self.calls.append(f"{req.method} {req.url.path}{('?' + req.url.query.decode()) if req.url.query else ''}")
        host = req.url.host
        if host == "crm":
            return self._crm(req)
        return self._mail(req)

    def _crm(self, req: httpx.Request) -> httpx.Response:
        page = int(req.url.params.get("page", 1))
        per = 2
        chunk = self.customers[(page - 1) * per: page * per]
        nxt = f"/customers?page={page + 1}" if page * per < len(self.customers) else None
        return httpx.Response(200, json={"data": chunk, "next": nxt})

    def _mail(self, req: httpx.Request) -> httpx.Response:
        if self.throttle_once:
            self.throttle_once = False
            return httpx.Response(429, headers={"Retry-After": "0"})
        if self.flaky_503 > 0:
            self.flaky_503 -= 1
            return httpx.Response(503)
        body = json.loads(req.content or b"{}")
        if req.method == "POST":
            self._next_id += 1
            self.subscribers[self._next_id] = body
            return httpx.Response(201, json={"id": self._next_id})
        if req.method == "PUT":
            sid = int(req.url.path.rsplit("/", 1)[1])
            assert sid in self.subscribers, "update of unknown subscriber"
            self.subscribers[sid] = body
            return httpx.Response(200, json={"id": sid})
        return httpx.Response(404)


CUSTOMERS = [
    {"id": 1, "name": "Ada Lovelace", "email": "Ada@Example.com", "segments": ["vip"]},
    {"id": 2, "name": "Linus T", "email": "linus@example.com", "opted_out": True},
    {"id": 3, "name": "No Email", "email": ""},
    {"id": 4, "name": "Grace Hopper", "email": "grace@example.com", "segments": ["navy", "vip"]},
    {"id": 5, "name": "Solo", "email": "solo@example.com"},
]


def make(fake: FakeApis):
    t = httpx.MockTransport(fake.handler)
    src = ApiClient("http://crm", transport=t, sleep=lambda s: None)
    dst = ApiClient("http://mail", transport=t, sleep=lambda s: None, backoff=0)
    return src, dst


def test_mapper_normalises():
    p = customer_to_subscriber(CUSTOMERS[0])
    assert p == {"email": "ada@example.com", "first_name": "Ada", "last_name": "Lovelace",
                 "tags": ["crm", "vip"], "subscribed": True}
    assert fingerprint(p) == fingerprint(dict(reversed(list(p.items()))))  # order-independent


def test_first_run_creates_and_isolates_bad_record(tmp_path):
    fake = FakeApis(list(CUSTOMERS))
    src, dst = make(fake)
    st = State(tmp_path / "s.db")
    s = sync_contacts(src, dst, st)
    assert (s.created, s.updated, s.skipped, s.failed) == (4, 0, 0, 1)
    assert s.errors[0]["source_id"] == "3" and "e-mail" in s.errors[0]["error"]
    assert len(fake.subscribers) == 4
    assert st.count() == 4
    # pagination was followed: 3 pages of 2
    assert sum(c.startswith("GET /customers") for c in fake.calls) == 3
    # a failure means the cursor is NOT advanced (so the bad record is retried)
    assert st.get_meta("cursor") is None


def test_second_run_is_idempotent_and_updates_changes(tmp_path):
    good = [c for c in CUSTOMERS if c["email"]]
    fake = FakeApis(good)
    src, dst = make(fake)
    st = State(tmp_path / "s.db")
    s1 = sync_contacts(src, dst, st)
    assert s1.created == 4 and s1.failed == 0
    assert st.get_meta("cursor")                          # cursor saved after clean run

    # nothing changed -> everything skipped, no writes to the target
    before = len(fake.calls)
    s2 = sync_contacts(src, dst, st)
    assert (s2.created, s2.updated, s2.skipped) == (0, 0, 4)
    assert not any(c.startswith(("POST", "PUT")) for c in fake.calls[before:])

    # one record changed -> exactly one PUT, no duplicate POST
    good[0]["segments"] = ["vip", "newsletter"]
    s3 = sync_contacts(src, dst, st)
    assert (s3.created, s3.updated, s3.skipped) == (0, 1, 3)
    assert len(fake.subscribers) == 4
    assert "newsletter" in next(iter(fake.subscribers.values()))["tags"]


def test_retries_on_429_and_503(tmp_path):
    fake = FakeApis(CUSTOMERS[:1], flaky_503=2, throttle_once=True)
    src, dst = make(fake)
    s = sync_contacts(src, dst, State(tmp_path / "s.db"))
    assert s.created == 1 and s.failed == 0
    assert dst.stats["retries"] == 3            # 1x429 + 2x503, then success


def test_gives_up_after_max_retries(tmp_path):
    fake = FakeApis(CUSTOMERS[:1], flaky_503=99)
    src, dst = make(fake)
    dst.max_retries = 2
    s = sync_contacts(src, dst, State(tmp_path / "s.db"))
    assert s.failed == 1 and "after 3 attempts" in s.errors[0]["error"]


def test_dry_run_writes_nothing(tmp_path):
    fake = FakeApis(CUSTOMERS[:2])
    src, dst = make(fake)
    st = State(tmp_path / "s.db")
    s = sync_contacts(src, dst, st, dry_run=True)
    assert s.created == 2 and fake.subscribers == {} and st.count() == 0
