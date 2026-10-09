"""A small HTTP client wrapper that does the boring-but-essential things:
retries with exponential back-off + jitter on 429/5xx/network errors,
honours ``Retry-After``, applies a simple token-bucket rate limit and logs
every request with its duration.  Built on ``httpx`` so it can be given a
``MockTransport`` in tests."""

from __future__ import annotations

import logging
import random
import time
from typing import Any

import httpx

log = logging.getLogger("syncbridge.http")

RETRY_STATUS = {408, 425, 429, 500, 502, 503, 504}


class RateLimiter:
    """Token bucket: at most ``rate`` requests per second (fractional ok)."""

    def __init__(self, rate: float):
        self.rate = rate
        self._next = 0.0

    def wait(self) -> None:
        if self.rate <= 0:
            return
        now = time.monotonic()
        if now < self._next:
            time.sleep(self._next - now)
        self._next = max(now, self._next) + 1.0 / self.rate


class ApiClient:
    def __init__(
        self,
        base_url: str,
        headers: dict[str, str] | None = None,
        *,
        max_retries: int = 5,
        backoff: float = 0.5,
        rate_per_sec: float = 0,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        sleep=time.sleep,
    ):
        self._client = httpx.Client(base_url=base_url, headers=headers or {},
                                    timeout=timeout, transport=transport)
        self.max_retries = max_retries
        self.backoff = backoff
        self.limiter = RateLimiter(rate_per_sec)
        self._sleep = sleep
        self.stats = {"requests": 0, "retries": 0}

    def close(self) -> None:
        self._client.close()

    def request(self, method: str, url: str, **kw: Any) -> httpx.Response:
        attempt = 0
        while True:
            self.limiter.wait()
            t0 = time.perf_counter()
            try:
                self.stats["requests"] += 1
                resp = self._client.request(method, url, **kw)
                dur = (time.perf_counter() - t0) * 1000
                log.debug("%s %s -> %s (%.0f ms)", method, url, resp.status_code, dur)
                if resp.status_code not in RETRY_STATUS:
                    resp.raise_for_status()
                    return resp
                reason = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After")
            except httpx.TransportError as exc:
                reason, retry_after = f"{type(exc).__name__}: {exc}", None

            attempt += 1
            if attempt > self.max_retries:
                raise RuntimeError(f"{method} {url} failed after {attempt} attempts: {reason}")
            delay = self._delay(attempt, retry_after)
            self.stats["retries"] += 1
            log.warning("%s %s: %s — retry %d/%d in %.1fs", method, url, reason,
                        attempt, self.max_retries, delay)
            self._sleep(delay)

    def _delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after and retry_after.isdigit():
            return float(retry_after)
        base = self.backoff * (2 ** (attempt - 1))
        return min(base + random.uniform(0, base / 2), 60.0)

    # convenience
    def get(self, url: str, **kw: Any) -> Any:
        return self.request("GET", url, **kw).json()

    def post(self, url: str, json: Any, **kw: Any) -> Any:
        return self.request("POST", url, json=json, **kw).json()

    def put(self, url: str, json: Any, **kw: Any) -> Any:
        return self.request("PUT", url, json=json, **kw).json()

    def paginate(self, url: str, *, items_key: str = "data", next_key: str = "next",
                 params: dict | None = None):
        """Yield items across pages for APIs that return {data: [...], next: url}."""
        first = True
        seen: set[str] = set()
        while url:
            if url in seen:
                raise RuntimeError(f"pagination loop detected at {url}")
            seen.add(url)
            # only the first request takes explicit params; a next-URL carries
            # its own query string (passing params={} would strip it)
            page = self.get(url, params=params) if first and params else self.get(url)
            first = False
            yield from page.get(items_key, [])
            url = page.get(next_key)
