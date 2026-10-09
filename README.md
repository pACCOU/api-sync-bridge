# api-sync-bridge

"Push our CRM contacts into the mailing tool, every night, without
duplicates" — the kind of integration every business needs and that breaks
in the same five ways every time. This is a small, dependency-light Python
engine that gets those five things right once, so a new integration is just
two adapter classes and a mapping function.

```
$ SRC_TOKEN=… DST_TOKEN=… syncbridge contacts \
    --src-url https://crm.example.com/api --dst-url https://mail.example.com/v3 \
    --state /var/lib/syncbridge/contacts.db --json
2026-10-07 02:00:01 INFO    syncing customers updated since 2026-10-06T02:00:00+00:00
2026-10-07 02:00:04 WARNING POST /subscribers: HTTP 429 — retry 1/5 in 2.0s
2026-10-07 02:00:09 INFO    sync done: 12 created, 37 updated, 1840 skipped, 0 failed
```

## The five things

| Problem                                   | How it is handled                                                               |
|-------------------------------------------|---------------------------------------------------------------------------------|
| **Duplicates** after a re-run or a crash  | SQLite state maps `source_id → target_id`; existing records are *updated*       |
| **Needless writes** (API quotas, noise)   | SHA-256 fingerprint of the payload; unchanged records are skipped               |
| **Rate limits / flaky APIs**              | exponential back-off + jitter on 429/5xx/timeouts, honours `Retry-After`, token-bucket limiter |
| **One bad record kills the batch**        | per-record error isolation with a failure cap; errors listed in the summary     |
| **Full re-scan every night**              | incremental cursor (`updated_since`) persisted only after a clean run           |

Plus: `--dry-run`, JSON summary for monitoring, secrets only via environment
variables, structured logs with request timings.

## Adapting it to your APIs

Three small pieces in `syncbridge/sync.py`:

```python
class CrmSource:        # how to read records (pagination handled by ApiClient.paginate)
class MailingTarget:    # how to create / update on the destination
def customer_to_subscriber(c): ...   # field mapping + validation, in one place
```

Swap them for Shopify → QuickBooks, HubSpot → Mailchimp, Zoho CRM → Google
Sheets, a Laravel back-office → a Flutter app's Firebase, etc. The engine
(`run_sync`) does not change.

## Install & test

```bash
pip install -e .[dev]
pytest -v        # 6 tests, fully offline: fake APIs with pagination, 429 and 503 injected
```

```
tests/test_sync.py::test_mapper_normalises PASSED
tests/test_sync.py::test_first_run_creates_and_isolates_bad_record PASSED
tests/test_sync.py::test_second_run_is_idempotent_and_updates_changes PASSED
tests/test_sync.py::test_retries_on_429_and_503 PASSED
tests/test_sync.py::test_gives_up_after_max_retries PASSED
tests/test_sync.py::test_dry_run_writes_nothing PASSED
```

## Running it on a schedule

systemd timer or cron; exit code is `1` when any record failed so your
monitoring (Healthchecks.io, Uptime Kuma, Zabbix…) can alert:

```cron
0 2 * * * cd /opt/syncbridge && SRC_TOKEN=$(cat .src) DST_TOKEN=$(cat .dst) \
  syncbridge contacts --src-url … --dst-url … --state contacts.db --json > last.json \
  || curl -fsS https://hc-ping.com/<uuid>/fail
```

MIT License — Florian Whannou
