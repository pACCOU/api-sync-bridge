"""Sync state: which source records have already been pushed, with the hash
of what was pushed, so unchanged records are skipped and changed records
are updated instead of duplicated (idempotency).  Stored as SQLite so it is
transactional and survives crashes; a single file, no server."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path


def fingerprint(payload: dict) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:32]


class State:
    def __init__(self, path: Path | str):
        self.conn = sqlite3.connect(str(path))
        self.conn.execute(
            """CREATE TABLE IF NOT EXISTS mapping(
                   source_id TEXT PRIMARY KEY,
                   target_id TEXT NOT NULL,
                   fingerprint TEXT NOT NULL,
                   updated_at TEXT NOT NULL DEFAULT (datetime('now'))
               )"""
        )
        self.conn.execute("CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)")
        self.conn.commit()

    def lookup(self, source_id: str) -> tuple[str, str] | None:
        row = self.conn.execute(
            "SELECT target_id, fingerprint FROM mapping WHERE source_id=?", (source_id,)
        ).fetchone()
        return (row[0], row[1]) if row else None

    def record(self, source_id: str, target_id: str, fp: str) -> None:
        self.conn.execute(
            """INSERT INTO mapping(source_id, target_id, fingerprint) VALUES(?,?,?)
               ON CONFLICT(source_id) DO UPDATE SET target_id=excluded.target_id,
               fingerprint=excluded.fingerprint, updated_at=datetime('now')""",
            (source_id, target_id, fp),
        )
        self.conn.commit()

    def get_meta(self, key: str, default: str | None = None) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        self.conn.commit()

    def count(self) -> int:
        return self.conn.execute("SELECT count(*) FROM mapping").fetchone()[0]

    def close(self) -> None:
        self.conn.close()
