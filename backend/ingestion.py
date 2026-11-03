#!/usr/bin/env python3
"""
Wikimedia recentchange -> Neon Postgres (slim version).

Keeps only what is needed for:
  * quality control   (bot/minor/patrolled flags, size change, revision ids, comment, log actions)
  * US activity       (English Wikipedia only; bucket by America/New_York time in SQL)
  * hot pages         (title + namespace + type)

Space savers:
  * only enwiki, only types edit / new / log
  * at most --max-per-minute rows stored per minute (default 1000)
  * comments truncated to 200 chars
  * old rows purged after --retention-days (default 7)

Run (PowerShell):
    $env:DATABASE_URL = "postgresql://USER:PASSWORD@HOST/DBNAME?sslmode=require"
    python wikimedia_enwiki_qc_to_neon.py
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "wikimedia-events-dashboard/0.1 (your-email@example.com)"  # put your contact here

WIKI = "enwiki"
KEEP_TYPES = {"edit", "new", "log"}
COMMENT_MAX = 200

log = logging.getLogger("wikistream")

SCHEMA = """
CREATE TABLE IF NOT EXISTS enwiki_changes (
    event_id      UUID PRIMARY KEY,
    event_time    TIMESTAMPTZ NOT NULL,   -- UTC; convert to America/New_York in queries
    type          TEXT NOT NULL,          -- edit | new | log
    namespace     INTEGER,
    title         TEXT,                   -- hot pages
    "user"        TEXT,
    is_bot        BOOLEAN,
    is_minor      BOOLEAN,
    is_patrolled  BOOLEAN,
    length_delta  INTEGER,                -- new - old bytes (edits only)
    revision_old  BIGINT,
    revision_new  BIGINT,
    comment       TEXT,                   -- truncated; revert/undo markers help QC
    log_type      TEXT,                   -- block, delete, move, protect ... (log only)
    log_action    TEXT
);

CREATE INDEX IF NOT EXISTS idx_enwc_time  ON enwiki_changes (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_enwc_title ON enwiki_changes (title, event_time DESC);

CREATE TABLE IF NOT EXISTS stream_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    last_event_id TEXT,
    updated_at    TIMESTAMPTZ
);
"""

COLUMNS = [
    "event_id", "event_time", "type", "namespace", "title", "user", "is_bot",
    "is_minor", "is_patrolled", "length_delta", "revision_old", "revision_new",
    "comment", "log_type", "log_action",
]
INSERT_SQL = (
    "INSERT INTO enwiki_changes ("
    + ", ".join(f'"{c}"' if c == "user" else c for c in COLUMNS)
    + ") VALUES %s ON CONFLICT (event_id) DO NOTHING"
)


# --------------------------------------------------------------------------- #
# Transform
# --------------------------------------------------------------------------- #
def _bool(v):
    return None if v is None else bool(v)


def _clean(text, limit=None):
    if not isinstance(text, str):
        return text
    text = text.replace("\x00", "")
    return text[:limit] if limit else text


def transform(evt: dict) -> tuple | None:
    """Return a row tuple for events we care about, else None."""
    if evt.get("wiki") != WIKI or evt.get("type") not in KEEP_TYPES:
        return None
    meta = evt.get("meta") or {}
    if not meta.get("id") or not meta.get("dt"):
        return None

    length = evt.get("length") or {}
    revision = evt.get("revision") or {}
    l_old, l_new = length.get("old"), length.get("new")
    delta = (l_new - l_old) if (l_old is not None and l_new is not None) else None

    return (
        meta["id"],
        meta["dt"],
        evt["type"],
        evt.get("namespace"),
        _clean(evt.get("title")),
        _clean(evt.get("user")),
        _bool(evt.get("bot")),
        _bool(evt.get("minor")),
        _bool(evt.get("patrolled")),
        delta,
        revision.get("old"),
        revision.get("new"),
        _clean(evt.get("comment"), COMMENT_MAX),
        evt.get("log_type"),
        evt.get("log_action"),
    )


# --------------------------------------------------------------------------- #
# Load
# --------------------------------------------------------------------------- #
def delete_older_than(conn, hours: int = 48) -> int:
    """Delete rows whose event_time is older than `hours`. Returns rows deleted."""
    with conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM enwiki_changes WHERE event_time < now() - make_interval(hours => %s)",
            (hours,),
        )
        return cur.rowcount


def is_live(evt: dict, within_seconds: float = 10) -> bool:
    """True when the event happened within the last few seconds (we've caught up)."""
    if evt.get("wiki") != WIKI or evt.get("type") not in KEEP_TYPES:
        return False
    dt = (evt.get("meta") or {}).get("dt")
    if not dt:
        return False
    try:
        ts = datetime.fromisoformat(dt.replace("Z", "+00:00"))
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - ts).total_seconds() <= within_seconds


class NeonSink:
    def __init__(self, dsn, batch_size, flush_seconds, max_per_minute, retention_hours):
        self.dsn = dsn
        self.batch_size = batch_size
        self.flush_seconds = flush_seconds
        self.max_per_minute = max_per_minute
        self.retention_hours = retention_hours

        self.buffer: list[tuple] = []
        self.last_event_id = None
        self.last_flush = time.monotonic()
        self.last_purge = 0.0
        self.total = 0

        # per-minute cap state
        self.minute = None
        self.minute_count = 0
        self.dropped = 0

        self.conn = None
        self._connect()
        with self.conn, self.conn.cursor() as cur:
            cur.execute(SCHEMA)

    def _connect(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except Exception:
                pass
        self.conn = psycopg2.connect(self.dsn, connect_timeout=15)
        log.info("connected to Postgres")

    def load_last_event_id(self):
        with self.conn, self.conn.cursor() as cur:
            cur.execute("SELECT last_event_id FROM stream_state WHERE id = 1")
            row = cur.fetchone()
        return row[0] if row else None

    def allow(self, event_time: str) -> bool:
        """Cap rows per minute, using the event's own timestamp (UTC minute)."""
        minute = event_time[:16]  # 'YYYY-MM-DDTHH:MM'
        if minute != self.minute:
            if self.dropped:
                log.info("minute %s capped: dropped %d rows", self.minute, self.dropped)
            self.minute, self.minute_count, self.dropped = minute, 0, 0
        if self.minute_count >= self.max_per_minute:
            self.dropped += 1
            return False
        self.minute_count += 1
        return True

    def add(self, row, sse_id):
        if sse_id:
            self.last_event_id = sse_id
        if self.allow(row[1]):
            self.buffer.append(row)
        if (
            len(self.buffer) >= self.batch_size
            or time.monotonic() - self.last_flush >= self.flush_seconds
        ):
            self.flush()

    def _write(self):
        with self.conn, self.conn.cursor() as cur:
            if self.buffer:
                psycopg2.extras.execute_values(cur, INSERT_SQL, self.buffer, page_size=500)
            if self.last_event_id:
                cur.execute(
                    """INSERT INTO stream_state (id, last_event_id, updated_at)
                       VALUES (1, %s, now())
                       ON CONFLICT (id) DO UPDATE
                       SET last_event_id = EXCLUDED.last_event_id,
                           updated_at    = EXCLUDED.updated_at""",
                    (self.last_event_id,),
                )

    def flush(self):
        if not self.buffer and not self.last_event_id:
            return
        try:
            self._write()
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
            log.warning("db connection problem (%s), reconnecting", exc)
            self._connect()
            self._write()
        self.total += len(self.buffer)
        if self.buffer:
            log.info("flushed %d rows (total %d)", len(self.buffer), self.total)
        self.buffer.clear()
        self.last_flush = time.monotonic()
        # in-process hourly purge (only for long-running mode; 0 disables)
        if self.retention_hours and time.monotonic() - self.last_purge > 3600:
            deleted = delete_older_than(self.conn, self.retention_hours)
            self.last_purge = time.monotonic()
            log.info("retention purge: deleted %d rows older than %dh", deleted, self.retention_hours)

    def close(self):
        try:
            self.flush()
        finally:
            self.conn.close()


# --------------------------------------------------------------------------- #
# Extract (SSE)
# --------------------------------------------------------------------------- #
def iter_sse(response):
    event_id, data_lines = None, []
    for raw in response.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        if raw == "":
            if data_lines:
                yield event_id, "\n".join(data_lines)
            event_id, data_lines = None, []
            continue
        if raw.startswith(":"):
            continue
        field, _, value = raw.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if field == "id":
            event_id = value
        elif field == "data":
            data_lines.append(value)


def stream_events(last_event_id):
    headers = {"Accept": "text/event-stream", "User-Agent": USER_AGENT}
    if last_event_id:
        headers["Last-Event-ID"] = last_event_id
    with requests.get(STREAM_URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        log.info("connected to %s", STREAM_URL)
        yield from iter_sse(r)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL"))
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--flush-seconds", type=float, default=5.0)
    ap.add_argument("--max-per-minute", type=int, default=1000)
    ap.add_argument("--retention-hours", type=int, default=48,
                    help="delete rows older than this many hours (0 = never delete in-process)")
    ap.add_argument("--cleanup-only", action="store_true",
                    help="just delete rows older than --retention-hours and exit")
    ap.add_argument("--stop-when-live", action="store_true",
                    help="exit once the backlog is caught up (for scheduled batch runs)")
    ap.add_argument("--run-seconds", type=int, default=0,
                    help="hard time limit for this run (0 = no limit)")
    ap.add_argument("--no-resume", action="store_true")
    args = ap.parse_args()

    if not args.dsn:
        sys.exit("Set DATABASE_URL (or pass --dsn) to your Neon connection string.")

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.cleanup_only:
        hours = args.retention_hours or 48
        conn = psycopg2.connect(args.dsn, connect_timeout=15)
        try:
            deleted = delete_older_than(conn, hours)
        finally:
            conn.close()
        log.info("cleanup: deleted %d rows older than %dh", deleted, hours)
        return

    sink = NeonSink(args.dsn, args.batch_size, args.flush_seconds,
                    args.max_per_minute, args.retention_hours)

    def _stop(*_):
        log.info("shutdown requested, flushing...")
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    backoff = 1
    done = False
    deadline = time.monotonic() + args.run_seconds if args.run_seconds else None
    try:
        while not done:
            last_id = None if args.no_resume else (sink.last_event_id or sink.load_last_event_id())
            try:
                for sse_id, data in stream_events(last_id):
                    backoff = 1
                    if deadline and time.monotonic() > deadline:
                        log.info("run-seconds limit reached")
                        done = True
                        break
                    try:
                        evt = json.loads(data)
                    except json.JSONDecodeError:
                        continue
                    row = transform(evt)
                    if row:
                        sink.add(row, sse_id)
                    elif sse_id:
                        sink.last_event_id = sse_id
                    if args.stop_when_live and is_live(evt):
                        log.info("caught up with the live stream, stopping")
                        done = True
                        break
            except requests.RequestException as exc:
                log.warning("stream error: %s - reconnecting in %ss", exc, backoff)
                sink.flush()
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
    except KeyboardInterrupt:
        pass
    finally:
        sink.close()
        log.info("done. total rows loaded: %d", sink.total)


if __name__ == "__main__":
    main()