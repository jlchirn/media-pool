"""
SQLite database for social features.
One database per group: data/{group_id}.db
"""

import sqlite3
import time
from pathlib import Path


def _conn(db_path: str) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(db_path, timeout=10, check_same_thread=False)


def init_db(db_path: str) -> None:
    con = _conn(db_path)
    cur = con.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reactions (
            id INTEGER PRIMARY KEY,
            file_name TEXT NOT NULL,
            user_session TEXT NOT NULL,
            emoji TEXT NOT NULL,
            timestamp REAL NOT NULL,
            UNIQUE(file_name, user_session, emoji)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS views (
            id INTEGER PRIMARY KEY,
            file_name TEXT NOT NULL,
            user_session TEXT NOT NULL,
            timestamp REAL NOT NULL,
            UNIQUE(file_name, user_session)
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY,
            file_name TEXT NOT NULL,
            user_session TEXT NOT NULL,
            nickname TEXT NOT NULL DEFAULT '',
            text TEXT NOT NULL,
            timestamp REAL NOT NULL
        )
    """)
    # Migration: add nickname column to existing databases
    cols = {r[1] for r in cur.execute("PRAGMA table_info(comments)").fetchall()}
    if "nickname" not in cols:
        cur.execute("ALTER TABLE comments ADD COLUMN nickname TEXT NOT NULL DEFAULT ''")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS media_meta (
            file_name TEXT PRIMARY KEY,
            lat REAL,
            lng REAL,
            captured_at TEXT,
            caption TEXT,
            ai_captioned INTEGER DEFAULT 0,
            caption_source TEXT,
            caption_updated_at REAL,
            caption_updated_by_session TEXT,
            uploader_session TEXT
        )
    """)
    # Migrations
    cols = {r[1] for r in cur.execute("PRAGMA table_info(media_meta)").fetchall()}
    if "uploader_session" not in cols:
        cur.execute("ALTER TABLE media_meta ADD COLUMN uploader_session TEXT")
    if "caption_source" not in cols:
        cur.execute("ALTER TABLE media_meta ADD COLUMN caption_source TEXT")
    if "caption_updated_at" not in cols:
        cur.execute("ALTER TABLE media_meta ADD COLUMN caption_updated_at REAL")
    if "caption_updated_by_session" not in cols:
        cur.execute("ALTER TABLE media_meta ADD COLUMN caption_updated_by_session TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS caption_jobs (
            file_name TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            attempt_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            next_attempt_at REAL NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_caption_jobs_status_next ON caption_jobs(status, next_attempt_at)")

    con.commit()
    con.close()


# ── Reactions ────────────────────────────────────────────────────────────────

def add_or_remove_reaction(db_path: str, file_name: str, user_session: str, emoji: str) -> bool:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM reactions WHERE file_name=? AND user_session=? AND emoji=?",
            (file_name, user_session, emoji),
        )
        if cur.fetchone():
            cur.execute(
                "DELETE FROM reactions WHERE file_name=? AND user_session=? AND emoji=?",
                (file_name, user_session, emoji),
            )
            con.commit()
            return False
        cur.execute(
            "INSERT INTO reactions (file_name, user_session, emoji, timestamp) VALUES (?,?,?,?)",
            (file_name, user_session, emoji, time.time()),
        )
        con.commit()
        return True
    finally:
        con.close()


def get_reactions(db_path: str, file_name: str) -> dict:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT emoji, COUNT(*) FROM reactions WHERE file_name=? GROUP BY emoji",
            (file_name,),
        )
        return {e: c for e, c in cur.fetchall()}
    finally:
        con.close()


def get_reactions_by_file(db_path: str, file_names: list) -> dict:
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        result = {fn: {} for fn in file_names}
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"SELECT file_name, emoji, COUNT(*) FROM reactions WHERE file_name IN ({ph}) GROUP BY file_name, emoji",
            file_names,
        )
        for fn, emoji, cnt in cur.fetchall():
            result[fn][emoji] = cnt
        return result
    finally:
        con.close()


# ── Views ────────────────────────────────────────────────────────────────────

def increment_view(db_path: str, file_name: str, user_session: str) -> int:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT OR IGNORE INTO views (file_name, user_session, timestamp) VALUES (?,?,?)",
            (file_name, user_session, time.time()),
        )
        con.commit()
        cur.execute("SELECT COUNT(*) FROM views WHERE file_name=?", (file_name,))
        return cur.fetchone()[0]
    finally:
        con.close()


def get_view_counts(db_path: str, file_names: list) -> dict:
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        result = {fn: 0 for fn in file_names}
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"SELECT file_name, COUNT(*) FROM views WHERE file_name IN ({ph}) GROUP BY file_name",
            file_names,
        )
        for fn, cnt in cur.fetchall():
            result[fn] = cnt
        return result
    finally:
        con.close()


# ── Comments ─────────────────────────────────────────────────────────────────

def add_comment(db_path: str, file_name: str, user_session: str, nickname: str, text: str) -> int:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO comments (file_name, user_session, nickname, text, timestamp) VALUES (?,?,?,?,?)",
            (file_name, user_session, nickname, text[:500], time.time()),
        )
        con.commit()
        return cur.lastrowid
    finally:
        con.close()


def get_comments(db_path: str, file_name: str) -> list:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT id, user_session, nickname, text, timestamp FROM comments WHERE file_name=? ORDER BY timestamp ASC LIMIT 200",
            (file_name,),
        )
        return [
            {"id": r[0], "user_session": r[1], "nickname": r[2], "text": r[3], "timestamp": r[4]}
            for r in cur.fetchall()
        ]
    finally:
        con.close()


def delete_comment(db_path: str, comment_id: int, user_session: str) -> bool:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "DELETE FROM comments WHERE id=? AND user_session=?",
            (comment_id, user_session),
        )
        con.commit()
        return cur.rowcount > 0
    finally:
        con.close()


def get_comment_counts(db_path: str, file_names: list) -> dict:
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        result = {fn: 0 for fn in file_names}
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"SELECT file_name, COUNT(*) FROM comments WHERE file_name IN ({ph}) GROUP BY file_name",
            file_names,
        )
        for fn, cnt in cur.fetchall():
            result[fn] = cnt
        return result
    finally:
        con.close()


# ── Media metadata (GPS, captions) ───────────────────────────────────────────

def set_media_meta(db_path: str, file_name: str, lat=None, lng=None,
                   captured_at=None, force_gps: bool = False) -> None:
    """Upsert media metadata.
    GPS (lat/lng): uses COALESCE by default (won't overwrite existing with NULL).
    Pass force_gps=True to always write lat/lng (e.g. for rescan that may clear wrong 0,0 values).
    captured_at: always COALESCE (never overwrite a good timestamp with NULL).
    """
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        if force_gps:
            gps_sql = "lat=excluded.lat, lng=excluded.lng"
        else:
            gps_sql = "lat=COALESCE(excluded.lat, lat), lng=COALESCE(excluded.lng, lng)"
        cur.execute(
            f"""INSERT INTO media_meta (file_name, lat, lng, captured_at)
               VALUES (?,?,?,?)
               ON CONFLICT(file_name) DO UPDATE SET
                 {gps_sql},
                 captured_at=COALESCE(excluded.captured_at, captured_at)""",
            (file_name, lat, lng, captured_at),
        )
        con.commit()
    finally:
        con.close()


def set_caption(db_path: str, file_name: str, caption: str, user_session: str = "") -> None:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        if caption == "":
            cur.execute(
                """INSERT INTO media_meta
                     (file_name, caption, ai_captioned, caption_source,
                      caption_updated_at, caption_updated_by_session)
                   VALUES (?,NULL,0,NULL,?,?)
                   ON CONFLICT(file_name) DO UPDATE SET
                     caption=NULL,
                     ai_captioned=0,
                     caption_source=NULL,
                     caption_updated_at=excluded.caption_updated_at,
                     caption_updated_by_session=excluded.caption_updated_by_session""",
                (file_name, now, user_session),
            )
            cur.execute(
                """INSERT INTO caption_jobs
                     (file_name, status, attempt_count, last_error,
                      next_attempt_at, created_at, updated_at)
                   VALUES (?, 'pending', 0, NULL, 0, ?, ?)
                   ON CONFLICT(file_name) DO UPDATE SET
                     status='pending',
                     attempt_count=0,
                     last_error=NULL,
                     next_attempt_at=0,
                     updated_at=excluded.updated_at""",
                (file_name, now, now),
            )
            con.commit()
            return
        cur.execute(
            """INSERT INTO media_meta
                 (file_name, caption, ai_captioned, caption_source,
                  caption_updated_at, caption_updated_by_session)
               VALUES (?,?,1,'manual',?,?)
               ON CONFLICT(file_name) DO UPDATE SET
                 caption=excluded.caption,
                 ai_captioned=1,
                 caption_source='manual',
                 caption_updated_at=excluded.caption_updated_at,
                 caption_updated_by_session=excluded.caption_updated_by_session""",
            (file_name, caption, now, user_session),
        )
        cur.execute(
            """UPDATE caption_jobs
               SET status='done', last_error=NULL, updated_at=?
               WHERE file_name=?""",
            (now, file_name),
        )
        con.commit()
    finally:
        con.close()


# Caption jobs

def enqueue_caption_job(db_path: str, file_name: str) -> None:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            """INSERT INTO caption_jobs
                 (file_name, status, attempt_count, last_error, next_attempt_at, created_at, updated_at)
               VALUES (?, 'pending', 0, NULL, 0, ?, ?)
               ON CONFLICT(file_name) DO UPDATE SET
                 status=CASE
                   WHEN caption_jobs.status='done' THEN caption_jobs.status
                   ELSE 'pending'
                 END,
                 last_error=CASE
                   WHEN caption_jobs.status='done' THEN caption_jobs.last_error
                   ELSE NULL
                 END,
                 next_attempt_at=CASE
                   WHEN caption_jobs.status='done' THEN caption_jobs.next_attempt_at
                   ELSE 0
                 END,
                 updated_at=?""",
            (file_name, now, now, now),
        )
        con.commit()
    finally:
        con.close()


def claim_caption_job(db_path: str) -> dict | None:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """SELECT file_name, attempt_count
               FROM caption_jobs
               WHERE status IN ('pending', 'failed')
                 AND next_attempt_at <= ?
               ORDER BY created_at ASC
               LIMIT 1""",
            (now,),
        )
        row = cur.fetchone()
        if not row:
            con.commit()
            return None
        file_name, attempts = row
        cur.execute(
            """UPDATE caption_jobs
               SET status='processing', updated_at=?
               WHERE file_name=?""",
            (now, file_name),
        )
        con.commit()
        return {"file_name": file_name, "attempt_count": attempts}
    finally:
        con.close()


def complete_caption_job(db_path: str, file_name: str, caption: str) -> str:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        row = cur.execute(
            "SELECT caption, caption_source FROM media_meta WHERE file_name=?",
            (file_name,),
        ).fetchone()
        existing_caption = (row[0] if row else "") or ""
        if existing_caption:
            final_caption = existing_caption
        else:
            final_caption = caption
            cur.execute(
                """INSERT INTO media_meta
                     (file_name, caption, ai_captioned, caption_source,
                      caption_updated_at, caption_updated_by_session)
                   VALUES (?,?,1,'ai',?,NULL)
                   ON CONFLICT(file_name) DO UPDATE SET
                     caption=excluded.caption,
                     ai_captioned=1,
                     caption_source='ai',
                     caption_updated_at=excluded.caption_updated_at,
                     caption_updated_by_session=NULL""",
                (file_name, caption, now),
            )
        cur.execute(
            """UPDATE caption_jobs
               SET status='done', last_error=NULL, updated_at=?
               WHERE file_name=?""",
            (now, file_name),
        )
        con.commit()
        return final_caption
    finally:
        con.close()


def mark_caption_job_done(db_path: str, file_name: str) -> None:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            """UPDATE caption_jobs
               SET status='done', last_error=NULL, updated_at=?
               WHERE file_name=?""",
            (now, file_name),
        )
        con.commit()
    finally:
        con.close()


def fail_caption_job(db_path: str, file_name: str, error: str) -> None:
    init_db(db_path)
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute("SELECT attempt_count FROM caption_jobs WHERE file_name=?", (file_name,))
        row = cur.fetchone()
        attempts = (row[0] if row else 0) + 1
        backoffs = [30, 120, 600]
        status = "failed" if attempts >= 3 else "pending"
        next_at = now + backoffs[min(attempts - 1, len(backoffs) - 1)]
        cur.execute(
            """INSERT INTO caption_jobs
                 (file_name, status, attempt_count, last_error, next_attempt_at, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(file_name) DO UPDATE SET
                 status=excluded.status,
                 attempt_count=excluded.attempt_count,
                 last_error=excluded.last_error,
                 next_attempt_at=excluded.next_attempt_at,
                 updated_at=excluded.updated_at""",
            (file_name, status, attempts, error[:500], next_at, now, now),
        )
        con.commit()
    finally:
        con.close()


def reset_stuck_caption_jobs(db_path: str, older_than_seconds: int = 300) -> int:
    init_db(db_path)
    cutoff = time.time() - older_than_seconds
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            """UPDATE caption_jobs
               SET status='pending', next_attempt_at=0, updated_at=?
               WHERE status='processing' AND updated_at < ?""",
            (time.time(), cutoff),
        )
        con.commit()
        return cur.rowcount
    finally:
        con.close()


def get_caption_job_stats(db_path: str) -> dict:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        rows = cur.execute(
            "SELECT status, COUNT(*) FROM caption_jobs GROUP BY status"
        ).fetchall()
        recent = cur.execute(
            """SELECT file_name, status, attempt_count, last_error, next_attempt_at, updated_at
               FROM caption_jobs
               WHERE last_error IS NOT NULL
               ORDER BY updated_at DESC
               LIMIT 5"""
        ).fetchall()
        return {
            "counts": {status: count for status, count in rows},
            "recent_errors": [
                {
                    "file": file_name,
                    "status": status,
                    "attempt_count": attempts,
                    "last_error": error,
                    "next_attempt_at": next_attempt_at,
                    "updated_at": updated_at,
                }
                for file_name, status, attempts, error, next_attempt_at, updated_at in recent
            ],
        }
    finally:
        con.close()


def enqueue_missing_caption_jobs(db_path: str, file_names: list[str]) -> int:
    init_db(db_path)
    if not file_names:
        return 0
    now = time.time()
    con = _conn(db_path)
    cur = con.cursor()
    try:
        queued = 0
        for file_name in file_names:
            cur.execute(
                "SELECT caption FROM media_meta WHERE file_name=?",
                (file_name,),
            )
            row = cur.fetchone()
            if row and row[0]:
                continue
            job = cur.execute(
                "SELECT status FROM caption_jobs WHERE file_name=?",
                (file_name,),
            ).fetchone()
            if job and job[0] != "done":
                continue
            if job and job[0] == "done":
                cur.execute("DELETE FROM caption_jobs WHERE file_name=?", (file_name,))
            cur.execute(
                """INSERT INTO caption_jobs
                     (file_name, status, attempt_count, last_error,
                      next_attempt_at, created_at, updated_at)
                   VALUES (?, 'pending', 0, NULL, 0, ?, ?)""",
                (file_name, now, now),
            )
            queued += 1
        con.commit()
        return queued
    finally:
        con.close()


def get_caption_job_statuses(db_path: str, file_names: list[str]) -> dict:
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"""SELECT file_name, status, attempt_count, last_error, next_attempt_at
                FROM caption_jobs WHERE file_name IN ({ph})""",
            file_names,
        )
        return {
            fn: {
                "status": status,
                "attempt_count": attempts,
                "last_error": error,
                "next_attempt_at": next_attempt_at,
            }
            for fn, status, attempts, error, next_attempt_at in cur.fetchall()
        }
    finally:
        con.close()


def get_media_meta(db_path: str, file_names: list) -> dict:
    """Returns {file_name: {lat, lng, captured_at, caption, caption_source}, ...}"""
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"""SELECT file_name, lat, lng, captured_at, caption,
                       caption_source, caption_updated_at
                FROM media_meta WHERE file_name IN ({ph})""",
            file_names,
        )
        return {
            fn: {
                "lat": lat,
                "lng": lng,
                "captured_at": ca,
                "caption": cap,
                "caption_source": source,
                "caption_updated_at": updated_at,
            }
            for fn, lat, lng, ca, cap, source, updated_at in cur.fetchall()
        }
    finally:
        con.close()


def get_all_captions(db_path: str) -> list:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            "SELECT file_name, caption FROM media_meta WHERE caption IS NOT NULL AND caption != ''"
        )
        return [{"file": fn, "caption": cap} for fn, cap in cur.fetchall()]
    finally:
        con.close()


# ── Uploader tracking ────────────────────────────────────────────────────────

def set_uploader_session(db_path: str, file_name: str, user_session: str) -> None:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute(
            """INSERT INTO media_meta (file_name, uploader_session)
               VALUES (?,?)
               ON CONFLICT(file_name) DO UPDATE SET uploader_session=excluded.uploader_session""",
            (file_name, user_session),
        )
        con.commit()
    finally:
        con.close()


def get_uploader_sessions(db_path: str, file_names: list) -> dict:
    """Returns {file_name: uploader_session_id}. Files with no record are omitted."""
    init_db(db_path)
    if not file_names:
        return {}
    con = _conn(db_path)
    cur = con.cursor()
    try:
        ph = ",".join("?" * len(file_names))
        cur.execute(
            f"SELECT file_name, uploader_session FROM media_meta WHERE file_name IN ({ph})",
            file_names,
        )
        return {fn: sess for fn, sess in cur.fetchall() if sess}
    finally:
        con.close()


# ── Cascade delete ────────────────────────────────────────────────────────────

def delete_file_data(db_path: str, file_name: str) -> None:
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM reactions WHERE file_name=?", (file_name,))
        cur.execute("DELETE FROM views WHERE file_name=?", (file_name,))
        cur.execute("DELETE FROM comments WHERE file_name=?", (file_name,))
        cur.execute("DELETE FROM media_meta WHERE file_name=?", (file_name,))
        cur.execute("DELETE FROM caption_jobs WHERE file_name=?", (file_name,))
        con.commit()
    finally:
        con.close()


def prune_stale_files(db_path: str, live_files: list[str]) -> int:
    """Remove DB rows for files no longer present in Dropbox. Returns count pruned."""
    if not live_files:
        return 0
    init_db(db_path)
    con = _conn(db_path)
    cur = con.cursor()
    try:
        placeholders = ",".join("?" * len(live_files))
        stale = [r[0] for r in cur.execute(
            f"SELECT DISTINCT file_name FROM media_meta WHERE file_name NOT IN ({placeholders})",
            live_files,
        ).fetchall()]
        for fn in stale:
            cur.execute("DELETE FROM reactions WHERE file_name=?", (fn,))
            cur.execute("DELETE FROM views WHERE file_name=?", (fn,))
            cur.execute("DELETE FROM comments WHERE file_name=?", (fn,))
            cur.execute("DELETE FROM media_meta WHERE file_name=?", (fn,))
            cur.execute("DELETE FROM caption_jobs WHERE file_name=?", (fn,))
        con.commit()
        return len(stale)
    finally:
        con.close()
