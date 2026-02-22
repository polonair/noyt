#!/usr/bin/env python3
import os, json, re, time, hashlib, subprocess, shutil, calendar, sqlite3
from contextlib import contextmanager
from collections import defaultdict
from urllib.parse import parse_qs, urlparse
from io import BytesIO
from PIL import Image
from datetime import datetime, date
import feedparser
import requests
from mutagen.id3 import ID3, APIC, USLT, TIT2, TPE1, TALB, TDRC
from mutagen.mp3 import MP3
import random

BASE = os.path.expanduser("~/yt-audio")
SEEN_JSON_PATH = os.path.join(BASE, "data", "seen.json")
CHANNEL_STATE_JSON_PATH = os.path.join(BASE, "data", "channel_state.json")
STATE_DB_PATH = os.path.join(BASE, "data", "state.db")
LOG_PATH = os.path.join(BASE, "logs", "run.log")
TMP_DIR  = os.path.join(BASE, "tmp")
LOCK_PATH = os.path.join(BASE, "yt_audio.lock")
DEBUG = False
RETRIES = 2
RETRY_BACKOFF_SEC = 5

def log(msg: str):
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")

def debug_log(msg: str):
    if DEBUG:
        log(f"DEBUG: {msg}")


def _get_proc_start_ticks(pid: int) -> int | None:
    stat_path = f"/proc/{pid}/stat"
    try:
        with open(stat_path, "r", encoding="utf-8") as f:
            stat_line = f.read().strip()
    except Exception:
        return None

    close_idx = stat_line.rfind(")")
    if close_idx == -1:
        return None

    rest = stat_line[close_idx + 2 :].split()
    if len(rest) < 20:
        return None

    try:
        return int(rest[19])
    except Exception:
        return None


def _read_lock_info() -> dict | None:
    try:
        with open(LOCK_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _is_lock_owner_alive(lock_info: dict | None) -> bool:
    if not isinstance(lock_info, dict):
        return False

    try:
        pid = int(lock_info.get("pid"))
    except Exception:
        return False

    if pid <= 0:
        return False

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True

    expected_ticks = lock_info.get("proc_start_ticks")
    if expected_ticks is not None:
        current_ticks = _get_proc_start_ticks(pid)
        if current_ticks is None:
            return False
        try:
            if int(expected_ticks) != int(current_ticks):
                return False
        except Exception:
            return False

    script_name = os.path.basename(__file__)
    cmdline_path = f"/proc/{pid}/cmdline"
    try:
        with open(cmdline_path, "rb") as f:
            cmdline = f.read().replace(b"\x00", b" ").decode("utf-8", errors="ignore")
        # cmdline используется только как дополнительная эвристика и не должен
        # опровергать валидность живого процесса-владельца (например, при запуске
        # через обёртку, симлинк или `python -m`).
        if script_name and script_name in cmdline:
            debug_log(f"Lock owner cmdline matches script name: pid={pid}")
    except Exception:
        # Если cmdline недоступен, PID+start_ticks уже дают достаточную защиту.
        pass

    return True


@contextmanager
def single_instance_lock():
    os.makedirs(BASE, exist_ok=True)
    lock_fd = None
    lock_acquired = False
    lock_info = {
        "pid": os.getpid(),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "script": os.path.abspath(__file__),
        "proc_start_ticks": _get_proc_start_ticks(os.getpid()),
    }

    while True:
        try:
            lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            os.write(lock_fd, json.dumps(lock_info, ensure_ascii=False).encode("utf-8"))
            os.fsync(lock_fd)
            lock_acquired = True
            log(f"Single instance lock acquired: {LOCK_PATH} (pid={lock_info['pid']})")
            break
        except FileExistsError:
            existing = None
            for _ in range(5):
                existing = _read_lock_info()
                if isinstance(existing, dict):
                    break
                # Защита от гонки: файл уже создан, но владелец ещё не успел
                # дописать JSON-метаданные. Нельзя сразу считать lock устаревшим.
                time.sleep(0.05)

            if _is_lock_owner_alive(existing):
                owner_pid = existing.get("pid") if isinstance(existing, dict) else "unknown"
                log(f"Refusing to start: another instance is running (pid={owner_pid})")
                break

            if existing is None:
                log(f"Lock file exists but metadata is not readable yet: {LOCK_PATH}; refusing to start")
                break

            try:
                os.remove(LOCK_PATH)
                log(f"Removed stale lock file: {LOCK_PATH}")
            except FileNotFoundError:
                continue
            except Exception as ex:
                log(f"Failed to remove stale lock file: {LOCK_PATH} ({ex})")
                break

    try:
        yield lock_acquired
    finally:
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except Exception:
                pass

        if lock_acquired:
            try:
                current = _read_lock_info()
                if isinstance(current, dict) and int(current.get("pid") or -1) == os.getpid():
                    os.remove(LOCK_PATH)
            except FileNotFoundError:
                pass
            except Exception as ex:
                log(f"Failed to release lock file: {LOCK_PATH} ({ex})")


def with_retries(action_name: str, fn, retries: int | None = None, backoff_sec: int | None = None):
    max_attempts = max(1, (RETRIES if retries is None else retries) + 1)
    wait_base = RETRY_BACKOFF_SEC if backoff_sec is None else backoff_sec

    last_ex = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Exception as ex:
            last_ex = ex
            if attempt >= max_attempts:
                break
            wait_s = max(0, wait_base) * attempt
            log(f"{action_name} failed (attempt {attempt}/{max_attempts}): {ex}; retry in {wait_s}s")
            if wait_s > 0:
                time.sleep(wait_s)

    raise last_ex


def is_within_tmp_dir(path: str) -> bool:
    abs_tmp = os.path.abspath(TMP_DIR)
    abs_path = os.path.abspath(path)
    return abs_path == abs_tmp or abs_path.startswith(abs_tmp + os.sep)


def _dir_size_bytes(path: str) -> int:
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            fp = os.path.join(root, name)
            try:
                total += os.path.getsize(fp)
            except Exception:
                pass
    return total


def cleanup_tmp_out(tmp_out: str, mp3_path: str | None = None):
    if not tmp_out or not is_within_tmp_dir(tmp_out):
        debug_log(f"Skip tmp cleanup outside TMP_DIR: {tmp_out}")
        return

    if mp3_path and os.path.exists(mp3_path):
        try:
            os.remove(mp3_path)
            debug_log(f"Removed temp mp3: {mp3_path}")
        except Exception as ex:
            debug_log(f"Failed to remove temp mp3: {mp3_path} -> {ex}")

    if not os.path.exists(tmp_out):
        return

    try:
        os.rmdir(tmp_out)
        debug_log(f"Removed empty temp dir: {tmp_out}")
    except Exception:
        shutil.rmtree(tmp_out, ignore_errors=True)
        debug_log(f"Removed temp dir recursively: {tmp_out}")


def cleanup_tmp_dir(max_age_hours: int = 24):
    os.makedirs(TMP_DIR, exist_ok=True)
    threshold_ts = time.time() - max(0, int(max_age_hours)) * 3600

    removed_dirs = 0
    freed_bytes = 0

    for name in os.listdir(TMP_DIR):
        path = os.path.join(TMP_DIR, name)
        if not os.path.isdir(path):
            continue
        if not is_within_tmp_dir(path):
            debug_log(f"Skip cleanup outside TMP_DIR: {path}")
            continue

        try:
            mtime = os.path.getmtime(path)
        except Exception:
            continue

        if mtime > threshold_ts:
            continue

        size_bytes = _dir_size_bytes(path)
        shutil.rmtree(path, ignore_errors=True)
        if not os.path.exists(path):
            removed_dirs += 1
            freed_bytes += size_bytes
            debug_log(f"Removed stale tmp dir: {path}")

    freed_mb = freed_bytes / (1024 * 1024)
    log(f"TMP cleanup: removed {removed_dirs} directories, freed approx {freed_mb:.1f} MB")


def safe_name(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:180]


def _load_json_file(path: str, label: str, backup_on_failure: bool):
    if not os.path.exists(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as ex:
        if backup_on_failure:
            bak = f"{path}.bak.{int(time.time())}"
            try:
                os.replace(path, bak)
                log(f"{label} parse failed, moved to backup: {bak} ({ex})")
            except Exception as bak_ex:
                log(f"{label} parse failed and backup failed: {ex}; backup error: {bak_ex}")
        else:
            log(f"Failed to load {label}: {ex}; starting fresh")
        return None


def _parse_seen_payload(data):
    if isinstance(data, dict):
        return data

    converted = {}
    if isinstance(data, list):
        for item in data:
            if isinstance(item, str):
                converted[item] = {}
            elif isinstance(item, dict):
                vid = item.get("vid") or item.get("id") or item.get("video_id")
                if vid:
                    converted[str(vid)] = item
    elif data is not None:
        log(f"Unexpected seen.json format: {type(data).__name__}; using empty state")

    if converted:
        log(f"Converted seen.json to dict format with {len(converted)} entries")
    return converted


def _parse_channel_state_payload(data):
    state = {}
    if not isinstance(data, dict):
        return state

    for cid, meta in data.items():
        if isinstance(meta, dict):
            state[str(cid)] = {
                "last_checked_at": int(meta.get("last_checked_at") or 0),
                "fail_count": int(meta.get("fail_count") or 0),
            }
    return state


def _seen_sort_key(item):
    vid, meta = item
    if isinstance(meta, dict):
        dt = str(meta.get("downloaded_at") or "")
    else:
        dt = ""
    return (dt, str(vid))


class StateStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen (
                    video_id TEXT PRIMARY KEY,
                    title TEXT,
                    channel TEXT,
                    url TEXT,
                    downloaded_at TEXT,
                    skipped_reason TEXT,
                    skipped_at TEXT,
                    published_date TEXT,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_state (
                    channel_id TEXT PRIMARY KEY,
                    last_checked_at INTEGER NOT NULL DEFAULT 0,
                    fail_count INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                )
                """
            )
        self._migrate_seen_schema_if_needed()


    def _migrate_seen_schema_if_needed(self):
        cols = {
            row["name"]: row
            for row in self.conn.execute("PRAGMA table_info(seen)").fetchall()
        }
        required = ["title", "channel", "url", "downloaded_at", "skipped_reason", "skipped_at", "published_date"]
        self._seen_has_metadata_json = "metadata_json" in cols

        with self.conn:
            for col in required:
                if col not in cols:
                    self.conn.execute(f"ALTER TABLE seen ADD COLUMN {col} TEXT")

        if "metadata_json" not in cols:
            return

        rows = self.conn.execute("SELECT video_id, metadata_json FROM seen").fetchall()
        with self.conn:
            for row in rows:
                meta = {}
                try:
                    decoded = json.loads(row["metadata_json"])
                    if isinstance(decoded, dict):
                        meta = decoded
                except Exception:
                    pass

                self.conn.execute(
                    """
                    UPDATE seen
                    SET
                        title=COALESCE(title, ?),
                        channel=COALESCE(channel, ?),
                        url=COALESCE(url, ?),
                        downloaded_at=COALESCE(downloaded_at, ?),
                        skipped_reason=COALESCE(skipped_reason, ?),
                        skipped_at=COALESCE(skipped_at, ?),
                        published_date=COALESCE(published_date, ?)
                    WHERE video_id = ?
                    """,
                    (
                        meta.get("title"),
                        meta.get("channel"),
                        meta.get("url"),
                        meta.get("downloaded_at"),
                        meta.get("skipped_reason"),
                        meta.get("skipped_at"),
                        meta.get("published_date"),
                        row["video_id"],
                    ),
                )

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    def load_seen(self):
        rows = self.conn.execute(
            """
            SELECT video_id, title, channel, url, downloaded_at, skipped_reason, skipped_at, published_date
            FROM seen
            """
        ).fetchall()
        result = {}
        for row in rows:
            meta = {}
            for key in ("title", "channel", "url", "downloaded_at", "skipped_reason", "skipped_at", "published_date"):
                value = row[key]
                if value is not None:
                    meta[key] = value
            result[row["video_id"]] = meta
        return result

    def has_seen(self, video_id: str) -> bool:
        row = self.conn.execute("SELECT 1 FROM seen WHERE video_id = ? LIMIT 1", (str(video_id),)).fetchone()
        return row is not None

    def _upsert_seen_row(self, video_id: str, payload: dict, now: int):
        if getattr(self, "_seen_has_metadata_json", False):
            self.conn.execute(
                """
                INSERT INTO seen(
                    video_id, title, channel, url, downloaded_at, skipped_reason, skipped_at, published_date, metadata_json, updated_at
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    title=excluded.title,
                    channel=excluded.channel,
                    url=excluded.url,
                    downloaded_at=excluded.downloaded_at,
                    skipped_reason=excluded.skipped_reason,
                    skipped_at=excluded.skipped_at,
                    published_date=excluded.published_date,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                (
                    str(video_id),
                    payload.get("title"),
                    payload.get("channel"),
                    payload.get("url"),
                    payload.get("downloaded_at"),
                    payload.get("skipped_reason"),
                    payload.get("skipped_at"),
                    payload.get("published_date"),
                    json.dumps(payload, ensure_ascii=False),
                    now,
                ),
            )
            return

        self.conn.execute(
            """
            INSERT INTO seen(
                video_id, title, channel, url, downloaded_at, skipped_reason, skipped_at, published_date, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(video_id) DO UPDATE SET
                title=excluded.title,
                channel=excluded.channel,
                url=excluded.url,
                downloaded_at=excluded.downloaded_at,
                skipped_reason=excluded.skipped_reason,
                skipped_at=excluded.skipped_at,
                published_date=excluded.published_date,
                updated_at=excluded.updated_at
            """,
            (
                str(video_id),
                payload.get("title"),
                payload.get("channel"),
                payload.get("url"),
                payload.get("downloaded_at"),
                payload.get("skipped_reason"),
                payload.get("skipped_at"),
                payload.get("published_date"),
                now,
            ),
        )

    def upsert_seen_item(self, video_id: str, meta, seen_max_items=5000):
        seen_max_items = max(1, int(seen_max_items))
        payload = meta if isinstance(meta, dict) else {}
        now = int(time.time())
        with self.conn:
            self._upsert_seen_row(video_id, payload, now)
        self.prune_seen(seen_max_items)

    def save_seen(self, seen, seen_max_items=5000):
        seen_max_items = max(1, int(seen_max_items))
        now = int(time.time())
        with self.conn:
            keep_ids = {str(vid) for vid in seen.keys()}
            if keep_ids:
                placeholders = ",".join("?" for _ in keep_ids)
                self.conn.execute(f"DELETE FROM seen WHERE video_id NOT IN ({placeholders})", tuple(keep_ids))
            else:
                self.conn.execute("DELETE FROM seen")

            for vid, meta in seen.items():
                payload = meta if isinstance(meta, dict) else {}
                self._upsert_seen_row(str(vid), payload, now)
        self.prune_seen(seen_max_items)

    def prune_seen(self, seen_max_items=5000):
        seen_max_items = max(1, int(seen_max_items))
        total = int(self.conn.execute("SELECT COUNT(*) FROM seen").fetchone()[0])
        overflow = total - seen_max_items
        if overflow <= 0:
            return

        with self.conn:
            self.conn.execute(
                """
                DELETE FROM seen
                WHERE video_id IN (
                    SELECT video_id
                    FROM seen
                    ORDER BY
                        COALESCE(downloaded_at, ''),
                        video_id
                    LIMIT ?
                )
                """,
                (overflow,),
            )

        log(f"Pruned seen entries: removed={overflow}, kept={total - overflow}")

    def load_channel_state(self, channel_ids):
        rows = self.conn.execute("SELECT channel_id, last_checked_at, fail_count FROM channel_state").fetchall()
        state = {
            row["channel_id"]: {
                "last_checked_at": int(row["last_checked_at"] or 0),
                "fail_count": int(row["fail_count"] or 0),
            }
            for row in rows
        }
        changed = False
        for cid in channel_ids:
            if cid not in state:
                state[cid] = {"last_checked_at": 0, "fail_count": 0}
                changed = True
        if changed:
            self.save_channel_state(state)
        return state

    def save_channel_state(self, state):
        now = int(time.time())
        with self.conn:
            keep_ids = {str(cid) for cid in state.keys()}
            if keep_ids:
                placeholders = ",".join("?" for _ in keep_ids)
                self.conn.execute(f"DELETE FROM channel_state WHERE channel_id NOT IN ({placeholders})", tuple(keep_ids))
            else:
                self.conn.execute("DELETE FROM channel_state")

            for cid, meta in state.items():
                payload = meta if isinstance(meta, dict) else {}
                self.conn.execute(
                    """
                    INSERT INTO channel_state(channel_id, last_checked_at, fail_count, updated_at)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(channel_id) DO UPDATE SET
                        last_checked_at=excluded.last_checked_at,
                        fail_count=excluded.fail_count,
                        updated_at=excluded.updated_at
                    """,
                    (str(cid), int(payload.get("last_checked_at") or 0), int(payload.get("fail_count") or 0), now),
                )

    def touch_channel_checked(self, channel_id: str, checked_at: int | None = None):
        ts = int(time.time()) if checked_at is None else int(checked_at)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO channel_state(channel_id, last_checked_at, fail_count, updated_at)
                VALUES(?, ?, 0, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    last_checked_at=excluded.last_checked_at,
                    updated_at=excluded.updated_at
                """,
                (str(channel_id), ts, ts),
            )


_STATE_STORE = None


def get_state_store() -> StateStore:
    global _STATE_STORE
    if _STATE_STORE is None:
        _STATE_STORE = StateStore(STATE_DB_PATH)
    return _STATE_STORE




def close_state_store():
    global _STATE_STORE
    if _STATE_STORE is None:
        return
    try:
        _STATE_STORE.close()
    finally:
        _STATE_STORE = None

def run_state_migration_if_needed():
    os.makedirs(os.path.dirname(STATE_DB_PATH), exist_ok=True)

    db_exists = os.path.exists(STATE_DB_PATH)
    seen_exists = os.path.exists(SEEN_JSON_PATH)
    channel_exists = os.path.exists(CHANNEL_STATE_JSON_PATH)

    if db_exists:
        return

    if not (seen_exists or channel_exists):
        return

    log("State migration: detected legacy JSON state and no SQLite DB; starting migration")

    seen_payload = _load_json_file(SEEN_JSON_PATH, "seen.json", backup_on_failure=True)
    channel_payload = _load_json_file(CHANNEL_STATE_JSON_PATH, "channel_state.json", backup_on_failure=False)

    seen_state = _parse_seen_payload(seen_payload)
    channel_state = _parse_channel_state_payload(channel_payload)

    store = StateStore(STATE_DB_PATH)
    try:
        if seen_state:
            store.save_seen(dict(seen_state), seen_max_items=max(1, len(seen_state)))
        if channel_state:
            store.save_channel_state(channel_state)
    finally:
        store.close()

    suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    for path in (SEEN_JSON_PATH, CHANNEL_STATE_JSON_PATH):
        if os.path.exists(path):
            migrated_path = f"{path}.migrated.{suffix}"
            try:
                os.replace(path, migrated_path)
                log(f"Renamed migrated legacy state file: {migrated_path}")
            except Exception as ex:
                log(f"Failed to rename migrated state file {path}: {ex}")

    log(
        f"State migration completed: seen={len(seen_state)}, channel_state={len(channel_state)}, db={STATE_DB_PATH}"
    )


def select_channels(channel_ids, state, channels_per_run):
    if not channel_ids:
        return []
    ordered = sorted(channel_ids, key=lambda cid: int(state.get(cid, {}).get("last_checked_at", 0)))
    if channels_per_run is None:
        return ordered
    return ordered[:max(0, channels_per_run)]

def run_once(cmd):
    log("RUN: " + " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        log("STDOUT:\n" + (p.stdout[-2000:] if p.stdout else ""))
        log("STDERR:\n" + (p.stderr[-2000:] if p.stderr else ""))
        raise RuntimeError(f"Command failed: {cmd[0]}")
    return p.stdout


def run(cmd):
    return with_retries(f"Command {' '.join(cmd[:2])}", lambda: run_once(cmd))

def _thumb_rank(item):
    if not isinstance(item, dict):
        return (-1, 0)

    url = str(item.get("url") or "")
    url_no_query = url.lower().split("?", 1)[0]
    jpg_bonus = 1 if url_no_query.endswith((".jpg", ".jpeg")) else 0

    try:
        h = int(item.get("height") or 0)
    except (TypeError, ValueError):
        h = 0
    try:
        w = int(item.get("width") or 0)
    except (TypeError, ValueError):
        w = 0

    return (h * w, jpg_bonus)


def pick_thumbnail_url(j):
    thumbs = j.get("thumbnails")
    if isinstance(thumbs, list) and thumbs:
        best = max(thumbs, key=_thumb_rank)
        best_url = best.get("url")
        if best_url:
            return str(best_url)

    thumb = j.get("thumbnail")
    return str(thumb) if thumb else None


def yt_meta(url: str):
    # Получаем метаданные одним вызовом yt-dlp
    out = run(["yt-dlp", "-J", "--no-warnings", "--no-playlist", url])
    j = json.loads(out)
    return {
        "id": j.get("id"),
        "title": j.get("title") or "",
        "channel": j.get("channel") or j.get("uploader") or "",
        "upload_date": j.get("upload_date"),  # YYYYMMDD
        "thumbnail": pick_thumbnail_url(j),
        "description": j.get("description") or "",
        "webpage_url": j.get("webpage_url") or url,
        "is_live": j.get("is_live"),
        "live_status": j.get("live_status"),
        "duration": j.get("duration"),
        "availability": j.get("availability"),
    }


def should_download(meta, min_duration_sec, max_duration_sec):
    live_status = str(meta.get("live_status") or "").lower()
    active_live_statuses = {"is_live", "live", "is_upcoming", "upcoming"}
    if bool(meta.get("is_live")) or live_status in active_live_statuses:
        log(
            f"Skip (live/upcoming): {meta.get('webpage_url')} "
            f"is_live={meta.get('is_live')} live_status={meta.get('live_status')}"
        )
        return False

    duration = meta.get("duration")
    try:
        duration_sec = int(duration)
    except (TypeError, ValueError):
        duration_sec = 0

    if duration_sec <= 0:
        log(f"Skip (invalid duration={duration}): {meta.get('webpage_url')}")
        return False

    if duration_sec < min_duration_sec:
        log(f"Skip (duration {duration_sec}s < min_duration_sec={min_duration_sec}): {meta.get('webpage_url')}")
        return False

    if max_duration_sec is not None and duration_sec > max_duration_sec:
        log(f"Skip (duration {duration_sec}s > max_duration_sec={max_duration_sec}): {meta.get('webpage_url')}")
        return False

    return True

def extract_vid(entry) -> str | None:
    yt_videoid = entry.get("yt_videoid")
    if yt_videoid:
        return str(yt_videoid)

    entry_id = entry.get("id")
    if entry_id:
        match = re.match(r"^yt:video:([A-Za-z0-9_-]{6,})$", str(entry_id))
        if match:
            return match.group(1)

    link = entry.get("link")
    if link:
        parsed = urlparse(str(link))
        vid = parse_qs(parsed.query).get("v", [None])[0]
        if vid:
            return vid

    return None


def get_entry_publish_dt(entry) -> datetime | None:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if not parsed:
        return None

    try:
        ts_utc = calendar.timegm(parsed)
    except Exception:
        return None

    return datetime.utcfromtimestamp(ts_utc)


def parse_min_publish_date(cfg_value) -> date | None:
    if cfg_value is None:
        return None

    raw = str(cfg_value).strip()
    if not raw:
        return None

    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        log(f"Invalid min_publish_date value '{cfg_value}', expected YYYY-MM-DD; feature disabled")
        return None

def download_audio(url: str, outdir: str, basename: str) -> str:
    os.makedirs(outdir, exist_ok=True)
    # yt-dlp сам вытащит лучшее аудио и сконвертит в mp3 через ffmpeg
    template = os.path.join(outdir, basename + ".%(ext)s")
    run([
        "yt-dlp",
        "-f", "bestaudio/best",
        "--no-playlist",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "-o", template,
        url
    ])
    # Найдём получившийся mp3
    mp3 = os.path.join(outdir, basename + ".mp3")
    if not os.path.exists(mp3):
        # на всякий случай: ищем любой mp3 с этим basename
        for fn in os.listdir(outdir):
            if fn.startswith(basename) and fn.endswith(".mp3"):
                return os.path.join(outdir, fn)
        raise FileNotFoundError("mp3 not found after download")
    return mp3

def fetch_bytes(url: str) -> bytes:
    def _do_fetch():
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.content

    return with_retries(f"Fetch failed: {url}", _do_fetch)

def to_jpeg_bytes(img_bytes: bytes) -> bytes:
    im = Image.open(BytesIO(img_bytes))
    if im.mode in ("RGBA", "P"):
        im = im.convert("RGB")
    out = BytesIO()
    im.save(out, format="JPEG", quality=92, optimize=True)
    return out.getvalue()

def is_jpeg_bytes(data: bytes | None) -> bool:
    return bool(data) and data[:3] == b"\xff\xd8\xff"


def set_tags_mp3(path: str, title: str, artist: str, album: str, date_str: str|None, lyrics: str, cover_jpg: bytes|None):
    audio = MP3(path, ID3=ID3)
    debug_log("set_tags_mp3: ensuring ID3 tag container")
    try:
        audio.add_tags()
    except Exception:
        pass
    debug_log("set_tags_mp3: clearing APIC/USLT")
    tags = audio.tags
    tags.delall("APIC")
    tags.delall("USLT")
    debug_log("set_tags_mp3: writing text frames")
    tags.add(TIT2(encoding=3, text=title))
    tags.add(TPE1(encoding=3, text=artist))
    tags.add(TALB(encoding=3, text=album))
    if date_str:
        tags.add(TDRC(encoding=3, text=date_str))

    if lyrics:
        tags.add(USLT(encoding=3, lang="rus", desc="desc", text=lyrics))
    if cover_jpg:
        cover_mime = "image/jpeg" if is_jpeg_bytes(cover_jpg) else "application/octet-stream"
        if cover_mime != "image/jpeg":
            log("Cover bytes are not JPEG; writing APIC with generic mime")
        tags.add(APIC(
            encoding=3,
            mime=cover_mime,
            type=3,
            desc="cover",
            data=cover_jpg
        ))

    #if cover_jpg and isinstance(cover_jpg, (bytes, bytearray)) and len(cover_jpg) > 0:
    #    try:
    #        tags.add(APIC(
    #            encoding=3,
    #            mime="image/jpeg",
    #            type=3,
    #            desc="cover",
    #            data=cover_jpg
    #        ))
    #    except Exception as ex:
    #        log(f"APIC failed: {ex}")
    audio.save(v2_version=3)
    debug_log("set_tags_mp3: save complete")

def main():
    global DEBUG, RETRIES, RETRY_BACKOFF_SEC
    os.makedirs(TMP_DIR, exist_ok=True)

    run_state_migration_if_needed()

    cfg_path = os.path.join(BASE, "config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    DEBUG = bool(cfg.get("debug", False))
    RETRIES = int(cfg.get("retries", 2))
    RETRY_BACKOFF_SEC = int(cfg.get("retry_backoff_sec", 5))

    download_per_run = int(cfg.get("download_per_run", 2))
    randomize_feeds = bool(cfg.get("randomize_feeds", True))
    library_dir = cfg["library_dir"]
    channel_ids = cfg.get("channel_ids", [])
    channels_per_run_raw = cfg.get("channels_per_run")
    channels_per_run = None if channels_per_run_raw is None else int(channels_per_run_raw)
    max_per_feed = int(cfg.get("max_per_feed", 20))
    min_duration_sec = int(cfg.get("min_duration_sec", 30))
    max_duration_sec_raw = cfg.get("max_duration_sec", 46060)
    max_duration_sec = None if max_duration_sec_raw is None else int(max_duration_sec_raw)
    jitter_sec = float(cfg.get("jitter_sec", 0.0))
    seen_max_items = int(cfg.get("seen_max_items", 5000))
    tmp_max_age_hours = int(cfg.get("tmp_max_age_hours", 24))
    min_publish_date = parse_min_publish_date(cfg.get("min_publish_date"))

    cleanup_tmp_dir(max_age_hours=tmp_max_age_hours)

    os.makedirs(library_dir, exist_ok=True)
    state_store = get_state_store()
    channel_state = state_store.load_channel_state(channel_ids)

    selected_channels = select_channels(channel_ids, channel_state, channels_per_run)
    if randomize_feeds:
        random.shuffle(selected_channels)

    total_new = 0
    channels_checked = 0
    skipped_by_reason = defaultdict(int)

    def finish_run():
        reason_parts = ", ".join(f"{k}={v}" for k, v in sorted(skipped_by_reason.items())) or "none"
        log(
            f"Summary: channels_checked={channels_checked}/{len(selected_channels)}, "
            f"downloaded={total_new}, skipped={{ {reason_parts} }}"
        )
        if jitter_sec > 0:
            jitter_sleep = random.uniform(0, jitter_sec)
            log(f"Jitter sleep: {jitter_sleep:.2f}s")
            time.sleep(jitter_sleep)

    for channel_id in selected_channels:
        channels_checked += 1
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        log(f"Feed: {feed_url}")
        d = feedparser.parse(feed_url)
        #log(f"d = {d}")
        entries = d.entries[:max_per_feed]

        for e in entries:
            url = getattr(e, "link", None)
            if not url:
                skipped_by_reason["entry_without_link"] += 1
                continue
            log(f"Link: {url}")
            # ключ дедупликации — видео id, если нет — хэш ссылки
            vid = extract_vid(e)
            if not vid:
                log(f"Could not extract video id from entry: {url}")

            if vid and state_store.has_seen(vid):
                skipped_by_reason["already_seen"] += 1
                continue

            entry_publish_dt = get_entry_publish_dt(e)
            entry_publish_date = entry_publish_dt.date() if entry_publish_dt else None

            if min_publish_date and vid and entry_publish_date and entry_publish_date < min_publish_date:
                state_store.upsert_seen_item(vid, {
                    "url": url,
                    "skipped_reason": "older_than_min_publish_date",
                    "skipped_at": datetime.now().isoformat(timespec="seconds"),
                    "published_date": entry_publish_date.isoformat(),
                    "title": str(getattr(e, "title", "") or "") or None,
                    "channel": str(getattr(e, "author", "") or "") or None,
                }, seen_max_items=seen_max_items)
                skipped_by_reason["older_than_min_publish_date"] += 1
                continue

            try:
                meta = yt_meta(url)
            except Exception as ex:
                skipped_by_reason["meta_failed"] += 1
                log(f"Meta failed: {url} -> {ex}")
                continue

            vid = vid or meta["id"] or hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]

            if state_store.has_seen(vid):
                skipped_by_reason["already_seen"] += 1
                continue

            if min_publish_date and entry_publish_date and entry_publish_date < min_publish_date:
                state_store.upsert_seen_item(vid, {
                    "url": meta.get("webpage_url") or url,
                    "skipped_reason": "older_than_min_publish_date",
                    "skipped_at": datetime.now().isoformat(timespec="seconds"),
                    "published_date": entry_publish_date.isoformat(),
                    "title": meta.get("title") or str(getattr(e, "title", "") or "") or None,
                    "channel": meta.get("channel") or str(getattr(e, "author", "") or "") or None,
                }, seen_max_items=seen_max_items)
                skipped_by_reason["older_than_min_publish_date"] += 1
                continue

            if min_publish_date and not entry_publish_date:
                upload_date = str(meta.get("upload_date") or "")
                meta_publish_date = None
                if len(upload_date) == 8 and upload_date.isdigit():
                    try:
                        meta_publish_date = datetime.strptime(upload_date, "%Y%m%d").date()
                    except ValueError:
                        skipped_by_reason["invalid_upload_date"] += 1
                        log(f"Invalid upload_date in metadata: {upload_date} ({url})")

                if meta_publish_date and meta_publish_date < min_publish_date:
                    state_store.upsert_seen_item(vid, {
                        "url": meta.get("webpage_url") or url,
                        "skipped_reason": "older_than_min_publish_date",
                        "skipped_at": datetime.now().isoformat(timespec="seconds"),
                        "published_date": meta_publish_date.isoformat(),
                        "title": meta.get("title") or None,
                        "channel": meta.get("channel") or None,
                    }, seen_max_items=seen_max_items)
                    skipped_by_reason["older_than_min_publish_date"] += 1
                    continue

            if not should_download(meta, min_duration_sec=min_duration_sec, max_duration_sec=max_duration_sec):
                skipped_by_reason["filtered_out"] += 1
                continue

            if total_new >= download_per_run:
                log(f"Reached download_per_run={download_per_run}, stopping.")
                checked_at = int(time.time())
                channel_state[channel_id]["last_checked_at"] = checked_at
                state_store.touch_channel_checked(channel_id, checked_at)
                finish_run()
                return

            title = meta["title"]
            channel = meta["channel"] or "YouTube"
            upload_date = meta.get("upload_date")
            date_iso = None
            if upload_date and len(upload_date) == 8:
                date_iso = f"{upload_date[0:4]}-{upload_date[4:6]}-{upload_date[6:8]}"

            # финальное имя файла
            base = safe_name(f"{vid}")
            tmp_out = os.path.join(TMP_DIR, base)
            os.makedirs(tmp_out, exist_ok=True)

            mp3_path = None
            try:
                mp3_path = download_audio(meta["webpage_url"], tmp_out, base)
                log(f"{mp3_path}")
                cover = None
                if meta.get("thumbnail"):
                    try:
                        thumb_url = meta["thumbnail"]
                        cover_raw = fetch_bytes(thumb_url)
                        thumb_path = thumb_url.lower().split("?", 1)[0]
                        if thumb_path.endswith((".jpg", ".jpeg")) and is_jpeg_bytes(cover_raw):
                            cover = cover_raw
                            debug_log(f"Cover kept as JPEG: {thumb_url}")
                        else:
                            cover = to_jpeg_bytes(cover_raw)
                            debug_log(f"Cover converted to JPEG: {thumb_url}")
                    except Exception as ex:
                        skipped_by_reason["cover_fetch_or_convert_failed"] += 1
                        log(f"Cover fetch/convert failed: {ex}")
                debug_log("main: cover prepared")
                lyrics = meta.get("description", "")
                if meta.get("webpage_url"):
                    # добавим ссылку в конец, чтобы всегда можно было открыть оригинал
                    lyrics = (lyrics or "").rstrip() + "\n\n" + meta["webpage_url"]
                debug_log("main: writing ID3 tags")
                set_tags_mp3(
                    mp3_path,
                    title=title,
                    artist=channel,
                    album="Polifm",
                    date_str=date_iso,
                    lyrics=lyrics,
                    cover_jpg=cover
                )
                debug_log("main: tags written")
                # атомарное перемещение в библиотеку
                #final_path = os.path.join(library_dir, os.path.basename(mp3_path))

                # формируем имя вида YYYY.MM.DD.<epoch>.mp3

                # дата публикации
                if date_iso:
                    pub = date_iso.replace("-", ".")
                else:
                    pub = datetime.now().strftime("%Y.%m.%d")

                epoch = int(time.time())
                final_name = f"{pub}-{epoch}.mp3"

                final_path = os.path.join(library_dir, final_name)

                tmp_final = final_path + ".tmp"
                # copy+replace на случай разных FS
                with open(mp3_path, "rb") as src, open(tmp_final, "wb") as dst:
                    while True:
                        b = src.read(1024 * 1024)
                        if not b:
                            break
                        dst.write(b)
                os.replace(tmp_final, final_path)
                cleanup_tmp_out(tmp_out, mp3_path=mp3_path)

                state_store.upsert_seen_item(vid, {
                    "title": title,
                    "channel": channel,
                    "url": meta["webpage_url"],
                    "downloaded_at": datetime.now().isoformat(timespec="seconds")
                }, seen_max_items=seen_max_items)

                total_new += 1
                log(f"OK: {final_path}")

                # уведомление на телефон (если установлен termux-api)
                try:
                    subprocess.run(["termux-notification", "--title", "YT Audio", "--content", title], check=False)
                except Exception as ex:
                    debug_log(f"Notification failed: {ex}")

            except Exception as ex:
                cleanup_tmp_out(tmp_out, mp3_path=mp3_path)
                skipped_by_reason["download_or_tag_failed"] += 1
                log(f"FAIL: {url} -> {ex}")

        checked_at = int(time.time())
        channel_state[channel_id]["last_checked_at"] = checked_at
        state_store.touch_channel_checked(channel_id, checked_at)

    finish_run()

if __name__ == "__main__":
    with single_instance_lock() as acquired:
        try:
            if acquired:
                main()
        finally:
            close_state_store()
