#!/usr/bin/env python3
import os, json, re, time, hashlib, subprocess
from collections import defaultdict
from urllib.parse import parse_qs, urlparse
from io import BytesIO
from PIL import Image
from datetime import datetime
import feedparser
import requests
from mutagen.id3 import ID3, APIC, USLT, TIT2, TPE1, TALB, TDRC
from mutagen.mp3 import MP3
import random

BASE = os.path.expanduser("~/yt-audio")
DB_PATH = os.path.join(BASE, "data", "seen.json")
CHANNEL_STATE_PATH = os.path.join(BASE, "data", "channel_state.json")
LOG_PATH = os.path.join(BASE, "logs", "run.log")
TMP_DIR  = os.path.join(BASE, "tmp")
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


def safe_name(s: str) -> str:
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s[:180]

def load_seen():
    if not os.path.exists(DB_PATH):
        return {}

    try:
        with open(DB_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as ex:
        bak = f"{DB_PATH}.bak.{int(time.time())}"
        try:
            os.replace(DB_PATH, bak)
            log(f"seen.json parse failed, moved to backup: {bak} ({ex})")
        except Exception as bak_ex:
            log(f"seen.json parse failed and backup failed: {ex}; backup error: {bak_ex}")
        return {}

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
    else:
        log(f"Unexpected seen.json format: {type(data).__name__}; using empty state")

    if converted:
        log(f"Converted seen.json to dict format with {len(converted)} entries")
    return converted

def _seen_sort_key(item):
    vid, meta = item
    if isinstance(meta, dict):
        dt = str(meta.get("downloaded_at") or "")
    else:
        dt = ""
    return (dt, str(vid))


def save_seen(seen, seen_max_items=5000):
    seen_max_items = max(1, int(seen_max_items))

    if len(seen) > seen_max_items:
        overflow = len(seen) - seen_max_items
        oldest = sorted(seen.items(), key=_seen_sort_key)[:overflow]
        for vid, _ in oldest:
            seen.pop(vid, None)
        log(f"Pruned seen.json entries: removed={overflow}, kept={len(seen)}")

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    tmp = DB_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(seen, f, ensure_ascii=False, indent=2)
    os.replace(tmp, DB_PATH)

def load_channel_state(channel_ids):
    state = {}
    if os.path.exists(CHANNEL_STATE_PATH):
        try:
            with open(CHANNEL_STATE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                for cid, meta in data.items():
                    if isinstance(meta, dict):
                        state[cid] = {
                            "last_checked_at": int(meta.get("last_checked_at") or 0),
                            "fail_count": int(meta.get("fail_count") or 0),
                        }
        except Exception as ex:
            log(f"Failed to load channel_state.json: {ex}; starting fresh")

    changed = False
    for cid in channel_ids:
        if cid not in state:
            state[cid] = {"last_checked_at": 0, "fail_count": 0}
            changed = True

    if changed or not os.path.exists(CHANNEL_STATE_PATH):
        save_channel_state(state)

    return state


def save_channel_state(state):
    os.makedirs(os.path.dirname(CHANNEL_STATE_PATH), exist_ok=True)
    tmp = CHANNEL_STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHANNEL_STATE_PATH)


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

    os.makedirs(library_dir, exist_ok=True)
    seen = load_seen()
    channel_state = load_channel_state(channel_ids)

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

            if vid and vid in seen:
                skipped_by_reason["already_seen"] += 1
                continue

            try:
                meta = yt_meta(url)
            except Exception as ex:
                skipped_by_reason["meta_failed"] += 1
                log(f"Meta failed: {url} -> {ex}")
                continue

            vid = vid or meta["id"] or hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]

            if vid in seen:
                skipped_by_reason["already_seen"] += 1
                continue

            if not should_download(meta, min_duration_sec=min_duration_sec, max_duration_sec=max_duration_sec):
                skipped_by_reason["filtered_out"] += 1
                continue

            if total_new >= download_per_run:
                log(f"Reached download_per_run={download_per_run}, stopping.")
                channel_state[channel_id]["last_checked_at"] = int(time.time())
                save_channel_state(channel_state)
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

                seen[vid] = {
                    "title": title,
                    "channel": channel,
                    "url": meta["webpage_url"],
                    "downloaded_at": datetime.now().isoformat(timespec="seconds")
                }
                save_seen(seen, seen_max_items=seen_max_items)

                total_new += 1
                log(f"OK: {final_path}")

                # уведомление на телефон (если установлен termux-api)
                try:
                    subprocess.run(["termux-notification", "--title", "YT Audio", "--content", title], check=False)
                except Exception as ex:
                    debug_log(f"Notification failed: {ex}")

            except Exception as ex:
                skipped_by_reason["download_or_tag_failed"] += 1
                log(f"FAIL: {url} -> {ex}")

        channel_state[channel_id]["last_checked_at"] = int(time.time())
        save_channel_state(channel_state)

    finish_run()

if __name__ == "__main__":
    main()
