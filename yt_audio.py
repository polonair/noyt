#!/usr/bin/env python3
import os, json, re, time, hashlib, subprocess
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

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")

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

def save_seen(seen):
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
    return ordered[:max(0, channels_per_run)]

def run(cmd):
    log("RUN: " + " ".join(cmd))
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    if p.returncode != 0:
        log("STDOUT:\n" + (p.stdout[-2000:] if p.stdout else ""))
        log("STDERR:\n" + (p.stderr[-2000:] if p.stderr else ""))
        raise RuntimeError(f"Command failed: {cmd[0]}")
    return p.stdout

def yt_meta(url: str):
    # Получаем метаданные одним вызовом yt-dlp
    out = run(["yt-dlp", "-J", "--no-warnings", "--no-playlist", url])
    j = json.loads(out)
    return {
        "id": j.get("id"),
        "title": j.get("title") or "",
        "channel": j.get("channel") or j.get("uploader") or "",
        "upload_date": j.get("upload_date"),  # YYYYMMDD
        "thumbnail": j.get("thumbnail") or (j.get("thumbnails")[-1]["url"] if j.get("thumbnails") else None),
        "description": j.get("description") or "",
        "webpage_url": j.get("webpage_url") or url
    }

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
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.content

def to_jpeg_bytes(img_bytes: bytes) -> bytes:
    im = Image.open(BytesIO(img_bytes))
    if im.mode in ("RGBA", "P"):
        im = im.convert("RGB")
    out = BytesIO()
    im.save(out, format="JPEG", quality=92, optimize=True)
    return out.getvalue()

def set_tags_mp3(path: str, title: str, artist: str, album: str, date_str: str|None, lyrics: str, cover_jpg: bytes|None):
    audio = MP3(path, ID3=ID3)
    log("2.1")
    try:
        audio.add_tags()
    except Exception:
        pass
    log("2.2")
    tags = audio.tags
    tags.delall("APIC")
    tags.delall("USLT")
    log("2.3")
    tags.add(TIT2(encoding=3, text=title))
    tags.add(TPE1(encoding=3, text=artist))
    tags.add(TALB(encoding=3, text=album))
    if date_str:
        tags.add(TDRC(encoding=3, text=date_str))

    if lyrics:
        tags.add(USLT(encoding=3, lang="rus", desc="desc", text=lyrics))
    if cover_jpg:
        tags.add(APIC(
            encoding=3,
            mime="image/jpeg",
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
    log("2.L")
    audio.save(v2_version=3)
    log("2.L+1")

def main():
    os.makedirs(TMP_DIR, exist_ok=True)

    cfg_path = os.path.join(BASE, "config.json")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    download_per_run = int(cfg.get("download_per_run", 2))
    channels_per_run = int(cfg.get("channels_per_run", 3))
    randomize_feeds = bool(cfg.get("randomize_feeds", True))

    library_dir = cfg["library_dir"]

    channel_ids = cfg.get("channel_ids", [])
    max_per_feed = int(cfg.get("max_per_feed", 20))

    os.makedirs(library_dir, exist_ok=True)
    seen = load_seen()
    channel_state = load_channel_state(channel_ids)

    selected_channels = select_channels(channel_ids, channel_state, channels_per_run)
    if randomize_feeds:
        random.shuffle(selected_channels)

    total_new = 0

    for channel_id in selected_channels:
        feed_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        log(f"Feed: {feed_url}")
        d = feedparser.parse(feed_url)
        #log(f"d = {d}")
        entries = d.entries[:max_per_feed]

        for e in entries:
            url = getattr(e, "link", None)
            if not url:
                continue
            log(f"Link: {url}")
            # ключ дедупликации — видео id, если нет — хэш ссылки
            vid = extract_vid(e)
            if not vid:
                log(f"Could not extract video id from entry: {url}")

            if vid and vid in seen:
                continue

            try:
                meta = yt_meta(url)
            except Exception as ex:
                log(f"Meta failed: {url} -> {ex}")
                continue

            vid = vid or meta["id"] or hashlib.sha1(url.encode("utf-8")).hexdigest()[:12]

            if vid in seen:
                continue

            if total_new >= download_per_run:
                log(f"Reached download_per_run={download_per_run}, stopping.")
                channel_state[channel_id]["last_checked_at"] = int(time.time())
                save_channel_state(channel_state)
                log(f"Done. New items: {total_new}")
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
                        cover = fetch_bytes(meta["thumbnail"])
                    except Exception as ex:
                        log(f"Cover fetch failed: {ex}")
                log("1")
                lyrics = meta.get("description", "")
                if meta.get("webpage_url"):
                    # добавим ссылку в конец, чтобы всегда можно было открыть оригинал
                    lyrics = (lyrics or "").rstrip() + "\n\n" + meta["webpage_url"]
                log("2")
                set_tags_mp3(
                    mp3_path,
                    title=title,
                    artist=channel,
                    album="Polifm",
                    date_str=date_iso,
                    lyrics=lyrics,
                    cover_jpg=cover
                )
                log("3")
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
                log("4")
                # copy+replace на случай разных FS
                with open(mp3_path, "rb") as src, open(tmp_final, "wb") as dst:
                    while True:
                        b = src.read(1024 * 1024)
                        if not b:
                            break
                        dst.write(b)
                log("5")
                os.replace(tmp_final, final_path)

                seen[vid] = {
                    "title": title,
                    "channel": channel,
                    "url": meta["webpage_url"],
                    "downloaded_at": datetime.now().isoformat(timespec="seconds")
                }
                save_seen(seen)

                total_new += 1
                log(f"OK: {final_path}")

                # уведомление на телефон (если установлен termux-api)
                #try:
                #    subprocess.run(["termux-notification", "--title", "YT Audio", "--content", title], check=False)
                #except Exception:
                #    pass

            except Exception as ex:
                log(f"FAIL: {url} -> {ex}")

        channel_state[channel_id]["last_checked_at"] = int(time.time())
        save_channel_state(channel_state)

    log(f"Done. New items: {total_new}")

if __name__ == "__main__":
    main()
