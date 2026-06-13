import asyncio
import base64
import io
import json
import logging
import os
import secrets
import sqlite3
import subprocess
import tempfile
import threading
import time
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

BASE_DIR = Path(__file__).parent.parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

def _configure_logging() -> None:
    root = logging.getLogger()
    if getattr(root, "_mediapool_configured", False):
        return
    root.handlers.clear()
    root.setLevel(logging.INFO)

    file_handler = RotatingFileHandler(
        LOG_DIR / "mediapool.log",
        maxBytes=5_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s"
    ))

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    root.addHandler(file_handler)
    root.addHandler(console_handler)
    root._mediapool_configured = True
    logging.getLogger("uvicorn.access").disabled = True

_configure_logging()
log = logging.getLogger("mediapool")

from dotenv import load_dotenv
load_dotenv(override=True)   # .env values take precedence over OS environment

from fastapi import BackgroundTasks, Cookie, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, StreamingResponse
from dropbox import files as dbx_files

from server.auth import create_event_token, verify_event_token
from server.dropbox_client import DropboxClient
from server.db import (
    init_db,
    add_or_remove_reaction, get_reactions, get_reactions_by_file,
    increment_view, get_view_counts,
    add_comment, get_comments, delete_comment, get_comment_counts,
    set_media_meta, set_caption, get_media_meta, get_all_captions,
    set_uploader_session, get_uploader_sessions,
    delete_file_data,
    prune_stale_files,
    enqueue_caption_job, claim_caption_job, complete_caption_job, fail_caption_job,
    mark_caption_job_done, reset_stuck_caption_jobs, get_caption_job_stats,
    enqueue_missing_caption_jobs, get_caption_job_statuses,
)

CLIENT_DIR = BASE_DIR / "client"

app = FastAPI(title="Media Pool")

# ── In-memory state ──────────────────────────────────────────────────────────

_sessions: dict[str, dict] = {}
_admin_sessions: set[str] = set()

_groups_cache: dict = {"data": None, "expires": 0.0}
_groups_lock = threading.Lock()  # prevents concurrent Dropbox fetches during cache miss
_list_caches: dict[str, dict] = {}
_thumb_cache: dict[str, bytes] = {}
_missing_thumb_cache: dict[str, float] = {}

# SSE: list of (group_id, asyncio.Queue)
_sse_queues: list[tuple[str, asyncio.Queue]] = []

# Cached event summaries: group_id → summary text
_event_summaries: dict[str, str] = {}

_caption_worker_started = False

# ── Constants ────────────────────────────────────────────────────────────────

SECRET_KEY     = os.getenv("SECRET_KEY")
if not SECRET_KEY:
    SECRET_KEY = secrets.token_hex(32)
    log.warning("SECRET_KEY not configured; generated a temporary key for this process")
ADMIN_PIN      = os.getenv("ADMIN_PIN",   "")
ARENA_URL      = os.getenv("ARENA_URL",   "http://localhost:6059")
LANGUAGE       = "English"   # default when event.json has no "language" field
SESSION_COOKIE = "mp_session"
ADMIN_COOKIE   = "mp_admin"

MEDIA_EXTS     = {"jpg","jpeg","png","gif","webp","heic","heif","mp4","mov","avi","m4v"}
IMAGE_EXTS     = {"jpg","jpeg","png","gif","webp","heic","heif"}
TRANSCODE_EXTS = {"mov","avi","m4v"}
HEIC_EXTS      = {"heic","heif"}


# ── Config helpers ───────────────────────────────────────────────────────────

def _multi_group_mode() -> bool:
    return bool(os.getenv("DROPBOX_GROUPS_ROOT", "").strip())

def _dropbox_folder_single() -> str:
    raw = os.getenv("DROPBOX_FOLDER_PATH", "/MediaPool").replace("\\", "/")
    return raw if raw.startswith("/") else f"/{raw}"

def _db_path(group_id: str) -> str:
    db_dir = BASE_DIR / "data"
    db_dir.mkdir(parents=True, exist_ok=True)
    return str(db_dir / f"{group_id}.db")


# ── Group loading ────────────────────────────────────────────────────────────

def _load_groups() -> dict:
    now = time.time()
    if _groups_cache["expires"] > now and _groups_cache["data"] is not None:
        return _groups_cache["data"]

    with _groups_lock:
        # Re-check after acquiring lock — another thread may have loaded while we waited
        now = time.time()
        if _groups_cache["expires"] > now and _groups_cache["data"] is not None:
            return _groups_cache["data"]

        groups: dict[str, dict] = {}
        dbx = DropboxClient()

        if _multi_group_mode():
            root = os.getenv("DROPBOX_GROUPS_ROOT", "").strip()
            root = root if root.startswith("/") else f"/{root}"
            try:
                result = dbx._dbx.files_list_folder(root)
                folders = [
                    (entry.name, f"{root}/{entry.name}")
                    for entry in result.entries
                    if isinstance(entry, dbx_files.FolderMetadata)
                ]

                def load_group(item):
                    gid, folder = item
                    try:
                        raw = dbx.download_text(f"{folder}/event.json")
                        event = json.loads(raw)
                        return gid, {"folder": folder, "event": event}
                    except Exception as e:
                        log.warning("Skipping group %s: %s", gid, e)
                        return gid, None

                with ThreadPoolExecutor(max_workers=4) as ex:
                    for gid, data in [f.result() for f in as_completed(
                        {ex.submit(load_group, item): item[0] for item in folders}
                    )]:
                        if data:
                            groups[gid] = data
                            log.info("Group loaded: %s  open=%s", gid, _event_is_open(data["event"]))
            except Exception as e:
                log.error("Failed to list groups root %s: %s", root, e)
        else:
            folder = _dropbox_folder_single()
            try:
                raw   = dbx.download_text(f"{folder}/event.json")
                event = json.loads(raw)
                groups["default"] = {"folder": folder, "event": event}
            except Exception as e:
                log.error("Failed to load single-group event: %s", e, exc_info=True)

        elapsed = time.time() - now
        log.info("Groups loaded from Dropbox in %.2fs (%d groups)", elapsed, len(groups))
        _groups_cache.update(data=groups, expires=now + 3600)
        return groups

def _get_group(group_id: str) -> dict | None:
    return _load_groups().get(group_id)


# ── Event helpers ────────────────────────────────────────────────────────────

def _event_is_open(event: dict) -> bool:
    now = datetime.now()
    try:
        vf = datetime.fromisoformat(event.get("valid_from", "1970-01-01T00:00:00"))
        vu = datetime.fromisoformat(event["valid_until"])
        return vf.replace(tzinfo=None) <= now <= vu.replace(tzinfo=None)
    except Exception as e:
        log.error("_event_is_open error: %s", e)
        return False


# ── Session / auth helpers ───────────────────────────────────────────────────

def _require_auth_ctx(mp_session: Optional[str]) -> dict:
    if not mp_session or mp_session not in _sessions:
        raise HTTPException(401, "Not authenticated")
    sess     = _sessions[mp_session]
    group_id = sess.get("group_id", "default")
    group    = _get_group(group_id)
    if not group:
        raise HTTPException(503, "Event group not available")
    event      = group["event"]
    idle_limit = event.get("session_idle_hours", 24) * 3600
    if time.time() - sess["last_seen"] > idle_limit:
        _sessions.pop(mp_session, None)
        raise HTTPException(401, "Session expired")
    _sessions[mp_session]["last_seen"] = time.time()
    return {"group_id": group_id, "folder": group["folder"], "event": event}

def _require_admin(mp_admin: Optional[str] = Cookie(default=None)):
    if not mp_admin or mp_admin not in _admin_sessions:
        raise HTTPException(403, "Admin access required")


# ── LAN IP / public base ─────────────────────────────────────────────────────

_lan_ip_cache: str = ""
_lan_ip_cache_ts: float = 0.0
_LAN_IP_TTL = 3600.0

def _lan_ip() -> str:
    """Best-effort LAN IP for the localhost-access fallback in _public_base().
    In normal use the admin opens the page via a real LAN IP, so the Host header
    is used directly and this function is never called.

    Strategy:
    1. UDP probe to 8.8.8.8 — asks the OS which interface it uses for internet
       traffic.  This naturally returns the active gateway interface (hotspot,
       home router, corporate WiFi) regardless of how many NICs are present.
    2. Hostname enumeration fallback — in case UDP probe fails (no internet,
       strict firewall, etc.).
    """
    global _lan_ip_cache, _lan_ip_cache_ts
    if _lan_ip_cache and time.time() - _lan_ip_cache_ts < _LAN_IP_TTL:
        return _lan_ip_cache

    import socket
    VM_SUBNETS = ("192.168.122.", "192.168.124.", "192.168.136.", "192.168.56.", "192.168.99.")

    def _is_usable(ip: str) -> bool:
        return (not ip.startswith("127.") and not ip.startswith("169.254.")
                and not any(ip.startswith(v) for v in VM_SUBNETS))

    # ── 1. UDP probe (preferred) ──────────────────────────────────────────────
    # Connecting a UDP socket doesn't send any packets; it just asks the OS to
    # fill in the source address from the routing table.  Works offline too.
    probe_ip: str | None = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(1)
        s.connect(("8.8.8.8", 80))
        probe_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass

    if probe_ip and _is_usable(probe_ip):
        _lan_ip_cache = probe_ip
        _lan_ip_cache_ts = time.time()
        return probe_ip

    # ── 2. Hostname enumeration fallback ─────────────────────────────────────
    ips: list[str] = []
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ip = info[4][0]
            if not ip.startswith("127.") and not ip.startswith("169.254."):
                ips.append(ip)
    except Exception:
        pass

    good = [ip for ip in ips if _is_usable(ip)]
    pool = good or ips  # last resort: include VM adapters

    def _score(ip: str) -> int:
        # No arbitrary preference for 10.x over 192.168.x —
        # both are equally valid private ranges.
        if ip.startswith("10.") or ip.startswith("172.") or ip.startswith("192.168."):
            return 2
        return 1

    best = max(pool, key=_score) if pool else "localhost"
    _lan_ip_cache = best
    _lan_ip_cache_ts = time.time()
    return best

def _public_base(req: Request) -> str:
    """Return the base URL to embed in QR codes and join links.

    Priority:
    1. PUBLIC_URL env var — always wins (use for stable hostnames / HTTPS / ngrok).
    2. Host header from the request — the IP the admin's browser actually used.
       If the admin opens the page at http://172.16.x.x:7000/qrs, QR codes get
       that same IP, which is always reachable by phones on the same Wi-Fi.
    3. _lan_ip() auto-detect — fallback only when request came via localhost,
       which is unreachable from phones anyway.
    """
    pub = os.getenv("PUBLIC_URL", "").rstrip("/")
    if pub:
        return pub
    host = req.headers.get("host", "")
    host_ip = host.split(":")[0] if host else ""
    # Trust the Host header when the admin opened via a real LAN IP.
    # (localhost / 127.0.0.1 / 0.0.0.0 → not reachable by phones → fall back.)
    if host_ip and host_ip not in ("localhost", "127.0.0.1", "0.0.0.0", ""):
        return f"{req.url.scheme}://{host}"
    # Localhost access: auto-detect best LAN IP as a best-effort fallback
    port = os.getenv("PORT", "7000")
    return f"{req.url.scheme}://{_lan_ip()}:{port}"


def _is_https_host() -> bool:
    pub = os.getenv("PUBLIC_URL", "")
    return pub.startswith("https://")


# ── QR image ─────────────────────────────────────────────────────────────────

def _make_labeled_qr(url: str, title: str, subtitle: str = "") -> bytes:
    import qrcode
    from PIL import Image, ImageDraw, ImageFont

    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_L, box_size=10, border=4)
    qr.add_data(url); qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    w, h   = qr_img.size

    def _font(size):
        candidates = [
            ("C:/Windows/Fonts/msyh.ttc",    0),   # Microsoft YaHei (Simplified Chinese)
            ("C:/Windows/Fonts/msjh.ttc",    0),   # Microsoft JhengHei (Traditional Chinese)
            ("C:/Windows/Fonts/simsun.ttc",  0),   # SimSun
            ("C:/Windows/Fonts/mingliu.ttc", 0),   # MingLiU (Traditional Chinese)
            ("C:/Windows/Fonts/arial.ttf",   None),
            ("C:/Windows/Fonts/calibri.ttf", None),
            ("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc", 0),
            ("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",        None),
        ]
        probe_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
        for path, idx in candidates:
            if not os.path.exists(path):
                continue
            try:
                f = ImageFont.truetype(path, size, index=idx if idx is not None else 0)
                # Verify the font actually renders CJK — a font can load but lack the glyphs
                bbox = probe_draw.textbbox((0, 0), "中", font=f)
                if (bbox[2] - bbox[0]) > 0:
                    log.info("QR font selected: %s (idx=%s)", path, idx)
                    return f
                log.warning("QR font %s loaded but has no CJK glyphs — skipping", path)
            except Exception as exc:
                log.warning("QR font %s failed to load: %s", path, exc)
        log.error("QR font: no CJK-capable font found — labels will show boxes for non-ASCII text")
        return ImageFont.load_default()

    font_big, font_sm = _font(22), _font(16)
    n_lines = 1 + (1 if subtitle else 0)
    extra_h = n_lines * 34 + 24
    canvas  = Image.new("RGB", (w, h + extra_h), "white")
    canvas.paste(qr_img, (0, 0))
    draw = ImageDraw.Draw(canvas)

    def _center(y, text, font, color):
        try:
            bbox = draw.textbbox((0, 0), text, font=font); tw = bbox[2] - bbox[0]
        except Exception:
            tw = len(text) * 12
        draw.text(((w - tw) // 2, y), text, fill=color, font=font)

    _center(h + 12, title, font_big, (20, 20, 20))
    if subtitle:
        _center(h + 12 + 34, subtitle, font_sm, (100, 100, 100))

    buf = io.BytesIO(); canvas.save(buf, format="PNG"); buf.seek(0)
    return buf.getvalue()


# ── File / media helpers ─────────────────────────────────────────────────────

def _ext(filename: str) -> str:
    return filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

def _extract_video_frame(content: bytes) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=".mp4", delete=False) as fin:
        fin.write(content); in_path = fin.name
    out_path = in_path + ".jpg"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", in_path, "-vf", "scale=640:-2", "-vframes", "1", out_path],
            check=True, capture_output=True,
        )
        return Path(out_path).read_bytes()
    finally:
        os.unlink(in_path)
        if os.path.exists(out_path): os.unlink(out_path)

def _heic_to_jpeg(content: bytes) -> bytes:
    import pillow_heif
    from PIL import Image
    pillow_heif.register_heif_opener()
    img = Image.open(io.BytesIO(content))
    exif_bytes = img.info.get("exif") or b""
    rgb = img.convert("RGB")
    buf = io.BytesIO()
    if exif_bytes:
        rgb.save(buf, format="JPEG", quality=92, exif=exif_bytes)
    else:
        rgb.save(buf, format="JPEG", quality=92)
    return buf.getvalue()

def _transcode_to_mp4(content: bytes, src_ext: str) -> bytes:
    with tempfile.NamedTemporaryFile(suffix=f".{src_ext}", delete=False) as fin:
        fin.write(content); in_path = fin.name
    out_path = in_path[: -(len(src_ext))] + "mp4"
    try:
        subprocess.run(
            ["ffmpeg", "-y", "-i", in_path, "-c:v", "libx264", "-c:a", "aac",
             "-movflags", "+faststart", out_path],
            check=True, capture_output=True,
        )
        return Path(out_path).read_bytes()
    finally:
        os.unlink(in_path)
        if os.path.exists(out_path): os.unlink(out_path)

def _extract_exif_gps(content: bytes, filename: str = "") -> tuple:
    """Return (lat, lng, captured_at_str) from image EXIF. Any field may be None."""
    tag = f"EXIF[{filename}]" if filename else "EXIF"
    try:
        from PIL import Image
        from PIL.ExifTags import GPSTAGS
        img = Image.open(io.BytesIO(content))

        exif = img.getexif()
        if not exif:
            log.info("%s: no EXIF data in image", tag)
            return None, None, None

        # Try DateTimeOriginal (36867), then DateTimeDigitized (36868), then DateTime (306)
        captured_at = None
        for dt_tag in (36867, 36868, 306):
            dt_raw = exif.get(dt_tag)
            if dt_raw:
                captured_at = str(dt_raw)   # "YYYY:MM:DD HH:MM:SS"
                log.info("%s: DateTime(tag=%d) = %s", tag, dt_tag, captured_at)
                break
        if not captured_at:
            log.info("%s: no DateTimeOriginal tag", tag)

        # Tag 34853 = GPSInfo IFD
        gps_ifd = exif.get_ifd(34853)
        if not gps_ifd:
            log.info("%s: no GPS IFD", tag)
            return None, None, captured_at

        gps = {GPSTAGS.get(t, t): v for t, v in gps_ifd.items()}
        log.info("%s: GPS tags present: %s", tag, list(gps.keys()))

        lat_vals = gps.get("GPSLatitude")
        lat_ref  = gps.get("GPSLatitudeRef",  "N")
        lng_vals = gps.get("GPSLongitude")
        lng_ref  = gps.get("GPSLongitudeRef", "E")

        if not lat_vals or not lng_vals:
            log.warning("%s: GPS IFD found but lat/lng values missing (lat=%s lng=%s)",
                        tag, lat_vals, lng_vals)
            return None, None, captured_at

        def _safe_float(r):
            """Convert Pillow IFDRational to float.
            Returns 0.0 for zero-denominator (0/0 = no GPS fix), NaN, or Inf values
            so the downstream (0,0) guard can uniformly reject them.
            """
            import math
            try:
                v = float(r)
                return 0.0 if (math.isnan(v) or math.isinf(v)) else v
            except (ZeroDivisionError, ValueError):
                return 0.0

        def dms(vals, ref):
            d = _safe_float(vals[0])
            m = _safe_float(vals[1])
            s = _safe_float(vals[2])
            dd = d + m / 60 + s / 3600
            return -dd if ref in ("S", "W") else dd

        lat = dms(lat_vals, lat_ref)
        lng = dms(lng_vals, lng_ref)

        # (0.0, 0.0) means the phone had no GPS lock — reject as invalid
        if lat == 0.0 and lng == 0.0:
            log.info("%s: GPS is (0,0) — no GPS fix; treating as null", tag)
            return None, None, captured_at

        log.info("%s: GPS resolved → lat=%.6f lng=%.6f", tag, lat, lng)

        # If no DateTime tags, fall back to GPSDateStamp + GPSTimeStamp (UTC)
        if not captured_at:
            date_stamp = gps.get("GPSDateStamp")   # "YYYY:MM:DD"
            time_stamp = gps.get("GPSTimeStamp")   # tuple of rationals (H, M, S)
            if date_stamp and time_stamp:
                try:
                    h  = int(_safe_float(time_stamp[0]))
                    mn = int(_safe_float(time_stamp[1]))
                    s  = int(_safe_float(time_stamp[2]))
                    captured_at = f"{date_stamp} {h:02d}:{mn:02d}:{s:02d}"
                    log.info("%s: using GPS timestamp (UTC): %s", tag, captured_at)
                except Exception as gps_exc:
                    log.warning("%s: could not parse GPS timestamp: %s", tag, gps_exc)

        return lat, lng, captured_at

    except Exception as exc:
        log.warning("%s: extraction failed: %s", tag, exc)
        return None, None, None


# ── SSE broadcast ─────────────────────────────────────────────────────────────

def _caption_payload_image(content: bytes) -> bytes:
    """Return a smaller JPEG payload for Arena vision requests."""
    try:
        from PIL import Image, ImageOps
        img = Image.open(io.BytesIO(content))
        img = ImageOps.exif_transpose(img)
        img.thumbnail((1280, 1280))
        if img.mode not in ("RGB", "L"):
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=80, optimize=True)
        return buf.getvalue()
    except Exception as exc:
        log.warning("Caption image resize failed, sending original payload: %s", exc)
        return content


def _video_placeholder_thumb(filename: str) -> bytes:
    from PIL import Image, ImageDraw, ImageFont
    img = Image.new("RGB", (640, 480), (20, 24, 32))
    draw = ImageDraw.Draw(img)
    try:
        font_sm = ImageFont.truetype("C:/Windows/Fonts/arial.ttf", 24)
    except Exception:
        font_sm = None
    draw.ellipse((260, 170, 380, 290), fill=(67, 56, 202))
    draw.polygon([(306, 205), (306, 255), (350, 230)], fill=(255, 255, 255))
    label = "Video"
    try:
        bbox = draw.textbbox((0, 0), label, font=font_sm)
        x = (640 - (bbox[2] - bbox[0])) / 2
    except Exception:
        x = 292
    draw.text((x, 315), label, fill=(220, 220, 230), font=font_sm)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=82)
    return buf.getvalue()


def _upsert_media_cache(gid: str, file_data: dict) -> None:
    lc = _list_caches.get(gid)
    if not lc or lc.get("data") is None:
        return
    media = [f for f in lc["data"] if f.get("name") != file_data.get("name")]
    media.insert(0, file_data)
    lc["data"] = media
    lc["expires"] = max(lc.get("expires", 0), time.time() + 60)


def _remove_media_cache(gid: str, filename: str) -> None:
    lc = _list_caches.get(gid)
    if lc and lc.get("data") is not None:
        lc["data"] = [f for f in lc["data"] if f.get("name") != filename]
    cache_key = f"{gid}:{filename}"
    _thumb_cache.pop(cache_key, None)
    _missing_thumb_cache.pop(cache_key, None)


def _update_cached_caption(gid: str, filename: str, caption: str,
                           source: str = "ai", status: str = "done") -> None:
    lc = _list_caches.get(gid)
    if lc and lc.get("data") is not None:
        for f in lc["data"]:
            if f.get("name") == filename:
                f["caption"] = caption
                f["caption_source"] = source
                f["caption_status"] = status
                break


async def _broadcast(group_id: str, event: dict) -> None:
    msg  = f"data: {json.dumps(event)}\n\n"
    dead = []
    for gid, q in _sse_queues:
        if gid != group_id:
            continue
        try:
            q.put_nowait(msg)
        except asyncio.QueueFull:
            dead.append((gid, q))
    for item in dead:
        try: _sse_queues.remove(item)
        except ValueError: pass


# ── Arena caption (best-effort, runs in background thread) ───────────────────

def _arena_token_file() -> Path:
    """Return the token file path for the current ARENA_URL.
    Remote servers use a separate file so the local Claude Code token is not mixed in."""
    is_local = "localhost" in ARENA_URL or "127.0.0.1" in ARENA_URL
    return Path.home() / (".arena-token" if is_local else ".arena-token-remote")


def _arena_token() -> str | None:
    """Return a valid Arena JWT for ARENA_URL. Auto-refreshes using ARENA_USER/ARENA_PASS."""
    # Explicit env var always wins (user-managed)
    t = os.getenv("ARENA_TOKEN")
    if t:
        return t
    # Try server-specific cached token
    token_file = _arena_token_file()
    if token_file.exists():
        t = token_file.read_text().strip()
        if t:
            return t
    # Auto-login if credentials are available
    user = os.getenv("ARENA_USER"); passwd = os.getenv("ARENA_PASS")
    if not user or not passwd:
        log.warning("Arena: no token and ARENA_USER/ARENA_PASS not set — captions disabled")
        return None
    try:
        payload = json.dumps({"username": user, "password": passwd}).encode()
        req = urllib.request.Request(
            f"{ARENA_URL}/api/auth/login", data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = json.loads(resp.read())
            t = body.get("token")
            if t:
                token_file.write_text(t)
                log.info("Arena: token refreshed via credentials (saved to %s)", token_file)
                return t
    except Exception as exc:
        log.warning("Arena: auto-login failed: %s", exc)
    return None

CAPTION_PROMPT_TEMPLATE = (
    "Describe this photo in one short, vivid sentence in {language}. "
    "No more than 100 letters or 50 Chinese characters. "
    "Use a more specific noun for a place, an animal, a person, etc, "
    "rather than a general term. No preamble, no label."
)
SUMMARY_PROMPT_TEMPLATE = (
    "Write a warm, vivid 2-3 sentence summary of this event in {language}, "
    "based on the following photo captions:\n{captions}\n\nSummary:"
)

def _call_arena_caption(file_name: str, image_bytes: bytes, language: str = "") -> str:
    """Return an AI caption for an image via Arena, or raise for the queue retry policy."""
    token = _arena_token()
    if not token:
        raise RuntimeError("Arena token unavailable")
    lang = language or LANGUAGE
    caption_prompt = CAPTION_PROMPT_TEMPLATE.format(language=lang)
    log.info("Caption[%s]: model=arena:auto/cheapest  lang=%s  prompt=%r",
             file_name, lang, caption_prompt)
    def _do_request(tok):
        payload_image = _caption_payload_image(image_bytes)
        b64 = base64.b64encode(payload_image).decode()
        payload = json.dumps({
            "model": "arena:auto/cheapest",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": caption_prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{b64}"}},
                ],
            }],
            "max_tokens": 160,
        }).encode()
        req = urllib.request.Request(
            f"{ARENA_URL}/v1/chat/completions",
            data=payload,
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {tok}"},
        )
        return urllib.request.urlopen(req, timeout=60)

    try:
        try:
            resp = _do_request(token)
        except urllib.request.HTTPError as e:
            if e.code == 401:
                # Token rejected — delete cached file and re-login once
                log.warning("Arena caption: 401 for %s — refreshing token and retrying", file_name)
                tf = _arena_token_file()
                if tf.exists(): tf.unlink()
                token = _arena_token()
                if not token:
                    raise RuntimeError("Arena token refresh failed")
                resp = _do_request(token)
            elif e.code == 422:
                raise RuntimeError("Arena vision/multimodal not supported")
            else:
                raise
        body = json.loads(resp.read())
        choice = body["choices"][0]
        finish_reason = choice.get("finish_reason")
        if finish_reason == "length":
            log.warning("Arena caption may be truncated by token limit for %s", file_name)
        else:
            log.info("Arena caption finish_reason[%s]: %s", file_name, finish_reason)
        caption = choice["message"]["content"].strip()
        if not caption:
            raise RuntimeError("Arena returned an empty caption")
        return caption
    except Exception as e:
        log.warning("Arena caption failed for %s: %s", file_name, e)
        raise


# ── Startup warmup ───────────────────────────────────────────────────────────

def _process_one_caption_job_sync(gid: str, folder: str, event: dict) -> dict | None:
    db_path = _db_path(gid)
    reset_stuck_caption_jobs(db_path)
    job = claim_caption_job(db_path)
    if not job:
        return None

    file_name = job["file_name"]
    try:
        existing = get_media_meta(db_path, [file_name]).get(file_name, {})
        if existing.get("caption"):
            mark_caption_job_done(db_path, file_name)
            _update_cached_caption(
                gid,
                file_name,
                existing["caption"],
                existing.get("caption_source") or "ai",
                "done",
            )
            return None
        content, _ = DropboxClient().download(f"{folder}/{file_name}")
        generated = _call_arena_caption(file_name, content, event.get("language", ""))
        caption = complete_caption_job(db_path, file_name, generated)
        _update_cached_caption(gid, file_name, caption, "ai", "done")
        log.info("Caption job done[%s/%s]", gid, file_name)
        return {"ok": True, "file": file_name, "caption": caption}
    except Exception as exc:
        fail_caption_job(db_path, file_name, str(exc))
        log.warning("Caption job failed[%s/%s]: %s", gid, file_name, exc)
        return {"ok": False, "file": file_name, "error": str(exc)}


async def _caption_worker_loop() -> None:
    log.info("Caption worker started")
    while True:
        did_work = False
        try:
            groups = _load_groups()
            for gid, group in groups.items():
                result = await asyncio.to_thread(
                    _process_one_caption_job_sync,
                    gid,
                    group["folder"],
                    group["event"],
                )
                if not result:
                    continue
                did_work = True
                if result.get("ok"):
                    await _broadcast(gid, {
                        "type": "caption",
                        "file": result["file"],
                        "caption": result["caption"],
                        "source": "ai",
                        "status": "done",
                        "ts": time.time(),
                    })
        except Exception as exc:
            log.warning("Caption worker loop error: %s", exc)
        await asyncio.sleep(0.2 if did_work else 5)


def _arena_ping() -> dict:
    """Test Arena connectivity. Returns a status dict."""
    token = _arena_token()
    result = {"url": ARENA_URL, "token_file": str(_arena_token_file()),
              "token_found": bool(token), "reachable": False, "authed": False, "error": None}
    try:
        req = urllib.request.Request(
            f"{ARENA_URL}/v1/models",
            headers={"Authorization": f"Bearer {token}"} if token else {},
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            result["reachable"] = True
            result["authed"] = True
            result["status_code"] = r.status
    except urllib.request.HTTPError as e:
        result["reachable"] = True
        result["error"] = f"HTTP {e.code} — {'auth failure, token may need refresh' if e.code==401 else e.reason}"
    except Exception as exc:
        result["error"] = str(exc)
    return result


@app.on_event("startup")
async def _warmup():
    global _caption_worker_started
    log.info("=" * 54)
    log.info("  Media Pool started — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 54)
    # Log Arena configuration and test connectivity
    log.info("Arena URL : %s", ARENA_URL)
    if os.getenv("ARENA_TOKEN"):
        log.info("Arena token : ARENA_TOKEN env var set (length=%d)", len(os.getenv("ARENA_TOKEN")))
    else:
        tf = _arena_token_file()
        log.info("Arena token file: %s  exists=%s", tf, tf.exists())
    arena_status = _arena_ping()
    if arena_status["reachable"] and arena_status["authed"]:
        log.info("Arena : reachable and authenticated ✓")
    elif arena_status["reachable"]:
        log.warning("Arena : reachable but auth FAILED — check token")
    else:
        log.warning("Arena : unreachable (%s) — AI captions disabled", arena_status.get("error"))
    # Log all available network addresses so the admin knows which URL to open
    port = os.getenv("PORT", "7000")
    pub  = os.getenv("PUBLIC_URL", "")
    if pub:
        log.info("PUBLIC_URL override: %s  (QR codes always use this)", pub)
    else:
        import socket as _sock
        _VM = ("192.168.122.", "192.168.124.", "192.168.136.", "192.168.56.", "192.168.99.")
        _hostname_ips: list[str] = []
        try:
            for _info in _sock.getaddrinfo(_sock.gethostname(), None, _sock.AF_INET):
                _ip = _info[4][0]
                if not _ip.startswith("127.") and not _ip.startswith("169.254."):
                    _hostname_ips.append(_ip)
        except Exception:
            pass
        _probe_ip: str | None = None
        try:
            _s = _sock.socket(_sock.AF_INET, _sock.SOCK_DGRAM)
            _s.connect(("8.8.8.8", 80)); _probe_ip = _s.getsockname()[0]; _s.close()
        except Exception:
            pass

        log.info("─" * 54)
        log.info("  Network addresses on this machine:")
        _seen = set()
        for _ip in _hostname_ips:
            if _ip in _seen: continue
            _seen.add(_ip)
            _probe_marker = "  ← default-route ★" if _ip == _probe_ip else ""
            if any(_ip.startswith(v) for v in _VM):
                log.info("    %s  ← VM/virtual adapter (skip)", _ip)
            else:
                log.info("    %s  ← LAN adapter  ✓%s", _ip, _probe_marker)
        if _probe_ip and _probe_ip not in _seen:
            log.info("    %s  ← default-route probe IP (not in hostname list)", _probe_ip)
        # The recommended IP is the UDP-probe result (default-route interface).
        # That is the same interface phones use to reach this machine — it is
        # always correct regardless of how many NICs the PC has.
        _recommended = _probe_ip or next(
            (_ip for _ip in _hostname_ips if not any(_ip.startswith(v) for v in _VM)), None)
        if _recommended:
            log.info("  → RECOMMENDED: open admin at http://%s:%s/qrs", _recommended, port)
            log.info("  → QR codes will embed whichever IP is in your browser's address bar.")
            log.info("  → If QR codes show the wrong IP, set PUBLIC_URL=http://%s:%s in .env",
                     _recommended, port)
        else:
            log.info("  → No LAN IPs detected. Open admin at http://localhost:%s/qrs", port)
            log.info("  → Set PUBLIC_URL=http://<your-ip>:%s in .env if QR codes are wrong.", port)
        log.info("─" * 54)
    try:
        groups = _load_groups()
        for gid, g in groups.items():
            log.info("Startup: group='%s'  open=%s", gid, _event_is_open(g["event"]))
            init_db(_db_path(gid))
            try:
                files = DropboxClient().list_folder(g["folder"])
                image_names = [
                    f["name"] for f in files
                    if _ext(f["name"]) in IMAGE_EXTS and not f["name"].startswith("_")
                ]
                queued = enqueue_missing_caption_jobs(_db_path(gid), image_names)
                if queued:
                    log.info("Startup: group='%s' queued %d missing caption jobs", gid, queued)
            except Exception as exc:
                log.warning("Startup: group='%s' caption backfill scan failed: %s", gid, exc)
            # Log GPS stats vs actual DB count (note: may include stale entries until first media list)
            db = _db_path(gid)
            try:
                con = sqlite3.connect(db)
                cur = con.cursor()
                cur.execute("SELECT COUNT(*) FROM media_meta WHERE lat IS NOT NULL")
                gps_count = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM media_meta")
                total = cur.fetchone()[0]
                con.close()
                log.info("Startup: group='%s'  GPS in DB: %d / %d (stale entries pruned on first media list)",
                         gid, gps_count, total)
            except Exception:
                pass
    except Exception as exc:
        log.error("Startup warmup failed: %s", exc)
    if not _caption_worker_started:
        asyncio.create_task(_caption_worker_loop())
        _caption_worker_started = True


# ── Pages ────────────────────────────────────────────────────────────────────

_NO_CACHE = {"Cache-Control": "no-store"}   # always serve fresh HTML (never stale JS)

def _html(path) -> FileResponse:
    """Serve an HTML page. no-store ensures browsers never run stale JavaScript."""
    return FileResponse(path, headers=_NO_CACHE)


@app.get("/")
async def root(mp_session: Optional[str] = Cookie(default=None)):
    # Authenticated friend with a valid open-event session → gallery
    if mp_session and mp_session in _sessions:
        sess     = _sessions[mp_session]
        group_id = sess.get("group_id", "default")
        group    = _get_group(group_id)
        if group and _event_is_open(group["event"]):
            try:
                _require_auth_ctx(mp_session)
                if sess.get("nickname") is None:
                    return RedirectResponse("/nickname", status_code=303)
                return _html(CLIENT_DIR / "index.html")
            except HTTPException:
                pass
    # Everyone else (host, unauthenticated visitor) → host landing page
    return _html(CLIENT_DIR / "home.html")

@app.get("/login")
async def login_page():
    return _html(CLIENT_DIR / "login.html")

@app.get("/closed")
async def closed_page():
    return _html(CLIENT_DIR / "closed.html")

@app.get("/qrs")
async def qrs_page():
    return _html(CLIENT_DIR / "qrs.html")

@app.get("/nickname")
async def nickname_page(mp_session: Optional[str] = Cookie(default=None)):
    if not mp_session or mp_session not in _sessions:
        # Friend's session cookie is stale (server restarted) — send to re-scan page
        return RedirectResponse("/login?error=rescan", status_code=303)
    if _sessions[mp_session].get("nickname") is not None:
        return RedirectResponse("/", status_code=303)
    return _html(CLIENT_DIR / "nickname.html")


# ── API: admin ───────────────────────────────────────────────────────────────

@app.post("/api/admin/unlock")
async def admin_unlock(req: Request, response: Response):
    body = await req.json()
    if not ADMIN_PIN:
        log.warning("Admin unlock attempt but ADMIN_PIN not configured")
        raise HTTPException(503, "ADMIN_PIN not configured in .env")
    if body.get("pin") != ADMIN_PIN:
        log.warning("Admin unlock: wrong PIN from %s", req.client.host if req.client else "?")
        raise HTTPException(403, "Wrong PIN")
    token = secrets.token_urlsafe(32)
    _admin_sessions.add(token)
    response.set_cookie(ADMIN_COOKIE, token, httponly=True, samesite="lax",
                        secure=_is_https_host())
    log.info("Admin unlocked from %s", req.client.host if req.client else "?")
    return {"ok": True}

@app.post("/api/admin/refresh-network")
async def admin_refresh_network(req: Request, mp_admin: Optional[str] = Cookie(default=None)):
    """Force re-detection of LAN IP (clears the 60-second cache immediately)."""
    _require_admin(mp_admin)
    global _lan_ip_cache, _lan_ip_cache_ts
    old_ip = _lan_ip_cache
    _lan_ip_cache = ""
    _lan_ip_cache_ts = 0.0
    new_ip = _lan_ip()   # re-detect now
    port = os.getenv("PORT", "7000")
    log.info("Network refresh: %s → %s (admin %s)", old_ip or "(none)", new_ip,
             req.client.host if req.client else "?")
    return {"old_ip": old_ip, "new_ip": new_ip, "qr_base": f"http://{new_ip}:{port}"}


@app.get("/api/admin/groups")
async def admin_groups(mp_admin: Optional[str] = Cookie(default=None)):
    _require_admin(mp_admin)
    t0 = time.time()
    cached = _groups_cache.get("expires", 0) > t0 and _groups_cache.get("data") is not None
    groups = _load_groups()
    log.info("Admin groups: %d groups returned (cache %s, %.2fs)",
             len(groups), "HIT" if cached else "MISS — fetching from Dropbox", time.time() - t0)
    result = []
    for gid, g in groups.items():
        ev = g["event"]; vf = ev.get("valid_from","")[:10]; vu = ev.get("valid_until","")[:10]
        result.append({
            "group_id": gid, "name": ev.get("name", gid),
            "valid_from": vf, "valid_until": vu,
            "is_open": _event_is_open(ev),
            "qr_url": f"/api/auth/qr?group={urllib.parse.quote(gid)}",
        })
    return result


# ── API: admin diagnostics ────────────────────────────────────────────────────

@app.get("/api/admin/diagnostics")
async def admin_diagnostics(mp_admin: Optional[str] = Cookie(default=None)):
    """Returns Arena status and GPS stats for all groups. Admin-only."""
    _require_admin(mp_admin)
    arena = _arena_ping()
    gps_stats = {}
    caption_stats = {}
    try:
        for gid in _load_groups():
            db = _db_path(gid)
            try:
                con = sqlite3.connect(db)
                cur = con.cursor()
                cur.execute("SELECT COUNT(*) FROM media_meta WHERE lat IS NOT NULL AND lng IS NOT NULL")
                with_gps = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM media_meta WHERE lat IS NULL OR lng IS NULL")
                without_gps = cur.fetchone()[0]
                cur.execute("SELECT file_name, lat, lng FROM media_meta WHERE lat IS NOT NULL AND lng IS NOT NULL LIMIT 5")
                samples = [{"file": r[0], "lat": r[1], "lng": r[2]} for r in cur.fetchall()]
                con.close()
                gps_stats[gid] = {"with_gps": with_gps, "without_gps": without_gps, "samples": samples}
            except Exception as exc:
                gps_stats[gid] = {"error": str(exc)}
            try:
                caption_stats[gid] = get_caption_job_stats(db)
            except Exception as exc:
                caption_stats[gid] = {"error": str(exc)}
    except Exception as exc:
        gps_stats = {"error": str(exc)}
        caption_stats = {"error": str(exc)}
    return {"arena": arena, "gps_stats": gps_stats, "caption_stats": caption_stats,
            "arena_url_env": os.getenv("ARENA_URL", "(not set, using default)"),
            "public_url_env": os.getenv("PUBLIC_URL", "(not set)")}


@app.post("/api/admin/captions/backfill")
async def admin_caption_backfill(mp_admin: Optional[str] = Cookie(default=None)):
    _require_admin(mp_admin)
    result = {}
    groups = _load_groups()
    for gid, group in groups.items():
        db_path = _db_path(gid)
        files = DropboxClient().list_folder(group["folder"])
        names = [
            f["name"] for f in files
            if _ext(f["name"]) in IMAGE_EXTS and not f["name"].startswith("_")
        ]
        result[gid] = {"queued": enqueue_missing_caption_jobs(db_path, names), "images": len(names)}
    return {"ok": True, "groups": result}


@app.get("/api/admin/gps-sample")
async def gps_sample(
    mp_admin: Optional[str] = Cookie(default=None),
    n: int = 5,
):
    """Download up to n images from Dropbox and test whether they carry GPS EXIF.
    Use this to check if source files have GPS data at all — before blaming the app.
    Admin-only.
    """
    _require_admin(mp_admin)
    groups = _load_groups()
    results = []
    for gid, g in groups.items():
        folder = g["folder"]
        lc = _list_caches.get(gid, {}).get("data") or []
        images = [f["name"] for f in lc if _ext(f["name"]) in IMAGE_EXTS]
        if not images:
            results.append({
                "group": gid,
                "error": "No images in list cache — open the gallery first to populate the cache, then retry.",
            })
            continue
        dbx = DropboxClient()
        for name in images[:n]:
            try:
                content, _ = dbx.download(f"{folder}/{name}")
                lat, lng, captured_at = _extract_exif_gps(content, name)
                results.append({
                    "group":        gid,
                    "file":         name,
                    "has_gps":      lat is not None and lng is not None,
                    "lat":          lat,
                    "lng":          lng,
                    "captured_at":  captured_at,
                })
            except Exception as exc:
                results.append({"group": gid, "file": name, "error": str(exc)})
    gps_count   = sum(1 for r in results if r.get("has_gps"))
    total_files = sum(1 for r in results if "file" in r)
    return {
        "samples":    results,
        "gps_found":  gps_count,
        "total_checked": total_files,
        "note": (
            "These files were downloaded fresh from Dropbox. "
            "If has_gps=false, the original file has no GPS EXIF — "
            "check that the phone's location permission was on when photos were taken."
        ),
    }


@app.post("/api/admin/rescan-gps")
async def rescan_gps(bg: BackgroundTasks, mp_admin: Optional[str] = Cookie(default=None)):
    """Re-extract GPS from Dropbox for every image that has no GPS in DB. Admin-only."""
    _require_admin(mp_admin)

    async def _do_rescan():
        groups = _load_groups()
        for gid, g in groups.items():
            folder  = g["folder"]
            db_path = _db_path(gid)
            # Use cache if available, otherwise fetch from Dropbox
            lc = _list_caches.get(gid, {}).get("data")
            if lc is None:
                log.info("rescan-gps[%s]: cache empty, loading from Dropbox…", gid)
                try:
                    lc = DropboxClient().list_folder(folder)
                except Exception as exc:
                    log.warning("rescan-gps[%s]: could not load file list: %s", gid, exc)
                    continue
            all_images = [f["name"] for f in lc if _ext(f["name"]) in IMAGE_EXTS]
            log.info("rescan-gps[%s]: %d images to check", gid, len(all_images))
            dbx = DropboxClient()
            found = 0
            for name in all_images:
                try:
                    meta_row = get_media_meta(db_path, [name]).get(name, {})
                    lat_db = meta_row.get("lat")
                    lng_db = meta_row.get("lng")
                    # Skip files that already have valid non-zero GPS
                    if lat_db is not None and lng_db is not None and not (lat_db == 0.0 and lng_db == 0.0):
                        continue
                    content, _ = dbx.download(f"{folder}/{name}")
                    lat, lng, captured_at = _extract_exif_gps(content, name)
                    # force_gps=True so we can overwrite wrong (0,0) with NULL
                    set_media_meta(db_path, name, lat=lat, lng=lng,
                                   captured_at=captured_at, force_gps=True)
                    log.info("rescan-gps[%s/%s]: lat=%s lng=%s", gid, name, lat, lng)
                    found += 1
                except Exception as exc:
                    log.warning("rescan-gps[%s/%s]: %s", gid, name, exc)
            _list_caches.pop(gid, None)
            log.info("rescan-gps[%s]: done — %d files updated", gid, found)

    bg.add_task(_do_rescan)
    return {"ok": True, "message": "GPS rescan started in background — check server logs for progress"}


# ── API: event ───────────────────────────────────────────────────────────────

@app.get("/api/event")
async def get_event(mp_session: Optional[str] = Cookie(default=None)):
    ctx = _require_auth_ctx(mp_session)
    return ctx["event"]

@app.get("/api/event/online")
async def event_online(mp_session: Optional[str] = Cookie(default=None)):
    ctx = _require_auth_ctx(mp_session)
    gid = ctx["group_id"]
    count = sum(1 for s in _sessions.values() if s.get("group_id") == gid)
    return {"count": count}

@app.get("/api/event/summary")
async def get_event_summary(mp_session: Optional[str] = Cookie(default=None)):
    ctx = _require_auth_ctx(mp_session)
    gid = ctx["group_id"]
    summary = _event_summaries.get(gid, "")
    if not summary:
        captions = get_all_captions(_db_path(gid))
        if captions:
            summary = _event_summaries.get(gid, "")
    return {"summary": summary, "caption_count": len(get_all_captions(_db_path(gid)))}

@app.post("/api/event/summary")
async def generate_event_summary(
    bg: BackgroundTasks,
    mp_session: Optional[str] = Cookie(default=None),
):
    ctx = _require_auth_ctx(mp_session)
    gid = ctx["group_id"]
    captions = get_all_captions(_db_path(gid))
    if not captions:
        raise HTTPException(400, "No captions available yet — generate captions for photos first")

    def _do_summary():
        token = _arena_token()
        if not token:
            return
        lang = ctx["event"].get("language", "") or LANGUAGE
        caption_text = "\n".join(f"- {c['caption']}" for c in captions)
        summary_prompt = SUMMARY_PROMPT_TEMPLATE.format(
            language=lang, captions=caption_text
        )
        log.info("Summary[%s]: model=arena:auto/balanced  lang=%s  caption_count=%d",
                 gid, lang, len(captions))
        payload = json.dumps({
            "model": "arena:auto/balanced",
            "messages": [{"role": "user", "content": summary_prompt}],
            "max_tokens": 300,
        }).encode()
        try:
            req = urllib.request.Request(
                f"{ARENA_URL}/v1/chat/completions",
                data=payload,
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"},
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                body    = json.loads(resp.read())
                summary = body["choices"][0]["message"]["content"].strip()
                _event_summaries[gid] = summary
                log.info("Summary[%s]: result: %s", gid, summary[:120])
        except Exception as e:
            log.warning("Summary generation failed: %s", e)

    bg.add_task(_do_summary)
    return {"ok": True, "caption_count": len(captions), "message": "Generating summary…"}


# ── API: auth ────────────────────────────────────────────────────────────────

@app.get("/api/auth/qr")
async def get_qr(req: Request, group: Optional[str] = None):
    groups = _load_groups()
    if not groups:
        raise HTTPException(503, "No event groups available")
    if group:
        g = groups.get(group)
        if not g: raise HTTPException(404, f"Group '{group}' not found")
        group_id = group
    elif len(groups) == 1:
        group_id = next(iter(groups)); g = groups[group_id]
    else:
        raise HTTPException(400, "Specify ?group=<id> when multiple groups exist")
    ev    = g["event"]
    token = create_event_token(SECRET_KEY, group_id)
    base  = _public_base(req)
    log.info("QR[%s]: Host=%s  _lan_ip=%s  base=%s", group_id,
             req.headers.get("host","?"), _lan_ip_cache or "?", base)
    url   = f"{base}/api/auth/verify?token={token}"
    vf    = ev.get("valid_from","")[:10]; vu = ev.get("valid_until","")[:10]
    data  = _make_labeled_qr(url, ev.get("name", group_id), f"{vf} – {vu}" if vf and vu else "")
    return StreamingResponse(
        io.BytesIO(data), media_type="image/png",
        headers={"Cache-Control": "no-store"},
    )

@app.get("/api/auth/join-link")
async def join_link(req: Request, group: Optional[str] = None):
    """Return the shareable friend join URL (with embedded token) for a group."""
    groups = _load_groups()
    if group:
        g = groups.get(group)
        if not g:
            raise HTTPException(404, f"Group '{group}' not found")
        group_id = group
    elif len(groups) == 1:
        group_id = next(iter(groups))
    else:
        raise HTTPException(400, "Specify ?group=<id>")
    token = create_event_token(SECRET_KEY, group_id)
    base  = _public_base(req)
    log.info("JoinLink[%s]: Host=%s  _lan_ip=%s  base=%s", group_id,
             req.headers.get("host","?"), _lan_ip_cache or "?", base)
    url   = f"{base}/api/auth/verify?token={token}"
    return {"url": url}

@app.get("/api/auth/verify")
async def verify(req: Request, token: str):
    ua = req.headers.get("user-agent", "")
    log.info("verify: UA=%s", ua[:60])
    group_id = verify_event_token(token, SECRET_KEY)
    if not group_id:
        log.warning("verify: token invalid or expired")
        return RedirectResponse("/login?error=expired", status_code=303)
    group = _get_group(group_id)
    if not group:
        log.warning("verify: group_id='%s' not found in loaded groups", group_id)
        return RedirectResponse("/closed", status_code=303)
    if not _event_is_open(group["event"]):
        log.info("verify: group_id='%s' exists but event is not open", group_id)
        return RedirectResponse("/closed", status_code=303)
    session_id = secrets.token_urlsafe(32)
    _sessions[session_id] = {"created_at": time.time(), "last_seen": time.time(), "group_id": group_id}
    log.info("verify: session created for group='%s'", group_id)

    # iOS Safari ITP "bounce tracking" detection:
    # When the Camera app opens a QR URL, iOS treats it as a cross-site navigation
    # from a null origin.  ITP drops any Set-Cookie headers received during a
    # redirect chain that looks like tracking (short-lived landing URL that
    # immediately 303s away).  Android Chrome does NOT apply this restriction.
    #
    # Fix: return a tiny HTML page that sets the cookie via document.cookie
    # (a first-party same-site JS write — ITP never blocks these) and then
    # immediately calls location.replace('/nickname').
    secure_flag = "; secure" if _is_https_host() else ""
    return HTMLResponse(
        content=f"""<!doctype html>
<html><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="Cache-Control" content="no-store">
</head><body>
<script>
document.cookie = "{SESSION_COOKIE}={session_id}; path=/; samesite=lax; max-age=86400{secure_flag}";
location.replace("/nickname");
</script>
<noscript><p style="font-family:sans-serif;text-align:center;padding:2em">
  JavaScript is required to use this app.
</p></noscript>
</body></html>""",
        headers={"Cache-Control": "no-store"},
    )

@app.post("/api/auth/logout")
async def logout(response: Response, mp_session: Optional[str] = Cookie(default=None)):
    if mp_session: _sessions.pop(mp_session, None)
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}

@app.post("/api/auth/nickname")
async def set_nickname(req: Request, mp_session: Optional[str] = Cookie(default=None)):
    if not mp_session or mp_session not in _sessions:
        raise HTTPException(401, "Not authenticated")
    body = await req.json()
    raw  = (body.get("nickname") or "").strip()[:20]
    _sessions[mp_session]["nickname"] = raw
    return {"ok": True, "nickname": raw}

@app.get("/api/auth/me")
async def get_me(mp_session: Optional[str] = Cookie(default=None)):
    if not mp_session or mp_session not in _sessions:
        raise HTTPException(401, "Not authenticated")
    sess = _sessions[mp_session]
    return {"nickname": sess.get("nickname") or "", "group_id": sess.get("group_id", "default")}


# ── API: media list ──────────────────────────────────────────────────────────

@app.get("/api/media/list")
async def list_media(mp_session: Optional[str] = Cookie(default=None)):
    ctx    = _require_auth_ctx(mp_session)
    gid    = ctx["group_id"]
    folder = ctx["folder"]
    event  = ctx["event"]

    now = time.time()
    lc  = _list_caches.get(gid, {})
    if lc.get("expires", 0) > now and lc.get("data") is not None:
        media = lc["data"]
        log.info("MediaList[%s]: cache HIT — %d files", gid, len(media))
    else:
        t0 = time.time()
        all_files = DropboxClient().list_folder(folder)
        media = [
            f for f in all_files
            if _ext(f["name"]) in MEDIA_EXTS
            and f["name"] != "event.json"
            and not f["name"].startswith("_")
        ]
        _list_caches[gid] = {"data": media, "expires": now + 60}
        log.info("MediaList[%s]: cache MISS — %d files fetched from Dropbox in %.2fs",
                 gid, len(media), time.time() - t0)
        # Prune DB entries for files that no longer exist in Dropbox
        db_path_tmp = _db_path(gid)
        live_names  = [f["name"] for f in media]
        try:
            pruned = prune_stale_files(db_path_tmp, live_names)
            if pruned:
                log.info("MediaList[%s]: pruned %d stale DB entries for deleted files", gid, pruned)
        except Exception as exc:
            log.warning("MediaList[%s]: DB prune failed: %s", gid, exc)

    db_path   = _db_path(gid)
    names     = [f["name"] for f in media]
    image_names = [n for n in names if _ext(n) in IMAGE_EXTS]
    try:
        queued_missing = enqueue_missing_caption_jobs(db_path, image_names)
        if queued_missing:
            log.info("MediaList[%s]: queued %d missing caption jobs", gid, queued_missing)
    except Exception as exc:
        log.warning("MediaList[%s]: missing-caption enqueue failed: %s", gid, exc)
    reactions = get_reactions_by_file(db_path, names)
    views     = get_view_counts(db_path, names)
    comments  = get_comment_counts(db_path, names)
    meta      = get_media_meta(db_path, names)
    caption_jobs = get_caption_job_statuses(db_path, image_names)
    uploaders = get_uploader_sessions(db_path, names)

    for f in media:
        n = f["name"]
        f["reactions"]     = reactions.get(n, {})
        f["view_count"]    = views.get(n, 0)
        f["comment_count"] = comments.get(n, 0)
        m = meta.get(n, {})
        _lat = m.get("lat"); _lng = m.get("lng")
        # Treat (0, 0) as no GPS — it means the phone had no GPS lock
        f["lat"] = _lat if (_lat and _lng and not (_lat == 0.0 and _lng == 0.0)) else None
        f["lng"] = _lng if (_lat and _lng and not (_lat == 0.0 and _lng == 0.0)) else None
        f["caption"]       = m.get("caption", "")
        f["caption_source"] = m.get("caption_source") or ("ai" if f["caption"] else "")
        job = caption_jobs.get(n, {})
        f["caption_status"] = "done" if f["caption"] else job.get("status", "none")
        ca = m.get("captured_at")           # "YYYY:MM:DD HH:MM:SS" or None
        if ca:
            try:
                # Convert EXIF colon-date to ISO so the frontend can parse it uniformly
                f["captured_at"] = datetime.strptime(ca, "%Y:%m:%d %H:%M:%S").isoformat()
            except Exception:
                f["captured_at"] = None
        else:
            f["captured_at"] = None
        # Effective sort key: prefer original capture time, fall back to Dropbox modified
        f["_sort_ts"] = f["captured_at"] or f["modified"]

    # Re-sort by effective timestamp (newest first) after joining with DB meta
    media.sort(key=lambda f: f["_sort_ts"], reverse=True)

    # "mine" is session-specific — build a fresh list so we don't store it in the cache
    result = [
        {**f, "mine": uploaders.get(f["name"]) == mp_session}
        for f in media
    ]
    return {"files": result, "event": event}


# ── API: upload ──────────────────────────────────────────────────────────────

@app.post("/api/media/upload")
async def upload(
    bg: BackgroundTasks,
    file: UploadFile = File(...),
    upload_mode: str = Form("original"),
    client_modified: Optional[str] = Form(None),
    mp_session: Optional[str] = Cookie(default=None),
):
    timings = {}
    t_total = time.time()
    ctx    = _require_auth_ctx(mp_session)
    gid    = ctx["group_id"]
    folder = ctx["folder"]
    event  = ctx["event"]

    original_name = file.filename or "upload"
    ext           = _ext(original_name)
    video_exts    = MEDIA_EXTS - IMAGE_EXTS

    if ext not in IMAGE_EXTS and (ext not in video_exts or not event.get("allow_video")):
        raise HTTPException(400, "File type not allowed")

    t0 = time.time()
    content   = await file.read()
    timings["read"] = time.time() - t0
    save_name = original_name

    t0 = time.time()
    if ext in HEIC_EXTS:
        try:
            content   = _heic_to_jpeg(content)
            save_name = original_name[:-(len(ext)+1)] + ".jpg"
        except Exception as e:
            raise HTTPException(500, f"HEIC conversion failed: {e}")
    elif ext in TRANSCODE_EXTS:
        try:
            content   = _transcode_to_mp4(content, ext)
            save_name = original_name[:-(len(ext)+1)] + ".mp4"
        except subprocess.CalledProcessError as e:
            raise HTTPException(500, f"Video transcoding failed: {e.stderr.decode()[-200:]}")
    timings["conversion"] = time.time() - t0

    # Extract EXIF before upload so captured_at can set client_modified on Dropbox
    db_path = _db_path(gid)
    client_mod = None
    lat, lng, captured_at = None, None, None
    t0 = time.time()
    if _ext(save_name) in IMAGE_EXTS:
        lat, lng, captured_at = _extract_exif_gps(content, save_name)
        log.info("Upload[%s]: lat=%s lng=%s captured_at=%s", save_name, lat, lng, captured_at)
        if captured_at:
            try:
                client_mod = datetime.strptime(captured_at, "%Y:%m:%d %H:%M:%S")
                log.info("Upload[%s]: setting client_modified = %s", save_name, client_mod)
            except Exception as exc:
                log.warning("Upload[%s]: could not parse captured_at '%s': %s",
                            save_name, captured_at, exc)
        elif client_modified:
            try:
                client_mod = datetime.fromisoformat(client_modified.replace("Z", "+00:00")).replace(tzinfo=None)
                captured_at = client_mod.strftime("%Y:%m:%d %H:%M:%S")
                log.info("Upload[%s]: using browser client_modified = %s", save_name, client_mod)
            except Exception as exc:
                log.warning("Upload[%s]: could not parse client_modified '%s': %s",
                            save_name, client_modified, exc)
    timings["exif"] = time.time() - t0

    dbx = DropboxClient()
    t0 = time.time()
    dbx.upload(f"{folder}/{save_name}", content, client_modified=client_mod)
    timings["dropbox_upload"] = time.time() - t0

    # Extract video thumbnail
    t0 = time.time()
    if _ext(save_name) not in IMAGE_EXTS:
        try:
            thumb = _extract_video_frame(content)
            dbx.upload(f"{folder}/_thumbs/{save_name}.jpg", thumb)
        except Exception:
            pass
    timings["video_thumb"] = time.time() - t0

    # Save EXIF metadata to DB
    t0 = time.time()
    if _ext(save_name) in IMAGE_EXTS:
        if any(v is not None for v in (lat, lng, captured_at)):
            try:
                set_media_meta(db_path, save_name, lat=lat, lng=lng, captured_at=captured_at)
                log.info("Upload[%s]: meta saved to DB", save_name)
            except Exception as exc:
                log.error("Upload[%s]: failed to save meta: %s", save_name, exc)
        else:
            log.info("Upload[%s]: no EXIF metadata to store", save_name)
        enqueue_caption_job(db_path, save_name)
        log.info("Upload[%s]: caption job queued", save_name)

    # Track uploader for the session-specific "mine" marker in media lists.
    if mp_session:
        try:
            set_uploader_session(db_path, save_name, mp_session)
        except Exception as exc:
            log.warning("Upload[%s]: failed to save uploader session: %s", save_name, exc)
    timings["db"] = time.time() - t0

    modified = (client_mod or datetime.utcnow()).isoformat()
    _upsert_media_cache(gid, {"name": save_name, "size": len(content), "modified": modified})

    # Broadcast upload event
    sess = _sessions.get(mp_session, {})
    await _broadcast(gid, {
        "type": "upload",
        "file": save_name,
        "nickname": sess.get("nickname") or "Someone",
        "ts": time.time(),
    })

    timings["total"] = time.time() - t_total
    log.info(
        "UploadTiming[%s/%s]: mode=%s size=%d read=%.3fs convert=%.3fs exif=%.3fs "
        "dropbox=%.3fs thumb=%.3fs db=%.3fs total=%.3fs",
        gid, save_name, upload_mode, len(content), timings["read"], timings["conversion"],
        timings["exif"], timings["dropbox_upload"], timings["video_thumb"], timings["db"],
        timings["total"],
    )
    return {"ok": True, "name": save_name}


# ── API: delete media ─────────────────────────────────────────────────────────

@app.delete("/api/media/{filename}")
async def delete_media(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx     = _require_auth_ctx(mp_session)
    gid     = ctx["group_id"]
    folder  = ctx["folder"]
    db_path = _db_path(gid)

    dbx = DropboxClient()
    try:
        deleted = dbx.delete(f"{folder}/{filename}", missing_ok=True)
        if deleted:
            log.info("Delete[%s/%s]: removed from Dropbox by session %s", gid, filename, mp_session)
        else:
            log.warning("Delete[%s/%s]: Dropbox file was already missing; cleaning local state", gid, filename)
    except Exception as exc:
        log.error("Delete[%s/%s]: Dropbox delete failed: %s", gid, filename, exc)
        raise HTTPException(500, "Failed to delete file from storage")

    try:
        dbx.delete(f"{folder}/_thumbs/{filename}.jpg", missing_ok=True)
    except Exception as exc:
        log.warning("Delete[%s/%s]: thumbnail cleanup failed: %s", gid, filename, exc)

    delete_file_data(db_path, filename)
    _remove_media_cache(gid, filename)

    await _broadcast(gid, {"type": "delete", "file": filename, "ts": time.time()})
    return {"ok": True}


# ── API: thumbnails ──────────────────────────────────────────────────────────

@app.get("/api/media/{filename}/thumb")
async def get_thumb(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx       = _require_auth_ctx(mp_session)
    gid       = ctx["group_id"]
    folder    = ctx["folder"]
    cache_key = f"{gid}:{filename}"

    try:
        increment_view(_db_path(gid), filename, mp_session)
    except Exception:
        pass

    if cache_key not in _thumb_cache:
        dbx = DropboxClient()
        if _ext(filename) in IMAGE_EXTS:
            try:
                data = dbx.get_thumbnail(f"{folder}/{filename}")
            except Exception:
                raw, _ = dbx.download(f"{folder}/{filename}")
                from PIL import Image
                img = Image.open(io.BytesIO(raw)); img.thumbnail((640, 640))
                buf = io.BytesIO(); img.save(buf, format="JPEG"); data = buf.getvalue()
        else:
            try:
                data, _ = dbx.download(f"{folder}/_thumbs/{filename}.jpg")
            except Exception:
                missing_key = f"{cache_key}:missing"
                if _missing_thumb_cache.get(missing_key, 0) < time.time():
                    log.info("Thumb[%s]: video thumbnail missing; serving placeholder", filename)
                    _missing_thumb_cache[missing_key] = time.time() + 3600
                data = _video_placeholder_thumb(filename)
        _thumb_cache[cache_key] = data

    return StreamingResponse(
        io.BytesIO(_thumb_cache[cache_key]),
        media_type="image/jpeg",
        headers={"Cache-Control": "public, max-age=86400, immutable"},
    )


# ── API: reactions ────────────────────────────────────────────────────────────

@app.post("/api/media/{filename}/react")
async def react(filename: str, req: Request, mp_session: Optional[str] = Cookie(default=None)):
    ctx     = _require_auth_ctx(mp_session)
    gid     = ctx["group_id"]
    body    = await req.json()
    emoji   = body.get("emoji", "").strip()
    if not emoji:
        raise HTTPException(400, "emoji is required")
    allowed = {"❤️","👍","🔥","😂"}
    if emoji not in allowed:
        raise HTTPException(400, f"Use one of: {', '.join(allowed)}")

    db_path  = _db_path(gid)
    was_added = add_or_remove_reaction(db_path, filename, mp_session, emoji)
    reactions = get_reactions(db_path, filename)
    count     = reactions.get(emoji, 0)

    sess = _sessions.get(mp_session, {})
    await _broadcast(gid, {
        "type": "reaction", "file": filename, "emoji": emoji,
        "count": count, "added": was_added,
        "nickname": sess.get("nickname") or "Someone",
        "ts": time.time(),
    })

    nick = sess.get("nickname") or "anon"
    log.info("React[%s/%s]: %s %s by '%s' (count=%d)", gid, filename, emoji,
             "added" if was_added else "removed", nick, count)
    return {"ok": True, "emoji": emoji, "count": count, "added": was_added}


# ── API: comments ─────────────────────────────────────────────────────────────

@app.get("/api/media/{filename}/comments")
async def get_file_comments(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx     = _require_auth_ctx(mp_session)
    rows    = get_comments(_db_path(ctx["group_id"]), filename)
    # Mark which comments belong to current session
    for r in rows:
        r["is_mine"] = (r["user_session"] == mp_session)
        del r["user_session"]
    return {"comments": rows}

@app.post("/api/media/{filename}/comments")
async def post_comment(filename: str, req: Request, mp_session: Optional[str] = Cookie(default=None)):
    ctx  = _require_auth_ctx(mp_session)
    gid  = ctx["group_id"]
    body = await req.json()
    text = (body.get("text") or "").strip()
    if not text:
        raise HTTPException(400, "text is required")
    if len(text) > 500:
        raise HTTPException(400, "Comment too long (max 500 chars)")

    sess     = _sessions.get(mp_session, {})
    nickname = sess.get("nickname") or ""
    db_path  = _db_path(gid)
    comment_id = add_comment(db_path, filename, mp_session, nickname, text)

    comment = {"id": comment_id, "nickname": nickname, "text": text, "timestamp": time.time(), "is_mine": True}

    await _broadcast(gid, {
        "type": "comment", "file": filename,
        "id": comment_id, "nickname": nickname, "text": text, "ts": time.time(),
    })
    log.info("Comment[%s/%s]: posted by '%s' (%d chars)", gid, filename, nickname or "anon", len(text))
    return {"ok": True, "comment": comment}

@app.delete("/api/media/{filename}/comments/{comment_id}")
async def del_comment(filename: str, comment_id: int, mp_session: Optional[str] = Cookie(default=None)):
    ctx     = _require_auth_ctx(mp_session)
    deleted = delete_comment(_db_path(ctx["group_id"]), comment_id, mp_session)
    if not deleted:
        raise HTTPException(404, "Comment not found or not yours")
    return {"ok": True}


# ── API: EXIF / caption ───────────────────────────────────────────────────────

@app.get("/api/media/{filename}/exif")
async def get_exif(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx    = _require_auth_ctx(mp_session)
    gid    = ctx["group_id"]
    folder = ctx["folder"]
    meta   = get_media_meta(_db_path(gid), [filename]).get(filename)
    if meta and (meta.get("lat") is not None):
        return meta
    # On-demand extraction
    if _ext(filename) not in IMAGE_EXTS:
        return {"lat": None, "lng": None, "captured_at": None, "caption": None}
    try:
        content, _ = DropboxClient().download(f"{folder}/{filename}")
        lat, lng, captured_at = _extract_exif_gps(content, filename)
        log.info("exif-endpoint[%s]: lat=%s lng=%s captured_at=%s", filename, lat, lng, captured_at)
        if any(v is not None for v in (lat, lng, captured_at)):
            set_media_meta(_db_path(gid), filename, lat=lat, lng=lng, captured_at=captured_at)
        return {"lat": lat, "lng": lng, "captured_at": captured_at,
                "caption": meta.get("caption") if meta else None}
    except Exception as exc:
        log.warning("exif-endpoint[%s]: failed: %s", filename, exc)
        return {"lat": None, "lng": None, "captured_at": None, "caption": None}

@app.post("/api/media/{filename}/caption")
async def set_file_caption(filename: str, req: Request, mp_session: Optional[str] = Cookie(default=None)):
    ctx     = _require_auth_ctx(mp_session)
    body    = await req.json()
    caption = (body.get("caption") or "").strip()
    if _ext(filename) not in IMAGE_EXTS:
        raise HTTPException(400, "captions are only supported for images")
    db_path = _db_path(ctx["group_id"])
    meta = get_media_meta(db_path, [filename]).get(filename, {})
    if not meta.get("caption"):
        raise HTTPException(409, "AI caption is not ready yet")
    set_caption(db_path, filename, caption, mp_session or "")
    source = "manual" if caption else ""
    status = "done" if caption else "pending"
    _update_cached_caption(ctx["group_id"], filename, caption, source, status)
    await _broadcast(ctx["group_id"], {
        "type": "caption",
        "file": filename,
        "caption": caption,
        "source": source,
        "status": status,
        "ts": time.time(),
    })
    return {"ok": True, "caption": caption, "source": source, "status": status}


# ── API: SSE live feed ────────────────────────────────────────────────────────

@app.get("/api/media/feed")
async def media_feed(mp_session: Optional[str] = Cookie(default=None)):
    ctx = _require_auth_ctx(mp_session)
    gid = ctx["group_id"]

    queue: asyncio.Queue = asyncio.Queue(maxsize=100)
    entry = (gid, queue)
    _sse_queues.append(entry)

    async def event_stream():
        try:
            # Send initial online count
            count = sum(1 for s in _sessions.values() if s.get("group_id") == gid)
            yield f"data: {json.dumps({'type': 'online', 'count': count})}\n\n"

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": ping\n\n"
        finally:
            try: _sse_queues.remove(entry)
            except ValueError: pass

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── API: highlight video ──────────────────────────────────────────────────────

@app.post("/api/media/highlight-video")
async def highlight_video(req: Request, mp_session: Optional[str] = Cookie(default=None)):
    ctx    = _require_auth_ctx(mp_session)
    gid    = ctx["group_id"]
    folder = ctx["folder"]
    body   = await req.json()
    selected = [f for f in body.get("files", []) if _ext(f) in IMAGE_EXTS]

    if not selected:
        # Auto-select: top images by reactions + views
        lc = _list_caches.get(gid, {}).get("data", [])
        db = _db_path(gid)
        names    = [f["name"] for f in lc if _ext(f["name"]) in IMAGE_EXTS]
        reacts   = get_reactions_by_file(db, names)
        views    = get_view_counts(db, names)
        def score(n):
            r = sum(reacts.get(n, {}).values())
            v = views.get(n, 0)
            return r * 3 + v
        selected = sorted(names, key=score, reverse=True)[:15]

    if not selected:
        raise HTTPException(400, "No photos available for highlight video")

    dbx    = DropboxClient()
    tmp_images = []
    try:
        for name in selected[:20]:
            content, _ = dbx.download(f"{folder}/{name}")
            tmp = tempfile.NamedTemporaryFile(suffix=".jpg", delete=False)
            tmp.write(content); tmp.close()
            tmp_images.append(tmp.name)

        if not tmp_images:
            raise HTTPException(400, "Could not download any images")

        out_path = tempfile.mktemp(suffix=".mp4")

        # Build ffmpeg concat filter
        inputs = []
        for p in tmp_images:
            inputs += ["-loop", "1", "-t", "3", "-i", p]

        fparts = []
        for i in range(len(tmp_images)):
            fparts.append(
                f"[{i}:v]scale=1080:1080:force_original_aspect_ratio=decrease,"
                f"pad=1080:1080:(ow-iw)/2:(oh-ih)/2,setsar=1[v{i}]"
            )
        concat_in = "".join(f"[v{i}]" for i in range(len(tmp_images)))
        fparts.append(f"{concat_in}concat=n={len(tmp_images)}:v=1:a=0[outv]")
        filter_complex = ";".join(fparts)

        cmd = (["ffmpeg", "-y"] + inputs +
               ["-filter_complex", filter_complex, "-map", "[outv]",
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-movflags", "+faststart", out_path])

        subprocess.run(cmd, check=True, capture_output=True, timeout=180)

        def _stream_and_cleanup():
            try:
                with open(out_path, "rb") as f:
                    while chunk := f.read(65536):
                        yield chunk
            finally:
                for p in tmp_images + [out_path]:
                    try: os.unlink(p)
                    except Exception: pass

        return StreamingResponse(
            _stream_and_cleanup(),
            media_type="video/mp4",
            headers={"Content-Disposition": 'attachment; filename="highlight.mp4"'},
        )
    except HTTPException:
        raise
    except subprocess.CalledProcessError as e:
        for p in tmp_images:
            try: os.unlink(p)
            except Exception: pass
        raise HTTPException(500, f"ffmpeg failed: {e.stderr.decode()[-300:]}")
    except Exception as e:
        for p in tmp_images:
            try: os.unlink(p)
            except Exception: pass
        raise HTTPException(500, str(e))


# ── API: download / stream ────────────────────────────────────────────────────

@app.post("/api/media/download-zip")
async def download_zip(req: Request, mp_session: Optional[str] = Cookie(default=None)):
    ctx       = _require_auth_ctx(mp_session)
    folder    = ctx["folder"]
    body      = await req.json()
    filenames = [f for f in body.get("files", []) if _ext(f) in MEDIA_EXTS]
    if not filenames:
        raise HTTPException(400, "No valid files specified")
    dbx = DropboxClient()
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name in filenames:
            try:
                content, _ = dbx.download(f"{folder}/{name}"); zf.writestr(name, content)
            except Exception:
                pass
    buf.seek(0)
    return StreamingResponse(buf, media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="media.zip"'})

@app.get("/api/media/{filename}/link")
async def get_media_link(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    """Return a temporary Dropbox direct-access URL — supports Range requests for video seeking."""
    ctx    = _require_auth_ctx(mp_session)
    folder = ctx["folder"]
    try:
        url = DropboxClient().get_temporary_link(f"{folder}/{filename}")
        return {"url": url}
    except Exception as e:
        raise HTTPException(500, f"Could not get temporary link: {e}")

@app.get("/api/media/{filename}/download")
async def download_media(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx    = _require_auth_ctx(mp_session)
    folder = ctx["folder"]
    stream, mime = DropboxClient().download_stream(f"{folder}/{filename}")
    return StreamingResponse(stream, media_type=mime,
        headers={"Content-Disposition": f'attachment; filename="{filename.replace(chr(34),"")}"'})

@app.get("/api/media/{filename}")
async def get_media(filename: str, mp_session: Optional[str] = Cookie(default=None)):
    ctx    = _require_auth_ctx(mp_session)
    folder = ctx["folder"]
    stream, mime = DropboxClient().download_stream(f"{folder}/{filename}")
    return StreamingResponse(stream, media_type=mime)
