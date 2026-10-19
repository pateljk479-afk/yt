import os
import shutil
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

# Telegram Credentials
# Obtain API_ID and API_HASH from https://my.telegram.org
# Obtain BOT_TOKEN from https://t.me/BotFather
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Telegram File Size Limits
# MTProto standard max upload is 2000 MiB (~2097152000 bytes).
# We keep a safe margin of 2000 MB (2,000,000,000 bytes) below 2 GB (2,147,483,648 bytes)
# to guarantee uploads never exceed Telegram limits.
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(2000 * 1024 * 1024)))  # 2000 MB in bytes

# Download & Temporary Storage
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", os.path.abspath("./downloads"))
os.makedirs(DOWNLOAD_DIR, exist_ok=True)
# Budget for a 2 GB host, reserving space for app/dependencies.
DOWNLOAD_STORAGE_LIMIT_BYTES = int(os.getenv("DOWNLOAD_STORAGE_LIMIT_MB", "1536")) * 1024 * 1024
MIN_FREE_DISK_BYTES = int(os.getenv("MIN_FREE_DISK_MB", "128")) * 1024 * 1024

# Performance & Hardware Optimization
# Configured for high bandwidth (600 Mbps) & multi-core CPU (4 cores, 1.5 GHz, 2 GB RAM)
CONCURRENT_FRAGMENT_DOWNLOADS = int(os.getenv("CONCURRENT_FRAGMENT_DOWNLOADS", "4"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "1"))
MAX_CONCURRENT_TRANSMISSIONS = int(os.getenv("MAX_CONCURRENT_TRANSMISSIONS", "1"))
UPLOAD_WORKERS = max(1, min(16, int(os.getenv("UPLOAD_WORKERS", "8"))))
UPLOAD_REQUESTS_PER_CONNECTION = max(1, min(8, int(os.getenv("UPLOAD_REQUESTS_PER_CONNECTION", "4"))))
UPLOAD_ADAPTIVE = os.getenv("UPLOAD_ADAPTIVE", "true").lower() in ("true", "1", "yes")
UPLOAD_MAX_WORKERS = max(UPLOAD_WORKERS, min(16, int(os.getenv("UPLOAD_MAX_WORKERS", "16"))))
UPLOAD_MAX_REQUESTS_PER_CONNECTION = max(UPLOAD_REQUESTS_PER_CONNECTION, min(8, int(
    os.getenv("UPLOAD_MAX_REQUESTS_PER_CONNECTION", "8"))))
CRYPTO_WORKERS = max(1, min(8, int(os.getenv("CRYPTO_WORKERS", "4"))))
UPLOAD_TUNE_INTERVAL = max(5, float(os.getenv("UPLOAD_TUNE_INTERVAL", "5")))
UPLOAD_RECOVERY_SECONDS = max(10, float(os.getenv("UPLOAD_RECOVERY_SECONDS", "30")))
PYROGRAM_WAIT_TIMEOUT = int(os.getenv("PYROGRAM_WAIT_TIMEOUT", "60"))
PREFETCH_PLAYLIST = os.getenv("PREFETCH_PLAYLIST", "false").lower() in ("true", "1", "yes")
# Stream MP4 output directly into Telegram instead of storing input + merged copies.
STREAM_VIDEO_UPLOADS = os.getenv("STREAM_VIDEO_UPLOADS", "true").lower() in ("true", "1", "yes")
STREAM_READ_TIMEOUT = max(30, int(os.getenv("STREAM_READ_TIMEOUT", "90")))
STREAM_BUFFER_BYTES = max(1, min(256, int(os.getenv("STREAM_BUFFER_MB", "128")))) * 1024 * 1024
STREAM_PARALLEL_DOWNLOADS = os.getenv("STREAM_PARALLEL_DOWNLOADS", "true").lower() in ("true", "1", "yes")
STREAM_DOWNLOAD_WORKERS = int(os.getenv("STREAM_DOWNLOAD_WORKERS", "8"))
STREAM_DOWNLOAD_CHUNK_BYTES = int(os.getenv("STREAM_DOWNLOAD_CHUNK_KB", "2048")) * 1024
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", str(1024 * 1024)))  # 1 MB I/O buffer
HTTP_CHUNK_SIZE = int(os.getenv("HTTP_CHUNK_SIZE", str(10 * 1024 * 1024)))  # 10 MB HTTP chunks
COOKIES_FILE = os.getenv("COOKIES_FILE", os.path.abspath("cookies.txt") if os.path.exists("cookies.txt") else "")
COOKIES_DATA = os.getenv("COOKIES_DATA", os.getenv("YOUTUBE_COOKIES", ""))
def parse_proxy_list(raw: str) -> list[str]:
    """Parse comma, semicolon, or newline-separated proxy URLs from environment."""
    if not raw:
        return []
    import re
    tokens = re.split(r"[,;\n\r]+", raw)
    proxies = []
    for token in tokens:
        cleaned = token.strip().strip("'\"")
        if cleaned:
            proxies.append(cleaned)
    return proxies

# Optional authenticated egress proxy for YouTube. Supports single proxy or comma-separated list.
YOUTUBE_PROXY_RAW = os.getenv("YOUTUBE_PROXY", "").strip()
YOUTUBE_PROXIES = parse_proxy_list(YOUTUBE_PROXY_RAW)
YOUTUBE_PROXY = YOUTUBE_PROXIES[0] if YOUTUBE_PROXIES else ""
# Media downloads never use this proxy; no media fallback is permitted.

# Auto-generate cookies.txt from environment variable (supports raw text or base64)
if COOKIES_DATA:
    try:
        import base64
        _raw = COOKIES_DATA.strip()
        if "\\n" in _raw and "\n" not in _raw:
            _raw = _raw.replace("\\n", "\n")
        if not (_raw.startswith("# Netscape") or "\t" in _raw):
            try:
                _raw = base64.b64decode(_raw).decode("utf-8")
            except Exception:
                pass
        abs_cookie_target = os.path.abspath("cookies.txt")
        with open(abs_cookie_target, "w", encoding="utf-8") as _f:
            _f.write(_raw)
        COOKIES_FILE = abs_cookie_target
    except Exception as _e:
        pass

# Check if aria2c external downloader is available on system
ARIA2C_AVAILABLE = bool(shutil.which("aria2c"))

# Web Server / Health Check for Cloud Hosting (Render, Railway, Pterodactyl, BotKeep)
PORT = int(os.getenv("PORT", os.getenv("SERVER_PORT", "8080")))
HOST = os.getenv("HOST", "0.0.0.0")
ENABLE_WEB_SERVER = os.getenv("ENABLE_WEB_SERVER", "true").lower() in ("true", "1", "yes")

# Progress update throttle interval (seconds) to prevent Telegram FloodWait
PROGRESS_UPDATE_INTERVAL = max(5.0, float(os.getenv("PROGRESS_UPDATE_INTERVAL", "5.0")))

# Session TTL in seconds (1 hour default)
SESSION_TTL_SECONDS = int(os.getenv("SESSION_TTL_SECONDS", "3600"))

def validate_config() -> list[str]:
    """Validate core credentials and return list of missing configuration warnings/errors."""
    errors = []
    if not API_ID:
        errors.append("API_ID is not set or invalid (must be an integer from https://my.telegram.org).")
    if not API_HASH:
        errors.append("API_HASH is not set (obtain from https://my.telegram.org).")
    if not BOT_TOKEN:
        errors.append("BOT_TOKEN is not set (obtain from @BotFather on Telegram).")
    return errors
