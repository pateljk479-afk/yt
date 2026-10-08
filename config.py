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

# Performance & Hardware Optimization
# Configured for high bandwidth (600 Mbps) & multi-core CPU (4 cores, 1.5 GHz, 2 GB RAM)
CONCURRENT_FRAGMENT_DOWNLOADS = int(os.getenv("CONCURRENT_FRAGMENT_DOWNLOADS", "16"))
MAX_CONCURRENT_DOWNLOADS = int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3"))
MAX_CONCURRENT_TRANSMISSIONS = int(os.getenv("MAX_CONCURRENT_TRANSMISSIONS", "8"))
BUFFER_SIZE = int(os.getenv("BUFFER_SIZE", str(1024 * 1024)))  # 1 MB I/O buffer
HTTP_CHUNK_SIZE = int(os.getenv("HTTP_CHUNK_SIZE", str(10 * 1024 * 1024)))  # 10 MB HTTP chunks
COOKIES_FILE = os.getenv("COOKIES_FILE", "cookies.txt" if os.path.exists("cookies.txt") else "")

# Check if aria2c external downloader is available on system
ARIA2C_AVAILABLE = bool(shutil.which("aria2c"))

# Web Server / Health Check for Cloud Hosting (Render, Railway, Koyeb)
PORT = int(os.getenv("PORT", "8080"))
HOST = os.getenv("HOST", "0.0.0.0")
ENABLE_WEB_SERVER = os.getenv("ENABLE_WEB_SERVER", "true").lower() in ("true", "1", "yes")

# Progress update throttle interval (seconds) to prevent Telegram FloodWait
PROGRESS_UPDATE_INTERVAL = float(os.getenv("PROGRESS_UPDATE_INTERVAL", "3.0"))

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
