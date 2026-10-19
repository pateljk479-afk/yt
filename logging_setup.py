"""Console logging that coexists with the bot's in-memory /logs handler."""
import logging
from logging.handlers import RotatingFileHandler
import os
import sys


def configure_logging(root=None, stream=None):
    root = root if root is not None else logging.getLogger()
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    if not isinstance(level, int):
        level = logging.INFO
    # Do not enable wire-level Pyrogram DEBUG dumps when debugging transfers.
    root.setLevel(max(logging.INFO, level))
    for name in ("bot", "streaming", "direct_download", "upload_tuning", "downloader", "main"):
        logging.getLogger(name).setLevel(level)
    handler = next((h for h in root.handlers if getattr(h, "_bot_console", False)), None)
    if handler is None:
        handler = logging.StreamHandler(stream if stream is not None else sys.stdout)
        handler._bot_console = True
        root.addHandler(handler)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    log_path = os.getenv("LOG_FILE", "bot.log").strip()
    file_handler = next((h for h in root.handlers if getattr(h, "_bot_file", False)), None)
    if log_path and file_handler is None:
        try:
            file_handler = RotatingFileHandler(log_path, maxBytes=2 * 1024 * 1024,
                                               backupCount=2, encoding="utf-8")
            file_handler._bot_file = True
            root.addHandler(file_handler)
        except OSError:
            root.warning("File logging unavailable; console and /logs remain active")
    if file_handler:
        file_handler.setLevel(level)
        file_handler.setFormatter(handler.formatter)
    return handler
