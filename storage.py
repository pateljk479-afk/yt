"""Storage checks and narrowly scoped cleanup for bot-owned downloads."""
import logging
import os
import shutil
import config

logger = logging.getLogger(__name__)


def directory_size(path):
    total = 0
    for root, dirs, files in os.walk(path, followlinks=False):
        dirs[:] = [name for name in dirs if not os.path.islink(os.path.join(root, name))]
        for name in files:
            item = os.path.join(root, name)
            try:
                if not os.path.islink(item):
                    total += os.path.getsize(item)
            except FileNotFoundError:
                pass
    return total


def ensure_download_space(extra=0):
    used = directory_size(config.DOWNLOAD_DIR)
    free = shutil.disk_usage(config.DOWNLOAD_DIR).free
    if used + extra > config.DOWNLOAD_STORAGE_LIMIT_BYTES or free < extra + config.MIN_FREE_DISK_BYTES:
        raise RuntimeError("Not enough server storage for this download/merge. "
                           "Choose a lower quality or wait for the current upload to finish.")


def cleanup_download_folder(path):
    root = os.path.realpath(config.DOWNLOAD_DIR)
    target = os.path.abspath(path)
    if (os.path.dirname(os.path.realpath(target)) != root
            or not os.path.basename(target).startswith("yt_dl_")
            or os.path.islink(target)):
        raise ValueError("Refusing cleanup outside a bot-owned download folder")
    if os.path.isdir(target):
        shutil.rmtree(target)
        logger.info("Removed temporary download folder: %s", os.path.basename(target))


def cleanup_stream_part(path):
    """Delete one bot-owned staged stream part, never an arbitrary file."""
    root = os.path.realpath(config.DOWNLOAD_DIR)
    target = os.path.abspath(path)
    if (os.path.dirname(os.path.realpath(target)) != root
            or not os.path.basename(target).startswith("stream_part_")
            or os.path.islink(target)):
        raise ValueError("Refusing cleanup outside a bot-owned stream part")
    if os.path.isfile(target):
        os.unlink(target)
        logger.info("Removed temporary stream part: %s", os.path.basename(target))


def cleanup_stale_downloads():
    """Startup only: run before any download workers are started."""
    removed = 0
    for entry in os.scandir(config.DOWNLOAD_DIR):
        if entry.name.startswith("yt_dl_") and entry.is_dir(follow_symlinks=False):
            try:
                cleanup_download_folder(entry.path)
                removed += 1
            except OSError:
                logger.exception("Could not remove abandoned download folder")
        elif entry.name.startswith("stream_part_") and entry.is_file(follow_symlinks=False):
            try:
                cleanup_stream_part(entry.path)
                removed += 1
            except OSError:
                logger.exception("Could not remove abandoned stream part")
    return removed
