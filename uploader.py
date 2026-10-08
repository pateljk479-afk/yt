import os
import shutil
import subprocess
import logging
from typing import Optional
from PIL import Image

logger = logging.getLogger(__name__)


def human_readable_size(size_bytes: int) -> str:
    """Format bytes into readable units (B, KB, MB, GB)."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def human_readable_time(seconds: int) -> str:
    """Format seconds into HH:MM:SS or MM:SS."""
    if seconds <= 0:
        return "00:00"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h > 0:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def format_progress_bar(
    current: int,
    total: int,
    speed: float,
    eta: int,
    stage: str = "Downloading",
) -> str:
    """
    Generate a sleek Telegram progress bar message.
    stage: 'Downloading' or 'Uploading'
    """
    if total <= 0:
        percentage = 0.0
    else:
        percentage = min(100.0, (current / total) * 100)

    filled_blocks = int(percentage // 10)
    empty_blocks = 10 - filled_blocks
    bar = "█" * filled_blocks + "░" * empty_blocks

    icon = "📥" if "download" in stage.lower() else "📤"
    speed_str = human_readable_size(int(speed)) + "/s" if speed > 0 else "-- MB/s"
    eta_str = human_readable_time(eta) if eta > 0 else "--:--"

    text = (
        f"{icon} <b>{stage}...</b>\n\n"
        f"<code>[{bar}] {percentage:.1f}%</code>\n"
        f"📊 <b>Progress:</b> {human_readable_size(current)} / {human_readable_size(total)}\n"
        f"⚡ <b>Speed:</b> {speed_str} | ⏱️ <b>ETA:</b> {eta_str}"
    )
    return text


def verify_media_integrity(file_path: str, is_audio: bool = False) -> bool:
    """
    Verify that the downloaded file exists, is non-empty, and possesses a valid container.
    Uses ffprobe if available for deep inspection.
    """
    if not os.path.exists(file_path):
        logger.error("Media file does not exist: %s", file_path)
        return False

    size = os.path.getsize(file_path)
    if size < 1024:  # At least 1 KB
        logger.error("Media file too small (%d bytes): %s", size, file_path)
        return False

    # Check MP4 container magic header if not audio
    if not is_audio and file_path.lower().endswith(".mp4"):
        try:
            with open(file_path, "rb") as f:
                header = f.read(16)
                # Check for standard 'ftyp' box signature
                if b"ftyp" not in header:
                    logger.warning("MP4 header missing 'ftyp' signature: %s", file_path)
        except Exception as e:
            logger.error("Error reading file header: %s", e)
            return False

    # Deep verification with ffprobe if present
    ffprobe_cmd = shutil.which("ffprobe")
    if ffprobe_cmd:
        try:
            cmd = [
                ffprobe_cmd,
                "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                file_path,
            ]
            result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=15)
            if result.returncode != 0:
                logger.error("ffprobe check failed: %s", result.stderr.decode(errors="ignore"))
                return False
            duration_str = result.stdout.decode().strip()
            if not duration_str or float(duration_str) <= 0:
                logger.warning("ffprobe returned invalid duration: %s", duration_str)
                return False
        except Exception as e:
            logger.warning("ffprobe check encountered exception (skipping deep check): %s", e)

    return True


def prepare_thumbnail(thumb_path: Optional[str], output_dir: str) -> Optional[str]:
    """
    Convert thumbnail to standard JPEG format compatible with Telegram send_video.
    Safely handles WEBP, PNG, RGBA, and palette images.
    """
    if not thumb_path or not os.path.exists(thumb_path):
        return None

    try:
        out_jpg = os.path.join(output_dir, "thumb.jpg")
        with Image.open(thumb_path) as img:
            # Handle alpha channel
            if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
                bg = Image.new("RGB", img.size, (255, 255, 255))
                alpha = img.convert("RGBA").split()[-1]
                bg.paste(img.convert("RGBA"), mask=alpha)
                img = bg
            else:
                img = img.convert("RGB")

            # Resize to max 320x320 if larger (Telegram recommended thumbnail dimension)
            img.thumbnail((320, 320), Image.Resampling.LANCZOS)
            img.save(out_jpg, "JPEG", quality=90, optimize=True)

        return out_jpg
    except Exception as e:
        logger.error("Failed to prepare thumbnail %s: %s", thumb_path, e)
        return None


def safe_cleanup(path: Optional[str]):
    """Recursively delete temporary directory or file safely."""
    if not path or not os.path.exists(path):
        return
    try:
        if os.path.isdir(path):
            shutil.rmtree(path, ignore_errors=True)
        else:
            os.remove(path)
    except Exception as e:
        logger.warning("Failed cleaning up %s: %s", path, e)
