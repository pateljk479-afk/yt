import os
import re
import tempfile
import logging
from dataclasses import dataclass
from typing import Optional, Callable, List, Tuple
import yt_dlp

import config

logger = logging.getLogger(__name__)

# Standard resolution tiers and user-facing labels
RESOLUTION_TIERS = [
    (2160, "4K (2160p)"),
    (1440, "2K (1440p)"),
    (1080, "1080p (FHD)"),
    (720, "720p (HD)"),
    (480, "480p (SD)"),
    (360, "360p (SD)"),
    (240, "240p (Low)"),
    (144, "144p (Low)"),
]


@dataclass
class QualityOption:
    height: int
    label: str
    estimated_size: int  # in bytes
    format_selector: str
    is_max: bool = False


@dataclass
class VideoQualityAnalysis:
    title: str
    video_id: str
    duration: int
    thumbnail_url: str
    available_qualities: List[QualityOption]
    max_quality: Optional[QualityOption]
    audio_size: int
    excluded_qualities: List[Tuple[str, int]]  # Qualities exceeding 2GB limit


@dataclass
class DownloadResult:
    file_path: str
    file_name: str
    title: str
    duration: int
    width: Optional[int]
    height: Optional[int]
    thumbnail_path: Optional[str]
    file_size: int
    is_audio: bool
    temp_dir: str


def sanitize_filename(name: str) -> str:
    """Sanitize title for filesystem safety while preserving readable characters."""
    if not name:
        return "video"
    # Replace invalid filesystem characters on Windows and Unix
    clean = re.sub(r'[\\/*?:"<>|]', "_", name)
    # Remove redundant whitespace and truncate length safely
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean[:200] if len(clean) > 200 else clean


def estimate_audio_size(formats: list, duration: int) -> int:
    """Estimate best audio stream size in bytes."""
    best_audio_size = 0
    best_abr = 0
    
    for f in formats:
        if f.get("vcodec") == "none" and f.get("acodec") != "none":
            size = f.get("filesize") or f.get("filesize_approx")
            if size and size > best_audio_size:
                best_audio_size = size
            abr = f.get("abr") or f.get("tbr") or 0
            if abr > best_abr:
                best_abr = abr
                
    if best_audio_size > 0:
        return best_audio_size
    elif best_abr > 0 and duration > 0:
        return int((best_abr * 1000 / 8) * duration)
    elif duration > 0:
        # Fallback to standard 128 kbps audio
        return int((128 * 1000 / 8) * duration)
    return 10 * 1024 * 1024  # 10 MB fallback


def analyze_video_qualities(info: dict) -> VideoQualityAnalysis:
    """
    Analyze available video formats and estimate sizes.
    Strictly filters out any quality whose estimated size exceeds MAX_FILE_SIZE (2 GB).
    Selects the highest remaining quality as 'Max / High Quality'.
    """
    title = info.get("title", "Unknown Video")
    video_id = info.get("id", "")
    duration = int(info.get("duration") or 0)
    thumbnail_url = info.get("thumbnail", "")
    formats = info.get("formats", [])
    
    audio_size = estimate_audio_size(formats, duration)
    
    # Identify video heights available in the stream formats
    detected_heights = set()
    for f in formats:
        h = f.get("height")
        if h and isinstance(h, int) and h > 0:
            detected_heights.add(h)
            
    available_qualities: List[QualityOption] = []
    excluded_qualities: List[Tuple[str, int]] = []
    
    for tier_height, tier_label in RESOLUTION_TIERS:
        # Check if this resolution or higher is present
        matching_heights = [h for h in detected_heights if abs(h - tier_height) <= 20 or h == tier_height]
        if not matching_heights and not any(h >= tier_height for h in detected_heights):
            continue
            
        # Find best video format corresponding to this resolution
        best_size = 0
        best_vbr = 0
        has_direct_format = False
        
        for f in formats:
            fh = f.get("height") or 0
            if abs(fh - tier_height) <= 20:
                has_direct_format = True
                f_size = f.get("filesize") or f.get("filesize_approx")
                if f_size and f_size > best_size:
                    best_size = f_size
                vbr = f.get("vbr") or f.get("tbr") or 0
                if vbr > best_vbr:
                    best_vbr = vbr
                    
        if not has_direct_format and not any(h >= tier_height for h in detected_heights):
            continue
            
        estimated_video_size = 0
        if best_size > 0:
            estimated_video_size = best_size
        elif best_vbr > 0 and duration > 0:
            estimated_video_size = int((best_vbr * 1000 / 8) * duration)
        elif duration > 0:
            # Empirical fallback bitrates (kbps) based on resolution
            bitrate_map = {
                2160: 18000,
                1440: 9000,
                1080: 4500,
                720: 2500,
                480: 1200,
                360: 600,
                240: 350,
                144: 200,
            }
            estimated_video_size = int((bitrate_map.get(tier_height, 2000) * 1000 / 8) * duration)
        else:
            estimated_video_size = 100 * 1024 * 1024
            
        total_estimated = estimated_video_size + audio_size
        
        # Enforce Telegram 2 GB Limit
        if total_estimated > config.MAX_FILE_SIZE:
            logger.info("Quality %s exceeds 2GB (%s bytes) -> EXCLUDED", tier_label, total_estimated)
            excluded_qualities.append((tier_label, total_estimated))
        else:
            selector = f"bv*[height<={tier_height}]+ba[ext=m4a]/bv*[height<={tier_height}]+ba/b[height<={tier_height}]/best"
            available_qualities.append(
                QualityOption(
                    height=tier_height,
                    label=tier_label,
                    estimated_size=total_estimated,
                    format_selector=selector,
                )
            )
            
    # Mark the highest available quality <= 2 GB as max_quality
    max_quality = None
    if available_qualities:
        # Highest resolution is first in list because RESOLUTION_TIERS is descending
        max_quality = available_qualities[0]
        max_quality.is_max = True
        
    return VideoQualityAnalysis(
        title=title,
        video_id=video_id,
        duration=duration,
        thumbnail_url=thumbnail_url,
        available_qualities=available_qualities,
        max_quality=max_quality,
        audio_size=audio_size,
        excluded_qualities=excluded_qualities,
    )


def extract_info(url: str, is_playlist: bool = False) -> dict:
    """Extract YouTube metadata synchronously via yt-dlp."""
    # Clean URL if single video
    clean_url = url
    if not is_playlist and ("watch?v=" in url or "youtu.be/" in url):
        # Strip list parameter if single video was requested
        clean_url = re.sub(r"&list=[^&]+", "", clean_url)
        clean_url = re.sub(r"\?list=[^&]+&", "?", clean_url)

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": not is_playlist,
        "remote_components": ["ejs:github"],
    }
    cookies_path = config.COOKIES_FILE if (config.COOKIES_FILE and os.path.exists(config.COOKIES_FILE)) else ("cookies.txt" if os.path.exists("cookies.txt") else None)
    if cookies_path:
        ydl_opts["cookiefile"] = cookies_path
        
    if is_playlist:
        ydl_opts["extract_flat"] = "in_playlist"
        
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(clean_url, download=False)
        return info


def build_ydl_options(
    target_dir: str,
    format_selector: str,
    progress_hook: Optional[Callable] = None,
    is_audio: bool = False,
) -> dict:
    """Construct optimized yt-dlp options maximizing speed for 600 Mbps server."""
    out_tmpl = os.path.join(target_dir, "%(title)s.%(ext)s")
    
    opts = {
        "format": format_selector,
        "outtmpl": out_tmpl,
        "quiet": True,
        "no_warnings": True,
        "retries": 10,
        "fragment_retries": 10,
        "buffersize": config.BUFFER_SIZE,
        "http_chunk_size": config.HTTP_CHUNK_SIZE,
        "concurrent_fragment_downloads": config.CONCURRENT_FRAGMENT_DOWNLOADS,
        "writethumbnail": True,
        "nocheckcertificate": True,
        "ignoreerrors": False,
        "logtostderr": False,
        "remote_components": ["ejs:github"],
    }
    cookies_path = config.COOKIES_FILE if (config.COOKIES_FILE and os.path.exists(config.COOKIES_FILE)) else ("cookies.txt" if os.path.exists("cookies.txt") else None)
    if cookies_path:
        opts["cookiefile"] = cookies_path
    
    if is_audio:
        opts["format"] = "ba/b"
        opts["postprocessors"] = [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "320",
            }
        ]
    else:
        opts["merge_output_format"] = "mp4"
        opts["postprocessor_args"] = {
            "ffmpeg": ["-movflags", "+faststart"]
        }
        
    # Use aria2c if available on the system
    if config.ARIA2C_AVAILABLE:
        opts["external_downloader"] = "aria2c"
        opts["external_downloader_args"] = [
            "-x", "16",
            "-s", "16",
            "-j", "16",
            "-k", "1M",
        ]
        
    if progress_hook:
        opts["progress_hooks"] = [progress_hook]
        
    return opts


def download_media(
    url: str,
    quality_key: str,
    progress_hook: Optional[Callable] = None,
    max_quality_height: Optional[int] = None,
) -> DownloadResult:
    """
    Download video or audio using optimal multithreaded yt-dlp settings.
    Ensures MP4 format, 2 GB limit degradation, valid thumbnail extraction, and returns DownloadResult.
    """
    temp_dir = tempfile.mkdtemp(prefix="yt_dl_", dir=config.DOWNLOAD_DIR)
    is_audio = (quality_key == "audio")
    
    # 1. Determine format selector with strict <= 2GB degradation
    if is_audio:
        format_selector = "ba[ext=m4a]/ba/b"
    elif quality_key == "max":
        target_h = max_quality_height
        if not target_h:
            try:
                meta = extract_info(url, is_playlist=False)
                analysis = analyze_video_qualities(meta)
                target_h = analysis.max_quality.height if analysis.max_quality else 1080
            except Exception as e:
                logger.warning("Could not pre-analyze for max quality: %s", e)
                target_h = 1080
        format_selector = f"bv*[ext=mp4][height<={target_h}]+ba[ext=m4a]/b[ext=mp4][height<={target_h}]/bv*[height<={target_h}]+ba/best"
    else:
        try:
            requested_h = int(quality_key)
            target_h = requested_h
            # Check if requested format exceeds 2 GB limit; degrade if necessary
            try:
                meta = extract_info(url, is_playlist=False)
                analysis = analyze_video_qualities(meta)
                if analysis.max_quality and requested_h > analysis.max_quality.height:
                    logger.info("Requested quality %dp exceeds 2GB -> degrading to %dp", requested_h, analysis.max_quality.height)
                    target_h = analysis.max_quality.height
            except Exception as e:
                logger.debug("Format check skipped: %s", e)
            format_selector = f"bv*[ext=mp4][height<={target_h}]+ba[ext=m4a]/b[ext=mp4][height<={target_h}]/bv*[height<={target_h}]+ba/best"
        except ValueError:
            format_selector = "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/bv*+ba/best"
            
    ydl_opts = build_ydl_options(
        target_dir=temp_dir,
        format_selector=format_selector,
        progress_hook=progress_hook,
        is_audio=is_audio,
    )
    
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        
    title = info.get("title", "video")
    duration = int(info.get("duration") or 0)
    width = info.get("width")
    height = info.get("height")
    
    # Locate downloaded media file
    media_file = None
    thumb_file = None
    
    target_exts = [".mp3", ".m4a"] if is_audio else [".mp4", ".mkv", ".webm"]
    thumb_exts = [".jpg", ".jpeg", ".webp", ".png"]
    
    for fname in os.listdir(temp_dir):
        fpath = os.path.join(temp_dir, fname)
        ext = os.path.splitext(fname)[1].lower()
        if ext in thumb_exts:
            thumb_file = fpath
        elif ext in target_exts and not media_file:
            media_file = fpath
            
    if not media_file:
        for fname in os.listdir(temp_dir):
            fpath = os.path.join(temp_dir, fname)
            ext = os.path.splitext(fname)[1].lower()
            if ext not in thumb_exts and os.path.isfile(fpath):
                media_file = fpath
                break
                
    if not media_file or not os.path.exists(media_file):
        raise FileNotFoundError("Downloaded media file could not be found.")
        
    # Fallback to download thumbnail directly from info metadata if missing on disk
    if not thumb_file:
        thumb_url = info.get("thumbnail")
        if thumb_url:
            try:
                import urllib.request
                target_thumb = os.path.join(temp_dir, "yt_thumb.jpg")
                req = urllib.request.Request(
                    thumb_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                )
                with urllib.request.urlopen(req, timeout=10) as resp, open(target_thumb, "wb") as out_f:
                    out_f.write(resp.read())
                if os.path.exists(target_thumb) and os.path.getsize(target_thumb) > 0:
                    thumb_file = target_thumb
            except Exception as e:
                logger.warning("Failed to download thumbnail fallback from %s: %s", thumb_url, e)

    file_size = os.path.getsize(media_file)
    # Check Telegram 2 GB ceiling: if exceeded, degrade to lower resolution automatically
    if file_size > config.MAX_FILE_SIZE and not is_audio:
        logger.warning(
            "Downloaded file size (%d bytes) exceeds Telegram 2GB limit! Degrading resolution to 720p/480p...",
            file_size,
        )
        try:
            os.remove(media_file)
            fallback_selector = "bv*[ext=mp4][height<=720]+ba[ext=m4a]/b[ext=mp4][height<=720]/bv*[height<=720]+ba/best"
            ydl_opts_fb = build_ydl_options(
                target_dir=temp_dir,
                format_selector=fallback_selector,
                progress_hook=progress_hook,
                is_audio=is_audio,
            )
            with yt_dlp.YoutubeDL(ydl_opts_fb) as ydl_fb:
                info = ydl_fb.extract_info(url, download=True)
            for fname in os.listdir(temp_dir):
                fpath = os.path.join(temp_dir, fname)
                ext = os.path.splitext(fname)[1].lower()
                if ext in target_exts:
                    media_file = fpath
                    break
            file_size = os.path.getsize(media_file) if media_file and os.path.exists(media_file) else file_size
        except Exception as e:
            logger.error("Degradation fallback download failed: %s", e)

    # Preserve original YouTube title in file name on disk
    clean_base = sanitize_filename(title)
    final_ext = ".mp3" if is_audio else ".mp4"
    desired_filename = f"{clean_base}{final_ext}"
    desired_path = os.path.join(temp_dir, desired_filename)
    if os.path.abspath(media_file) != os.path.abspath(desired_path) and not os.path.exists(desired_path):
        try:
            os.rename(media_file, desired_path)
            media_file = desired_path
        except Exception as e:
            logger.warning("Could not rename to desired filename %s: %s", desired_path, e)

    original_filename = os.path.basename(media_file)
    
    return DownloadResult(
        file_path=media_file,
        file_name=original_filename,
        title=title,
        duration=duration,
        width=width,
        height=height,
        thumbnail_path=thumb_file,
        file_size=file_size,
        is_audio=is_audio,
        temp_dir=temp_dir,
    )
