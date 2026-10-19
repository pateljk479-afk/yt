"""Direct CDN -> FFmpeg -> bounded MTProto upload, without a media file on disk.

Telegram's streamed-upload protocol uses total_parts=-1 until EOF:
https://core.telegram.org/api/files#streamed-uploads
"""
import asyncio
import html
import io
import logging
import math
import os
import re
import shutil
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import aiohttp
from PIL import Image
from pyrogram import raw, enums
from pyrogram.errors import FloodWait, InternalServerError
from pyrogram.session import Session

import config
from downloader import extract_info, sanitize_filename
from uploader import human_readable_size, human_readable_time
from direct_download import DirectRangeRelay, media_headers

logger = logging.getLogger(__name__)
PART_SIZE = 512 * 1024


class UnsupportedStream(ValueError):
    """This source needs the regular yt-dlp download path."""


@dataclass
class StreamPlan:
    info: dict
    formats: list
    estimated_size: int
    # Matches the playlist's cleanup contract; streamed videos own no disk files.
    temp_dir: str = None


@dataclass
class TransferStats:
    ready_at: float = 0
    finished_at: float = 0
    source_bytes: int = 0
    uploaded_bytes: int = 0
    buffered_bytes: int = 0
    peak_buffered_bytes: int = 0
    in_flight: int = 0
    source_wait: float = 0
    source_bps: float = 0
    upload_bps: float = 0
    sampled_at: float = 0

    @property
    def seconds(self):
        return max(0.001, (self.finished_at or time.monotonic()) - self.ready_at)


def _size(fmt, duration):
    return int(fmt.get("filesize") or fmt.get("filesize_approx") or
               ((fmt.get("tbr") or fmt.get("abr") or 0) * 1000 / 8 * duration))


def select_streams(info, quality_key, max_quality_height=None):
    """Select complete HTTP resources only; never feed a manifest to FFmpeg.

    Restrict protocols so FFmpeg cannot follow manifest URLs through a proxy or
    download unbounded live streams. DASH/HLS-only sources use yt-dlp instead.
    """
    if info.get("is_live") or info.get("live_status") == "is_live":
        raise UnsupportedStream("Live streams require the regular downloader")
    duration = int(info.get("duration") or 0)
    limit = min(config.MAX_FILE_SIZE, 2000 * 1024 * 1024)
    height = (max_quality_height or 4320) if quality_key == "max" else int(quality_key)
    formats = [f for f in info.get("formats", [])
               if f.get("protocol") in ("https", "http")
               and urlsplit(f.get("url", "")).scheme in ("http", "https")
               and not f.get("fragments") and not f.get("has_drm")]
    videos = sorted((f for f in formats
                     if f.get("vcodec") not in (None, "none")
                     and f.get("ext") == "mp4" and 0 < (f.get("height") or 0) <= height),
                    key=lambda f: (f.get("height") or 0, f.get("tbr") or 0), reverse=True)
    audios = sorted((f for f in formats if f.get("vcodec") == "none"
                     and f.get("acodec") not in (None, "none") and f.get("ext") == "m4a"),
                    key=lambda f: f.get("abr") or f.get("tbr") or 0, reverse=True)
    for video in videos:
        selected = [video]
        if video.get("acodec") in (None, "none"):
            if not audios:
                continue
            selected += [audios[0]]
        sizes = [_size(f, duration) for f in selected]
        estimate = sum(sizes)
        # Reserve muxing overhead. Unknown sizes are checked while uploading.
        if all(sizes) and estimate * 1.02 > limit:
            continue
        return StreamPlan(info, selected, estimate)
    raise UnsupportedStream("No suitable direct MP4 streams within the Telegram limit")


def prepare_stream(url, quality_key, progress_hook=None, max_quality_height=None):
    if not shutil.which("ffmpeg"):
        raise RuntimeError("FFmpeg is required for storage-efficient uploads. Install FFmpeg on the server.")
    return select_streams(extract_info(url), quality_key, max_quality_height)


def ffmpeg_command(plan, input_urls=None):
    cmd = [shutil.which("ffmpeg") or "ffmpeg", "-hide_banner", "-loglevel", "error",
           "-nostdin", "-nostats", "-progress", "pipe:2"]
    for index, fmt in enumerate(plan.formats):
        # Explicitly override both FFmpeg's proxy option and inherited proxy env.
        cmd += ["-protocol_whitelist", "http,https,tcp,tls,crypto", "-http_proxy", "",
                "-rw_timeout", "30000000", "-reconnect", "1",
                "-reconnect_streamed", "1", "-reconnect_delay_max", "5"]
        safe_headers = media_headers(plan.info, fmt)
        if safe_headers:
            cmd += ["-headers", "".join(f"{k}: {v}\r\n" for k, v in safe_headers.items())]
        cmd += ["-i", input_urls[index] if input_urls else fmt["url"]]
    cmd += ["-map", "0:v:0", "-map", "1:a:0" if len(plan.formats) > 1 else "0:a:0",
            "-c", "copy", "-map_metadata", "-1", "-movflags",
            "+frag_keyframe+empty_moov+default_base_moof", "-frag_duration", "2000000",
            "-f", "mp4", "pipe:1"]
    return cmd


async def _invoke_part(session, rpc):
    for attempt in range(5):
        try:
            if not await session.invoke(rpc):
                raise OSError("Telegram rejected an upload chunk")
            return
        except FloodWait as exc:
            if attempt == 4:
                raise
            logger.warning("Telegram chunk %d waiting %ds (FloodWait)", rpc.file_part, exc.value)
            await asyncio.sleep(exc.value)
        except (OSError, asyncio.TimeoutError, InternalServerError) as exc:
            if attempt == 4:
                raise
            logger.warning("Telegram chunk %d retry %d/5 after %s", rpc.file_part, attempt + 1, type(exc).__name__)
            await asyncio.sleep(0.5 * (attempt + 1))


async def upload_stream(client, reader, filename, estimated_size=0, progress=None,
                        stats=None, check_header=True):
    """Bounded producer buffer + work-conserving pipelined Telegram requests.

    Every free connection slot takes the next chunk, so a slow connection cannot
    monopolize the upload window. Progress messages run outside the data path.
    """
    stats = stats or TransferStats()
    workers = max(1, min(config.UPLOAD_WORKERS, 16))
    depth = config.UPLOAD_REQUESTS_PER_CONNECTION
    window = workers * depth
    queue = asyncio.Queue(max(1, config.STREAM_BUFFER_BYTES // PART_SIZE))
    sessions = []
    pending = {}
    loads = [0] * workers
    producer = reporter = None
    total = part = 0
    file_id = client.rnd_id()
    limit = min(config.MAX_FILE_SIZE, 2000 * 1024 * 1024)
    dc_id = await client.storage.dc_id()
    auth_key = await client.storage.auth_key()
    test_mode = await client.storage.test_mode()

    async def produce():
        while True:
            started = time.monotonic()
            try:
                chunk = await asyncio.wait_for(reader.readexactly(PART_SIZE), config.STREAM_READ_TIMEOUT)
                eof = False
            except asyncio.IncompleteReadError as exc:
                chunk, eof = exc.partial, True
            except asyncio.TimeoutError as exc:
                raise TimeoutError("Direct video stream stalled; retry the video.") from exc
            stats.source_wait += time.monotonic() - started
            stats.source_bytes += len(chunk)
            if stats.source_bytes > limit:
                raise ValueError("Stream exceeds the Telegram file size limit; choose a lower quality.")
            await queue.put((chunk, eof))
            stats.buffered_bytes = queue.qsize() * PART_SIZE
            stats.peak_buffered_bytes = max(stats.peak_buffered_bytes, stats.buffered_bytes)
            if eof:
                return

    async def report():
        previous_time, previous_source, previous_upload = stats.ready_at, 0, 0
        while True:
            await asyncio.sleep(config.PROGRESS_UPDATE_INTERVAL)
            now = time.monotonic()
            elapsed = max(0.001, now - previous_time)
            stats.source_bps = (stats.source_bytes - previous_source) / elapsed
            stats.upload_bps = (stats.uploaded_bytes - previous_upload) / elapsed
            stats.sampled_at = now
            previous_time, previous_source, previous_upload = now, stats.source_bytes, stats.uploaded_bytes
            logger.info("Transfer feed %.1f KiB/s -> Telegram %.1f KiB/s; buffered %.1f MiB; in-flight %d/%d",
                        stats.source_bps / 1024, stats.upload_bps / 1024,
                        stats.buffered_bytes / (1024 * 1024), stats.in_flight, window)
            if progress:
                try:
                    await asyncio.wait_for(progress(
                        stats.uploaded_bytes, max(estimated_size, stats.uploaded_bytes + 1)),
                        config.PROGRESS_UPDATE_INTERVAL)
                except Exception:
                    logger.debug("Transfer progress update unavailable")

    async def send(index, rpc):
        await _invoke_part(sessions[index], rpc)
        stats.uploaded_bytes += len(rpc.bytes)

    async def collect(done):
        for task in done:
            loads[pending.pop(task)] -= 1
        stats.in_flight = len(pending)
        results = await asyncio.gather(*done, return_exceptions=True)
        for result in results:
            if isinstance(result, BaseException):
                raise result

    async def drain(one=False):
        done, _ = await asyncio.wait(pending, return_when=(
            asyncio.FIRST_COMPLETED if one else asyncio.ALL_COMPLETED))
        await collect(done)

    try:
        for _ in range(workers):
            session = Session(client, dc_id, auth_key, test_mode, is_media=True)
            sessions.append(session)
        starts = await asyncio.gather(*(s.start() for s in sessions), return_exceptions=True)
        for result in starts:
            if isinstance(result, BaseException):
                raise result
        stats.ready_at = time.monotonic()
        producer = asyncio.create_task(produce())
        reporter = asyncio.create_task(report())
        while True:
            if len(pending) >= window:
                await drain(one=True)
            read = asyncio.create_task(queue.get())
            try:
                while not read.done():
                    if producer.done():
                        producer.result()
                    waiters = set(pending) | {read}
                    if not producer.done():
                        waiters.add(producer)
                    done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
                    await collect(done & pending.keys())
                if producer.done():
                    producer.result()
                await collect({task for task in pending if task.done()})
                chunk, eof = await read
                stats.buffered_bytes = queue.qsize() * PART_SIZE
            finally:
                if not read.done():
                    read.cancel()
                await asyncio.gather(read, return_exceptions=True)
            total += len(chunk)
            if check_header and part == 0 and (len(chunk) < 12 or chunk[4:8] != b"ftyp"):
                raise ValueError("FFmpeg did not produce a valid MP4 stream")
            if eof:
                if pending:
                    await drain()
                # Telegram requires a final empty part on exact chunk boundaries.
                await _invoke_part(sessions[0], raw.functions.upload.SaveBigFilePart(
                    file_id=file_id, file_part=part,
                    file_total_parts=math.ceil(total / PART_SIZE), bytes=chunk))
                stats.uploaded_bytes += len(chunk)
                stats.finished_at = time.monotonic()
                logger.info("Telegram upload complete: %.2f MiB/s, %d connections, %d requests/connection, %.1f MiB peak buffer",
                            total / stats.seconds / (1024 * 1024), workers, depth,
                            stats.peak_buffered_bytes / (1024 * 1024))
                return raw.types.InputFileBig(id=file_id, parts=math.ceil(total / PART_SIZE), name=filename), total
            rpc = raw.functions.upload.SaveBigFilePart(
                file_id=file_id, file_part=part, file_total_parts=-1, bytes=chunk)
            index = min(range(workers), key=lambda i: loads[i])
            loads[index] += 1
            pending[asyncio.create_task(send(index, rpc))] = index
            stats.in_flight = len(pending)
            part += 1
    finally:
        tasks = list(pending) + [t for t in (producer, reporter) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        stats.in_flight = stats.buffered_bytes = 0
        await asyncio.gather(*(s.stop() for s in sessions), return_exceptions=True)


async def telegram_speed_probe(client, size=16 * 1024 * 1024):
    """Use the real uploader with an in-memory source; exclude session startup."""
    class ProbeReader:
        remaining = size
        payload = bytes(PART_SIZE)

        async def readexactly(self, count):
            length = min(count, self.remaining)
            self.remaining -= length
            if length < count:
                raise asyncio.IncompleteReadError(self.payload[:length], count)
            return self.payload

    stats = TransferStats()
    await upload_stream(client, ProbeReader(), "speed-probe.bin", size, stats=stats, check_header=False)
    return size / stats.seconds


async def _stderr_tail(reader):
    tail = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            return tail
        tail = (tail + chunk)[-8192:]


async def _thumbnail(client, url):
    if not url or urlsplit(url).scheme not in ("http", "https"):
        return None
    try:
        async with aiohttp.ClientSession(trust_env=False, timeout=aiohttp.ClientTimeout(total=15)) as http:
            async with http.get(url) as response:
                response.raise_for_status()
                chunks, size = [], 0
                async for block in response.content.iter_chunked(64 * 1024):
                    chunks.append(block)
                    size += len(block)
                    if size > 2 * 1024 * 1024:
                        return None
        def convert():
            with Image.open(io.BytesIO(b"".join(chunks))) as source:
                source.thumbnail((320, 320))
                image = source.convert("RGB")
                thumb = io.BytesIO()
                thumb.name = "thumb.jpg"
                image.save(thumb, "JPEG", quality=85)
                thumb.seek(0)
                return thumb
        thumb = await asyncio.to_thread(convert)
        try:
            return await client.save_file(thumb)
        finally:
            thumb.close()
    except Exception:
        logger.warning("Could not prepare streamed video's thumbnail")
        return None


async def stream_video(client, chat_id, plan, progress=None, stats=None):
    async with DirectRangeRelay(plan) as relay:
        return await _stream_video(client, chat_id, plan, relay, progress, stats)


async def _stream_video(client, chat_id, plan, relay, progress=None, stats=None):
    """Publish only after FFmpeg exits successfully and all chunks are saved."""
    info = plan.info
    filename = sanitize_filename(info.get("title", "video")) + ".mp4"
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    logger.info("Streaming route: server direct CDN -> FFmpeg -> Telegram; no media files on disk")
    process = await asyncio.create_subprocess_exec(
        *ffmpeg_command(plan, relay.urls), stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE, env=env, limit=PART_SIZE * 2)
    errors = asyncio.create_task(_stderr_tail(process.stderr))
    try:
        file, size = await upload_stream(client, process.stdout, filename, plan.estimated_size, progress, stats)
        code = await asyncio.wait_for(process.wait(), 30)
        diagnostics = await errors
        if relay.failure:
            raise relay.failure
        if code:
            # FFmpeg stderr can contain signed CDN URLs and cookies. Do not echo it.
            raise RuntimeError("Direct video transfer failed in FFmpeg. Check CDN access or retry the video.")
        durations = re.findall(rb"out_time_us=(\d+)", diagnostics)
        expected = float(info.get("duration") or 0)
        actual = int(durations[-1]) / 1_000_000 if durations else 0
        if b"progress=end" not in diagnostics or (expected and actual < expected - max(5, expected * 0.02)):
            raise ValueError("The video stream ended early; retry this video. It was not sent to the chat.")
        thumb = await _thumbnail(client, info.get("thumbnail"))
        video = plan.formats[0]
        title = html.escape(str(info.get("title", "Video")))
        caption = (f"🎬 <b>{title}</b>\n\n"
                   f"⏱️ <b>Duration:</b> {human_readable_time(int(info.get('duration') or 0))}\n"
                   f"💾 <b>Size:</b> {human_readable_size(size)}")
        media = raw.types.InputMediaUploadedDocument(
            file=file, mime_type="video/mp4", thumb=thumb,
            attributes=[raw.types.DocumentAttributeVideo(
                duration=int(info.get("duration") or 0), w=int(video.get("width") or 0),
                h=int(video.get("height") or 0), supports_streaming=True),
                raw.types.DocumentAttributeFilename(file_name=filename)])
        parsed = await client.parser.parse(caption, enums.ParseMode.HTML)
        request = raw.functions.messages.SendMedia(
            peer=await client.resolve_peer(chat_id), media=media, random_id=client.rnd_id(),
            message=parsed["message"], entities=parsed["entities"])
        # Reuse random_id on a timeout so a retried send cannot post duplicates.
        for attempt in range(3):
            try:
                return await client.invoke(request)
            except FloodWait as exc:
                if attempt == 2:
                    raise
                await asyncio.sleep(exc.value)
            except (OSError, asyncio.TimeoutError, InternalServerError):
                if attempt == 2:
                    raise
                await asyncio.sleep(2 * (attempt + 1))
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        # Drain stdout even after an upload error/cancellation so a full pipe
        # cannot prevent subprocess transport shutdown.
        while await process.stdout.read(64 * 1024):
            pass
        await process.wait()
        await asyncio.gather(errors, return_exceptions=True)
