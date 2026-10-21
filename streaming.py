"""Direct CDN -> FFmpeg -> simultaneous bounded MP4/MTProto transfer."""
import asyncio
import hashlib
import io
import logging
import math
import os
import re
import secrets
import shutil
import time
from dataclasses import dataclass, field
from types import SimpleNamespace
from urllib.parse import urlsplit

import aiohttp
from PIL import Image
from pyrogram import raw, enums
from pyrogram.errors import FloodWait, InternalServerError, RPCError
from ordered_session import OrderedMediaSession as Session

import config
from downloader import extract_info, sanitize_filename
from direct_download import DirectRangeRelay, media_headers
from upload_tuning import AdaptiveUploadWindow, previous_upload_setting, remember_upload_setting
from video_parts import FragmentedVideo, plan_parts
from resolution_options import nominal_height, resolution_tag
from chat_ui import video_caption
from live_spool import LivePartSpool
from transfer_runtime import (bounded, retry_stage, close_media_sessions,
                              TransferStageError)

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
    output_quality: str = ""
    collection: object = None


@dataclass
class DeliveryCheckpoint:
    # Retries recreate the same stream, discard ACKed parts without uploading
    # them again, and check the original part boundaries before continuing.
    parts: list = field(default_factory=list)
    last_sent: object = None
    estimate: int = 0


@dataclass
class TransferStats:
    part_index: int = 1
    part_count: int = 1
    completed_parts: int = 0
    completed_bytes: int = 0
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
    active_connections: int = 0
    request_capacity: int = 0
    retry_events: int = 0
    throttle_events: int = 0
    transfer_id: str = ""
    wait_stage: str = "starting"

    @property
    def seconds(self):
        return max(0.001, (self.finished_at or time.monotonic()) - self.ready_at)


def _size(fmt, duration):
    return int(fmt.get("filesize") or fmt.get("filesize_approx") or
               ((fmt.get("tbr") or fmt.get("abr") or 0) * 1000 / 8 * duration))


def select_streams(info, quality_key, max_quality_height=None):
    """Keep the requested resolution; complete HTTP and VOD HLS stream directly.

    Fragment-list-only sources without a playable URL use the disk fallback.
    Live input remains excluded because its final duration/size is unbounded.
    """
    if info.get("is_live") or info.get("live_status") == "is_live":
        raise UnsupportedStream("Live streams require the regular downloader")
    duration = int(info.get("duration") or 0)
    height = nominal_height((max_quality_height or 4320) if quality_key == "max" else int(quality_key))
    formats = [f for f in info.get("formats", [])
               if f.get("protocol") in ("https", "http", "m3u8", "m3u8_native")
               and urlsplit(f.get("url", "")).scheme in ("http", "https")
               and (not f.get("fragments") or f.get("protocol") in ("m3u8", "m3u8_native"))
               and not f.get("has_drm")]
    videos = sorted((f for f in formats
                     if f.get("vcodec") not in (None, "none")
                     and f.get("ext") in ("mp4", "webm") and 0 < nominal_height(f.get("height")) <= height),
                    key=lambda f: (f.get("height") or 0, f.get("ext") == "mp4", f.get("tbr") or 0), reverse=True)
    audios = sorted((f for f in formats if f.get("vcodec") == "none"
                     and f.get("acodec") not in (None, "none") and f.get("ext") in ("m4a", "webm")),
                    key=lambda f: (f.get("ext") == "m4a", f.get("abr") or f.get("tbr") or 0), reverse=True)
    for video in videos:
        selected = [video]
        if video.get("acodec") in (None, "none"):
            if not audios:
                continue
            selected += [audios[0]]
        sizes = [_size(f, duration) for f in selected]
        estimate = sum(sizes)
        # Size affects the number of attachments, never the selected quality.
        return StreamPlan(info, selected, estimate)
    raise UnsupportedStream("No suitable direct MP4 streams for the selected quality")


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
            "-c", "copy", "-map_metadata", "-1"]
    audio = plan.formats[-1]
    if audio.get("protocol") in ("m3u8", "m3u8_native") and str(audio.get("acodec", "")).startswith(("mp4a", "aac")):
        # HLS AAC carries ADTS headers. Convert container framing, not samples,
        # before the empty MP4 moov is written (automatic filtering is too late).
        cmd += ["-bsf:a", "aac_adtstoasc"]
    cmd += ["-movflags",
            "+frag_keyframe+empty_moov+default_base_moof", "-frag_size", str(8 * 1024 * 1024),
            "-f", "mp4", "pipe:1"]
    return cmd


async def _invoke_part(session, rpc, on_retry=None, transfer_id="-"):
    for attempt in range(3):
        try:
            # Surface throttling/retries to our controller instead of hiding
            # them inside Pyrogram's own retry and FloodWait loops.
            response = await bounded(session.invoke(rpc, retries=0, sleep_threshold=0,
                                                    timeout=config.PYROGRAM_WAIT_TIMEOUT),
                                     config.TRANSFER_STAGE_TIMEOUT, "Telegram upload chunk")
            # Pyrogram returns BadMsgNotification instead of raising it.  It
            # is not an acknowledgement that Telegram saved the part, so do
            # not count it as success or finalize a damaged document/video.
            if isinstance(response, (raw.types.BadMsgNotification, raw.types.BadServerSalt)):
                logger.warning("Transfer %s Telegram rejected chunk %d with MTProto error %d; retrying",
                               transfer_id, rpc.file_part, response.error_code)
                raise OSError(f"Telegram rejected upload chunk with MTProto error {response.error_code}")
            if not response:
                raise OSError("Telegram rejected an upload chunk")
            return
        except FloodWait as exc:
            if on_retry:
                on_retry("throttle", exc.value)
            if attempt == 2 or exc.value > config.TRANSFER_STAGE_TIMEOUT:
                raise TransferStageError(f"Telegram upload throttled for {exc.value}s; retry budget exhausted") from exc
            logger.warning("Transfer %s Telegram chunk %d waiting %ds (FloodWait)", transfer_id, rpc.file_part, exc.value)
            await asyncio.sleep(exc.value)
        except RPCError as exc:
            # Request-shape and permission errors cannot be fixed by sending
            # the same bytes again.  Surface Telegram's exact error code in
            # the logs and fail immediately instead of hiding it behind a
            # generic "two retries" message.
            if not isinstance(exc, InternalServerError):
                logger.error("Transfer %s Telegram permanently rejected chunk=%d/%s: %s (%s)",
                             transfer_id, rpc.file_part,
                             getattr(rpc, "file_total_parts", "small"),
                             getattr(exc, "ID", type(exc).__name__), exc, exc_info=True)
                raise TransferStageError(
                    f"Telegram rejected upload chunk: {getattr(exc, 'ID', type(exc).__name__)}") from exc
            if on_retry:
                on_retry("network", 0)
            if attempt == 2:
                logger.error("Transfer %s Telegram server failure exhausted chunk=%d/%s: %s",
                             transfer_id, rpc.file_part, getattr(rpc, "file_total_parts", "small"), exc,
                             exc_info=True)
                raise TransferStageError("Telegram upload chunk failed after two retries") from exc
            logger.warning("Transfer %s Telegram server failure chunk=%d retry=%d/2: %s",
                           transfer_id, rpc.file_part, attempt + 1, exc, exc_info=True)
            await asyncio.sleep(0.5 * (attempt + 1))
        except Exception as exc:
            if on_retry:
                on_retry("network", 0)
            if attempt == 2:
                logger.error("Transfer %s Telegram chunk=%d/%s failed after 3 attempts: %s: %s",
                             transfer_id, rpc.file_part, getattr(rpc, "file_total_parts", "small"),
                             type(exc).__name__, exc, exc_info=True)
                raise TransferStageError("Telegram upload chunk failed after two retries") from exc
            logger.warning("Transfer %s Telegram chunk=%d/%s retry=%d/2 after %s: %s",
                           transfer_id, rpc.file_part, getattr(rpc, "file_total_parts", "small"),
                           attempt + 1, type(exc).__name__, exc, exc_info=True)
            await asyncio.sleep(0.5 * (attempt + 1))


async def upload_stream(client, reader, filename, estimated_size=0, progress=None,
                        stats=None, check_header=True, max_size=None, total_size=None,
                        worker_limit=None, max_worker_limit=None, request_depth_limit=None):
    """Bounded producer buffer + work-conserving pipelined Telegram requests.

    Every free connection slot takes the next chunk, so a slow connection cannot
    monopolize the upload window. Progress messages run outside the data path.
    """
    # Telegram documents unknown-length uploads: SaveBigFilePart(-1) for
    # intermediate chunks, then the actual count on the terminal chunk.
    # https://core.telegram.org/api/files#streamed-uploads
    live = total_size is None
    if not live:
        total_size = int(total_size)
    if not live and total_size <= 0:
        raise ValueError("Cannot upload an empty media part")
    stats = stats or TransferStats()
    # The same observable stats object survives a staged-file retry. Reset
    # attempt counters, not delivery checkpoints; otherwise re-reading a part
    # can add to the previous attempt's bytes and falsely exceed the disk cap.
    stats.__dict__.update(TransferStats(
        transfer_id=stats.transfer_id or secrets.token_hex(4),
        part_index=stats.part_index, part_count=stats.part_count,
        completed_parts=stats.completed_parts, completed_bytes=stats.completed_bytes,
        wait_stage="Starting Telegram upload").__dict__)
    workers = max(1, min(worker_limit or config.UPLOAD_WORKERS, 8))
    # Wire writes are ordered inside OrderedMediaSession; RPC replies can
    # overlap safely instead of leaving each socket idle for an entire RTT.
    depth = max(1, min(config.UPLOAD_REQUESTS_PER_CONNECTION, 8))
    if request_depth_limit is not None:
        depth = min(depth, request_depth_limit)
    max_depth = max(depth, min(config.UPLOAD_MAX_REQUESTS_PER_CONNECTION, 8))
    if request_depth_limit is not None:
        max_depth = depth
    ceiling = max_worker_limit if max_worker_limit is not None else config.UPLOAD_MAX_WORKERS
    pool_size = max(workers, min(ceiling, 8)) if config.UPLOAD_ADAPTIVE else workers
    control = AdaptiveUploadWindow(workers, depth, pool_size,
                                   max_depth, config.UPLOAD_ADAPTIVE,
                                   recovery_seconds=config.UPLOAD_RECOVERY_SECONDS)
    stats.active_connections, stats.request_capacity = control.workers, control.capacity
    queue = asyncio.Queue(max(1, config.STREAM_BUFFER_BYTES // PART_SIZE))
    sessions = []
    pending = {}
    loads = []
    producer = reporter = tuner = None
    peak_pending = 0
    total = part = 0
    is_big = live or total_size > 10 * 1024 * 1024
    file_total_parts = None if live else math.ceil(total_size / PART_SIZE)
    checksum = hashlib.md5() if not is_big else None
    pause_until = 0
    file_id = client.rnd_id()
    limit = min(config.MAX_FILE_SIZE, 2000 * 1024 * 1024)
    if max_size is not None:
        limit = min(limit, max_size)
    if limit <= 0 or (not live and total_size > limit):
        raise ValueError("Media exceeds the attachment size limit")
    dc_id = await client.storage.dc_id()
    auth_key = await client.storage.auth_key()
    test_mode = await client.storage.test_mode()
    if config.UPLOAD_ADAPTIVE:
        warm = previous_upload_setting(client, dc_id, test_mode, workers, depth, pool_size, max_depth)
        if warm:
            control = AdaptiveUploadWindow(workers, depth, pool_size,
                                           max_depth, True,
                                           recovery_seconds=config.UPLOAD_RECOVERY_SECONDS, start=warm)
            stats.active_connections, stats.request_capacity = control.workers, control.capacity
            logger.info("Transfer %s reusing confirmed upload setting: %d connections x %d requests",
                        stats.transfer_id, *warm)
    logger.info("Transfer %s starting: DC=%d, max-pool=%d, initial=%d x %d, buffer=%d MiB, estimate=%d bytes",
                stats.transfer_id, dc_id, pool_size, control.workers, control.depth,
                config.STREAM_BUFFER_BYTES // (1024 * 1024), estimated_size)

    async def produce():
        held = None
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
            if stats.source_bytes == len(chunk):
                logger.info("Transfer %s first source bytes after %.2fs", stats.transfer_id,
                            time.monotonic() - stats.ready_at)
            if stats.source_bytes > limit:
                raise ValueError("Stream exceeds the attachment size limit; video splitting is required.")
            if live:
                # One chunk of lookahead identifies a nonempty terminal
                # payload even when EOF falls exactly on a 512 KiB boundary.
                # This avoids sending an empty file part to older DC layers.
                if held is not None:
                    await queue.put((held, eof and not chunk))
                if eof:
                    if chunk or held is None:
                        await queue.put((chunk, True))
                else:
                    held = chunk
            else:
                await queue.put((chunk, eof))
            stats.buffered_bytes = queue.qsize() * PART_SIZE
            stats.peak_buffered_bytes = max(stats.peak_buffered_bytes, stats.buffered_bytes)
            if eof:
                logger.info("Transfer %s source EOF: %d bytes produced", stats.transfer_id, stats.source_bytes)
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
            # The stage-part reader owns this status while copying from the
            # source stream to the bounded disk file. Do not let the periodic
            # feed monitor relabel it as a Telegram upload before that begins.
            if stats.wait_stage != "Staging part on server":
                stats.wait_stage = "Telegram" if stats.buffered_bytes or stats.in_flight else "source"
            logger.info("Transfer %s feed=%.1f KiB/s Telegram=%.1f KiB/s; produced=%d acked=%d; "
                        "buffer=%.1f MiB; active=%d; in-flight=%d/%d; retries=%d throttles=%d; waiting=%s",
                        stats.transfer_id, stats.source_bps / 1024, stats.upload_bps / 1024,
                        stats.source_bytes, stats.uploaded_bytes, stats.buffered_bytes / (1024 * 1024),
                        control.workers, stats.in_flight, control.capacity,
                        stats.retry_events, stats.throttle_events, stats.wait_stage)
            if progress:
                try:
                    await asyncio.wait_for(progress(
                        stats.uploaded_bytes, max(estimated_size, stats.uploaded_bytes + 1)),
                        config.PROGRESS_UPDATE_INTERVAL)
                except Exception:
                    logger.debug("Transfer progress update unavailable")

    def retried(kind, delay):
        nonlocal pause_until
        stats.retry_events += 1
        if kind == "network":
            # A cached fast window can become unsafe between files.  Do not
            # seed the following video with a connection profile that has just
            # failed on Telegram's media DC.
            client._bot_upload_profile = None
        if kind == "throttle":
            stats.throttle_events += 1
            client._bot_upload_profile = None
            pause_until = max(pause_until, time.monotonic() + delay)

    async def tune():
        nonlocal peak_pending
        previous_time, previous_bytes, previous_retries = stats.ready_at, 0, 0
        while True:
            await asyncio.sleep(config.UPLOAD_TUNE_INTERVAL)
            now = time.monotonic()
            rate = (stats.uploaded_bytes - previous_bytes) / max(0.001, now - previous_time)
            changed = control.observe(rate, backlog=not queue.empty(),
                                      saturated=peak_pending >= control.capacity * 0.8,
                                      congested=stats.retry_events > previous_retries)
            previous_time, previous_bytes, previous_retries = now, stats.uploaded_bytes, stats.retry_events
            peak_pending = len(pending)
            stats.active_connections, stats.request_capacity = control.workers, control.capacity
            if changed:
                logger.info("Transfer %s upload tuning: %d connections x %d requests (%s); %.1f KiB/s; "
                            "resume-in=%.1fs", stats.transfer_id, control.workers, control.depth, control.reason,
                            rate / 1024, max(0, control.resume_at - now))

    async def send(index, rpc):
        await _invoke_part(sessions[index], rpc, retried, stats.transfer_id)
        stats.uploaded_bytes += len(rpc.bytes)

    async def ensure_media_sessions(required):
        """Start only the active window; grow after measured stable delivery.

        Starting every possible media TCP session in one burst triggers resets
        on constrained hosts.  Sessions are independent, so later growth is
        safe and does not disturb chunks already acknowledged by earlier ones.
        """
        required = min(required, pool_size)
        while len(sessions) < required:
            session = Session(client, dc_id, auth_key, test_mode, is_media=True)
            # Keep ownership before startup so a half-open connection is still
            # closed if its handshake fails.
            sessions.append(session)
            loads.append(0)
            try:
                await bounded(session.start(), config.TRANSFER_STAGE_TIMEOUT,
                              "Telegram media session startup")
            except Exception as exc:
                raise TransferStageError("Telegram media session startup failed: " + str(exc)) from exc
            logger.info("Transfer %s media session ready: %d/%d active capacity",
                        stats.transfer_id, len(sessions), required)

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
        setup_started = time.monotonic()
        await ensure_media_sessions(control.workers)
        logger.info("Transfer %s initial %d media sessions ready in %.2fs; max-pool=%d",
                    stats.transfer_id, len(sessions), time.monotonic() - setup_started, pool_size)
        stats.ready_at = time.monotonic()
        producer = asyncio.create_task(produce())
        reporter = asyncio.create_task(report())
        if config.UPLOAD_ADAPTIVE:
            tuner = asyncio.create_task(tune())
        while True:
            # A tuning backoff can shrink below the number already in flight.
            # Drain those requests without dropping or resending their bytes.
            while len(pending) >= control.capacity:
                await drain(one=True)
            await ensure_media_sessions(control.workers)
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
            if not live and total > total_size:
                raise ValueError("Staged media changed while uploading")
            if live and not chunk:
                raise ValueError("Cannot upload an empty media part")
            if check_header and part == 0 and (len(chunk) < 12 or chunk[4:8] != b"ftyp"):
                raise ValueError("FFmpeg did not produce a valid MP4 stream")
            # EOF after an exact 512 KiB boundary has no payload.  Telegram
            # explicitly rejects empty file parts, so finish after the already
            # acknowledged final data request instead of sending one.
            if not chunk:
                if not eof:
                    raise ValueError("Media reader returned an empty non-final chunk")
                if total != total_size:
                    raise ValueError("Staged media ended before its recorded size")
                if pending:
                    await drain()
                stats.finished_at = time.monotonic()
                remember_upload_setting(client, dc_id, test_mode, control, total, stats.throttle_events > 0)
                result = (raw.types.InputFileBig(id=file_id, parts=file_total_parts, name=filename)
                          if is_big else raw.types.InputFile(id=file_id, parts=file_total_parts,
                                                              name=filename, md5_checksum=checksum.hexdigest()))
                logger.info("Transfer %s Telegram upload complete: %.2f MiB/s, %d connections, "
                            "%d requests/connection, %.1f MiB peak buffer; retries=%d throttles=%d",
                            stats.transfer_id, total / stats.seconds / (1024 * 1024), control.workers, control.depth,
                            stats.peak_buffered_bytes / (1024 * 1024), stats.retry_events, stats.throttle_events)
                return result, total
            if checksum is not None:
                checksum.update(chunk)
            if live and eof:
                file_total_parts = math.ceil(total / PART_SIZE)
                if file_total_parts != part + 1:
                    raise ValueError("Terminal stream chunk does not match its final count")
                # Finalize only after every preceding RPC has been ACKed,
                # including requests sent on other media sessions.
                if pending:
                    await drain()
                while pause_until > time.monotonic():
                    await asyncio.sleep(pause_until - time.monotonic())
                await _invoke_part(sessions[0], raw.functions.upload.SaveBigFilePart(
                    file_id=file_id, file_part=part, file_total_parts=file_total_parts,
                    bytes=chunk), retried, stats.transfer_id)
                stats.uploaded_bytes += len(chunk)
                stats.finished_at = time.monotonic()
                remember_upload_setting(client, dc_id, test_mode, control, total,
                                        stats.retry_events > 0)
                logger.info("Transfer %s live upload finalized: bytes=%d parts=%d; %.2f MiB/s; "
                            "retries=%d throttles=%d", stats.transfer_id, total, file_total_parts,
                            total / stats.seconds / (1024 * 1024), stats.retry_events, stats.throttle_events)
                return raw.types.InputFileBig(id=file_id, parts=file_total_parts, name=filename), total
            rpc = raw.functions.upload.SaveBigFilePart(
                file_id=file_id, file_part=part, file_total_parts=-1 if live else file_total_parts,
                bytes=chunk) if is_big else \
                raw.functions.upload.SaveFilePart(file_id=file_id, file_part=part, bytes=chunk)
            while len(pending) >= control.capacity:
                await drain(one=True)
            await ensure_media_sessions(control.workers)
            while pause_until > time.monotonic():
                await asyncio.sleep(pause_until - time.monotonic())
            # Respect per-session depth even while a smaller window drains.
            # A slow connection must not block free slots on healthy ones.
            while not any(loads[i] < control.depth for i in range(control.workers)):
                await drain(one=True)
            index = min((i for i in range(control.workers) if loads[i] < control.depth),
                        key=lambda i: loads[i])
            loads[index] += 1
            pending[asyncio.create_task(send(index, rpc))] = index
            stats.in_flight = len(pending)
            peak_pending = max(peak_pending, len(pending))
            part += 1
            if eof:
                if total != total_size or part != file_total_parts:
                    raise ValueError("Staged media size does not match its Telegram part count")
                await drain()
                stats.finished_at = time.monotonic()
                remember_upload_setting(client, dc_id, test_mode, control, total, stats.throttle_events > 0)
                result = (raw.types.InputFileBig(id=file_id, parts=file_total_parts, name=filename)
                          if is_big else raw.types.InputFile(id=file_id, parts=file_total_parts,
                                                              name=filename, md5_checksum=checksum.hexdigest()))
                logger.info("Transfer %s Telegram upload complete: %.2f MiB/s, %d connections, "
                            "%d requests/connection, %.1f MiB peak buffer; retries=%d throttles=%d",
                            stats.transfer_id, total / stats.seconds / (1024 * 1024), control.workers, control.depth,
                            stats.peak_buffered_bytes / (1024 * 1024), stats.retry_events, stats.throttle_events)
                return result, total
    except BaseException as exc:
        logger.warning("Transfer %s interrupted: %s; produced=%d acked=%d in-flight=%d",
                       stats.transfer_id, type(exc).__name__, stats.source_bytes, stats.uploaded_bytes, len(pending))
        raise
    finally:
        tasks = list(pending) + [t for t in (producer, reporter, tuner) if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        stats.in_flight = stats.buffered_bytes = 0
        await close_media_sessions(sessions, "Transfer " + stats.transfer_id)
        logger.info("Transfer %s cleanup complete: %d sessions closed", stats.transfer_id, len(sessions))


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
    await upload_stream(client, ProbeReader(), "speed-probe.bin", size, stats=stats,
                        check_header=False, total_size=size)
    return size / stats.seconds


async def _stderr_tail(reader):
    tail = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            return tail
        tail = (tail + chunk)[-8192:]


async def _thumbnail(client, url):
    try:
        return await bounded(_prepare_thumbnail(client, url), config.THUMBNAIL_TIMEOUT, "Video thumbnail")
    except Exception:
        logger.warning("Thumbnail unavailable or timed out; publishing without it", exc_info=True)
        return None


async def _prepare_thumbnail(client, url):
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
    stats = stats or TransferStats()
    stats.transfer_id = stats.transfer_id or secrets.token_hex(4)
    logger.info("Transfer %s preparing MP4: height=%s; inputs=%d; estimated=%d bytes",
                stats.transfer_id, plan.formats[0].get("height"), len(plan.formats), plan.estimated_size)
    async def run(checkpoint):
        async with DirectRangeRelay(plan) as relay:
            if len(relay.routes) == len(plan.formats):
                exact = sum(route["size"] for route in relay.routes.values())
                logger.info("Transfer %s exact CDN input size=%d bytes (metadata estimate=%d)",
                            stats.transfer_id, exact, plan.estimated_size)
                plan.estimated_size = exact
            return await _stream_video(client, chat_id, plan, relay, progress, stats, checkpoint=checkpoint)
    return await _retry_video(run, stats, progress)


async def stream_local_video(client, chat_id, result, progress=None, stats=None):
    """Split a disk-fallback result through the same bounded MP4/MTProto path."""
    plan = StreamPlan({"title": result.title, "duration": result.duration},
                      [{"width": result.width, "height": result.height}], result.file_size,
                      output_quality=getattr(result, "output_quality", ""),
                      collection=getattr(result, "collection", None))
    stats = stats or TransferStats(transfer_id=secrets.token_hex(4))
    stats.transfer_id = stats.transfer_id or secrets.token_hex(4)
    async def run(checkpoint):
        relay = SimpleNamespace(failure=None, network_bytes=0, urls=None)
        return await _stream_video(client, chat_id, plan, relay, progress, stats,
                                   local_path=result.file_path, checkpoint=checkpoint)
    return await _retry_video(run, stats, progress)


async def _retry_video(run, stats, progress=None):
    """Recover source/FFmpeg failures twice, retaining confirmed deliveries."""
    checkpoint = DeliveryCheckpoint()
    root_id = stats.transfer_id
    for attempt in range(3):
        try:
            stats.transfer_id = root_id
            return await run(checkpoint)
        except TransferStageError:
            # The failing network stage already spent its two retries.
            raise
        except Exception as exc:
            if attempt == 2:
                raise
            logger.warning("Transfer %s source/processing retry=%d/2; confirmed parts=%d; reason=%s: %s",
                           stats.transfer_id, attempt + 1, len(checkpoint.parts), type(exc).__name__, exc)
            stats.wait_stage = f"Retrying source ({attempt + 1}/2)"
            if progress:
                try:
                    await bounded(progress(stats.uploaded_bytes, max(1, stats.source_bytes)),
                                  config.TRANSFER_STATUS_TIMEOUT, "Source retry status")
                except Exception:
                    logger.debug("Source retry status unavailable", exc_info=True)
            await asyncio.sleep(2 * (attempt + 1))


def video_part_limit():
    # One current-part recovery copy is retained during upload. Fit the actual
    # free disk space while preserving a recovery reserve for the application.
    free = shutil.disk_usage(config.DOWNLOAD_DIR).free - config.MIN_FREE_DISK_BYTES
    return min(config.VIDEO_PART_SIZE_BYTES, config.MAX_FILE_SIZE,
               2000 * 1024 * 1024, max(0, free))


class _StagedPartReader:
    """Async 512 KiB reader over the one temporary Telegram attachment."""

    def __init__(self, path):
        self._file = open(path, "rb")

    async def readexactly(self, count):
        block = await asyncio.to_thread(self._file.read, count)
        if len(block) < count:
            raise asyncio.IncompleteReadError(block, count)
        return block

    async def close(self):
        try:
            await bounded(asyncio.to_thread(self._file.close), config.TRANSFER_CLEANUP_TIMEOUT,
                          "Recovery reader cleanup")
        except Exception:
            logger.warning("Recovery reader cleanup unavailable", exc_info=True)


async def _upload_staged_part(client, path, filename, size, progress, stats, limit, attempts=3,
                              initial_backoff=0):
    """Upload a complete local part, reducing the MTProto window after resets."""
    workers = max(1, min(config.UPLOAD_WORKERS, 8))
    depth_limit = None
    if not 1 <= attempts <= 3 or initial_backoff not in (0, 1):
        raise ValueError("Invalid bounded attachment recovery budget")
    if initial_backoff:
        workers = max(1, workers // 2)
        depth_limit = max(1, config.UPLOAD_REQUESTS_PER_CONNECTION // 2)
    for attempt in range(attempts):
        reader = _StagedPartReader(path)
        try:
            return await upload_stream(client, reader, filename, size, progress, stats,
                                       max_size=limit, total_size=size,
                                       worker_limit=workers,
                                       max_worker_limit=workers if attempt or initial_backoff else None,
                                       request_depth_limit=depth_limit)
        except TransferStageError as exc:
            # Telegram's explicit validation errors are permanent.  Transport
            # exhaustion is retried from the staged file with a smaller fresh
            # session window, never by re-downloading the video or reusing a
            # broken socket.
            if "Telegram rejected upload chunk" in str(exc) or attempt == attempts - 1:
                raise
            next_workers = max(1, workers // 2)
            depth_limit = max(1, (depth_limit or config.UPLOAD_REQUESTS_PER_CONNECTION) // 2)
            logger.warning("Transfer %s staged part retry=%d/2 after %s; reducing media sessions %d -> %d",
                           stats.transfer_id, attempt + 1, exc, workers, next_workers)
            logger.info("Transfer %s recovery pipeline capped at %d request(s)/connection",
                        stats.transfer_id, depth_limit)
            workers = next_workers
            await asyncio.sleep(2 * (attempt + 1))
        finally:
            await reader.close()


async def _upload_live_part(client, part, filename, estimate, progress, stats, limit):
    """Overlap download/upload; keep only this bounded part for recovery."""
    async with LivePartSpool(part, limit) as source:
        logger.info("Transfer %s live part %d started: source download and Telegram upload overlap; "
                    "recovery disk cap=%d", stats.transfer_id, part.index, limit)
        try:
            return await upload_stream(client, source, filename, estimate, progress, stats,
                                       max_size=limit)
        except TransferStageError as exc:
            if source.failure is not None:
                raise source.failure
            # A permanent authorization/permission error cannot be repaired by
            # changing the file protocol. Only transport exhaustion and stream
            # count/terminal validation can use a known-size recovery attempt.
            if ("Telegram rejected upload chunk" in str(exc) and
                    not any(code in str(exc) for code in ("FILE_PARTS_INVALID", "FILE_PART_EMPTY"))):
                raise
            if "throttled for" in str(exc):
                raise
            logger.warning("Transfer %s live upload failed: %s; finishing the SAME source pass "
                           "for at most two known-size upload retries", stats.transfer_id, exc)
            stats.wait_stage = "Staging part on server"
            async def report_recovery():
                while True:
                    if progress:
                        try:
                            await bounded(progress(source.written, max(estimate, source.written, 1)),
                                          config.TRANSFER_STATUS_TIMEOUT, "Recovery progress")
                        except Exception:
                            logger.debug("Recovery status unavailable", exc_info=True)
                    await asyncio.sleep(config.PROGRESS_UPDATE_INTERVAL)
            reporter = asyncio.create_task(report_recovery())
            try:
                size = await source.finish_for_recovery()
            finally:
                reporter.cancel()
                await asyncio.gather(reporter, return_exceptions=True)
            stats.wait_stage = "Starting Telegram upload"
            return await _upload_staged_part(client, source.path, filename, size,
                                              progress, stats, limit, attempts=2, initial_backoff=1)


async def _publish_video_part(client, chat_id, plan, file, filename, size, duration,
                              start=0, index=None):
    """A stable SendMedia random_id prevents duplicate posts after timeouts."""
    info, video = plan.info, plan.formats[0]
    thumb = await _thumbnail(client, info.get("thumbnail"))
    quality = plan.output_quality or resolution_tag("source", video.get("height"))
    caption = video_caption(info.get("title", "Video"), quality, size, duration,
                            plan.collection, index, start)
    media = raw.types.InputMediaUploadedDocument(
        file=file, mime_type="video/mp4", thumb=thumb,
        attributes=[raw.types.DocumentAttributeVideo(
            duration=math.ceil(duration), w=int(video.get("width") or 0),
            h=int(video.get("height") or 0), supports_streaming=True),
            raw.types.DocumentAttributeFilename(file_name=filename)])
    parsed = await bounded(client.parser.parse(caption, enums.ParseMode.HTML),
                           config.TRANSFER_STAGE_TIMEOUT, "Media caption")
    peer = await retry_stage("Resolve Telegram peer", lambda: client.resolve_peer(chat_id),
                             config.TRANSFER_STAGE_TIMEOUT)
    request = raw.functions.messages.SendMedia(
        peer=peer, media=media, random_id=client.rnd_id(),
        message=parsed["message"], entities=parsed["entities"],
        reply_to_msg_id=plan.collection.reply_to(quality) if plan.collection else None)
    # Keep exactly this request (including random_id and uploaded file) for all
    # attempts. A lost response must not cause a fresh upload or duplicate post.
    async def publish():
        try:
            return await client.invoke(request, retries=0, timeout=config.TRANSFER_STAGE_TIMEOUT, sleep_threshold=0)
        except RPCError as exc:
            if request.reply_to_msg_id and getattr(exc, "ID", "") in (
                    "MESSAGE_ID_INVALID", "REPLY_MESSAGE_ID_INVALID", "REPLY_TO_MESSAGE_ID_INVALID"):
                # A user may delete the first part between uploads. Grouping
                # must not make an otherwise valid media transfer fail.
                if plan.collection:
                    plan.collection.forget(request.reply_to_msg_id)
                request.reply_to_msg_id = None
                raise OSError("Collection's first message was removed; retrying without the reply link") from exc
            raise
    sent = await retry_stage(f"Publish {filename}", publish, config.TRANSFER_STAGE_TIMEOUT)
    if plan.collection:
        plan.collection.record(sent, quality, request.random_id)
    return sent


async def _validate_video_end(process, errors, relay, expected):
    code = await asyncio.wait_for(process.wait(), 30)
    diagnostics = await bounded(errors, config.TRANSFER_CLEANUP_TIMEOUT, "FFmpeg diagnostics")
    logger.info("FFmpeg exited=%d; CDN bytes=%d", code, relay.network_bytes)
    if relay.failure:
        raise relay.failure
    if code:
        raise RuntimeError("Direct video transfer failed in FFmpeg. Check CDN access or retry the video.")
    durations = re.findall(rb"out_time_us=(\d+)", diagnostics)
    actual = int(durations[-1]) / 1_000_000 if durations else 0
    if b"progress=end" not in diagnostics or (expected and actual < expected - max(5, expected * 0.02)):
        raise ValueError("The video stream ended early; retry this video. The final part was not sent.")


async def _stream_video(client, chat_id, plan, relay, progress=None, stats=None, local_path=None, checkpoint=None):
    """One FFmpeg pass; publish bounded playable parts as they are acknowledged."""
    stats = stats or TransferStats(transfer_id=secrets.token_hex(4))
    info = plan.info
    base = sanitize_filename(info.get("title", "video"))
    if plan.output_quality:
        base += "_" + plan.output_quality
    limit = video_part_limit()
    checkpoint = checkpoint or DeliveryCheckpoint()
    if not checkpoint.parts:
        checkpoint.estimate = plan.estimated_size
    estimate = checkpoint.estimate
    count, target = plan_parts(estimate, limit)
    logger.info("Transfer %s split plan: estimate=%d bytes, estimated parts=%d, target=%d, "
                "hard limit=%d bytes; quality=%sp preserved; live keyframe parts with bounded recovery",
                stats.transfer_id, plan.estimated_size, count, target, limit, plan.formats[0].get("height"))
    env = {k: v for k, v in os.environ.items()
           if k.lower() not in ("http_proxy", "https_proxy", "all_proxy")}
    logger.info("Streaming route: server direct CDN -> FFmpeg -> concurrent Telegram upload; "
                "one current-part recovery file, no download-before-upload pass")
    command = ffmpeg_command(plan, relay.urls) if local_path is None else [
        shutil.which("ffmpeg") or "ffmpeg", "-hide_banner", "-loglevel", "error", "-nostdin",
        "-nostats", "-progress", "pipe:2", "-i", local_path, "-map", "0:v:0", "-map", "0:a:0?",
        "-c", "copy", "-map_metadata", "-1", "-movflags",
        "+frag_keyframe+empty_moov+default_base_moof", "-frag_size", str(8 * 1024 * 1024),
        "-f", "mp4", "pipe:1"]
    process = await bounded(asyncio.create_subprocess_exec(
        *command, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE, env=env, limit=PART_SIZE * 2),
        config.TRANSFER_STAGE_TIMEOUT, "FFmpeg startup")
    errors = asyncio.create_task(_stderr_tail(process.stderr))
    sent_count = len(checkpoint.parts)
    total_size = sum(item[0] for item in checkpoint.parts)
    sent = checkpoint.last_sent
    root_id = stats.transfer_id
    try:
        source = await asyncio.wait_for(FragmentedVideo(process.stdout, limit, estimate).start(),
                                        config.STREAM_READ_TIMEOUT)
        while (part := source.next_part()) is not None:
            if part.index <= len(checkpoint.parts):
                # Recreate stream boundaries without storing or uploading the
                # already delivered bytes. Abort if the source changed.
                discarded = 0
                while True:
                    try:
                        block = await bounded(part.readexactly(PART_SIZE), config.STREAM_READ_TIMEOUT,
                                              "Replay confirmed video part")
                        discarded += len(block)
                    except asyncio.IncompleteReadError as exc:
                        discarded += len(exc.partial)
                        break
                previous = checkpoint.parts[part.index - 1]
                if previous != (discarded, part.start, part.end):
                    raise TransferStageError("Source changed on retry; refusing to duplicate or skip video content")
                logger.info("Transfer %s replayed confirmed part %d; no upload or duplicate post", root_id, part.index)
                continue
            filename = f"{base}_part_{part.index:02d}.mp4"
            # Preserve the stats object observed by /speedtest while resetting
            # per-attachment ACK and source counters for the uploader.
            stats.__dict__.update(TransferStats(
                transfer_id=f"{root_id}-p{part.index}", part_index=part.index,
                part_count=max(count, part.index), completed_parts=sent_count,
                completed_bytes=total_size).__dict__)
            logger.info("Transfer %s part %d streaming; estimated count=%d, target=%d bytes",
                        root_id, part.index, count, target)
            stats.wait_stage = "Streaming source to Telegram"
            file, size = await _upload_live_part(client, part, filename, target, progress, stats, limit)
            if relay.failure:
                raise relay.failure
            if source.eof:
                stats.wait_stage = "Verifying final part"
                await _validate_video_end(process, errors, relay, float(info.get("duration") or 0))
            single = part.index == 1 and source.eof
            if single:
                filename = base + ".mp4"
                file.name = filename
            stats.wait_stage = "Publishing to Telegram"
            if progress:
                try:
                    await bounded(progress(size, size), config.TRANSFER_STATUS_TIMEOUT, "Publishing status")
                except Exception:
                    logger.debug("Publishing status unavailable", exc_info=True)
            logger.info("Transfer %s publishing part %d; uploaded=%d bytes", root_id, part.index, size)
            sent = await _publish_video_part(client, chat_id, plan, file, filename, size,
                                             part.duration, part.start, None if single else part.index)
            checkpoint.parts.append((size, part.start, part.end))
            checkpoint.last_sent = sent
            sent_count += 1
            total_size += size
            stats.completed_parts, stats.completed_bytes = sent_count, total_size
            logger.info("Transfer %s part %d published: %d bytes, timeline=%.3f..%.3fs; "
                        "total published=%d bytes", root_id, part.index, size, part.start, part.end, total_size)
        if not sent_count:
            raise ValueError("No complete video parts were produced")
        logger.info("Transfer %s complete: %d playable parts, %d bytes; original quality preserved",
                    root_id, sent_count, total_size)
        return sent
    except Exception as exc:
        if sent_count:
            error = TransferStageError if isinstance(exc, TransferStageError) else RuntimeError
            raise error(f"Transfer stopped after {sent_count} part(s) were delivered. "
                        "Those parts remain available; later parts are incomplete. " + str(exc)) from exc
        raise
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        # Drain stdout even after an upload error/cancellation so a full pipe
        # cannot prevent subprocess transport shutdown.
        async def reap():
            while await process.stdout.read(64 * 1024):
                pass
            await process.wait()
            await asyncio.gather(errors, return_exceptions=True)
        try:
            await bounded(reap(), config.TRANSFER_CLEANUP_TIMEOUT, "FFmpeg cleanup")
        except Exception:
            errors.cancel()
            logger.warning("Transfer %s FFmpeg cleanup timed out", root_id, exc_info=True)
