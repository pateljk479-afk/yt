import os
import time
import asyncio
import logging
import json
import tempfile
import threading
from html import escape
from typing import Optional, List

import config
from pyrogram import Client, filters
from pyrogram.session import Session
from pyrogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import FloodWait, MessageNotModified

# Optimize Pyrogram MTProto upload timeouts for cloud servers:
# Prevent "SaveBigFilePart: Request timed out" during large file uploads (>10MB).
# Must be integer because MTProto ping_delay_disconnect serializes as Int.
Session.WAIT_TIMEOUT = int(max(60, getattr(config, "PYROGRAM_WAIT_TIMEOUT", 60)))

# MTProto session stabilization:
# 1. Dynamically sync client clock offset with Telegram server time to eliminate
#    "The msg_id belongs to over 300 seconds in the past" on cloud containers with time drift.
# 2. Reset stored_msg_ids on SecurityCheckMismatch to eliminate the infinite reconnect loop
#    "The msg_id is lower than all the stored values".
import pyrogram.session.session as _py_session
from pyrogram.session.internals import MsgId as _MsgId

_orig_msg_id_new = _MsgId.__new__

def _patched_msg_id_new(cls, *args, **kwargs):
    now = int(time.time() + getattr(_MsgId, "time_offset", 0))
    cls.offset = (cls.offset + 4) if now == cls.last_time else 0
    msg_id = (now * 2 ** 32) + cls.offset
    cls.last_time = now
    return msg_id

_MsgId.__new__ = staticmethod(_patched_msg_id_new)
_MsgId.time_offset = 0

_orig_handle_packet = _py_session.Session.handle_packet

async def _stabilized_handle_packet(self, packet):
    try:
        data = await self.loop.run_in_executor(
            _py_session.pyrogram.crypto_executor,
            _py_session.mtproto.unpack,
            _py_session.BytesIO(packet),
            self.session_id,
            self.auth_key,
            self.auth_key_id
        )
    except Exception as e:
        _py_session.log.error("Failed to unpack packet: %s", e)
        return

    messages = (
        data.body.messages
        if isinstance(data.body, _py_session.MsgContainer)
        else [data]
    )

    for msg in messages:
        try:
            server_time = int(msg.msg_id >> 32)
            local_time = int(time.time())
            drift = server_time - local_time
            if abs(drift - getattr(_MsgId, "time_offset", 0)) > 5:
                _MsgId.time_offset = drift
        except Exception:
            pass

        if msg.seq_no % 2 != 0:
            if msg.msg_id in self.pending_acks:
                continue
            else:
                self.pending_acks.add(msg.msg_id)

        try:
            if len(self.stored_msg_ids) > _py_session.Session.STORED_MSG_IDS_MAX_SIZE:
                del self.stored_msg_ids[:_py_session.Session.STORED_MSG_IDS_MAX_SIZE // 2]

            if self.stored_msg_ids:
                if msg.msg_id < self.stored_msg_ids[0]:
                    raise _py_session.SecurityCheckMismatch("The msg_id is lower than all the stored values")

                if msg.msg_id in self.stored_msg_ids:
                    raise _py_session.SecurityCheckMismatch("The msg_id is equal to any of the stored values")

                time_diff = (msg.msg_id - _MsgId()) / 2 ** 32

                if time_diff > 30:
                    raise _py_session.SecurityCheckMismatch(
                        "The msg_id belongs to over 30 seconds in the future. "
                        "Most likely the client time has to be synchronized."
                    )

                if time_diff < -300:
                    raise _py_session.SecurityCheckMismatch(
                        "The msg_id belongs to over 300 seconds in the past. "
                        "Most likely the client time has to be synchronized."
                    )
        except _py_session.SecurityCheckMismatch as e:
            _py_session.log.info("Discarding packet: %s (clearing stored msg IDs for clean session recovery)", e)
            self.stored_msg_ids.clear()
            await self.connection.close()
            return
        else:
            _py_session.bisect.insort(self.stored_msg_ids, msg.msg_id)

        if isinstance(msg.body, (_py_session.raw.types.MsgDetailedInfo, _py_session.raw.types.MsgNewDetailedInfo)):
            self.pending_acks.add(msg.body.answer_msg_id)
            continue

        if isinstance(msg.body, _py_session.raw.types.NewSessionCreated):
            continue

        msg_id = None

        if isinstance(msg.body, (_py_session.raw.types.BadMsgNotification, _py_session.raw.types.BadServerSalt)):
            msg_id = msg.body.bad_msg_id
        elif isinstance(msg.body, (_py_session.FutureSalts, _py_session.raw.types.RpcResult)):
            msg_id = msg.body.req_msg_id
        elif isinstance(msg.body, _py_session.raw.types.Pong):
            msg_id = msg.body.msg_id
        else:
            if self.client is not None:
                self.loop.create_task(self.client.handle_updates(msg.body))

        if msg_id in self.results:
            self.results[msg_id].value = getattr(msg.body, "result", msg.body)
            self.results[msg_id].event.set()

    if len(self.pending_acks) >= self.ACKS_THRESHOLD:
        _py_session.log.debug("Sending %s acks", len(self.pending_acks))
        try:
            await self.send(_py_session.raw.types.MsgsAck(msg_ids=list(self.pending_acks)), False)
        except OSError:
            pass
        else:
            self.pending_acks.clear()

_py_session.Session.handle_packet = _stabilized_handle_packet

# Extend TCP transport socket timeout to prevent premature socket aborts
import pyrogram.connection.transport.tcp as _tcp_mod
_tcp_mod.TCP.TIMEOUT = int(max(60, getattr(config, "PYROGRAM_WAIT_TIMEOUT", 60)))

# In-memory circular log handler to power /logs command
from collections import deque

class MemoryLogHandler(logging.Handler):
    """In-memory circular log handler to power /logs command directly in Telegram."""
    def __init__(self, capacity: int = 500):
        super().__init__()
        self.buffer = deque(maxlen=capacity)

    def emit(self, record):
        try:
            self.buffer.append(self.format(record))
        except Exception:
            pass

    def get_logs(self, count: int = 60) -> List[str]:
        return list(self.buffer)[-count:]

memory_log_handler = MemoryLogHandler()
memory_log_handler.setLevel(logging.INFO)
memory_log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logging.root.setLevel(logging.INFO)
logging.root.addHandler(memory_log_handler)

# Fast parallel chunk uploader patch for Pyrogram
import pyrogram.methods.advanced.save_file as _sf_mod

_orig_pyrogram_save_file = _sf_mod.SaveFile.save_file

async def _fast_save_file(
    self: Client,
    path,
    file_id: int = None,
    file_part: int = 0,
    progress: _sf_mod.Callable = None,
    progress_args: tuple = ()
):
    async with self.save_file_semaphore:
        if path is None:
            return None

        part_size = 512 * 1024

        if isinstance(path, (str, _sf_mod.PurePath)):
            fp = open(path, "rb")
        elif isinstance(path, _sf_mod.io.IOBase):
            fp = path
        else:
            raise ValueError("Invalid file. Expected a file path as string or a binary file pointer")

        file_name = getattr(fp, "name", "file.jpg")
        fp.seek(0, _sf_mod.os.SEEK_END)
        file_size = fp.tell()
        fp.seek(0)

        if file_size == 0:
            if isinstance(path, (str, _sf_mod.PurePath)):
                fp.close()
            raise ValueError("File size equals to 0 B")

        file_size_limit_mib = 4000 if getattr(self, "me", None) and getattr(self.me, "is_premium", False) else 2000
        if file_size > file_size_limit_mib * 1024 * 1024:
            if isinstance(path, (str, _sf_mod.PurePath)):
                fp.close()
            raise ValueError(f"Can't upload files bigger than {file_size_limit_mib} MiB")

        file_total_parts = int(_sf_mod.math.ceil(file_size / part_size))
        is_big = file_size > 10 * 1024 * 1024
        is_missing_part = file_id is not None
        file_id = file_id or self.rnd_id()
        md5_sum = _sf_mod.md5() if not is_big and not is_missing_part else None

        # Multi-session parallel MTProto upload:
        # Avoid single TCP socket serialization bottleneck (which caps MTProto uploads to ~150 KB/s).
        # Open separate independent MTProto media sessions to parallelize chunk transmission across separate sockets.
        sessions_count = getattr(config, "UPLOAD_WORKERS", 4) if (is_big and not is_missing_part) else 1

        dc_id = await self.storage.dc_id()
        auth_key = await self.storage.auth_key()
        test_mode = await self.storage.test_mode()

        sessions = [
            _sf_mod.Session(self, dc_id, auth_key, test_mode, is_media=True)
            for _ in range(sessions_count)
        ]
        await _sf_mod.asyncio.gather(*(s.start() for s in sessions))

        queue = _sf_mod.asyncio.Queue(sessions_count * 2)
        uploaded_bytes = 0
        worker_exception = [None]

        async def worker(session: _sf_mod.Session):
            nonlocal uploaded_bytes
            while True:
                item = await queue.get()
                if item is None:
                    queue.task_done()
                    return
                rpc, chunk_len = item
                for attempt in range(5):
                    if worker_exception[0] is not None:
                        queue.task_done()
                        return
                    try:
                        await session.invoke(rpc)
                        break
                    except _sf_mod.FloodWait as e:
                        logger.warning("Upload FloodWait: sleeping %s seconds", e.value)
                        await _sf_mod.asyncio.sleep(e.value)
                    except Exception as e:
                        if attempt == 4:
                            logger.error("Chunk upload failed after 5 retries: %s", e)
                            worker_exception[0] = e
                            queue.task_done()
                            return
                        await _sf_mod.asyncio.sleep(0.5 * (attempt + 1))

                queue.task_done()
                uploaded_bytes += chunk_len

                if progress and worker_exception[0] is None:
                    try:
                        func = _sf_mod.functools.partial(
                            progress,
                            min(uploaded_bytes, file_size),
                            file_size,
                            *progress_args
                        )
                        if _sf_mod.inspect.iscoroutinefunction(progress):
                            await func()
                        else:
                            await self.loop.run_in_executor(self.executor, func)
                    except _sf_mod.StopTransmission:
                        worker_exception[0] = _sf_mod.StopTransmission()
                        return
                    except Exception:
                        pass

        workers = [self.loop.create_task(worker(s)) for s in sessions]

        try:
            if is_missing_part:
                fp.seek(part_size * file_part)
                chunk = fp.read(part_size)
                if is_big:
                    rpc = _sf_mod.raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=file_part,
                        file_total_parts=file_total_parts,
                        bytes=chunk
                    )
                else:
                    rpc = _sf_mod.raw.functions.upload.SaveFilePart(
                        file_id=file_id,
                        file_part=file_part,
                        bytes=chunk
                    )
                await sessions[0].invoke(rpc)
                return

            fp.seek(part_size * file_part)
            current_part = file_part

            while True:
                if worker_exception[0] is not None:
                    break
                chunk = fp.read(part_size)
                if not chunk:
                    if not is_big and not is_missing_part:
                        md5_sum = "".join([hex(i)[2:].zfill(2) for i in md5_sum.digest()])
                    break

                if is_big:
                    rpc = _sf_mod.raw.functions.upload.SaveBigFilePart(
                        file_id=file_id,
                        file_part=current_part,
                        file_total_parts=file_total_parts,
                        bytes=chunk
                    )
                else:
                    rpc = _sf_mod.raw.functions.upload.SaveFilePart(
                        file_id=file_id,
                        file_part=current_part,
                        bytes=chunk
                    )

                if not is_big and not is_missing_part:
                    md5_sum.update(chunk)

                await queue.put((rpc, len(chunk)))
                current_part += 1

            for _ in sessions:
                await queue.put(None)

            await _sf_mod.asyncio.gather(*workers)

            if worker_exception[0] is not None:
                raise worker_exception[0]

        except _sf_mod.StopTransmission:
            raise
        except Exception as e:
            _sf_mod.log.exception(e)
            raise
        else:
            if is_big:
                return _sf_mod.raw.types.InputFileBig(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,
                )
            else:
                return _sf_mod.raw.types.InputFile(
                    id=file_id,
                    parts=file_total_parts,
                    name=file_name,
                    md5_checksum=md5_sum
                )
        finally:
            await _sf_mod.asyncio.gather(*(s.stop() for s in sessions), return_exceptions=True)
            if isinstance(path, (str, _sf_mod.PurePath)):
                fp.close()

_sf_mod.SaveFile.save_file = _fast_save_file
Client.save_file = _fast_save_file

from downloader import (
    extract_info,
    analyze_video_qualities,
    download_media,
    DownloadResult,
    sanitize_filename,
    validate_youtube_cookie_file,
)
from streaming import (StreamPlan, TransferStats, UnsupportedStream,
                       prepare_stream, stream_video, stream_local_video, video_part_limit,
                       telegram_speed_probe)
from video_parts import plan_parts
from session_manager import session_manager, PlaylistSession, VideoSession
from playlist_queue import playlist_queue, PlaylistJob
from keyboards import (
    build_playlist_keyboard,
    build_playlist_quality_keyboard,
    build_video_quality_keyboard,
)
from uploader import (
    format_progress_bar,
    human_readable_size,
    human_readable_time,
    verify_media_integrity,
    prepare_thumbnail,
    safe_cleanup,
)

logger = logging.getLogger(__name__)

# Semaphore and lock to govern transfers and guarantee strict sequential execution
transfer_lock = asyncio.Lock()
download_semaphore = asyncio.Semaphore(1)
upload_semaphore = asyncio.Semaphore(1)
active_transfer_stats = None

# Initialize Pyrogram Bot Client
# API_ID and API_HASH default to fallback placeholders so unit tests can import safely
app = Client(
    name="youtube_downloader_bot",
    api_id=config.API_ID if config.API_ID else 12345,
    api_hash=config.API_HASH if config.API_HASH else "placeholder_hash",
    bot_token=config.BOT_TOKEN if config.BOT_TOKEN else "placeholder_token",
    workers=min(8, (os.cpu_count() or 2) * 2),
    max_concurrent_transmissions=config.MAX_CONCURRENT_TRANSMISSIONS,
)


class ThrottledProgressUpdater:
    """Helper to throttle Telegram message edits to avoid FloodWait and compute real transfer speed."""

    def __init__(self, message: Message, stage: str = "Downloading", interval: float = config.PROGRESS_UPDATE_INTERVAL):
        self.message = message
        self.stage = stage
        self.interval = interval
        self.last_update_time = 0.0
        self.start_upload_time = 0.0
        self.last_upload_bytes = 0
        self.active = True
        self._is_editing = False
        try:
            self.loop = asyncio.get_running_loop()
        except RuntimeError:
            self.loop = asyncio.get_event_loop()

    def sync_hook(self, d: dict):
        """yt-dlp progress hook running in worker thread."""
        if d.get("status") == "downloading":
            now = time.time()
            if now - self.last_update_time >= self.interval and not self._is_editing:
                self.last_update_time = now
                current = d.get("downloaded_bytes", 0)
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                speed = d.get("speed") or 0
                eta = d.get("eta") or 0
                text = format_progress_bar(current, total, speed, eta, stage=self.stage)
                # Schedule coroutine in event loop safely
                asyncio.run_coroutine_threadsafe(self._safe_edit(text), self.loop)

    async def async_upload_hook(self, current: int, total: int):
        """Pyrogram upload progress callback with real speed & ETA calculation."""
        if not self.active:
            return
        now = time.time()
        if self.start_upload_time == 0.0:
            self.start_upload_time = now
            self.last_upload_time = now
            self.last_upload_bytes = current

        dt = now - self.last_upload_time
        if dt >= self.interval:
            delta_bytes = current - self.last_upload_bytes
            speed = delta_bytes / dt if dt > 0 else 0
            eta = int((total - current) / speed) if speed > 0 and total > current else 0
            self.last_upload_time = now
            self.last_upload_bytes = current
            text = format_progress_bar(current, total, speed, eta, stage=self.stage)
            await self._safe_edit(text)

    async def _safe_edit(self, text: str):
        if not self.active:
            return
        try:
            await self.message.edit_text(text)
        except FloodWait as e:
            # Push forward throttle window instead of sleeping in transmission loop
            self.last_update_time = time.time() + e.value
        except MessageNotModified:
            pass
        except Exception as e:
            logger.debug("Progress edit skipped: %s", e)


def is_youtube_url(text: str) -> bool:
    """Validate if given text contains a YouTube video or playlist link."""
    patterns = [
        "youtube.com/watch",
        "youtu.be/",
        "youtube.com/playlist",
        "youtube.com/shorts/",
        "youtube.com/live/",
    ]
    return any(p in text for p in patterns)


def is_playlist_url(text: str) -> bool:
    """Check if the text represents a YouTube playlist."""
    return "list=" in text or "playlist" in text


# =====================================================================
# COMMAND HANDLERS
# =====================================================================

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    user_tag = getattr(message.from_user, "username", str(message.from_user.id)) if message.from_user else "Unknown"
    logger.info("📩 Received /start command from user @%s (chat_id=%s)", user_tag, message.chat.id)
    welcome_text = (
        "👋 <b>Welcome to High-Speed YouTube Downloader Bot!</b>\n\n"
        "⚡ <b>Features:</b>\n"
        "• Download single YouTube videos or entire playlists\n"
        "• Select the original quality, including large videos\n"
        "• Large videos arrive as numbered, playable MP4 parts\n"
        "• Interactive 10-per-page playlist navigation & multi-selection\n"
        "• Original file names and original YouTube thumbnails\n"
        "• Optimized MP4 streams ready for Telegram player playback\n\n"
        "📌 <b>Commands:</b>\n"
        "• <code>/video &lt;link&gt;</code> - Download a single video\n"
        "• <code>/playlist &lt;link&gt;</code> - Select & download from a playlist\n"
        "• <code>/queue</code> - View playlist queue, progress and cancel buttons\n"
        "• <code>/cancelqueue JOB_ID</code> - Cancel one queued or active playlist\n"
        "• <code>/speedtest</code> - Test server network download & upload speeds\n"
        "• <code>/logs</code> - View recent server logs directly on Telegram\n"
        "• <code>/help</code> - Detailed help and instructions\n\n"
        "Or simply paste any YouTube video or playlist link directly!"
    )
    await message.reply_text(welcome_text)


@app.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    logger.info("📩 Received /help command from chat_id=%s", message.chat.id)
    help_text = (
        "📖 <b>How to Use This Bot:</b>\n\n"
        "<b>1. Downloading a Single Video:</b>\n"
        "• Send <code>/video https://youtu.be/...</code>\n"
        "• You will see available resolutions (4K, 2K, 1080p, 720p, etc.)\n"
        "• <i>Note:</i> Large videos are split into playable parts without reducing quality.\n"
        "• Click <b>⭐ Max Quality</b> for the highest available quality.\n\n"
        "<b>2. Downloading Playlists:</b>\n"
        "• Send <code>/playlist https://youtube.com/playlist?list=...</code>\n"
        "• The bot displays 10 videos per page with clickable checkmarks.\n"
        "• Use <b>Next ➡️</b> and <b>⬅️ Prev</b> to browse pages.\n"
        "• Click <b>✅ Select All</b> to download the entire playlist at once.\n"
        "• Click <b>📥 Download Selected</b> to pick your desired resolution.\n\n"
        "• Playlists run in arrival order after you select the quality.\n"
        "• <code>/queue</code> shows progress, pages and individual Cancel buttons.\n"
        "• <code>/cancelqueue JOB_ID</code> cancels an active or waiting playlist.\n\n"
        "<b>3. Diagnostics:</b>\n"
        "• <code>/speedtest</code> - Check server download & upload bandwidth\n"
        "• <code>/logs</code> - View recent server application logs"
    )
    await message.reply_text(help_text)


@app.on_message(filters.command("speedtest"))
async def speedtest_command(client: Client, message: Message):
    logger.info("📩 Received /speedtest command from chat_id=%s", message.chat.id)
    if upload_semaphore.locked() or download_semaphore.locked():
        stats = active_transfer_stats
        if stats and stats.ready_at:
            elapsed = stats.seconds
            upload_rate = stats.upload_bps if stats.sampled_at else stats.uploaded_bytes / elapsed
            source_rate = stats.source_bps if stats.sampled_at else stats.source_bytes / elapsed
            await message.reply_text(
                "📊 <b>LIVE TRANSFER SPEED</b>\n\n"
                f"📥 <b>Video feed:</b> {human_readable_size(int(source_rate))}/s\n"
                f"📤 <b>Telegram delivery:</b> {human_readable_size(int(upload_rate))}/s\n"
                f"💾 <b>Buffered:</b> {human_readable_size(stats.buffered_bytes)}\n"
                f"⏳ <b>Waiting on:</b> {stats.wait_stage}\n"
                f"⚡ <b>Upload connections:</b> {stats.active_connections}\n\n"
                "<i>Showing the active transfer. Run /speedtest when it finishes for a full network test.</i>")
        else:
            await message.reply_text(
                "📊 <i>A media transfer is running. Run /speedtest when it finishes "
                "so the test can measure the available bandwidth.</i>")
        return
    status = await message.reply_text("🌐 <b>Running server network speed test...</b>\n\n📡 <i>Measuring latency...</i>")
    try:
        import aiohttp
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://speed.cloudflare.com/",
            "Origin": "https://speed.cloudflare.com",
        }
        async with aiohttp.ClientSession(headers=headers, trust_env=False) as session:
            # 1. Latency test
            pings = []
            for _ in range(3):
                t0 = time.time()
                async with session.get("https://speed.cloudflare.com/__down?bytes=0", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    resp.raise_for_status()
                    await resp.read()
                pings.append((time.time() - t0) * 1000)
            avg_ping = sum(pings) / len(pings)

            # 2. Download test (10 MB via Cloudflare)
            await status.edit_text(
                f"🌐 <b>Running server network speed test...</b>\n\n"
                f"📡 Latency: <code>{avg_ping:.0f} ms</code>\n"
                f"📥 <i>Testing download speed (10 MB)...</i>"
            )
            t0 = time.time()
            async with session.get("https://speed.cloudflare.com/__down?bytes=10000000", timeout=aiohttp.ClientTimeout(total=20)) as resp:
                resp.raise_for_status()
                data = await resp.read()
            down_sec = max(0.001, time.time() - t0)
            down_bytes = len(data)
            down_mbs = (down_bytes / (1024 * 1024)) / down_sec
            down_mbps = (down_bytes * 8) / (down_sec * 1_000_000)

            # 3. Upload test (10 MB via Cloudflare)
            await status.edit_text(
                f"🌐 <b>Running server network speed test...</b>\n\n"
                f"📡 Latency: <code>{avg_ping:.0f} ms</code>\n"
                f"📥 Download: <code>{down_mbs:.2f} MB/s • {down_mbps:.1f} Mbps</code>\n"
                f"📤 <i>Testing upload speed (10 MB)...</i>"
            )
            up_payload = b"0" * (10 * 1024 * 1024)
            t0 = time.time()
            async with session.post("https://speed.cloudflare.com/__up", data=up_payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                resp.raise_for_status()
                await resp.read()
            up_sec = max(0.001, time.time() - t0)
            up_mbs = (len(up_payload) / (1024 * 1024)) / up_sec
            up_mbps = (len(up_payload) * 8) / (up_sec * 1_000_000)

            # 4. Same parallel uploader as actual videos, with a memory source.
            await status.edit_text(
                f"🌐 <b>Running server network speed test...</b>\n\n"
                f"📡 Latency: <code>{avg_ping:.0f} ms</code>\n"
                f"📥 Download: <code>{down_mbs:.2f} MB/s • {down_mbps:.1f} Mbps</code>\n"
                f"📤 Upload: <code>{up_mbs:.2f} MB/s • {up_mbps:.1f} Mbps</code>\n"
                f"⚡ <i>Testing parallel Telegram upload speed (16 MB)...</i>"
            )
            tg_mbs, tg_mbps = 0.0, 0.0
            try:
                # Reserve the transfer slot so a new video cannot compete with
                # this measurement. Session setup is excluded by the probe.
                async with upload_semaphore:
                    tg_rate = await telegram_speed_probe(client)
                tg_mbs = tg_rate / (1024 * 1024)
                tg_mbps = tg_rate * 8 / 1_000_000
            except Exception as tg_err:
                logger.warning("Telegram MTProto speed probe skipped/failed: %s", tg_err)

            result_text = (
                f"🌐 <b>SERVER SPEED TEST RESULTS</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📥 <b>Internet Download:</b> <code>{down_mbs:.2f} MB/s • {down_mbps:.1f} Mbps</code>\n"
                f"📤 <b>Internet Upload:</b> <code>{up_mbs:.2f} MB/s • {up_mbps:.1f} Mbps</code>\n"
            )
            if tg_mbs > 0:
                result_text += f"⚡ <b>Telegram Parallel Upload:</b> <code>{tg_mbs:.2f} MB/s • {tg_mbps:.1f} Mbps</code>\n"
            result_text += (
                f"📡 <b>Network Latency:</b> <code>{avg_ping:.0f} ms</code>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"ℹ️ <i>Cloudflare and Telegram measure different routes. "
                f"Telegram uses the real parallel uploader; connection setup is excluded.</i>"
            )
            await status.edit_text(result_text)
    except Exception as e:
        logger.error("Speedtest failed: %s", e)
        try:
            await status.edit_text(f"❌ <b>Speed test failed:</b>\n<code>{str(e)[:300]}</code>")
        except Exception:
            pass


@app.on_message(filters.command("logs"))
async def logs_command(client: Client, message: Message):
    logger.info("📩 Received /logs command from chat_id=%s", message.chat.id)
    recent_logs = memory_log_handler.get_logs(count=60)
    if not recent_logs:
        await message.reply_text("📋 <i>No logs recorded in memory yet.</i>")
        return

    joined_text = "\n".join(recent_logs)
    if len(joined_text) <= 3500:
        await message.reply_text(f"📋 <b>Recent Server Logs:</b>\n\n<code>{joined_text}</code>")
    else:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False) as f:
            all_logs = memory_log_handler.get_logs(count=300)
            f.write("\n".join(all_logs))
            tmp_path = f.name
        try:
            preview = "\n".join(recent_logs[-15:])
            if len(preview) > 2500:
                preview = preview[-2500:]
            await message.reply_document(
                document=tmp_path,
                file_name="bot_logs.txt",
                caption=f"📋 <b>Recent Server Logs:</b>\n\n<code>{preview}</code>"
            )
        finally:
            safe_cleanup(tmp_path)


def format_playlist_caption(session: PlaylistSession) -> str:
    """Format rich playlist page view showing up to 10 videos with full titles and checkmarks."""
    start = session.page * session.per_page
    end = min(start + session.per_page, session.total_entries)
    page_entries = session.get_page_entries()

    text = (
        f"📋 <b>Playlist:</b> {session.playlist_title}\n"
        f"📊 <b>Total Videos:</b> {session.total_entries} | <b>Selected:</b> {len(session.selected_indices)}\n"
        f"📄 <b>Page:</b> {session.page + 1} / {session.total_pages} (Items {start + 1}–{end})\n\n"
        f"<b>Videos on this page:</b>\n"
    )

    for idx, entry in page_entries:
        is_sel = idx in session.selected_indices
        check = "✅" if is_sel else "⬜"
        title = entry.get("title", f"Video {idx + 1}")
        if len(title) > 42:
            title = title[:39] + "..."
        text += f"{check} <b>{idx + 1}.</b> {title}\n"

    text += "\n<i>Click items below to toggle, or click 'Select All':</i>"
    return text


@app.on_message(filters.command("video"))
async def video_command(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        if message.reply_to_message and message.reply_to_message.text:
            url = message.reply_to_message.text.strip()
            logger.info("📩 Received /video command (via reply) from chat_id=%s: %s", message.chat.id, url)
            await handle_video_request(client, message, url)
            return
        await message.reply_text("⚠️ <b>Please provide a YouTube video URL:</b>\n<code>/video https://youtu.be/...</code>")
        return
    url = parts[1].strip()
    logger.info("📩 Received /video command from chat_id=%s: %s", message.chat.id, url)
    await handle_video_request(client, message, url)


@app.on_message(filters.command("playlist"))
async def playlist_command(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        if message.reply_to_message and message.reply_to_message.text:
            url = message.reply_to_message.text.strip()
            logger.info("📩 Received /playlist command (via reply) from chat_id=%s: %s", message.chat.id, url)
            await handle_playlist_request(client, message, url)
            return
        await message.reply_text("⚠️ <b>Please provide a YouTube playlist URL:</b>\n<code>/playlist https://youtube.com/playlist?list=...</code>")
        return
    url = parts[1].strip()
    logger.info("📩 Received /playlist command from chat_id=%s: %s", message.chat.id, url)
    await handle_playlist_request(client, message, url)


def playlist_queue_view(chat_id, page=0):
    jobs = playlist_queue.visible(chat_id)
    per_page = 6
    pages = max(1, (len(jobs) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    lines = [f"📋 <b>Playlist queue</b> · {len(jobs)} active/waiting",
             f"Page {page + 1}/{pages}", ""]
    buttons = []
    for index, job in enumerate(jobs[page * per_page:(page + 1) * per_page], page * per_page + 1):
        percent = int(100 * job.completed / job.total) if job.total else 0
        bar = "█" * (percent // 10) + "░" * (10 - percent // 10)
        lines.extend([
            f"<b>{index}. {escape(job.title[:100])}</b> · {job.state}",
            f"<code>{job.id}</code> · {escape(job.quality)}",
            f"{bar} {percent}% · {job.completed}/{job.total} processed",
            f"✅ Sent {job.successful} · ❌ Failed {job.failed}",
            f"{escape(job.stage)}" + (f" · #{job.current}: {escape(job.current_title[:100])}" if job.current else ""), "",
        ])
        buttons.append([InlineKeyboardButton(f"🛑 Cancel #{index}", callback_data=f"pq_cancel:{job.id}:{page}")])
    if not jobs:
        lines.append("No playlists waiting or running. Choose videos and quality with /playlist to add one.")
    navigation = []
    if page:
        navigation.append(InlineKeyboardButton("◀ Previous", callback_data=f"pq_page:{page - 1}"))
    navigation.append(InlineKeyboardButton("🔄 Refresh", callback_data=f"pq_page:{page}"))
    if page + 1 < pages:
        navigation.append(InlineKeyboardButton("Next ▶", callback_data=f"pq_page:{page + 1}"))
    buttons.append(navigation)
    lines.append("/cancelqueue JOB_ID cancels that playlist. Completed uploads stay in the chat.")
    return "\n".join(lines), InlineKeyboardMarkup(buttons)


@app.on_message(filters.command(["queue", "queuestatus", "playlistqueue"]))
async def playlist_queue_command(client: Client, message: Message):
    parts = message.text.split()
    page = max(0, int(parts[1]) - 1) if len(parts) > 1 and parts[1].isdigit() else 0
    text, keyboard = playlist_queue_view(message.chat.id, page)
    await message.reply_text(text, reply_markup=keyboard)


@app.on_message(filters.command(["cancelqueue", "cancelplaylist"]))
async def cancel_playlist_command(client: Client, message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.reply_text("Use /queue and tap Cancel, or send <code>/cancelqueue JOB_ID</code>.")
        return
    job = playlist_queue.cancel(parts[1], message.chat.id)
    if not job:
        await message.reply_text("No active/waiting playlist with that ID in this chat. Use /queue.")
        return
    await message.reply_text(f"🛑 Playlist <code>{job.id}</code>: {job.state}. Use /queue for progress.")


@app.on_callback_query(filters.regex(r"^pq_(page|cancel):"))
async def callback_playlist_queue(client: Client, callback: CallbackQuery):
    parts = callback.data.split(":")
    page = 0
    if parts[0] == "pq_cancel":
        job = playlist_queue.cancel(parts[1], callback.message.chat.id)
        await callback.answer(f"Playlist {job.state}" if job else "Playlist already finished or not found")
        if len(parts) > 2 and parts[2].isdigit():
            page = int(parts[2])
    else:
        page = int(parts[1]) if parts[1].isdigit() else 0
        await callback.answer()
    text, keyboard = playlist_queue_view(callback.message.chat.id, page)
    try:
        await callback.message.edit_text(text, reply_markup=keyboard)
    except MessageNotModified:
        pass


@app.on_message(filters.text & filters.private)
async def auto_link_handler(client: Client, message: Message):
    text = message.text.strip()
    logger.info("📩 Received text message from chat_id=%s: %s", message.chat.id, text[:80])
    if not is_youtube_url(text):
        await message.reply_text("ℹ️ Send a YouTube video or playlist link, or type /help for instructions.")
        return

    if is_playlist_url(text):
        await handle_playlist_request(client, message, text)
    else:
        await handle_video_request(client, message, text)


def json_to_netscape(json_str: str) -> str:
    try:
        cookies = json.loads(json_str)
        if not isinstance(cookies, list):
            return ""
        
        lines = ["# Netscape HTTP Cookie File\n# http://curl.haxx.se/rfc/cookie_spec.html\n# This is a generated file!  Do not edit.\n"]
        for c in cookies:
            domain = c.get("domain", "")
            include_subdomains = "TRUE" if domain.startswith(".") else "FALSE"
            path = c.get("path", "/")
            secure = "TRUE" if c.get("secure", False) else "FALSE"
            expiration = c.get("expirationDate") or c.get("expiry") or 0
            expiry = str(int(expiration))
            name = c.get("name", "")
            value = c.get("value", "")
            lines.append(f"{domain}\t{include_subdomains}\t{path}\t{secure}\t{expiry}\t{name}\t{value}")
            
        return "\n".join(lines)
    except Exception:
        return ""

@app.on_message(filters.document & filters.private)
async def handle_document(client: Client, message: Message):
    doc = message.document
    if doc and doc.file_name:
        fname = doc.file_name.lower()
        if ("cookie" in fname or "youtube" in fname) and (fname.endswith(".txt") or fname.endswith(".json")):
            status_msg = await message.reply_text("📥 <i>Receiving cookies file...</i>")
            try:
                temp_path = await message.download()
                
                with open(temp_path, "r", encoding="utf-8") as f:
                    content = f.read()
                
                # Check if it's JSON
                if content.strip().startswith("[") and content.strip().endswith("]"):
                    netscape_content = json_to_netscape(content)
                    if netscape_content:
                        content = netscape_content
                
                # Validate in a staging file first. A bad export must not
                # overwrite a previously working cookies.txt file.
                abs_cookie_path = os.path.abspath("cookies.txt")
                staged_cookie = tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=".cookies.txt",
                    prefix="youtube_cookie_upload_",
                    dir=os.path.dirname(abs_cookie_path),
                    delete=False,
                )
                with staged_cookie as f:
                    f.write(content)
                validation = validate_youtube_cookie_file(staged_cookie.name)
                if not validation.valid:
                    try:
                        os.remove(staged_cookie.name)
                    except OSError:
                        pass
                    if os.path.abspath(temp_path) != abs_cookie_path:
                        try:
                            os.remove(temp_path)
                        except OSError:
                            pass
                    await status_msg.edit_text(
                        "❌ <b>Cookies were not installed.</b>\n\n"
                        f"<b>Reason:</b> {validation.reason}\n\n"
                        "Export fresh <code>youtube.com</code> cookies from a signed-in private/incognito window, then close that window after exporting. "
                        "The previous cookie file was kept unchanged."
                    )
                    return

                os.replace(staged_cookie.name, abs_cookie_path)
                config.COOKIES_FILE = abs_cookie_path

                if os.path.abspath(temp_path) != abs_cookie_path:
                    try: os.remove(temp_path)
                    except OSError: pass

                size_kb = os.path.getsize(abs_cookie_path) / 1024
                logger.info("Installed validated YouTube cookie export (%d YouTube cookies, %.1f KB)", validation.youtube_cookie_count, size_kb)
                await status_msg.edit_text(
                    f"✅ <b>Signed-in YouTube cookies installed ({size_kb:.1f} KB).</b>\n\n"
                    "The file passed the local account-cookie checks and will be used for the next download."
                )
            except Exception as e:
                logger.error("Failed to save cookies file: %s", e)
                await status_msg.edit_text(f"❌ Failed to save cookies file: {e}")


# =====================================================================
# REQUEST PROCESSORS
# =====================================================================

async def handle_video_request(client: Client, message: Message, url: str):
    status_msg = await message.reply_text("🔍 <i>Analyzing video formats and size limits...</i>")
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, extract_info, url, False)
        analysis = analyze_video_qualities(info)

        session = session_manager.create_video_session(
            chat_id=message.chat.id,
            video_url=url,
            analysis=analysis,
            message_id=status_msg.id,
        )

        caption = (
            f"🎬 <b>{analysis.title}</b>\n\n"
            f"⏱️ <b>Duration:</b> {human_readable_time(analysis.duration)}\n"
        )

        if analysis.max_quality:
            caption += f"⭐ <b>Highest Quality Available:</b> {analysis.max_quality.label}\n\n"
        caption += "📦 Large videos are split into playable parts; selected quality is preserved.\n"
        caption += "Select your desired download quality below:"

        keyboard = build_video_quality_keyboard(session)
        await status_msg.edit_text(caption, reply_markup=keyboard)

    except Exception as e:
        logger.error("Failed to process video %s: %s", url, e, exc_info=True)
        await status_msg.edit_text(f"❌ <b>Error:</b> Unable to process video.\n<code>{str(e)[:300]}</code>")


async def handle_playlist_request(client: Client, message: Message, url: str):
    status_msg = await message.reply_text("🔍 <i>Extracting playlist items... Please wait.</i>")
    try:
        loop = asyncio.get_running_loop()
        info = await loop.run_in_executor(None, extract_info, url, True)

        entries = list(info.get("entries", []))
        if not entries:
            await status_msg.edit_text("❌ <b>No videos found in this playlist.</b>")
            return

        title = info.get("title", "YouTube Playlist")
        session = session_manager.create_playlist_session(
            chat_id=message.chat.id,
            playlist_title=title,
            playlist_url=url,
            entries=entries,
            message_id=status_msg.id,
        )

        caption = format_playlist_caption(session)
        keyboard = build_playlist_keyboard(session)
        await status_msg.edit_text(caption, reply_markup=keyboard)

    except Exception as e:
        logger.error("Failed to process playlist %s: %s", url, e, exc_info=True)
        await status_msg.edit_text(f"❌ <b>Error:</b> Unable to extract playlist.\n<code>{str(e)[:300]}</code>")


# =====================================================================
# CALLBACK QUERY HANDLERS
# =====================================================================

@app.on_callback_query(filters.regex(r"^pl_noop"))
async def callback_noop(client: Client, callback: CallbackQuery):
    await callback.answer()


@app.on_callback_query(filters.regex(r"^pl_cancel:(.+)"))
async def callback_pl_cancel(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session_manager.remove_session(session_id)
    await callback.message.edit_text("❌ <i>Playlist selection cancelled.</i>")
    await callback.answer("Cancelled")


@app.on_callback_query(filters.regex(r"^vid_cancel:(.+)"))
async def callback_vid_cancel(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session_manager.remove_session(session_id)
    await callback.message.edit_text("❌ <i>Video download cancelled.</i>")
    await callback.answer("Cancelled")


@app.on_callback_query(filters.regex(r"^pl_tg:(.+):(\d+)"))
async def callback_pl_toggle(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    index = int(callback.matches[0].group(2))
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired. Please send playlist link again.", show_alert=True)
        return

    session.toggle(index)
    keyboard = build_playlist_keyboard(session)
    caption = format_playlist_caption(session)
    try:
        await callback.message.edit_text(caption, reply_markup=keyboard)
    except MessageNotModified:
        pass
    await callback.answer()


@app.on_callback_query(filters.regex(r"^pl_pg:(.+):(\d+)"))
async def callback_pl_page(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    page = int(callback.matches[0].group(2))
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    session.page = max(0, min(page, session.total_pages - 1))
    keyboard = build_playlist_keyboard(session)
    caption = format_playlist_caption(session)
    try:
        await callback.message.edit_text(caption, reply_markup=keyboard)
    except MessageNotModified:
        pass
    await callback.answer()


@app.on_callback_query(filters.regex(r"^pl_all:(.+)"))
async def callback_pl_all(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    session.select_all()
    keyboard = build_playlist_keyboard(session)
    caption = format_playlist_caption(session)
    try:
        await callback.message.edit_text(caption, reply_markup=keyboard)
    except MessageNotModified:
        pass
    await callback.answer(f"All {session.total_entries} videos selected!")


@app.on_callback_query(filters.regex(r"^pl_none:(.+)"))
async def callback_pl_none(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    session.deselect_all()
    keyboard = build_playlist_keyboard(session)
    caption = format_playlist_caption(session)
    try:
        await callback.message.edit_text(caption, reply_markup=keyboard)
    except MessageNotModified:
        pass
    await callback.answer("All deselected")


@app.on_callback_query(filters.regex(r"^pl_dl:(.+)"))
async def callback_pl_download_prompt(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    count = len(session.selected_indices)
    if count == 0:
        await callback.answer("⚠️ Please select at least one video to download!", show_alert=True)
        return

    text = (
        f"📥 <b>Ready to download {count} video(s) from:</b>\n"
        f"<i>{session.playlist_title}</i>\n\n"
        f"Select download quality:\n"
        f"<i>(Large videos are auto-split into playable MP4 parts; quality is preserved)</i>"
    )
    keyboard = build_playlist_quality_keyboard(session_id)
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()


@app.on_callback_query(filters.regex(r"^pl_back:(.+)"))
async def callback_pl_back(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    keyboard = build_playlist_keyboard(session)
    caption = format_playlist_caption(session)
    await callback.message.edit_text(caption, reply_markup=keyboard)
    await callback.answer()


@app.on_callback_query(filters.regex(r"^pl_q:(.+):(.+)"))
async def callback_pl_start_download(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    quality_key = callback.matches[0].group(2)
    session = session_manager.get_playlist_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    if session.chat_id != callback.message.chat.id:
        await callback.answer("This playlist belongs to another chat.", show_alert=True)
        return

    selected_entries = session.get_selected_entries()
    if not selected_entries:
        await callback.answer("⚠️ No videos selected.", show_alert=True)
        return

    # Consume before the first await so repeated taps cannot enqueue twice.
    session_manager.remove_session(session_id)
    status_msg = callback.message
    async def run(job):
        try:
            await process_playlist_downloads(client, status_msg, job.chat_id,
                                             job.entries, quality_key, job=job)
        except asyncio.CancelledError:
            try:
                await status_msg.edit_text(
                    f"🛑 <b>Playlist cancelled</b> · <code>{job.id}</code>\n"
                    f"Sent: {job.successful}/{job.total} · Failed: {job.failed}")
            except Exception:
                logger.debug("Cancelled playlist message unavailable", exc_info=True)
            raise
    job = PlaylistJob(callback.message.chat.id, session.playlist_title,
                      selected_entries, quality_key, run)
    # Acknowledge before starting the worker so the running progress is never
    # overwritten with a late 'queued' edit.
    await callback.answer("Playlist added to queue")
    try:
        await status_msg.edit_text(
            f"📋 <b>Playlist queued:</b> {escape(job.title)}\n"
            f"Job: <code>{job.id}</code> · Videos: {job.total}\n"
            "Starts automatically after earlier playlists. Use /queue for progress.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🛑 Cancel playlist", callback_data=f"pq_cancel:{job.id}")]]))
    finally:
        playlist_queue.enqueue(job)


@app.on_callback_query(filters.regex(r"^vid:(.+):(.+)"))
async def callback_vid_start_download(client: Client, callback: CallbackQuery):
    session_id = callback.matches[0].group(1)
    quality_key = callback.matches[0].group(2)
    session = session_manager.get_video_session(session_id)
    if not session:
        await callback.answer("⚠️ Session expired.", show_alert=True)
        return

    await callback.answer("Download started!")
    video_url = session.video_url
    max_h = session.analysis.max_quality.height if session.analysis.max_quality else 1080
    session_manager.remove_session(session_id)

    status_msg = callback.message
    asyncio.create_task(
        process_single_download(
            client=client,
            status_msg=status_msg,
            chat_id=callback.message.chat.id,
            url=video_url,
            quality_key=quality_key,
            max_quality_height=max_h,
        )
    )


# =====================================================================
# DOWNLOAD & UPLOAD PIPELINE
# =====================================================================

async def prepare_download(status_msg, url, quality_key, max_quality_height=None):
    """Prepare stream metadata or a disk download; drain workers on cancel."""
    updater = ThrottledProgressUpdater(status_msg, stage="Downloading 📥")
    stopped = threading.Event()
    def progress(data):
        if stopped.is_set():
            raise RuntimeError("Playlist download cancelled")
        updater.sync_hook(data)
    try:
        async with download_semaphore:
            await status_msg.edit_text("⚡ <b>Starting high-speed download...</b>")
            def prepare():
                if config.STREAM_VIDEO_UPLOADS and quality_key != "audio":
                    try:
                        return prepare_stream(url, quality_key, progress, max_quality_height)
                    except UnsupportedStream as exc:
                        logger.info("Using disk downloader for this source: %s", exc)
                return download_media(url, quality_key, progress, max_quality_height)

            worker = asyncio.create_task(asyncio.to_thread(prepare))
            try:
                return await asyncio.shield(worker)
            except asyncio.CancelledError:
                stopped.set()
                updater.active = False
                try:
                    result = await worker
                    safe_cleanup(result.temp_dir)
                except Exception:
                    # download_media owns cleanup when its cancellation hook raises.
                    logger.debug("Cancelled download worker stopped", exc_info=True)
                raise
    finally:
        updater.active = False


async def process_single_download(
    client: Client,
    status_msg: Message,
    chat_id: int,
    url: str,
    quality_key: str,
    max_quality_height: Optional[int] = None,
    prepared_result: Optional[DownloadResult] = None,
) -> bool:
    """Execute download, MP4 container verification, thumbnail processing, and upload. Returns True on success, False on error."""
    global active_transfer_stats
    # Uploads have their own slot, so a completed file doesn't block the next
    # download even on a server configured with one download worker.
    async with upload_semaphore:
        updater = ThrottledProgressUpdater(status_msg, stage="Downloading 📥")
        temp_dir = prepared_result.temp_dir if prepared_result else None
        try:
            result = prepared_result
            if result is None:
                result = await prepare_download(status_msg, url, quality_key, max_quality_height)
            temp_dir = result.temp_dir

            if isinstance(result, StreamPlan) or (not result.is_audio and result.file_size > video_part_limit()):
                updater.stage = "Transferring to Telegram 📤"
                estimate = result.estimated_size if isinstance(result, StreamPlan) else result.file_size
                estimated_parts, _ = plan_parts(estimate, video_part_limit())
                plan_text = (f"Estimated source: {human_readable_size(estimate)} · ~{estimated_parts} part(s)\n"
                             if estimate else "Source size unknown; parts are limited by actual bytes.\n")
                await status_msg.edit_text(
                    "📤 <i>Transferring at the selected quality...</i>\n" + plan_text +
                    f"Each part: up to {human_readable_size(video_part_limit())}.\n"
                    "Large videos arrive as numbered playable parts, at the selected quality.")
                current_part = 0

                async def part_progress(current, total):
                    nonlocal current_part
                    stats = active_transfer_stats
                    if stats.part_index != current_part:
                        current_part = stats.part_index
                        updater.stage = f"Transferring Part {current_part} (~{stats.part_count} total) 📤"
                        updater.start_upload_time = 0
                        updater.last_upload_bytes = 0
                    await updater.async_upload_hook(current, total)

                async with download_semaphore:
                    active_transfer_stats = TransferStats()
                    sender = stream_video if isinstance(result, StreamPlan) else stream_local_video
                    await sender(client, chat_id, result, part_progress, stats=active_transfer_stats)
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                return True

            # Verify integrity
            await status_msg.edit_text("🔍 <i>Verifying media stream integrity...</i>")
            is_valid = await asyncio.to_thread(verify_media_integrity, result.file_path, is_audio=result.is_audio)
            if not is_valid:
                raise ValueError("Downloaded file integrity check failed or file is corrupted.")

            # Prepare thumbnail
            thumb_jpg = None
            if result.thumbnail_path:
                thumb_jpg = await asyncio.to_thread(prepare_thumbnail, result.thumbnail_path, temp_dir)

            # Upload to Telegram
            updater.stage = "Uploading 📤"
            await status_msg.edit_text("📤 <i>Uploading to Telegram at maximum speed...</i>")

            caption = (
                f"🎬 <b>{result.title}</b>\n\n"
                f"⏱️ <b>Duration:</b> {human_readable_time(result.duration)}\n"
                f"💾 <b>Size:</b> {human_readable_size(result.file_size)}"
            )

            # Format file name preserving original YouTube title
            clean_filename = sanitize_filename(result.title) + (".mp3" if result.is_audio else ".mp4")

            max_upload_retries = 3
            upload_success = False
            last_upload_err = None

            for upload_attempt in range(1, max_upload_retries + 1):
                try:
                    if upload_attempt > 1:
                        logger.warning("Retrying upload for '%s' (attempt %d/%d)...", result.title, upload_attempt, max_upload_retries)
                        updater.start_upload_time = 0.0
                        await status_msg.edit_text(f"📤 <i>Retrying Telegram upload (attempt {upload_attempt}/{max_upload_retries})...</i>")

                    if result.is_audio:
                        await client.send_audio(
                            chat_id=chat_id,
                            audio=result.file_path,
                            caption=caption,
                            title=result.title,
                            duration=result.duration,
                            thumb=thumb_jpg,
                            file_name=clean_filename,
                            progress=updater.async_upload_hook,
                        )
                    else:
                        await client.send_video(
                            chat_id=chat_id,
                            video=result.file_path,
                            caption=caption,
                            duration=result.duration,
                            width=result.width,
                            height=result.height,
                            thumb=thumb_jpg,
                            file_name=clean_filename,
                            supports_streaming=True,
                            progress=updater.async_upload_hook,
                        )
                    upload_success = True
                    break
                except (TimeoutError, OSError, ConnectionError) as err:
                    last_upload_err = err
                    logger.warning("Upload attempt %d failed for '%s': %s", upload_attempt, result.title, err)
                    if upload_attempt < max_upload_retries:
                        await asyncio.sleep(2 * upload_attempt)
                    else:
                        raise

            if not upload_success and last_upload_err:
                raise last_upload_err

            updater.active = False
            try:
                await status_msg.delete()
            except Exception:
                pass
            return True

        except Exception as e:
            logger.error("Download failed for %s: %s", url, e, exc_info=True)
            try:
                await status_msg.edit_text(f"❌ <b>Download Failed:</b>\n<code>{str(e)[:300]}</code>")
            except Exception:
                pass
            return False
        finally:
            active_transfer_stats = None
            updater.active = False
            if temp_dir:
                safe_cleanup(temp_dir)


async def process_playlist_downloads(
    client: Client,
    status_msg: Message,
    chat_id: int,
    entries: List[dict],
    quality_key: str,
    job: Optional[PlaylistJob] = None,
):
    """Process playlist downloads. Sequentially by default to prevent download and upload bandwidth contention."""
    total = len(entries)
    successful = 0
    failed = 0

    def progress(index=None, stage=None):
        if job is None:
            return
        job.successful = successful
        job.failed = failed
        if index is not None:
            job.current = index
            job.current_title = entries[index - 1].get("title", f"Video {index}")
        if stage and job.state != "cancelling":
            job.stage = stage
        logger.info("Playlist progress job=%s item=%s/%s stage=%s sent=%s failed=%s",
                    job.id, job.current, total, job.stage, successful, failed)

    if not entries:
        try:
            await status_msg.edit_text(
                "🎉 <b>Playlist Download Completed!</b>\n\n✅ <b>Successfully sent:</b> 0 / 0"
            )
        except Exception as e:
            logger.debug("Final status edit skipped: %s", e)
        return

    if getattr(config, "PREFETCH_PLAYLIST", False):
        # Prefetch pipeline: overlap next download with current upload
        async def prepare_item(idx, entry):
            video_title = entry.get("title", f"Video {idx}")
            video_url = entry.get("url") or ""
            video_id = entry.get("id") or ""
            if not video_url or not video_url.startswith("http"):
                if video_id:
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                elif video_url:
                    video_url = f"https://www.youtube.com/watch?v={video_url}"

            item_status = None
            try:
                item_status = await client.send_message(
                    chat_id=chat_id,
                    text=f"📥 <i>[{idx}/{total}] Downloading:</i> <b>{video_title}</b>",
                )
                result = await prepare_download(item_status, video_url, quality_key)
                return item_status, video_url, result
            except Exception as e:
                logger.error("Failed playlist item %d (%s): %s", idx, video_url, e)
                if item_status:
                    try:
                        await item_status.edit_text(f"❌ <b>Download Failed:</b>\n<code>{str(e)[:300]}</code>")
                    except Exception:
                        pass
                return None

        pending = asyncio.create_task(prepare_item(1, entries[0])) if entries else None
        try:
            for offset in range(total):
                progress(offset + 1, "Downloading / preparing media")
                prepared = await asyncio.shield(pending)
                pending = None
                # Bound disk usage to the uploading file plus one prefetched file.
                if offset + 1 < total:
                    pending = asyncio.create_task(prepare_item(offset + 2, entries[offset + 1]))
                if prepared is None:
                    failed += 1
                    progress()
                    continue
                item_status, video_url, result = prepared
                progress(stage="Verifying / uploading to Telegram")
                try:
                    success = await process_single_download(
                        client, item_status, chat_id, video_url, quality_key,
                        prepared_result=result,
                    )
                finally:
                    safe_cleanup(result.temp_dir)
                successful += int(success)
                failed += int(not success)
                progress()
        finally:
            if pending is not None:
                prepared = await pending
                if prepared:
                    safe_cleanup(prepared[2].temp_dir)
    else:
        # Sequential pipeline: clean download followed by maximum speed upload
        for idx, entry in enumerate(entries, start=1):
            progress(idx, "Downloading / preparing media")
            video_title = entry.get("title", f"Video {idx}")
            video_url = entry.get("url") or ""
            video_id = entry.get("id") or ""
            if not video_url or not video_url.startswith("http"):
                if video_id:
                    video_url = f"https://www.youtube.com/watch?v={video_id}"
                elif video_url:
                    video_url = f"https://www.youtube.com/watch?v={video_url}"

            item_status = None
            prepared = None
            try:
                item_status = await client.send_message(
                    chat_id=chat_id,
                    text=f"📥 <i>[{idx}/{total}] Downloading:</i> <b>{video_title}</b>",
                )
                prepared = await prepare_download(item_status, video_url, quality_key)
            except Exception as e:
                logger.error("Failed playlist download item %d (%s): %s", idx, video_url, e)
                if item_status:
                    try:
                        await item_status.edit_text(f"❌ <b>Download Failed:</b>\n<code>{str(e)[:300]}</code>")
                    except Exception:
                        pass
                failed += 1
                progress()
                continue

            if prepared is None:
                failed += 1
                progress()
                continue

            progress(stage="Verifying / uploading to Telegram")
            try:
                success = await process_single_download(
                    client, item_status, chat_id, video_url, quality_key,
                    prepared_result=prepared,
                )
            finally:
                safe_cleanup(prepared.temp_dir)
            successful += int(success)
            failed += int(not success)
            progress()

    progress(stage="Completed")
    summary_text = (
        f"🎉 <b>Playlist Download Completed!</b>\n\n"
        f"✅ <b>Successfully sent:</b> {successful} / {total}\n"
    )
    if failed > 0:
        summary_text += f"⚠️ <b>Failed items:</b> {failed}\n"

    try:
        await status_msg.edit_text(summary_text)
    except Exception as e:
        logger.debug("Final status edit skipped: %s", e)
