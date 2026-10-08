import os
import time
import asyncio
import logging
from typing import Optional, List

from pyrogram import Client, filters
from pyrogram.types import Message, CallbackQuery
from pyrogram.errors import FloodWait, MessageNotModified

import config
from downloader import (
    extract_info,
    analyze_video_qualities,
    download_media,
    DownloadResult,
    sanitize_filename,
)
from session_manager import session_manager, PlaylistSession, VideoSession
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

# Semaphore to govern concurrent downloads and protect 2 GB RAM server
download_semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_DOWNLOADS)

# Initialize Pyrogram Bot Client
# API_ID and API_HASH default to fallback placeholders so unit tests can import safely
app = Client(
    name="youtube_downloader_bot",
    api_id=config.API_ID if config.API_ID else 12345,
    api_hash=config.API_HASH if config.API_HASH else "placeholder_hash",
    bot_token=config.BOT_TOKEN if config.BOT_TOKEN else "placeholder_token",
    workers=16,
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
        self.last_upload_time = 0.0
        self.loop = asyncio.get_event_loop()

    def sync_hook(self, d: dict):
        """yt-dlp progress hook running in worker thread."""
        if d.get("status") == "downloading":
            now = time.time()
            if now - self.last_update_time >= self.interval:
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
        now = time.time()
        if self.start_upload_time == 0.0:
            self.start_upload_time = now
            self.last_upload_time = now
            self.last_upload_bytes = current

        dt = now - self.last_upload_time
        if dt >= self.interval or current == total:
            delta_bytes = current - self.last_upload_bytes
            speed = delta_bytes / dt if dt > 0 else 0
            eta = int((total - current) / speed) if speed > 0 and total > current else 0
            self.last_upload_time = now
            self.last_upload_bytes = current
            text = format_progress_bar(current, total, speed, eta, stage=self.stage)
            await self._safe_edit(text)

    async def _safe_edit(self, text: str):
        try:
            await self.message.edit_text(text)
        except FloodWait as e:
            await asyncio.sleep(e.value)
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
    welcome_text = (
        "👋 <b>Welcome to High-Speed YouTube Downloader Bot!</b>\n\n"
        "⚡ <b>Features:</b>\n"
        "• Download single YouTube videos or entire playlists\n"
        "• Quality selection (Max High Quality up to 2 GB limit)\n"
        "• Automatic quality degradation if file exceeds 2 GB\n"
        "• Interactive 10-per-page playlist navigation & multi-selection\n"
        "• Original file names and original YouTube thumbnails\n"
        "• Optimized MP4 streams ready for Telegram player playback\n\n"
        "📌 <b>Commands:</b>\n"
        "• <code>/video &lt;link&gt;</code> - Download a single video\n"
        "• <code>/playlist &lt;link&gt;</code> - Select & download from a playlist\n"
        "• <code>/help</code> - Detailed help and instructions\n\n"
        "Or simply paste any YouTube video or playlist link directly!"
    )
    await message.reply_text(welcome_text)


@app.on_message(filters.command("help"))
async def help_command(client: Client, message: Message):
    help_text = (
        "📖 <b>How to Use This Bot:</b>\n\n"
        "<b>1. Downloading a Single Video:</b>\n"
        "• Send <code>/video https://youtu.be/...</code>\n"
        "• You will see available resolutions (4K, 2K, 1080p, 720p, etc.)\n"
        "• <i>Note:</i> Videos over 2 GB are automatically degraded to ensure Telegram upload compatibility.\n"
        "• Click <b>⭐ Max Quality</b> for the highest quality under 2 GB.\n\n"
        "<b>2. Downloading Playlists:</b>\n"
        "• Send <code>/playlist https://youtube.com/playlist?list=...</code>\n"
        "• The bot displays 10 videos per page with clickable checkmarks.\n"
        "• Use <b>Next ➡️</b> and <b>⬅️ Prev</b> to browse pages.\n"
        "• Click <b>✅ Select All</b> to download the entire playlist at once.\n"
        "• Click <b>📥 Download Selected</b> to pick your desired resolution."
    )
    await message.reply_text(help_text)


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
            await handle_video_request(client, message, url)
            return
        await message.reply_text("⚠️ <b>Please provide a YouTube video URL:</b>\n<code>/video https://youtu.be/...</code>")
        return
    url = parts[1].strip()
    await handle_video_request(client, message, url)


@app.on_message(filters.command("playlist"))
async def playlist_command(client: Client, message: Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        if message.reply_to_message and message.reply_to_message.text:
            url = message.reply_to_message.text.strip()
            await handle_playlist_request(client, message, url)
            return
        await message.reply_text("⚠️ <b>Please provide a YouTube playlist URL:</b>\n<code>/playlist https://youtube.com/playlist?list=...</code>")
        return
    url = parts[1].strip()
    await handle_playlist_request(client, message, url)


@app.on_message(filters.text & filters.private)
async def auto_link_handler(client: Client, message: Message):
    text = message.text.strip()
    if not is_youtube_url(text):
        await message.reply_text("ℹ️ Send a YouTube video or playlist link, or type /help for instructions.")
        return

    if is_playlist_url(text):
        await handle_playlist_request(client, message, text)
    else:
        await handle_video_request(client, message, text)


# =====================================================================
# REQUEST PROCESSORS
# =====================================================================

async def handle_video_request(client: Client, message: Message, url: str):
    status_msg = await message.reply_text("🔍 <i>Analyzing video formats and size limits...</i>")
    try:
        loop = asyncio.get_event_loop()
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

        if analysis.excluded_qualities:
            excluded_names = ", ".join([name for name, _ in analysis.excluded_qualities])
            caption += f"⚠️ <i>Qualities exceeding Telegram 2 GB limit (filtered): {excluded_names}</i>\n"

        if analysis.max_quality:
            caption += f"⭐ <b>Highest Quality Available (&le; 2GB):</b> {analysis.max_quality.label}\n\n"
        caption += "Select your desired download quality below:"

        keyboard = build_video_quality_keyboard(session)
        await status_msg.edit_text(caption, reply_markup=keyboard)

    except Exception as e:
        logger.error("Failed to process video %s: %s", url, e, exc_info=True)
        await status_msg.edit_text(f"❌ <b>Error:</b> Unable to process video.\n<code>{str(e)[:300]}</code>")


async def handle_playlist_request(client: Client, message: Message, url: str):
    status_msg = await message.reply_text("🔍 <i>Extracting playlist items... Please wait.</i>")
    try:
        loop = asyncio.get_event_loop()
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
        f"<i>(Qualities > 2GB will automatically downgrade to stay within Telegram limits)</i>"
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

    selected_entries = session.get_selected_entries()
    if not selected_entries:
        await callback.answer("⚠️ No videos selected.", show_alert=True)
        return

    await callback.answer("Download task started!")
    # Remove session from memory once download begins
    session_manager.remove_session(session_id)

    status_msg = callback.message
    asyncio.create_task(
        process_playlist_downloads(
            client=client,
            status_msg=status_msg,
            chat_id=callback.message.chat.id,
            entries=selected_entries,
            quality_key=quality_key,
        )
    )


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

async def process_single_download(
    client: Client,
    status_msg: Message,
    chat_id: int,
    url: str,
    quality_key: str,
    max_quality_height: Optional[int] = None,
) -> bool:
    """Execute download, MP4 container verification, thumbnail processing, and upload. Returns True on success, False on error."""
    async with download_semaphore:
        updater = ThrottledProgressUpdater(status_msg, stage="Downloading 📥")
        temp_dir = None
        try:
            await status_msg.edit_text("⚡ <b>Starting high-speed download...</b>")
            loop = asyncio.get_event_loop()

            # Execute yt-dlp download in background thread pool
            result: DownloadResult = await loop.run_in_executor(
                None,
                download_media,
                url,
                quality_key,
                updater.sync_hook,
                max_quality_height,
            )
            temp_dir = result.temp_dir

            # Verify integrity
            await status_msg.edit_text("🔍 <i>Verifying media stream integrity...</i>")
            is_valid = verify_media_integrity(result.file_path, is_audio=result.is_audio)
            if not is_valid:
                raise ValueError("Downloaded file integrity check failed or file is corrupted.")

            # Prepare thumbnail
            thumb_jpg = None
            if result.thumbnail_path:
                thumb_jpg = prepare_thumbnail(result.thumbnail_path, temp_dir)

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
            if temp_dir:
                safe_cleanup(temp_dir)


async def process_playlist_downloads(
    client: Client,
    status_msg: Message,
    chat_id: int,
    entries: List[dict],
    quality_key: str,
):
    """Sequentially download and upload selected playlist videos."""
    total = len(entries)
    successful = 0
    failed = 0

    for idx, entry in enumerate(entries, start=1):
        video_title = entry.get("title", f"Video {idx}")
        video_url = entry.get("url") or ""
        video_id = entry.get("id") or ""
        if not video_url or not video_url.startswith("http"):
            if video_id:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
            elif video_url:
                video_url = f"https://www.youtube.com/watch?v={video_url}"

        await status_msg.edit_text(
            f"🔄 <b>Processing [{idx}/{total}]:</b>\n"
            f"<i>{video_title}</i>\n\n"
            f"Progress: {successful} succeeded, {failed} failed"
        )

        try:
            # Create a separate status tracker for individual video progress
            item_status = await client.send_message(
                chat_id=chat_id,
                text=f"📥 <i>[{idx}/{total}] Downloading:</i> <b>{video_title}</b>",
            )
            success = await process_single_download(
                client=client,
                status_msg=item_status,
                chat_id=chat_id,
                url=video_url,
                quality_key=quality_key,
                max_quality_height=None,
            )
            if success:
                successful += 1
            else:
                failed += 1
        except Exception as e:
            logger.error("Failed playlist item %d (%s): %s", idx, video_url, e)
            failed += 1

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
