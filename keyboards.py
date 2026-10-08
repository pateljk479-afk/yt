from typing import List
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from session_manager import PlaylistSession, VideoSession
from uploader import human_readable_size


def build_playlist_keyboard(session: PlaylistSession) -> InlineKeyboardMarkup:
    """
    Build interactive playlist pagination and selection keyboard.
    Shows 10 videos per page with individual selection toggles,
    Prev/Next buttons, Select All, and Download action.
    """
    keyboard: List[List[InlineKeyboardButton]] = []

    # 1. Video items on current page (up to 10)
    page_entries = session.get_page_entries()
    for index, entry in page_entries:
        is_selected = index in session.selected_indices
        check_icon = "✅" if is_selected else "⬜"
        title = entry.get("title", f"Video {index + 1}")
        # Truncate title cleanly for mobile Telegram displays
        short_title = title[:30] + "..." if len(title) > 33 else title
        btn_text = f"{check_icon} {index + 1}. {short_title}"
        callback_data = f"pl_tg:{session.session_id}:{index}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=callback_data)])

    # 2. Navigation row: [⬅️ Prev] [📄 Page X/Y] [Next ➡️]
    nav_row = []
    if session.page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"pl_pg:{session.session_id}:{session.page - 1}")
        )
    else:
        nav_row.append(InlineKeyboardButton("⏹️", callback_data="pl_noop"))

    nav_row.append(
        InlineKeyboardButton(
            f"📄 {session.page + 1}/{session.total_pages}",
            callback_data="pl_noop",
        )
    )

    if session.page < session.total_pages - 1:
        nav_row.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"pl_pg:{session.session_id}:{session.page + 1}")
        )
    else:
        nav_row.append(InlineKeyboardButton("⏹️", callback_data="pl_noop"))

    keyboard.append(nav_row)

    # 3. Batch selection row: [✅ Select All] [⬜ Deselect All]
    batch_row = [
        InlineKeyboardButton("✅ Select All", callback_data=f"pl_all:{session.session_id}"),
        InlineKeyboardButton("⬜ Deselect All", callback_data=f"pl_none:{session.session_id}"),
    ]
    keyboard.append(batch_row)

    # 4. Action row: [📥 Download Selected (N)] [❌ Cancel]
    selected_count = len(session.selected_indices)
    action_row = [
        InlineKeyboardButton(
            f"📥 Download Selected ({selected_count})",
            callback_data=f"pl_dl:{session.session_id}",
        ),
        InlineKeyboardButton("❌ Cancel", callback_data=f"pl_cancel:{session.session_id}"),
    ]
    keyboard.append(action_row)

    return InlineKeyboardMarkup(keyboard)


def build_playlist_quality_keyboard(session_id: str) -> InlineKeyboardMarkup:
    """Build quality selection keyboard for downloading chosen playlist videos."""
    keyboard = [
        [
            InlineKeyboardButton(
                "⭐ Max Quality (Best <= 2GB)",
                callback_data=f"pl_q:{session_id}:max",
            )
        ],
        [
            InlineKeyboardButton("🎬 1080p FHD", callback_data=f"pl_q:{session_id}:1080"),
            InlineKeyboardButton("🎬 720p HD", callback_data=f"pl_q:{session_id}:720"),
        ],
        [
            InlineKeyboardButton("🎬 480p SD", callback_data=f"pl_q:{session_id}:480"),
            InlineKeyboardButton("🎬 360p SD", callback_data=f"pl_q:{session_id}:360"),
        ],
        [
            InlineKeyboardButton("🎵 Audio Only (MP3)", callback_data=f"pl_q:{session_id}:audio"),
        ],
        [
            InlineKeyboardButton("🔙 Back to Selection", callback_data=f"pl_back:{session_id}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"pl_cancel:{session_id}"),
        ],
    ]
    return InlineKeyboardMarkup(keyboard)


def build_video_quality_keyboard(session: VideoSession) -> InlineKeyboardMarkup:
    """
    Build quality selection keyboard for a single YouTube video.
    Strictly shows only qualities <= 2GB.
    Features 'Max Quality' button at top picking highest quality under 2GB limit.
    """
    keyboard: List[List[InlineKeyboardButton]] = []
    analysis = session.analysis

    # 1. Primary 'Max / High Quality' button
    if analysis.max_quality:
        max_q = analysis.max_quality
        size_str = human_readable_size(max_q.estimated_size)
        top_btn = InlineKeyboardButton(
            f"⭐ Max Quality ({max_q.label} ~ {size_str})",
            callback_data=f"vid:{session.session_id}:max",
        )
        keyboard.append([top_btn])

    # 2. Available resolution options (only those <= 2GB)
    row = []
    for q in analysis.available_qualities:
        size_str = human_readable_size(q.estimated_size)
        btn = InlineKeyboardButton(
            f"🎬 {q.label} (~{size_str})",
            callback_data=f"vid:{session.session_id}:{q.height}",
        )
        row.append(btn)
        if len(row) == 2:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)

    # 3. Audio only option
    audio_size_str = human_readable_size(analysis.audio_size)
    keyboard.append(
        [
            InlineKeyboardButton(
                f"🎵 Audio Only (~{audio_size_str})",
                callback_data=f"vid:{session.session_id}:audio",
            )
        ]
    )

    # 4. Cancel button
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data=f"vid_cancel:{session.session_id}")])

    return InlineKeyboardMarkup(keyboard)
