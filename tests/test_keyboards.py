from session_manager import PlaylistSession, VideoSession
from downloader import VideoQualityAnalysis, QualityOption
from keyboards import (
    build_playlist_keyboard,
    build_playlist_quality_keyboard,
    build_video_quality_keyboard,
)


def test_build_playlist_keyboard_layout():
    entries = [{"id": f"vid_{i}", "title": f"Awesome Video Title {i}"} for i in range(25)]
    session = PlaylistSession(
        session_id="pl123",
        chat_id=999,
        playlist_title="My Big Playlist",
        playlist_url="https://youtube.com/playlist?list=123",
        entries=entries,
        page=0,
        per_page=10,
    )
    # Select first video
    session.toggle(0)

    markup = build_playlist_keyboard(session)
    rows = markup.inline_keyboard

    # 10 video items + 1 nav row + 1 batch row + 1 action row = 13 rows
    assert len(rows) == 13

    # Check first item text has checkmark
    first_btn = rows[0][0]
    assert "✅" in first_btn.text
    assert first_btn.callback_data == "pl_tg:pl123:0"

    # Check second item has empty box
    second_btn = rows[1][0]
    assert "⬜" in second_btn.text
    assert second_btn.callback_data == "pl_tg:pl123:1"

    # Navigation row is row 10
    nav_row = rows[10]
    assert len(nav_row) == 3
    # On page 0, Prev is disabled (⏹️), Next is active
    assert nav_row[0].text == "⏹️"
    assert "1/3" in nav_row[1].text
    assert nav_row[2].text == "Next ➡️"

    # Batch selection row is row 11
    batch_row = rows[11]
    assert batch_row[0].text == "✅ Select All"
    assert batch_row[1].text == "⬜ Deselect All"

    # Action row is row 12
    action_row = rows[12]
    assert "Download Selected (1)" in action_row[0].text
    assert action_row[1].text == "❌ Cancel"

    # CRITICAL: Verify ALL callback_data strings are <= 64 bytes (Telegram API limit)
    for row in rows:
        for btn in row:
            if btn.callback_data:
                assert len(btn.callback_data.encode("utf-8")) <= 64, f"Callback data too long: {btn.callback_data}"


def test_build_video_quality_keyboard():
    analysis = VideoQualityAnalysis(
        title="Test 4K Video",
        video_id="xyz",
        duration=300,
        thumbnail_url="https://example.com/thumb.jpg",
        available_qualities=[
            QualityOption(height=1440, label="2K (1440p)", estimated_size=1500 * 1024 * 1024, format_selector="bv*", is_max=True),
            QualityOption(height=1080, label="1080p (FHD)", estimated_size=800 * 1024 * 1024, format_selector="bv*"),
            QualityOption(height=720, label="720p (HD)", estimated_size=400 * 1024 * 1024, format_selector="bv*"),
        ],
        max_quality=QualityOption(height=1440, label="2K (1440p)", estimated_size=1500 * 1024 * 1024, format_selector="bv*", is_max=True),
        audio_size=15 * 1024 * 1024,
        excluded_qualities=[("4K (2160p)", 2400 * 1024 * 1024)],
    )

    session = VideoSession(
        session_id="vid999",
        chat_id=123,
        video_url="https://youtube.com/watch?v=xyz",
        analysis=analysis,
    )

    markup = build_video_quality_keyboard(session)
    rows = markup.inline_keyboard

    # First row should be Max Quality
    top_btn = rows[0][0]
    assert "⭐ Max Quality" in top_btn.text
    assert "2K (1440p)" in top_btn.text
    assert top_btn.callback_data == "vid:vid999:max"

    # Make sure 4K is NOT in keyboard
    all_texts = [btn.text for row in rows for btn in row]
    assert not any("4K" in t for t in all_texts)

    # Check callback_data limits
    for row in rows:
        for btn in row:
            if btn.callback_data:
                assert len(btn.callback_data.encode("utf-8")) <= 64


def test_build_playlist_quality_keyboard():
    markup = build_playlist_quality_keyboard("pl555")
    rows = markup.inline_keyboard
    top_btn = rows[0][0]
    assert "Max Quality" in top_btn.text
    assert top_btn.callback_data == "pl_q:pl555:max"

    for row in rows:
        for btn in row:
            if btn.callback_data:
                assert len(btn.callback_data.encode("utf-8")) <= 64
