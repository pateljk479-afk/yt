import pytest
import time
from downloader import (
    analyze_video_qualities,
    sanitize_filename,
    VideoQualityAnalysis,
)
from session_manager import PlaylistSession
from keyboards import build_playlist_keyboard, build_video_quality_keyboard
import config


def test_edge_case_all_qualities_exceed_2gb():
    """If all resolutions are > 2 GB (e.g. huge long recording), none are available."""
    huge_size = int(3 * 1024 * 1024 * 1024)  # 3 GB
    mock_info = {
        "title": "Massive 24-Hour Recording",
        "id": "huge24h",
        "duration": 86400,
        "thumbnail": "https://example.com/thumb.jpg",
        "formats": [
            {"vcodec": "none", "acodec": "mp4a.4", "filesize": 500 * 1024 * 1024},
            {"height": 2160, "vcodec": "vp9", "acodec": "none", "filesize": 10 * huge_size},
            {"height": 1080, "vcodec": "avc1", "acodec": "none", "filesize": 5 * huge_size},
            {"height": 720, "vcodec": "avc1", "acodec": "none", "filesize": 3 * huge_size},
            {"height": 480, "vcodec": "avc1", "acodec": "none", "filesize": 2 * huge_size},
            {"height": 360, "vcodec": "avc1", "acodec": "none", "filesize": huge_size},
        ],
    }

    analysis = analyze_video_qualities(mock_info)
    assert len(analysis.available_qualities) == 0
    assert analysis.max_quality is None
    assert len(analysis.excluded_qualities) > 0


def test_edge_case_playlist_pagination_boundaries():
    # 0 entries
    session_0 = PlaylistSession("s0", 1, "Empty", "http://yt.com", [], per_page=10)
    assert session_0.total_pages == 1
    assert session_0.get_page_entries() == []

    # 1 entry
    session_1 = PlaylistSession("s1", 1, "Single", "http://yt.com", [{"id": "1", "title": "One"}], per_page=10)
    assert session_1.total_pages == 1
    assert len(session_1.get_page_entries()) == 1

    # Exactly 10 entries (1 page)
    entries_10 = [{"id": str(i), "title": f"T{i}"} for i in range(10)]
    session_10 = PlaylistSession("s10", 1, "Ten", "http://yt.com", entries_10, per_page=10)
    assert session_10.total_pages == 1
    assert len(session_10.get_page_entries()) == 10

    # 11 entries (2 pages)
    entries_11 = [{"id": str(i), "title": f"T{i}"} for i in range(11)]
    session_11 = PlaylistSession("s11", 1, "Eleven", "http://yt.com", entries_11, per_page=10)
    assert session_11.total_pages == 2
    session_11.page = 0
    assert len(session_11.get_page_entries()) == 10
    session_11.page = 1
    assert len(session_11.get_page_entries()) == 1


def test_edge_case_unicode_and_emojis_in_title():
    emoji_title = "🔥 Python Tutorial 2026! 日本語 & العربية (Complete Crash Course) #1"
    clean = sanitize_filename(emoji_title)
    # Emojis and unicode letters should be preserved cleanly
    assert "🔥" in clean
    assert "Python Tutorial 2026!" in clean
    assert "日本語" in clean
    assert "العربية" in clean
    assert len(clean) > 10


def test_edge_case_keyboard_callback_data_limits_with_large_indices():
    """Verify callback data stays <= 64 bytes even for index 9999."""
    entries = [{"id": f"vid_{i}", "title": f"Title {i}"} for i in range(100)]
    session = PlaylistSession(
        session_id="abcdef12",
        chat_id=1,
        playlist_title="Large Index Playlist",
        playlist_url="http://yt.com",
        entries=entries,
        page=9,
        per_page=10,
    )
    markup = build_playlist_keyboard(session)
    for row in markup.inline_keyboard:
        for btn in row:
            if btn.callback_data:
                assert len(btn.callback_data.encode("utf-8")) <= 64


def test_config_validation():
    # Test missing config detection
    original_bot_token = config.BOT_TOKEN
    original_api_id = config.API_ID
    original_api_hash = config.API_HASH

    try:
        config.BOT_TOKEN = ""
        config.API_ID = 0
        config.API_HASH = ""
        errors = config.validate_config()
        assert len(errors) == 3
        assert any("API_ID" in e for e in errors)
        assert any("API_HASH" in e for e in errors)
        assert any("BOT_TOKEN" in e for e in errors)

        config.BOT_TOKEN = "test_token"
        config.API_ID = 123456
        config.API_HASH = "test_hash"
        errors_valid = config.validate_config()
        assert len(errors_valid) == 0
    finally:
        config.BOT_TOKEN = original_bot_token
        config.API_ID = original_api_id
        config.API_HASH = original_api_hash


def test_max_concurrent_transmissions_config():
    assert hasattr(config, "MAX_CONCURRENT_TRANSMISSIONS")
    assert config.MAX_CONCURRENT_TRANSMISSIONS >= 1


@pytest.mark.asyncio
async def test_playlist_batch_failure_tracking():
    """Verify that failed playlist downloads are accurately counted as failures."""
    from unittest.mock import AsyncMock, MagicMock, patch
    from bot import process_playlist_downloads

    mock_client = MagicMock()
    mock_client.send_message = AsyncMock(return_value=MagicMock())
    
    mock_status_msg = MagicMock()
    mock_status_msg.edit_text = AsyncMock()

    entries = [
        {"title": "Video 1 Success", "url": "https://youtube.com/watch?v=good1"},
        {"title": "Video 2 Fail", "url": "https://youtube.com/watch?v=bad2"},
    ]

    # Patch process_single_download to return True for first, False for second
    side_effects = [True, False]
    with patch("bot.process_single_download", AsyncMock(side_effect=side_effects)):
        await process_playlist_downloads(
            client=mock_client,
            status_msg=mock_status_msg,
            chat_id=12345,
            entries=entries,
            quality_key="max",
        )

    # Check that final summary reflects 1 succeeded, 1 failed
    assert mock_status_msg.edit_text.call_count >= 1
    final_call = mock_status_msg.edit_text.call_args[0][0]
    assert "Successfully sent:</b> 1 / 2" in final_call
    assert "Failed items:</b> 1" in final_call
