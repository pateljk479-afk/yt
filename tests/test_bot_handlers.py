import pytest
from bot import is_youtube_url, is_playlist_url


def test_is_youtube_url():
    valid_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://youtube.com/playlist?list=PL12345678",
        "https://www.youtube.com/shorts/abcdef12345",
        "https://youtube.com/live/xyz123",
    ]
    for url in valid_urls:
        assert is_youtube_url(url) is True, f"Failed for {url}"

    invalid_urls = [
        "https://google.com",
        "https://vimeo.com/12345",
        "hello world",
        "ftp://youtube.com/something",
    ]
    for url in invalid_urls:
        assert is_youtube_url(url) is False, f"Should be invalid: {url}"


def test_is_playlist_url():
    playlist_urls = [
        "https://youtube.com/playlist?list=PL12345678",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&list=PL12345678",
    ]
    for url in playlist_urls:
        assert is_playlist_url(url) is True

    single_video_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://www.youtube.com/shorts/abcdef12345",
    ]
    for url in single_video_urls:
        assert is_playlist_url(url) is False


def test_format_playlist_caption():
    from bot import format_playlist_caption
    from session_manager import PlaylistSession

    entries = [{"id": f"id_{i}", "title": f"Video Tutorial Episode {i + 1}"} for i in range(25)]
    session = PlaylistSession(
        session_id="pl_test_123",
        chat_id=100,
        playlist_title="Python Deep Dive",
        playlist_url="https://youtube.com/playlist?list=PL123",
        entries=entries,
        page=0,
        per_page=10,
    )
    session.toggle(0)
    session.toggle(3)

    caption = format_playlist_caption(session)
    assert "Python Deep Dive" in caption
    assert "<b>Total Videos:</b> 25" in caption
    assert "<b>Selected:</b> 2" in caption
    assert "Page:</b> 1 / 3" in caption
    assert "✅ <b>1.</b> Video Tutorial Episode 1" in caption
    assert "⬜ <b>2.</b> Video Tutorial Episode 2" in caption
    assert "✅ <b>4.</b> Video Tutorial Episode 4" in caption
    assert "<b>10.</b> Video Tutorial Episode 10" in caption


@pytest.mark.asyncio
async def test_throttled_progress_updater_upload_speed():
    from unittest.mock import AsyncMock, MagicMock
    from bot import ThrottledProgressUpdater

    mock_msg = MagicMock()
    mock_msg.edit_text = AsyncMock()

    updater = ThrottledProgressUpdater(mock_msg, stage="Uploading 📤", interval=0.0)
    
    # Simulate first chunk
    await updater.async_upload_hook(current=10 * 1024 * 1024, total=100 * 1024 * 1024)
    # Simulate second chunk 1 second later
    updater.last_upload_time -= 1.0  # simulate 1 sec elapsed
    await updater.async_upload_hook(current=30 * 1024 * 1024, total=100 * 1024 * 1024)

    assert mock_msg.edit_text.call_count >= 1
    call_args = mock_msg.edit_text.call_args[0][0]
    assert "Uploading" in call_args
    assert "Progress:" in call_args
    # Speed should be non-zero and formatted
    assert "MB/s" in call_args
    assert "20.0 MB/s" in call_args


def test_playlist_entry_url_resolution():
    test_entries = [
        {"url": "https://www.youtube.com/watch?v=full_url_123"},
        {"id": "only_id_456"},
        {"url": "relative_id_789"},
        {"url": "", "id": "fallback_id_999"},
    ]

    resolved_urls = []
    for entry in test_entries:
        video_url = entry.get("url") or ""
        video_id = entry.get("id") or ""
        if not video_url or not video_url.startswith("http"):
            if video_id:
                video_url = f"https://www.youtube.com/watch?v={video_id}"
            elif video_url:
                video_url = f"https://www.youtube.com/watch?v={video_url}"
        resolved_urls.append(video_url)

    assert resolved_urls[0] == "https://www.youtube.com/watch?v=full_url_123"
    assert resolved_urls[1] == "https://www.youtube.com/watch?v=only_id_456"
    assert resolved_urls[2] == "https://www.youtube.com/watch?v=relative_id_789"
    assert resolved_urls[3] == "https://www.youtube.com/watch?v=fallback_id_999"
