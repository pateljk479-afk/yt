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
    from unittest.mock import AsyncMock, MagicMock, patch
    from bot import ThrottledProgressUpdater

    mock_msg = MagicMock()
    mock_msg.edit_text = AsyncMock()

    # Deterministic timestamps: chunk 1 at 1000.0, chunk 2 at 1001.0 (dt = 1.0s)
    with patch("time.time", side_effect=[1000.0, 1001.0]):
        updater = ThrottledProgressUpdater(mock_msg, stage="Uploading 📤", interval=0.0)
        # Simulate first chunk (10 MB)
        await updater.async_upload_hook(current=10 * 1024 * 1024, total=100 * 1024 * 1024)
        # Simulate second chunk (30 MB, delta = 20 MB over 1.0s)
        await updater.async_upload_hook(current=30 * 1024 * 1024, total=100 * 1024 * 1024)

    assert mock_msg.edit_text.call_count >= 1
    call_args = mock_msg.edit_text.call_args[0][0]
    assert "Uploading" in call_args
    assert "Progress:" in call_args
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


@pytest.mark.asyncio
async def test_logs_command_replies_text_when_short():
    from unittest.mock import AsyncMock, MagicMock
    from bot import logs_command, memory_log_handler

    memory_log_handler.buffer.clear()
    memory_log_handler.buffer.append("2026-10-10 21:00:00 [INFO] test: Server initialized")
    memory_log_handler.buffer.append("2026-10-10 21:01:00 [INFO] test: Bot active")

    mock_msg = MagicMock()
    mock_msg.chat = MagicMock(id=123)
    mock_msg.reply_text = AsyncMock()

    await logs_command(MagicMock(), mock_msg)
    mock_msg.reply_text.assert_awaited_once()
    assert "Server initialized" in mock_msg.reply_text.call_args[0][0]


@pytest.mark.asyncio
async def test_speedtest_command_execution():
    from unittest.mock import AsyncMock, MagicMock, patch
    from bot import speedtest_command

    mock_msg = MagicMock()
    mock_msg.chat = MagicMock(id=123)
    status_msg = MagicMock()
    status_msg.edit_text = AsyncMock()
    mock_msg.reply_text = AsyncMock(return_value=status_msg)

    # Mock aiohttp session to return synthetic speed numbers without outbound network calls
    class MockResp:
        def __init__(self, data=b"x" * 1024):
            self._data = data
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        async def read(self):
            return self._data

    class MockSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        def get(self, *args, **kwargs):
            return MockResp(b"x" * (1024 * 1024))
        def post(self, *args, **kwargs):
            return MockResp(b"ok")

    with patch("aiohttp.ClientSession", return_value=MockSession()):
        await speedtest_command(MagicMock(), mock_msg)

    assert status_msg.edit_text.call_count >= 1
    last_text = status_msg.edit_text.call_args[0][0]
    assert "SERVER SPEED TEST RESULTS" in last_text
    assert "Download:" in last_text
    assert "Upload:" in last_text
    assert "MB/s" in last_text
    assert "Mbps" in last_text
