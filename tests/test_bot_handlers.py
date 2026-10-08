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
