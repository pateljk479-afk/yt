import pytest
from downloader import (
    sanitize_filename,
    estimate_audio_size,
    analyze_video_qualities,
    build_ydl_options,
    RESOLUTION_TIERS,
)
import config


def test_sanitize_filename():
    raw_name = 'My: Cool? "Video" / With <Illegal> *Chars* | & More'
    sanitized = sanitize_filename(raw_name)
    assert ":" not in sanitized
    assert "?" not in sanitized
    assert '"' not in sanitized
    assert "/" not in sanitized
    assert "<" not in sanitized
    assert ">" not in sanitized
    assert "*" not in sanitized
    assert "|" not in sanitized
    assert "My_ Cool_ _Video_ _ With _Illegal_ _Chars_ _ & More" == sanitized


def test_sanitize_filename_edge_cases():
    assert sanitize_filename("") == "video"
    assert sanitize_filename(None) == "video"
    long_name = "a" * 300
    assert len(sanitize_filename(long_name)) == 200


def test_estimate_audio_size():
    formats = [
        {"vcodec": "none", "acodec": "mp4a.4", "filesize": 5000000},
        {"vcodec": "avc1", "acodec": "mp4a.4", "filesize": 50000000},
    ]
    size = estimate_audio_size(formats, duration=100)
    assert size == 5000000

    # Fallback to bitrate calculation
    formats_no_size = [
        {"vcodec": "none", "acodec": "opus", "abr": 128},
    ]
    size_calc = estimate_audio_size(formats_no_size, duration=60)
    assert size_calc == int((128 * 1000 / 8) * 60)


def test_analyze_video_qualities_2gb_limit_degradation():
    """
    Simulates the exact user requirement:
    4K is available but file size is 2.3 GB (> 2 GB).
    2K (1440p) is available with 1.6 GB (<= 2 GB).
    1080p is available with 850 MB.
    Expectation:
    - 4K is EXCLUDED and NOT shown in available qualities.
    - Max / High Quality is chosen as 2K (1440p).
    """
    # 2.3 GB = 2.3 * 1024 * 1024 * 1024 = 2,469,606,195 bytes
    size_4k = int(2.3 * 1024 * 1024 * 1024)
    # 1.6 GB = 1.6 * 1024 * 1024 * 1024 = 1,717,986,918 bytes
    size_2k = int(1.6 * 1024 * 1024 * 1024)
    # 850 MB
    size_1080p = 850 * 1024 * 1024

    mock_info = {
        "title": "4K Ultra HD Nature Video",
        "id": "mock4k123",
        "duration": 600,
        "thumbnail": "https://example.com/thumb.jpg",
        "formats": [
            # Audio format
            {"vcodec": "none", "acodec": "mp4a.4", "filesize": 20 * 1024 * 1024},
            # 4K format (2160p)
            {"height": 2160, "vcodec": "vp9", "acodec": "none", "filesize": size_4k},
            # 2K format (1440p)
            {"height": 1440, "vcodec": "vp9", "acodec": "none", "filesize": size_2k},
            # 1080p format
            {"height": 1080, "vcodec": "avc1", "acodec": "none", "filesize": size_1080p},
            # 720p format
            {"height": 720, "vcodec": "avc1", "acodec": "none", "filesize": 400 * 1024 * 1024},
        ],
    }

    analysis = analyze_video_qualities(mock_info)

    # 1. 4K MUST be in excluded qualities
    assert len(analysis.excluded_qualities) >= 1
    excluded_labels = [label for label, _ in analysis.excluded_qualities]
    assert any("4K" in l for l in excluded_labels)

    # 2. 4K MUST NOT be in available qualities
    available_labels = [q.label for q in analysis.available_qualities]
    assert not any("4K" in l for l in available_labels)

    # 3. Max quality MUST be 2K (1440p)
    assert analysis.max_quality is not None
    assert analysis.max_quality.height == 1440
    assert "2K" in analysis.max_quality.label
    assert analysis.max_quality.is_max is True

    # 4. 2K, 1080p, 720p must be present in available qualities
    heights = [q.height for q in analysis.available_qualities]
    assert 1440 in heights
    assert 1080 in heights
    assert 720 in heights


def test_analyze_video_qualities_under_limit():
    """Short video where 4K is under 2GB limit should retain 4K as Max Quality."""
    mock_info = {
        "title": "Short 4K Clip",
        "id": "short4k",
        "duration": 30,
        "thumbnail": "https://example.com/thumb.jpg",
        "formats": [
            {"vcodec": "none", "acodec": "mp4a.4", "filesize": 2 * 1024 * 1024},
            {"height": 2160, "vcodec": "vp9", "acodec": "none", "filesize": 80 * 1024 * 1024},
            {"height": 1080, "vcodec": "avc1", "acodec": "none", "filesize": 25 * 1024 * 1024},
        ],
    }

    analysis = analyze_video_qualities(mock_info)
    assert len(analysis.excluded_qualities) == 0
    assert analysis.max_quality is not None
    assert analysis.max_quality.height == 2160
    assert "4K" in analysis.max_quality.label


def test_build_ydl_options():
    opts = build_ydl_options("/tmp/test", "bv*[height<=1080]+ba/b", is_audio=False)
    assert opts["format"] == "bv*[height<=1080]+ba/b"
    assert opts["merge_output_format"] == "mp4"
    assert opts["concurrent_fragment_downloads"] == config.CONCURRENT_FRAGMENT_DOWNLOADS
    assert opts["writethumbnail"] is True
    assert opts["postprocessor_args"] == {"ffmpeg": ["-movflags", "+faststart"]}
    assert "extractor_args" in opts
    assert "android" in opts["extractor_args"]["youtube"]["player_client"]


def test_download_media_quality_degradation_logic():
    """Verify format selection logic degrades when requested height exceeds 2 GB."""
    from unittest.mock import patch, MagicMock
    from downloader import download_media

    size_4k = int(2.4 * 1024 * 1024 * 1024)  # 2.4 GB (> 2GB)
    size_2k = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB (<= 2GB)

    mock_info = {
        "title": "Super 4K Nature",
        "id": "mock4k",
        "duration": 600,
        "thumbnail": "https://example.com/thumb.jpg",
        "formats": [
            {"vcodec": "none", "acodec": "mp4a.4", "filesize": 20 * 1024 * 1024},
            {"height": 2160, "vcodec": "vp9", "acodec": "none", "filesize": size_4k},
            {"height": 1440, "vcodec": "vp9", "acodec": "none", "filesize": size_2k},
            {"height": 1080, "vcodec": "avc1", "acodec": "none", "filesize": 700 * 1024 * 1024},
        ],
    }

    # Patch extract_info to return mock_info
    with patch("downloader.extract_info", return_value=mock_info), \
         patch("yt_dlp.YoutubeDL") as mock_ydl_cls, \
         patch("os.listdir") as mock_listdir, \
         patch("os.path.exists", return_value=True), \
         patch("os.path.getsize", return_value=1500 * 1024 * 1024):

        mock_ydl_instance = MagicMock()
        mock_ydl_instance.extract_info.return_value = {
            "title": "Super 4K Nature",
            "duration": 600,
            "width": 2560,
            "height": 1440,
        }
        mock_ydl_cls.return_value.__enter__.return_value = mock_ydl_instance
        mock_listdir.return_value = ["Super 4K Nature.mp4", "Super 4K Nature.webp"]

        # Request 4K (2160), which exceeds 2 GB: should degrade to 1440 (2K)
        res = download_media("https://youtube.com/watch?v=mock4k", quality_key="2160")
        assert res.title == "Super 4K Nature"
        # Verify ydl was invoked with degraded format <= 1440
        ydl_call_args = mock_ydl_cls.call_args[0][0]
        assert "height<=1440" in ydl_call_args["format"]
