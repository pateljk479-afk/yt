import os
import shutil
import tempfile
from PIL import Image
from uploader import (
    human_readable_size,
    human_readable_time,
    format_progress_bar,
    verify_media_integrity,
    prepare_thumbnail,
    safe_cleanup,
)


def test_human_readable_size():
    assert human_readable_size(500) == "500 B"
    assert human_readable_size(1024) == "1.0 KB"
    assert human_readable_size(1024 * 1024) == "1.0 MB"
    assert human_readable_size(1500 * 1024 * 1024) == "1.46 GB"
    assert human_readable_size(2 * 1024 * 1024 * 1024) == "2.00 GB"


def test_human_readable_time():
    assert human_readable_time(0) == "00:00"
    assert human_readable_time(45) == "00:45"
    assert human_readable_time(125) == "02:05"
    assert human_readable_time(3665) == "01:01:05"


def test_format_progress_bar():
    text = format_progress_bar(
        current=500 * 1024 * 1024,
        total=1000 * 1024 * 1024,
        speed=15 * 1024 * 1024,
        eta=33,
        stage="Downloading",
    )
    assert "Downloading..." in text
    assert "50.0%" in text
    assert "500.0 MB / 1000.0 MB" in text
    assert "15.0 MB/s" in text
    assert "00:33" in text


def test_verify_media_integrity():
    with tempfile.TemporaryDirectory() as tmpdir:
        # 1. Non-existent file
        assert not verify_media_integrity(os.path.join(tmpdir, "missing.mp4"))

        # 2. Too small file (< 1024 bytes)
        small_file = os.path.join(tmpdir, "tiny.mp4")
        with open(small_file, "wb") as f:
            f.write(b"tiny")
        assert not verify_media_integrity(small_file)

        # 3. Corrupt file with invalid atoms should fail ffprobe verification
        corrupt_file = os.path.join(tmpdir, "corrupt.mp4")
        with open(corrupt_file, "wb") as f:
            f.write(b"\x00\x00\x00\x20ftypisom" + b"\x00" * 2040)
        assert not verify_media_integrity(corrupt_file)

        # 3b. File without ftyp atom should immediately fail
        no_ftyp_file = os.path.join(tmpdir, "no_ftyp.mp4")
        with open(no_ftyp_file, "wb") as f:
            f.write(b"\x00" * 2048)
        assert not verify_media_integrity(no_ftyp_file)

        # 4. Real valid MP4 container
        valid_file = os.path.join(tmpdir, "valid.mp4")
        if shutil.which("ffmpeg"):
            import subprocess
            subprocess.run(
                ["ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=blue:s=320x240:d=1", "-c:v", "libx264", valid_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            assert verify_media_integrity(valid_file)
        else:
            with open(valid_file, "wb") as f:
                f.write(b"\x00\x00\x00\x20ftypisom" + b"\x00" * 100 + b"moov" + b"\x00" * 2000)
            assert verify_media_integrity(valid_file)


def test_prepare_thumbnail():
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a test RGBA image (with transparency)
        png_path = os.path.join(tmpdir, "test.png")
        img = Image.new("RGBA", (800, 600), (255, 0, 0, 128))
        img.save(png_path, "PNG")

        # Convert to JPEG thumbnail
        jpg_thumb = prepare_thumbnail(png_path, tmpdir)
        assert jpg_thumb is not None
        assert os.path.exists(jpg_thumb)
        assert jpg_thumb.endswith(".jpg")

        # Verify output image properties
        with Image.open(jpg_thumb) as out_img:
            assert out_img.format == "JPEG"
            assert out_img.mode == "RGB"
            # Dimensions should be scaled down to max 320x320
            w, h = out_img.size
            assert w <= 320
            assert h <= 320


def test_safe_cleanup():
    tmpdir = tempfile.mkdtemp()
    subfile = os.path.join(tmpdir, "test.txt")
    with open(subfile, "w") as f:
        f.write("test")
    assert os.path.exists(tmpdir)
    safe_cleanup(tmpdir)
    assert not os.path.exists(tmpdir)


import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

@pytest.mark.asyncio
async def test_fast_save_file_multi_session():
    import bot
    from bot import _fast_save_file
    import pyrogram.methods.advanced.save_file as _sf_mod

    with tempfile.NamedTemporaryFile("wb", delete=False) as f:
        # Create 11 MB dummy file (exceeds 10MB -> triggers multi-session)
        f.write(b"x" * (11 * 1024 * 1024))
        tmp_path = f.name

    created_sessions = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            created_sessions.append(self)
            self.start = AsyncMock()
            self.stop = AsyncMock()
            self.invoke = AsyncMock(return_value=True)

    client = MagicMock()
    client.save_file_semaphore = asyncio.Semaphore(1)
    client.storage.dc_id = AsyncMock(return_value=5)
    client.storage.auth_key = AsyncMock(return_value=b"auth")
    client.storage.test_mode = AsyncMock(return_value=False)
    client.rnd_id = MagicMock(return_value=12345)
    client.loop = asyncio.get_running_loop()

    progress_calls = []
    async def mock_progress(current, total):
        progress_calls.append((current, total))

    try:
        with patch.object(_sf_mod, "Session", FakeSession):
            result = await _fast_save_file(client, tmp_path, progress=mock_progress)
            assert result is not None
            assert len(created_sessions) == 4
            assert all(s.start.called for s in created_sessions)
            assert all(s.stop.called for s in created_sessions)
            assert len(progress_calls) > 0
    finally:
        os.unlink(tmp_path)

