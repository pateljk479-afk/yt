import os
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

        # 4. Real valid MP4 container
        valid_file = os.path.join(tmpdir, "valid.mp4")
        import subprocess
        subprocess.run(
            ["ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=blue:s=320x240:d=1", "-c:v", "libx264", valid_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
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
