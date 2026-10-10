from types import SimpleNamespace
from unittest.mock import patch

import pytest
import config
import downloader
import storage


def test_startup_cleanup_only_removes_owned_folders(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    owned = tmp_path / "yt_dl_abandoned"
    owned.mkdir()
    (owned / "video.part").write_bytes(b"partial")
    protected = tmp_path / "keep"
    protected.mkdir()
    cookie = tmp_path / "cookies.txt"
    cookie.write_text("keep")
    assert storage.cleanup_stale_downloads() == 1
    assert not owned.exists()
    assert protected.exists() and cookie.exists()


def test_cleanup_refuses_outside_download_directory(tmp_path, monkeypatch):
    root = tmp_path / "downloads"
    root.mkdir()
    outside = tmp_path / "yt_dl_protected"
    outside.mkdir()
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(root))
    with pytest.raises(ValueError):
        storage.cleanup_download_folder(str(outside))
    assert outside.exists()


def test_failed_download_cleans_partial_files(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))

    def fail(url, quality, hook, height, temp_dir):
        from pathlib import Path
        (Path(temp_dir) / "partial.part").write_bytes(b"failed")
        raise RuntimeError("download failed")

    with patch.object(downloader, "_download_media_impl", side_effect=fail):
        with pytest.raises(RuntimeError, match="download failed"):
            downloader.download_media("url", "720")
    assert list(tmp_path.iterdir()) == []


def test_success_keeps_files_until_uploaded(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))

    def succeed(url, quality, hook, height, temp_dir):
        return SimpleNamespace(temp_dir=temp_dir)

    with patch.object(downloader, "_download_media_impl", side_effect=succeed):
        result = downloader.download_media("url", "720")
    assert len(list(tmp_path.iterdir())) == 1
    storage.cleanup_download_folder(result.temp_dir)


def test_budget_and_merge_headroom(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 100)
    monkeypatch.setattr(config, "MIN_FREE_DISK_BYTES", 10)
    (tmp_path / "data").write_bytes(b"x" * 80)
    with patch("storage.shutil.disk_usage", return_value=SimpleNamespace(free=1000)):
        storage.ensure_download_space()
        with pytest.raises(RuntimeError, match="storage"):
            storage.ensure_download_space(extra=30)
    with patch("storage.shutil.disk_usage", return_value=SimpleNamespace(free=5)):
        with pytest.raises(RuntimeError, match="storage"):
            storage.ensure_download_space()
