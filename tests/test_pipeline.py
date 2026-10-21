import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
import bot
import config


def result(name):
    return SimpleNamespace(temp_dir=name)


@pytest.mark.asyncio
async def test_playlist_overlaps_next_download_with_upload_and_preserves_order():
    next_started = asyncio.Event()
    release_upload = asyncio.Event()
    uploaded = []
    downloads = []

    async def prepare(message, url, quality):
        downloads.append(url)
        if url.endswith("two"):
            next_started.set()
        return result(url)

    async def upload(*args, prepared_result):
        uploaded.append(args[3])
        if len(uploaded) == 1:
            await release_upload.wait()
        return True

    client = SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock()))
    status = AsyncMock()
    entries = [{"id": name} for name in ("one", "two", "three")]
    with patch.object(config, "PREFETCH_PLAYLIST", True), \
         patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", side_effect=upload), \
         patch.object(bot, "safe_cleanup") as cleanup:
        task = asyncio.create_task(bot.process_playlist_downloads(client, status, 1, entries, "720"))
        await asyncio.wait_for(next_started.wait(), 2)
        assert len(uploaded) == 1
        assert len(downloads) == 2  # No unbounded third-file prefetch.
        release_upload.set()
        await asyncio.wait_for(task, 2)
    assert uploaded == downloads
    assert cleanup.call_count == 3
    assert "3 / 3" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
async def test_playlist_sequential_processing_when_prefetch_disabled():
    uploaded = []
    downloads = []
    in_flight_operations = 0
    max_concurrent = 0

    async def prepare(message, url, quality):
        nonlocal in_flight_operations, max_concurrent
        in_flight_operations += 1
        max_concurrent = max(max_concurrent, in_flight_operations)
        downloads.append(url)
        await asyncio.sleep(0.01)
        in_flight_operations -= 1
        return result(url)

    async def upload(*args, prepared_result):
        nonlocal in_flight_operations, max_concurrent
        in_flight_operations += 1
        max_concurrent = max(max_concurrent, in_flight_operations)
        uploaded.append(args[3])
        await asyncio.sleep(0.01)
        in_flight_operations -= 1
        return True

    client = SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock()))
    status = AsyncMock()
    entries = [{"id": name} for name in ("one", "two", "three")]
    with patch.object(config, "PREFETCH_PLAYLIST", False), \
         patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", side_effect=upload), \
         patch.object(bot, "safe_cleanup") as cleanup:
        await bot.process_playlist_downloads(client, status, 1, entries, "720")

    assert uploaded == downloads
    assert cleanup.call_count == 3
    assert max_concurrent == 1  # Never overlaps download and upload!
    assert "3 / 3" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
async def test_failed_download_and_upload_do_not_stop_playlist():
    async def prepare(message, url, quality):
        if url.endswith("bad"):
            raise ValueError("failed download")
        return result(url)

    client = SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock()))
    status = AsyncMock()
    with patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", new=AsyncMock(side_effect=[False, True])) as upload, \
         patch.object(bot, "safe_cleanup") as cleanup:
        await bot.process_playlist_downloads(client, status, 1,
                                             [{"id": "bad"}, {"id": "two"}, {"id": "three"}], "audio")
    assert upload.await_count == 2
    assert cleanup.call_count == 2
    assert "1 / 3" in status.edit_text.call_args.args[0]
    assert "Failed items:</b> 2" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
async def test_cancelled_playlist_drains_and_cleans_prefetch():
    started = asyncio.Event()
    release = asyncio.Event()

    async def prepare(*args):
        started.set()
        await release.wait()
        return result("prefetch")

    client = SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock()))
    with patch.object(config, "PREFETCH_PLAYLIST", True), \
         patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "safe_cleanup") as cleanup:
        task = asyncio.create_task(bot.process_playlist_downloads(client, AsyncMock(), 1, [{"id": "one"}], "720"))
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    cleanup.assert_called_once_with("prefetch")


@pytest.mark.asyncio
async def test_empty_playlist_finishes():
    status = AsyncMock()
    await bot.process_playlist_downloads(AsyncMock(), status, 1, [], "720")
    assert "0 / 0" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
async def test_upload_progress_starts_immediately_then_throttles_even_at_completion():
    status = AsyncMock()
    updater = bot.ThrottledProgressUpdater(status, interval=5)
    with patch("bot.time.time", side_effect=[100, 102, 105, 106]):
        await updater.async_upload_hook(0, 100)
        await updater.async_upload_hook(20, 100)
        await updater.async_upload_hook(70, 100)
        await updater.async_upload_hook(100, 100)
    assert status.edit_text.await_count == 2
    assert "0.0%" in status.edit_text.call_args_list[0].args[0]
    assert "70.0%" in status.edit_text.call_args_list[1].args[0]
    assert config.PROGRESS_UPDATE_INTERVAL >= 5


@pytest.mark.asyncio
@pytest.mark.parametrize("is_audio", [False, True])
@pytest.mark.parametrize("valid", [False, True])
async def test_single_download_retains_verification_upload_and_cleanup(is_audio, valid):
    media = SimpleNamespace(temp_dir="temp", file_path="video.mp4", is_audio=is_audio,
                            thumbnail_path=None, title="Video", duration=20,
                            file_size=10000, width=1280, height=720)
    client = AsyncMock()
    with patch.object(bot, "upload_semaphore", asyncio.Semaphore(1)), \
         patch.object(bot, "prepare_download", new=AsyncMock(return_value=media)) as prepare, \
         patch.object(bot, "verify_media_integrity", return_value=valid) as verify, \
         patch.object(bot, "safe_cleanup") as cleanup:
        assert await bot.process_single_download(client, AsyncMock(), 1, "url", "720") is valid
    prepare.assert_awaited_once()
    verify.assert_called_once_with("video.mp4", is_audio=is_audio)
    if valid:
        (client.send_audio if is_audio else client.send_video).assert_awaited_once()
    else:
        client.send_audio.assert_not_awaited()
        client.send_video.assert_not_awaited()
    cleanup.assert_called_once_with("temp")


@pytest.mark.asyncio
async def test_large_disk_fallback_uses_splitter_and_resets_part_progress(monkeypatch):
    media = SimpleNamespace(temp_dir="temp", file_path="video.mp4", is_audio=False,
                            title="Course", duration=20, width=1280, height=720,
                            file_size=bot.video_part_limit() + 1)
    status, client = AsyncMock(), AsyncMock()

    async def split(client, chat, result, progress, stats):
        for index in (1, 2):
            stats.part_index, stats.part_count = index, 2
            await progress(0, 100)
            await progress(50, 100)

    with patch.object(bot, "prepare_download", new=AsyncMock(return_value=media)), \
         patch.object(bot, "stream_local_video", new=AsyncMock(side_effect=split)) as sender, \
         patch.object(bot, "safe_cleanup") as cleanup, \
         patch("bot.time.time", side_effect=[100, 105, 110, 115]):
        assert await bot.process_single_download(client, status, 1, "url", "1080")
    sender.assert_awaited_once()
    client.send_video.assert_not_awaited()
    cleanup.assert_called_once_with("temp")
    updates = [call.args[0] for call in status.edit_text.call_args_list]
    assert any("Part 1" in text for text in updates)
    assert any("Part 2" in text for text in updates)
    assert all("-10" not in text for text in updates)
