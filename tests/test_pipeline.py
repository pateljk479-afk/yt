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
    with patch.object(bot, "prepare_download", side_effect=prepare), \
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
    with patch.object(bot, "prepare_download", side_effect=prepare), \
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
async def test_upload_progress_waits_five_seconds_even_at_completion():
    status = AsyncMock()
    updater = bot.ThrottledProgressUpdater(status, interval=5)
    with patch("bot.time.time", side_effect=[100, 102, 105, 106]):
        await updater.async_upload_hook(0, 100)
        await updater.async_upload_hook(20, 100)
        await updater.async_upload_hook(70, 100)
        await updater.async_upload_hook(100, 100)
    assert status.edit_text.await_count == 1
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
