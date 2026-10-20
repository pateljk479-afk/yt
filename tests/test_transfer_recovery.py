import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pyrogram import raw
from pyrogram.errors import FloodWait
from pyrogram.parser import Parser

import bot
import config
import streaming
import transfer_runtime
from transfer_runtime import TransferStageError


@pytest.fixture(autouse=True)
def fast_deadlines(monkeypatch):
    monkeypatch.setattr(config, "TRANSFER_STAGE_TIMEOUT", 0.02)
    monkeypatch.setattr(config, "TRANSFER_CLEANUP_TIMEOUT", 0.02)
    monkeypatch.setattr(config, "TRANSFER_STATUS_TIMEOUT", 0.02)
    monkeypatch.setattr(config, "THUMBNAIL_TIMEOUT", 0.02)
    monkeypatch.setattr(transfer_runtime, "RETRY_BACKOFF", 0)


def publisher():
    return SimpleNamespace(parser=Parser(None), rnd_id=Mock(return_value=42),
                           resolve_peer=AsyncMock(return_value=raw.types.InputPeerSelf()),
                           invoke=AsyncMock(), save_file=AsyncMock())


async def publish(client):
    return await streaming._publish_video_part(
        client, 1, streaming.StreamPlan({"title": "Course"}, [{"height": 1080}], 100),
        raw.types.InputFileBig(id=99, parts=1, name="course.mp4"), "course.mp4", 100, 20)


@pytest.mark.asyncio
async def test_hung_publish_retries_twice_with_same_uploaded_file_and_random_id():
    client = publisher()
    requests = []

    async def send(request, **kwargs):
        requests.append(request)
        assert kwargs["retries"] == 0 and kwargs["sleep_threshold"] == 0
        if len(requests) < 3:
            await asyncio.Event().wait()
        return "delivered"

    client.invoke.side_effect = send
    assert await asyncio.wait_for(publish(client), 1) == "delivered"
    assert len(requests) == 3
    assert all(request is requests[0] for request in requests)
    assert requests[0].random_id == 42 and requests[0].media.file.id == 99
    client.save_file.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_exhaustion_is_bounded_and_not_retried_as_a_whole_video():
    client = publisher()
    async def hang(*args, **kwargs):
        await asyncio.Event().wait()
    client.invoke.side_effect = hang
    runs = []
    async def attempt(checkpoint):
        runs.append(checkpoint)
        return await publish(client)
    with pytest.raises(TransferStageError, match="3 attempt"):
        await asyncio.wait_for(streaming._retry_video(attempt, streaming.TransferStats()), 1)
    assert client.invoke.await_count == 3 and len(runs) == 1


@pytest.mark.asyncio
async def test_cancelled_publish_is_not_retried():
    client = publisher()
    client.invoke.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await publish(client)
    assert client.invoke.await_count == 1


@pytest.mark.asyncio
async def test_long_floodwait_is_reported_instead_of_blocking_playlist():
    client = publisher()
    client.invoke.side_effect = FloodWait(3600)
    with pytest.raises(TransferStageError, match="3600"):
        await asyncio.wait_for(publish(client), 1)
    assert client.invoke.await_count == 1


@pytest.mark.asyncio
async def test_hung_thumbnail_is_optional_and_publication_continues(monkeypatch):
    client = publisher()
    client.invoke.return_value = "delivered"
    async def stuck(*args):
        await asyncio.Event().wait()
    monkeypatch.setattr(streaming, "_prepare_thumbnail", stuck)
    assert await asyncio.wait_for(publish(client), 1) == "delivered"
    assert client.invoke.call_args.args[0].media.thumb is None


@pytest.mark.asyncio
async def test_hung_session_cleanup_closes_transport_and_releases_caller():
    async def stuck():
        await asyncio.Event().wait()
    session = SimpleNamespace(stop=stuck, connection=SimpleNamespace(close=AsyncMock()))
    await asyncio.wait_for(transfer_runtime.close_media_sessions([session], "test"), 1)
    session.connection.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_dead_chunk_has_exactly_two_retries_and_identical_bytes():
    calls = []
    async def stuck(rpc, **kwargs):
        calls.append(rpc)
        await asyncio.Event().wait()
    session = SimpleNamespace(invoke=stuck)
    rpc = raw.functions.upload.SaveBigFilePart(file_id=99, file_part=4, file_total_parts=-1, bytes=b"video")
    with patch.object(streaming.asyncio, "sleep", new=AsyncMock()):
        with pytest.raises(TransferStageError, match="two retries"):
            await asyncio.wait_for(streaming._invoke_part(session, rpc), 1)
    assert len(calls) == 3 and all(call is rpc for call in calls)


@pytest.mark.asyncio
async def test_status_update_timeout_does_not_fail_successful_transfer():
    status = AsyncMock()
    async def stuck(*args, **kwargs):
        await asyncio.Event().wait()
    status.edit_text.side_effect = stuck
    plan = streaming.StreamPlan({"title": "Course"}, [{"height": 1080}], 100)
    with patch.object(bot, "stream_video", new=AsyncMock()) as send:
        assert await asyncio.wait_for(bot.process_single_download(
            AsyncMock(), status, 1, "url", "1080", prepared_result=plan), 1)
    send.assert_awaited_once()


@pytest.mark.asyncio
async def test_exhausted_transfer_releases_playlist_for_next_video():
    entries = [{"id": "first", "title": "First"}, {"id": "second", "title": "Second"}]
    plans = [streaming.StreamPlan({"title": entry["title"]}, [{"height": 720}], 100) for entry in entries]
    async def send(client, chat_id, plan, progress, stats):
        if plan.info["title"] == "First":
            raise TransferStageError("Publish failed after two retries")
    with patch.object(bot, "prepare_download", new=AsyncMock(side_effect=plans)), \
         patch.object(bot, "stream_video", new=AsyncMock(side_effect=send)) as transfer:
        await asyncio.wait_for(bot.process_playlist_downloads(AsyncMock(), AsyncMock(), 1, entries, "720"), 2)
    assert transfer.await_count == 2
    assert not bot.upload_semaphore.locked() and not bot.download_semaphore.locked()


@pytest.mark.asyncio
async def test_source_recovery_has_only_two_retries():
    run = AsyncMock(side_effect=OSError("source offline"))
    with patch.object(streaming.asyncio, "sleep", new=AsyncMock()):
        with pytest.raises(OSError, match="source offline"):
            await streaming._retry_video(run, streaming.TransferStats())
    assert run.await_count == 3


@pytest.mark.asyncio
async def test_file_session_startup_failure_finishes_all_starts_before_cleanup(tmp_path, monkeypatch):
    sessions = []
    class StartupSession:
        def __init__(self, *args, **kwargs):
            self.index = len(sessions)
            self.finished = False
            sessions.append(self)
        async def start(self):
            if self.index == 0:
                self.finished = True
                raise OSError("startup failed")
            await asyncio.sleep(0)
            self.finished = True
        async def stop(self):
            assert self.finished
    monkeypatch.setattr(bot._sf_mod, "Session", StartupSession)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    client = SimpleNamespace(save_file_semaphore=asyncio.Semaphore(1),
                             storage=SimpleNamespace(dc_id=AsyncMock(return_value=2),
                                                     auth_key=AsyncMock(return_value=b"auth"),
                                                     test_mode=AsyncMock(return_value=False)),
                             rnd_id=Mock(return_value=12))
    path = tmp_path / "upload.bin"
    path.write_bytes(b"v" * (11 * 1024 * 1024))
    with pytest.raises(OSError, match="startup failed"):
        await bot._fast_save_file(client, str(path))
    assert len(sessions) == 2 and all(session.finished for session in sessions)
