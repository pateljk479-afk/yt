import asyncio
import math
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from aiohttp import web
from pyrogram import raw
from pyrogram.parser import Parser

import bot
import config
import streaming
from streaming import PART_SIZE, StreamPlan


@pytest.fixture(autouse=True)
def fixed_upload_window(monkeypatch):
    # Existing protocol tests exercise fixed windows; adaptive behavior has its
    # own deterministic controller tests and an integration test below.
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", False)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 1)
    # Splitter tests use tiny synthetic parts and must not depend on the free
    # space of the developer's Windows drive; production reads this value from
    # the host before staging each real attachment.
    monkeypatch.setattr(streaming.shutil, "disk_usage",
                        lambda _: SimpleNamespace(free=4 * 1024 * 1024 * 1024))


def metadata():
    return {"title": "A <good> video", "duration": 20, "formats": [
        {"url": "https://cdn.example/video", "protocol": "https", "ext": "mp4",
         "vcodec": "avc1", "acodec": "none", "height": 1080, "width": 1920,
         "tbr": 1000, "filesize": 1500 * 1024 * 1024},
        {"url": "https://cdn.example/audio", "protocol": "https", "ext": "m4a",
         "vcodec": "none", "acodec": "mp4a", "abr": 128, "filesize": 20 * 1024 * 1024},
        {"url": "https://cdn.example/small", "protocol": "https", "ext": "mp4",
         "vcodec": "avc1", "acodec": "mp4a", "height": 720, "width": 1280,
         "tbr": 500, "filesize": 300 * 1024 * 1024},
    ]}


def fake_client():
    return SimpleNamespace(
        rnd_id=Mock(side_effect=range(123, 200)),
        storage=SimpleNamespace(dc_id=AsyncMock(return_value=2),
                                auth_key=AsyncMock(return_value=b"auth"),
                                test_mode=AsyncMock(return_value=False)),
        parser=Parser(None), resolve_peer=AsyncMock(return_value=raw.types.InputPeerSelf()),
        invoke=AsyncMock(return_value=object()), save_file=AsyncMock())


@pytest.fixture
def telegram(monkeypatch):
    calls, sessions = [], []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.start = AsyncMock()
            self.stop = AsyncMock()
            self.invoke = AsyncMock(side_effect=self.save)
            sessions.append(self)

        async def save(self, rpc, **kwargs):
            calls.append(rpc)
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    return calls, sessions


def media_bytes(size):
    return b"\x00\x00\x00\x18ftypisom" + b"x" * (size - 12)


def test_large_selection_is_independent_of_disk_budget(monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 1)
    plan = streaming.select_streams(metadata(), "1080")
    assert len(plan.formats) == 2
    assert plan.estimated_size == 1520 * 1024 * 1024
    assert plan.temp_dir is None


def test_selection_preserves_quality_despite_attachment_limit(monkeypatch):
    monkeypatch.setattr(config, "MAX_FILE_SIZE", 1000 * 1024 * 1024)
    assert streaming.select_streams(metadata(), "max").formats[0]["height"] == 1080
    assert streaming.select_streams(metadata(), "720").formats[0]["height"] == 720
    with pytest.raises(streaming.UnsupportedStream):
        streaming.select_streams(metadata(), "480")


@pytest.mark.parametrize("update", [{"is_live": True}, {"formats": []},
    {"formats": [{**metadata()["formats"][0], "protocol": "http_dash_segments"}]}])
def test_unsupported_sources_use_explicit_fallback(update):
    with pytest.raises(streaming.UnsupportedStream):
        streaming.select_streams({**metadata(), **update}, "1080")


def test_ffmpeg_uses_direct_requests_and_stream_copy():
    plan = streaming.select_streams(metadata(), "1080")
    plan.info["http_headers"] = {"User-Agent": "test", "Proxy-Authorization": "secret"}
    cmd = streaming.ffmpeg_command(plan)
    assert cmd.count("-http_proxy") == 2
    assert all(cmd[i + 1] == "" for i, value in enumerate(cmd) if value == "-http_proxy")
    assert "secret" not in " ".join(cmd)
    assert "copy" in cmd and "pipe:1" == cmd[-1]
    assert "+frag_keyframe+empty_moov+default_base_moof" in cmd


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1030, PART_SIZE, PART_SIZE * 3 + 123, PART_SIZE * 4])
async def test_streamed_protocol_and_byte_integrity(size, telegram):
    calls, sessions = telegram
    reader = asyncio.StreamReader()
    payload = media_bytes(size)
    reader.feed_data(payload)
    reader.feed_eof()
    file, actual = await streaming.upload_stream(fake_client(), reader, "test.mp4", total_size=size)
    assert actual == size and file.parts == math.ceil(size / PART_SIZE)
    assert b"".join(rpc.bytes for rpc in sorted(calls, key=lambda rpc: rpc.file_part)) == payload
    assert all(isinstance(rpc, raw.functions.upload.SaveFilePart) for rpc in calls)
    assert all(rpc.bytes for rpc in calls)
    assert len(calls[-1].bytes) == (size % PART_SIZE or PART_SIZE)
    assert all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [12, 1030, PART_SIZE - 1, PART_SIZE,
                                   PART_SIZE + 1, PART_SIZE * 4, PART_SIZE * 24 + 37])
async def test_unknown_length_finalization_uses_nonempty_last_chunk_and_exact_count(size, telegram):
    calls, _ = telegram
    payload = media_bytes(size)
    reader = asyncio.StreamReader()
    reader.feed_data(payload)
    reader.feed_eof()
    result, actual = await streaming.upload_stream(fake_client(), reader, "live.mp4")
    ordered = sorted(calls, key=lambda rpc: rpc.file_part)
    assert isinstance(result, raw.types.InputFileBig)
    assert result.parts == math.ceil(size / PART_SIZE)
    assert actual == size and len(ordered) == result.parts
    assert all(isinstance(rpc, raw.functions.upload.SaveBigFilePart) and rpc.bytes for rpc in ordered)
    assert [rpc.file_total_parts for rpc in ordered] == [-1] * (len(ordered) - 1) + [len(ordered)]
    assert {rpc.file_id for rpc in ordered} == {result.id}
    assert b"".join(rpc.bytes for rpc in ordered) == payload


@pytest.mark.asyncio
async def test_unknown_length_terminal_rpc_waits_for_all_previous_sessions(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 4)
    active = 0
    acknowledgements = []
    async def invoke(rpc, **kwargs):
        nonlocal active
        if rpc.file_total_parts == -1:
            active += 1
            try:
                await asyncio.sleep(0.01 if rpc.file_part % 2 else 0.02)
                acknowledgements.append(rpc.file_part)
            finally:
                active -= 1
        else:
            assert active == 0
            assert sorted(acknowledgements) == list(range(rpc.file_total_parts - 1))
        return True
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 7))
    reader.feed_eof()
    result, size = await streaming.upload_stream(fake_client(), reader, "live.mp4")
    assert result.parts == 7 and size == PART_SIZE * 7


@pytest.mark.asyncio
async def test_live_part_sends_to_telegram_before_source_eof_and_reads_source_once(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    first_ack = asyncio.Event()
    calls = []
    async def invoke(rpc, **kwargs):
        if not calls:
            assert not source.finished
        if rpc.file_total_parts != -1:
            assert source.finished
        calls.append(rpc)
        first_ack.set()
        return True
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    payload = media_bytes(2 * 1024 * 1024)
    class Source:
        index = 1
        calls = 0
        finished = False
        async def readexactly(self, count):
            self.calls += 1
            if self.calls == 1:
                return payload
            assert self.calls == 2  # Never restart the source download.
            await asyncio.wait_for(first_ack.wait(), 2)
            self.finished = True
            raise asyncio.IncompleteReadError(b"tail", count)
    source = Source()
    result, size = await asyncio.wait_for(streaming._upload_live_part(fake_client(), source,
        "live.mp4", len(payload) + 4, None, streaming.TransferStats(), len(payload) + 4), 4)
    assert size == len(payload) + 4 and result.parts == 5
    assert b"".join(rpc.bytes for rpc in sorted(calls, key=lambda r: r.file_part)) == payload + b"tail"
    assert source.calls == 2 and list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["Telegram upload chunk failed after two retries",
                                    "Telegram rejected upload chunk: FILE_PARTS_INVALID"])
async def test_live_transport_exhaustion_recovers_without_redownloading_source(tmp_path, monkeypatch, telegram, failure):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    payload = media_bytes(5 * 1024 * 1024 + 123)
    reader = asyncio.StreamReader()
    reader.index = 1
    reader.feed_data(payload)
    reader.feed_eof()
    original = streaming.upload_stream
    attempts = []
    async def fail_live(client, source, filename, estimate, progress=None, stats=None, **kwargs):
        attempts.append(kwargs.get("total_size"))
        if kwargs.get("total_size") is None:
            await source.readexactly(PART_SIZE)
            raise streaming.TransferStageError(failure)
        assert kwargs["request_depth_limit"] >= 1
        assert kwargs["max_worker_limit"] == kwargs["worker_limit"]
        return await original(client, source, filename, estimate, progress, stats, **kwargs)
    monkeypatch.setattr(streaming, "upload_stream", fail_live)
    result, size = await streaming._upload_live_part(fake_client(), reader, "live.mp4", len(payload),
        None, streaming.TransferStats(), len(payload))
    assert attempts == [None, len(payload)]
    assert size == len(payload)
    calls = [rpc for rpc in telegram[0] if rpc.file_id == result.id]
    assert b"".join(rpc.bytes for rpc in sorted(calls, key=lambda r: r.file_part)) == payload
    assert list(tmp_path.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [PART_SIZE * 3, PART_SIZE * 3 + 1])
async def test_stream_terminal_retry_reuses_id_count_index_and_payload(monkeypatch, size):
    seen = []
    terminal_attempts = 0
    async def invoke(rpc, **kwargs):
        nonlocal terminal_attempts
        seen.append(rpc)
        if rpc.file_total_parts != -1:
            terminal_attempts += 1
            if terminal_attempts == 1:
                raise TimeoutError("final response lost")
        return True
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(size))
    reader.feed_eof()
    stats = streaming.TransferStats()
    file, actual = await streaming.upload_stream(fake_client(), reader, "live.mp4", stats=stats)
    terminal = [rpc for rpc in seen if rpc.file_total_parts != -1]
    assert len(terminal) == 2 and terminal[0] is terminal[1]
    assert terminal[0].file_total_parts == file.parts == math.ceil(size / PART_SIZE)
    assert stats.uploaded_bytes == actual == size and stats.retry_events == 1


@pytest.mark.asyncio
async def test_permanent_authorization_error_does_not_buffer_or_retry_the_source(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    reader = asyncio.StreamReader()
    reader.index = 1
    error = streaming.TransferStageError("Telegram rejected upload chunk: AUTH_KEY_UNREGISTERED")
    with patch.object(streaming, "upload_stream", new=AsyncMock(side_effect=error)), \
         patch.object(streaming, "_upload_staged_part", new=AsyncMock()) as retry:
        with pytest.raises(streaming.TransferStageError, match="AUTH_KEY_UNREGISTERED"):
            await streaming._upload_live_part(fake_client(), reader, "live.mp4", 100,
                                              None, streaming.TransferStats(), 100)
    retry.assert_not_awaited()
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_partial_reads_are_assembled_before_upload(telegram):
    reader = asyncio.StreamReader()
    payload = media_bytes(PART_SIZE * 2 + 4)

    async def feed():
        for offset in range(0, len(payload), 10003):
            reader.feed_data(payload[offset:offset + 10003])
            await asyncio.sleep(0)
        reader.feed_eof()

    task = asyncio.create_task(feed())
    await streaming.upload_stream(fake_client(), reader, "test.mp4", total_size=len(payload))
    await task
    assert all(len(rpc.bytes) == PART_SIZE for rpc in telegram[0][:-1])


@pytest.mark.asyncio
async def test_limit_stops_upload_without_finalizing(monkeypatch, telegram):
    monkeypatch.setattr(config, "MAX_FILE_SIZE", PART_SIZE + 1)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE + 2))
    reader.feed_eof()
    with pytest.raises(ValueError, match="size limit"):
        await streaming.upload_stream(fake_client(), reader, "test.mp4", total_size=PART_SIZE + 2)
    assert all(rpc.bytes for rpc in telegram[0])
    assert all(s.stop.await_count == 1 for s in telegram[1])


@pytest.mark.asyncio
async def test_failed_chunk_interrupts_stalled_source(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=stopped, invoke=AsyncMock(side_effect=RuntimeError("chunk failed")))))
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE))  # Source now stalls, without EOF.
    with pytest.raises(RuntimeError, match="chunk failed"):
        await asyncio.wait_for(streaming.upload_stream(fake_client(), reader, "test.mp4", total_size=PART_SIZE), 2)
    assert stopped.await_count == config.UPLOAD_WORKERS


@pytest.mark.asyncio
async def test_slow_telegram_bounds_prefetch_and_cancellation_cleans_sessions(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "STREAM_BUFFER_BYTES", PART_SIZE * 4)
    release = asyncio.Event()
    stopped = AsyncMock()

    async def invoke(rpc, **kwargs):
        await release.wait()
        return True

    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=stopped, invoke=invoke)))
    reads = 0

    class EndlessReader:
        async def readexactly(self, size):
            nonlocal reads
            reads += 1
            return media_bytes(size)

    task = asyncio.create_task(streaming.upload_stream(fake_client(), EndlessReader(), "test.mp4",
                                                        total_size=PART_SIZE * 1000))
    for _ in range(200):
        await asyncio.sleep(0)
    try:
        # 4 queued chunks + one ordered request on each of 2 Sessions + one
        # producer read.  The bound proves Telegram backpressure reaches the
        # source without keeping an unbounded copy of the file in memory.
        assert 7 <= reads <= 8
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert stopped.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [TimeoutError(), asyncio.TimeoutError()])
async def test_chunk_retry_keeps_same_part_and_bytes(error):
    original = streaming._invoke_part
    # Fail one final-part request, then allow the identical request to succeed.
    with patch.object(streaming.asyncio, "sleep", new=AsyncMock()):
        session = SimpleNamespace(invoke=AsyncMock(side_effect=[error, True]))
        rpc = raw.functions.upload.SaveBigFilePart(file_id=1, file_part=0, file_total_parts=1, bytes=b"data")
        await original(session, rpc)
        assert session.invoke.call_args_list[0] == session.invoke.call_args_list[1]


@pytest.mark.asyncio
async def test_1_5_gib_stream_with_tiny_disk_budget(monkeypatch):
    """Send a synthetic large stream lazily, without allocating a large file."""
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 1024)
    size = 1536 * 1024 * 1024
    parts_sent, bytes_sent, rpcs = 0, 0, []
    payload = media_bytes(PART_SIZE)

    class LargeReader:
        remaining = size

        async def readexactly(self, count):
            if not self.remaining:
                raise asyncio.IncompleteReadError(b"", count)
            self.remaining -= count
            return payload

    async def invoke(rpc, **kwargs):
        nonlocal parts_sent, bytes_sent
        parts_sent += 1
        bytes_sent += len(rpc.bytes)
        rpcs.append(rpc)
        return True

    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    file, total = await streaming.upload_stream(fake_client(), LargeReader(), "large.mp4", total_size=size)
    assert total == bytes_sent == size
    assert file.parts == size // PART_SIZE
    assert parts_sent == file.parts
    # Every big-file request carries the same final count.  In particular,
    # there is no invalid -1 sentinel and no empty request at exact EOF.
    assert all(rpc.file_total_parts == file.parts and rpc.bytes for rpc in rpcs)


@pytest.mark.asyncio
async def test_startup_failure_closes_sessions(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(side_effect=OSError("offline")), stop=stopped)))
    with pytest.raises(streaming.TransferStageError, match="offline"):
        await streaming.upload_stream(fake_client(), asyncio.StreamReader(), "test.mp4", total_size=1)
    assert stopped.await_count == 1


@pytest.mark.asyncio
async def test_source_timeout_closes_sessions(monkeypatch, telegram):
    monkeypatch.setattr(config, "STREAM_READ_TIMEOUT", 0.01)
    with pytest.raises(TimeoutError):
        await streaming.upload_stream(fake_client(), asyncio.StreamReader(), "test.mp4", total_size=1)
    assert all(s.stop.await_count == 1 for s in telegram[1])


@pytest.mark.asyncio
async def test_fast_connection_keeps_working_while_another_is_slow(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 2)
    release, fast_completed = asyncio.Event(), asyncio.Event()
    counts, loads, peaks = [0, 0], [0, 0], [0, 0]
    sessions = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.index = len(sessions)
            sessions.append(self)
            self.start, self.stop = AsyncMock(), AsyncMock()

        async def invoke(self, rpc, **kwargs):
            i = self.index
            loads[i] += 1
            peaks[i] = max(peaks[i], loads[i])
            try:
                if i == 0:
                    await release.wait()
                else:
                    await asyncio.sleep(0)
                counts[i] += 1
                if i == 1 and counts[i] >= 8:
                    fast_completed.set()
                return True
            finally:
                loads[i] -= 1

    monkeypatch.setattr(streaming, "Session", FakeSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 12 + 7))
    reader.feed_eof()
    task = asyncio.create_task(streaming.upload_stream(fake_client(), reader, "test.mp4",
                                                        total_size=PART_SIZE * 12 + 7))
    try:
        await asyncio.wait_for(fast_completed.wait(), 2)
        assert counts[0] == 0 and counts[1] >= 8
        assert all(1 <= peak <= 2 for peak in peaks)
    finally:
        release.set()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_pipeline_keeps_multiple_requests_in_flight_with_bounded_depth(monkeypatch):
    """Hide RTT with concurrent replies; ordered_session tests cover wire order."""
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 8)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 8)
    active, peak = {}, {}
    full_window = asyncio.Event()

    class OrderedSession:
        def __init__(self, *args, **kwargs):
            self.index = len(active)
            active[self.index] = 0
            peak[self.index] = 0
            self.start, self.stop = AsyncMock(), AsyncMock()

        async def invoke(self, rpc, **kwargs):
            active[self.index] += 1
            peak[self.index] = max(peak[self.index], active[self.index])
            try:
                if sum(active.values()) == 16:
                    full_window.set()
                # Replies remain pending until the entire window has been
                # written; no wall-clock timing assumption is needed.
                await full_window.wait()
                return True
            finally:
                active[self.index] -= 1

    monkeypatch.setattr(streaming, "Session", OrderedSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 20 + 7))
    reader.feed_eof()
    stats = streaming.TransferStats()
    _, sent = await asyncio.wait_for(streaming.upload_stream(
        fake_client(), reader, "ordered.mp4", stats=stats, total_size=PART_SIZE * 20 + 7), 3)
    assert sent == PART_SIZE * 20 + 7
    assert stats.request_capacity == 16
    assert peak and all(1 < value <= 8 for value in peak.values())


@pytest.mark.asyncio
async def test_live_progress_cannot_block_or_fail_media_reads(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "PROGRESS_UPDATE_INTERVAL", 0.001)
    async def invoke(*args, **kwargs):
        await asyncio.sleep(0.01)
        return True
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    entered = asyncio.Event()
    async def blocked_progress(current, total):
        entered.set()
        await asyncio.Event().wait()

    payload = media_bytes(PART_SIZE * 2)
    reader = asyncio.StreamReader()
    reader.index = 1
    reader.feed_data(payload)
    reader.feed_eof()
    _, size = await asyncio.wait_for(streaming._upload_live_part(fake_client(), reader,
        "live.mp4", len(payload), blocked_progress, streaming.TransferStats(), len(payload)), 2)
    assert entered.is_set() and size == len(payload)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_staged_retry_reduces_pipeline_and_reuses_local_bytes(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 4)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 4)
    attempts = []
    async def upload(client, reader, filename, size, progress, stats, **kwargs):
        attempts.append((kwargs["worker_limit"], kwargs["request_depth_limit"],
                         await reader.readexactly(4)))
        if len(attempts) < 3:
            raise streaming.TransferStageError("transport failed")
        return "uploaded", size
    monkeypatch.setattr(streaming, "upload_stream", upload)
    monkeypatch.setattr(streaming.asyncio, "sleep", AsyncMock())
    path = tmp_path / "part.mp4"
    path.write_bytes(b"data")
    assert await streaming._upload_staged_part(None, path, "part.mp4", 4,
        None, streaming.TransferStats(), 4) == ("uploaded", 4)
    assert attempts == [(4, None, b"data"), (2, 2, b"data"), (1, 1, b"data")]


@pytest.mark.asyncio
async def test_reusing_attempt_stats_does_not_add_old_bytes_to_attachment_limit(telegram):
    stats = streaming.TransferStats(part_index=2, part_count=3, completed_parts=1,
                                    completed_bytes=1000)
    client = fake_client()
    for _ in range(2):
        reader = asyncio.StreamReader()
        reader.feed_data(media_bytes(4096))
        reader.feed_eof()
        _, size = await streaming.upload_stream(client, reader, "part.mp4", stats=stats,
                                                max_size=4096, total_size=4096)
        assert size == stats.source_bytes == stats.uploaded_bytes == 4096
        assert (stats.part_index, stats.completed_parts, stats.completed_bytes) == (2, 1, 1000)


@pytest.mark.asyncio
async def test_bad_mtproto_notification_is_retried_not_counted_as_an_ack(monkeypatch):
    session = SimpleNamespace(invoke=AsyncMock(side_effect=[
        raw.types.BadMsgNotification(bad_msg_id=1, bad_msg_seqno=1, error_code=33), True
    ]))
    retried = Mock()
    rpc = raw.functions.upload.SaveBigFilePart(file_id=1, file_part=0, file_total_parts=1, bytes=b"data")
    with patch.object(streaming.asyncio, "sleep", new=AsyncMock()):
        await streaming._invoke_part(session, rpc, retried, "test")
    assert session.invoke.await_count == 2
    retried.assert_called_once_with("network", 0)


@pytest.mark.asyncio
async def test_progress_edits_never_block_upload(monkeypatch, telegram):
    monkeypatch.setattr(config, "PROGRESS_UPDATE_INTERVAL", 0.001)
    editing = asyncio.Event()

    async def slow_progress(current, total):
        editing.set()
        await asyncio.Event().wait()

    class Reader:
        remaining = PART_SIZE * 10

        async def readexactly(self, count):
            await asyncio.sleep(0.002)
            if not self.remaining:
                raise asyncio.IncompleteReadError(b"", count)
            self.remaining -= count
            return media_bytes(count)

    _, size = await asyncio.wait_for(streaming.upload_stream(
        fake_client(), Reader(), "test.mp4", progress=slow_progress,
        total_size=PART_SIZE * 10), 2)
    assert editing.is_set() and size == PART_SIZE * 10


@pytest.mark.asyncio
async def test_speed_probe_uses_actual_parallel_pipeline(telegram):
    rate = await streaming.telegram_speed_probe(fake_client(), size=PART_SIZE * 24)
    assert rate > 0
    assert sum(len(r.bytes) for r in telegram[0]) == PART_SIZE * 24
    assert all(s.stop.await_count == 1 for s in telegram[1])


@pytest.mark.asyncio
async def test_adaptive_upload_activates_extra_connections_and_cleans_entire_pool(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 1)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 2)
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.03)
    expanded = asyncio.Event()
    sessions = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.index = len(sessions)
            sessions.append(self)
            self.start, self.stop = AsyncMock(), AsyncMock()

        async def invoke(self, rpc, **kwargs):
            if self.index == 1:
                expanded.set()
            await asyncio.sleep(0.02)
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 30))
    reader.feed_eof()
    stats = streaming.TransferStats()
    task = asyncio.create_task(streaming.upload_stream(
        fake_client(), reader, "test.mp4", stats=stats, total_size=PART_SIZE * 30))
    try:
        await asyncio.wait_for(expanded.wait(), 2)
        assert stats.active_connections == 2
        _, size = await asyncio.wait_for(task, 3)
        assert size == PART_SIZE * 30
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(sessions) == 2 and all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
async def test_ready_sockets_keep_sending_while_new_connection_handshakes(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.02)
    starting, release, continued, expanded = (asyncio.Event() for _ in range(4))
    sessions, calls = [], []
    sent_during_start = 0
    observations = []
    original = streaming.AdaptiveUploadWindow.observe

    def observe(control, *args, **kwargs):
        if starting.is_set() and not release.is_set():
            observations.append(kwargs["settled"])
        return original(control, *args, **kwargs)

    monkeypatch.setattr(streaming.AdaptiveUploadWindow, "observe", observe)

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.index = len(sessions)
            sessions.append(self)
            self.stop = AsyncMock()

        async def start(self):
            if self.index:
                starting.set()
                await release.wait()

        async def invoke(self, rpc, **kwargs):
            nonlocal sent_during_start
            calls.append(rpc.file_part)
            if self.index:
                expanded.set()
            elif starting.is_set() and not release.is_set():
                sent_during_start += 1
                if sent_during_start >= 12:
                    continued.set()
            await asyncio.sleep(0.01)
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 80 + 7))
    reader.feed_eof()
    stats = streaming.TransferStats()
    task = asyncio.create_task(streaming.upload_stream(
        fake_client(), reader, "growth.mp4", stats=stats))
    try:
        await asyncio.wait_for(starting.wait(), 2)
        await asyncio.wait_for(continued.wait(), 2)
        assert observations and not any(observations)
        assert stats.active_connections == stats.request_capacity == 1
        release.set()
        await asyncio.wait_for(expanded.wait(), 2)
        _, size = await asyncio.wait_for(task, 3)
        assert size == PART_SIZE * 80 + 7
        assert sorted(calls) == list(range(81))
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(sessions) == 2 and all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["failure", "cancel"])
async def test_background_growth_failure_and_cancellation_close_half_open_socket(monkeypatch, outcome):
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.05)
    starting, release = asyncio.Event(), asyncio.Event()
    sessions = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.index = len(sessions)
            sessions.append(self)
            self.stop = AsyncMock()

        async def start(self):
            if self.index:
                starting.set()
                await release.wait()
                raise OSError("handshake failed")

        async def invoke(self, rpc, **kwargs):
            await asyncio.sleep(0.01)
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 60))
    reader.feed_eof()
    task = asyncio.create_task(streaming.upload_stream(fake_client(), reader, "growth.mp4"))
    try:
        await asyncio.wait_for(starting.wait(), 2)
        if outcome == "cancel":
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            release.set()
            with pytest.raises(streaming.TransferStageError, match="session startup failed"):
                await asyncio.wait_for(task, 2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(sessions) == 2 and all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
async def test_fast_window_dispatches_64_chunks_before_waiting_for_first_ack(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 8)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 8)
    full, release = asyncio.Event(), asyncio.Event()
    sessions, calls = [], []
    active = peak = 0

    class FakeSession:
        def __init__(self, *args, **kwargs):
            sessions.append(self)
            self.start, self.stop = AsyncMock(), AsyncMock()
            self.active = self.peak = 0

        async def invoke(self, rpc, **kwargs):
            nonlocal active, peak
            calls.append(rpc)
            active += 1
            self.active += 1
            peak, self.peak = max(peak, active), max(self.peak, self.active)
            if active == 64:
                full.set()
            await release.wait()
            active -= 1
            self.active -= 1
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 70 + 7))
    reader.feed_eof()
    task = asyncio.create_task(streaming.upload_stream(fake_client(), reader, "fast.mp4"))
    try:
        await asyncio.wait_for(full.wait(), 2)
        assert len(calls) == 64 and all(s.peak == 8 for s in sessions)
        release.set()
        result, size = await asyncio.wait_for(task, 2)
        assert size == PART_SIZE * 70 + 7 and result.parts == 71
        assert peak == 64
        assert [rpc.file_part for rpc in calls] == list(range(71))
        assert all(rpc.file_total_parts == -1 for rpc in calls[:-1])
        assert calls[-1].file_total_parts == 71
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(sessions) == 8 and all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
async def test_transient_network_retry_does_not_collapse_upload_parallelism(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.02)
    failed = False

    async def invoke(rpc, **kwargs):
        nonlocal failed
        if rpc.file_part == 0 and not failed:
            failed = True
            raise OSError("temporary connection problem")
        await asyncio.sleep(0.01)
        return True

    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 20 + 7))
    reader.feed_eof()
    stats = streaming.TransferStats()
    await asyncio.wait_for(streaming.upload_stream(
        fake_client(), reader, "retry.mp4", stats=stats, total_size=PART_SIZE * 20 + 7), 3)
    assert stats.retry_events == 1 and stats.throttle_events == 0
    assert stats.active_connections == 2 and stats.request_capacity == 2


@pytest.mark.asyncio
async def test_real_floodwait_pauses_new_chunks_and_invalidates_warm_profile(monkeypatch):
    from pyrogram.errors import FloodWait
    import time
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.02)
    first_throttle = None
    later_chunks = []

    async def invoke(rpc, **kwargs):
        nonlocal first_throttle
        if rpc.file_part == 0 and first_throttle is None:
            first_throttle = time.monotonic()
            raise FloodWait(1)
        if rpc.file_part > 1:
            later_chunks.append(time.monotonic())
        await asyncio.sleep(0)
        return True

    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    client = fake_client()
    client._bot_upload_profile = "previous setting"
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 5 + 7))
    reader.feed_eof()
    stats = streaming.TransferStats()
    await asyncio.wait_for(streaming.upload_stream(
        client, reader, "flood.mp4", stats=stats, total_size=PART_SIZE * 5 + 7), 3)
    assert stats.throttle_events == 1 and stats.retry_events == 1
    assert client._bot_upload_profile is None
    assert later_chunks and min(later_chunks) - first_throttle >= 0.95


@pytest.mark.asyncio
async def test_consecutive_files_reuse_confirmed_fast_setting_without_leaking_sessions(monkeypatch):
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 1)
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 1)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 2)
    # Average enough ACKs per sample to avoid Windows timer granularity
    # turning a two-connection speedup into a noisy single-interval plateau.
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 0.15)
    sessions = []

    class FakeSession:
        def __init__(self, *args, **kwargs):
            self.start, self.stop = AsyncMock(), AsyncMock()
            sessions.append(self)

        async def invoke(self, rpc, **kwargs):
            await asyncio.sleep(0.02)
            return True

    monkeypatch.setattr(streaming, "Session", FakeSession)
    client = fake_client()
    first = asyncio.StreamReader()
    first.feed_data(media_bytes(PART_SIZE * 64))
    first.feed_eof()
    await asyncio.wait_for(streaming.upload_stream(client, first, "first.mp4", total_size=PART_SIZE * 64), 4)
    profile = client._bot_upload_profile
    assert profile.setting[0] == 2
    # Observe the warm start itself, without a later adaptive trial changing
    # the final window before the assertion.
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 10)
    second = asyncio.StreamReader()
    second.feed_data(media_bytes(PART_SIZE * 4))
    second.feed_eof()
    stats = streaming.TransferStats()
    await asyncio.wait_for(streaming.upload_stream(
        client, second, "second.mp4", stats=stats, total_size=PART_SIZE * 4), 2)
    assert (stats.active_connections, stats.request_capacity) == (profile.setting[0], profile.setting[0] * profile.setting[1])
    assert len(sessions) == 4 and all(s.stop.await_count == 1 for s in sessions)


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode,diagnostics", [
    (1, b"failure"), (0, b"out_time_us=1000000\nprogress=end\n")])
async def test_bad_or_truncated_ffmpeg_output_is_not_published(monkeypatch, telegram, returncode, diagnostics):
    monkeypatch.setattr(config, "STREAM_PARALLEL_DOWNLOADS", False)
    stdout, stderr = asyncio.StreamReader(), asyncio.StreamReader()
    stdout.feed_data(media_bytes(2048))
    stdout.feed_eof()
    stderr.feed_data(diagnostics)
    stderr.feed_eof()
    process = SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode,
                              wait=AsyncMock(return_value=returncode), kill=Mock())
    monkeypatch.setattr(streaming.asyncio, "create_subprocess_exec", AsyncMock(return_value=process))
    client = fake_client()
    with pytest.raises((ValueError, RuntimeError)):
        await streaming.stream_video(client, 1, streaming.select_streams(metadata(), "1080"))
    client.invoke.assert_not_awaited()
    assert all(s.stop.await_count == 1 for s in telegram[1])


@pytest.mark.asyncio
async def test_bot_uses_stream_plan_without_disk_verification(telegram):
    plan = streaming.select_streams(metadata(), "1080")
    with patch.object(bot, "prepare_download", new=AsyncMock(return_value=plan)), \
         patch.object(bot, "stream_video", new=AsyncMock()) as transfer, \
         patch.object(bot, "verify_media_integrity") as verify:
        assert await bot.process_single_download(fake_client(), AsyncMock(), 1, "url", "1080")
    transfer.assert_awaited_once()
    verify.assert_not_called()


@pytest.mark.asyncio
async def test_prepare_video_never_checks_disk(monkeypatch):
    plan = streaming.select_streams(metadata(), "1080")
    monkeypatch.setattr(config, "STREAM_VIDEO_UPLOADS", True)
    with patch.object(bot, "prepare_stream", return_value=plan), \
         patch.object(bot, "download_media") as disk:
        assert await bot.prepare_download(AsyncMock(), "url", "1080") is plan
    disk.assert_not_called()


@pytest.mark.asyncio
async def test_unsupported_video_and_audio_keep_disk_path(monkeypatch):
    monkeypatch.setattr(config, "STREAM_VIDEO_UPLOADS", True)
    with patch.object(bot, "prepare_stream", side_effect=streaming.UnsupportedStream("manifest")) as prepare, \
         patch.object(bot, "download_media", return_value="disk") as disk:
        assert await bot.prepare_download(AsyncMock(), "url", "1080") == "disk"
        assert await bot.prepare_download(AsyncMock(), "url", "audio") == "disk"
    assert prepare.call_count == 1 and disk.call_count == 2


def ffmpeg_executable():
    path = shutil.which("ffmpeg")
    if path:
        return path
    return pytest.importorskip("imageio_ffmpeg").get_ffmpeg_exe()


@pytest.mark.asyncio
async def test_real_ffmpeg_direct_http_mux_and_telegram_finalization(tmp_path, monkeypatch, telegram):
    """Exercise actual HTTP reads and streaming muxing; reconstruct ACKed chunks.

    No Telegram credentials/network required. Also decode the reconstructed file
    to catch an invalid fragmented MP4 or a missing audio/video track.
    """
    ffmpeg = ffmpeg_executable()
    video, audio = tmp_path / "v.mp4", tmp_path / "a.m4a"
    for path, source, codec in [(video, "testsrc=size=320x240:rate=25:duration=2", "libx264"),
                                (audio, "sine=frequency=400:duration=2", "aac")]:
        subprocess.run([ffmpeg, "-y", "-loglevel", "error", "-f", "lavfi", "-i", source,
                        "-c:v" if path == video else "-c:a", codec, str(path)], check=True, capture_output=True)
    requests = []

    async def serve(request):
        requests.append(request)
        return web.FileResponse(video if request.path == "/v" else audio)

    app = web.Application()
    app.router.add_get("/{name}", serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    info = metadata()
    info["duration"] = 2
    info["formats"][0]["url"] = f"http://127.0.0.1:{port}/v"
    info["formats"][1]["url"] = f"http://127.0.0.1:{port}/a"
    plan = streaming.select_streams(info, "1080")
    client = fake_client()
    monkeypatch.setattr(streaming.shutil, "which", lambda _: ffmpeg)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:1")
    monkeypatch.setenv("https_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 1)
    try:
        await asyncio.wait_for(streaming.stream_video(client, 1, plan), 20)
    finally:
        await runner.cleanup()
    assert {r.path for r in requests} == {"/v", "/a"}
    request = client.invoke.call_args.args[0]
    assert isinstance(request, raw.functions.messages.SendMedia)
    assert request.media.attributes[0].supports_streaming
    assert "A <good> video" in request.message
    reconstructed = b"".join(r.bytes for r in sorted(telegram[0], key=lambda r: r.file_part))
    assert b"moof" in reconstructed and b"moov" in reconstructed
    output = tmp_path / "reconstructed.mp4"
    output.write_bytes(reconstructed)
    subprocess.run([ffmpeg, "-v", "error", "-i", str(output), "-map", "0:v", "-map", "0:a",
                    "-f", "null", "-"], check=True, capture_output=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("known_size", [False, True])
@pytest.mark.parametrize("codec", ["libx264", "libvpx-vp9"])
async def test_real_split_parts_preserve_frames_audio_and_independent_playback(
        tmp_path, monkeypatch, telegram, known_size, codec):
    ffmpeg = ffmpeg_executable()
    extension = "webm" if codec == "libvpx-vp9" else "mp4"
    video, audio = tmp_path / f"video.{extension}", tmp_path / "audio.m4a"
    codec_args = ["-deadline", "realtime", "-cpu-used", "8"] if codec == "libvpx-vp9" else [
        "-keyint_min", "25", "-sc_threshold", "0", "-bf", "2"]
    subprocess.run([ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=320x240:rate=25:duration=8", "-c:v", codec,
                    "-g", "25", *codec_args,
                    "-b:v", "500k", str(video)], check=True, capture_output=True)
    subprocess.run([ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i",
                    "sine=frequency=400:duration=8", "-c:a", "aac", str(audio)],
                   check=True, capture_output=True)

    async def serve(request):
        return web.FileResponse(video if request.path == "/v" else audio)

    app = web.Application()
    app.router.add_get("/{name}", serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    info = metadata()
    info["duration"] = 8
    for fmt, name, path in zip(info["formats"], ["v", "a"], [video, audio]):
        fmt["url"] = f"http://127.0.0.1:{port}/{name}"
        fmt["filesize"] = path.stat().st_size if known_size else None
        fmt["tbr"] = fmt["abr"] = 0
    info["formats"][0]["ext"] = extension
    info["formats"][0]["vcodec"] = "vp9" if extension == "webm" else "avc1"
    plan = streaming.select_streams(info, "1080")
    plan.output_quality = "best_1080p" if known_size else "1080p"
    if not known_size:
        plan.estimated_size = 0
    monkeypatch.setattr(config, "STREAM_PARALLEL_DOWNLOADS", known_size)
    monkeypatch.setattr(config, "VIDEO_PART_SIZE_BYTES", 190_000)
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 1)
    monkeypatch.setattr(streaming.shutil, "which", lambda _: ffmpeg)
    client = fake_client()
    stats = streaming.TransferStats()
    try:
        await asyncio.wait_for(streaming.stream_video(client, 1, plan, stats=stats), 30)
    finally:
        await runner.cleanup()
    messages = [call.args[0] for call in client.invoke.call_args_list]
    assert len(messages) >= 3 and stats.completed_parts == len(messages)
    frame_hashes, all_audio_samples = [], 0
    packet_counts = []
    for index, message in enumerate(messages, 1):
        file_id = message.media.file.id
        calls = sorted([r for r in telegram[0] if r.file_id == file_id], key=lambda r: r.file_part)
        data = b"".join(r.bytes for r in calls)
        assert len(data) <= config.VIDEO_PART_SIZE_BYTES
        assert message.media.attributes[1].file_name.endswith(f"_part_{index:02d}.mp4")
        assert message.media.attributes[1].file_name.endswith(f"_{plan.output_quality}_part_{index:02d}.mp4")
        assert ("Best · 1080p" if known_size else "1080p") in message.message
        assert f"Part {index}" in message.message
        assert message.media.attributes[0].duration > 0
        output = tmp_path / f"part-{index}.mp4"
        output.write_bytes(data)
        # Decode EVERY part by itself, including B-frames and separate AAC audio.
        decoded = subprocess.run([ffmpeg, "-v", "error", "-xerror", "-i", str(output),
                                  "-map", "0:v", "-f", "framemd5", "-"],
                                 check=True, capture_output=True).stdout.decode()
        frame_hashes += [line.split(",")[-1].strip() for line in decoded.splitlines()
                         if line and not line.startswith("#")]
        samples = subprocess.run([ffmpeg, "-v", "error", "-xerror", "-i", str(output),
                                  "-map", "0:a", "-f", "s16le", "-"],
                                 check=True, capture_output=True).stdout
        assert samples
        all_audio_samples += len(samples) // 2
        packet_counts.append(data.count(b"moof"))
    original = subprocess.run([ffmpeg, "-v", "error", "-i", str(video), "-f", "framemd5", "-"],
                              check=True, capture_output=True).stdout.decode()
    expected = [line.split(",")[-1].strip() for line in original.splitlines()
                if line and not line.startswith("#")]
    assert frame_hashes == expected  # No missing, reordered, duplicated or changed frames.
    assert abs(all_audio_samples - 8 * 44100) < 2048
    assert sum(packet_counts) == 8
    assert all(session.stop.await_count == 1 for session in telegram[1])


@pytest.mark.asyncio
async def test_later_delivery_failure_reports_previously_sent_part(tmp_path, monkeypatch, telegram):
    import transfer_runtime
    monkeypatch.setattr(transfer_runtime, "RETRY_BACKOFF", 0)
    ffmpeg = ffmpeg_executable()
    source = tmp_path / "course.mp4"
    subprocess.run([ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=320x240:rate=25:duration=5", "-f", "lavfi", "-i",
                    "sine=duration=5", "-c:v", "libx264", "-g", "25", "-sc_threshold", "0",
                    "-c:a", "aac", "-shortest", str(source)], check=True, capture_output=True)
    monkeypatch.setattr(streaming.shutil, "which", lambda _: ffmpeg)
    monkeypatch.setattr(config, "VIDEO_PART_SIZE_BYTES", 80_000)
    result = SimpleNamespace(file_path=str(source), title="Course", duration=5,
                             width=320, height=240, file_size=source.stat().st_size)
    client, stats = fake_client(), streaming.TransferStats()
    client.invoke.side_effect = [object()] + [RuntimeError("SendMedia failed")] * 3
    with pytest.raises(RuntimeError, match=r"after 1 part\(s\) were delivered"):
        await streaming.stream_local_video(client, 1, result, stats=stats)
    assert stats.completed_parts == 1
    assert client.invoke.await_count == 4  # First part + three attempts for part 2.
    failed_requests = [call.args[0] for call in client.invoke.call_args_list[1:]]
    assert all(request is failed_requests[0] for request in failed_requests)
    first = client.invoke.call_args_list[0].args[0]
    assert first.media.attributes[1].file_name == "Course_part_01.mp4"
    assert all(session.stop.await_count == 1 for session in telegram[1])


@pytest.mark.asyncio
async def test_source_retry_replays_delivered_parts_without_duplicates_or_frame_loss(tmp_path, monkeypatch, telegram):
    ffmpeg = ffmpeg_executable()
    source = tmp_path / "recovery.mp4"
    subprocess.run([ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=320x240:rate=25:duration=5", "-f", "lavfi", "-i",
                    "sine=duration=5", "-c:v", "libx264", "-g", "25", "-sc_threshold", "0",
                    "-c:a", "aac", "-shortest", str(source)], check=True, capture_output=True)
    monkeypatch.setattr(streaming.shutil, "which", lambda _: ffmpeg)
    monkeypatch.setattr(config, "VIDEO_PART_SIZE_BYTES", 80_000)
    result = SimpleNamespace(file_path=str(source), title="Course", duration=5,
                             width=320, height=240, file_size=source.stat().st_size)
    original_upload = streaming._upload_live_part
    failed = False
    async def interrupt_once(client, part, filename, size, progress, stats, limit):
        nonlocal failed
        if stats.part_index == 2 and not failed:
            failed = True
            raise OSError("source transport broke")
        return await original_upload(client, part, filename, size, progress, stats, limit)
    monkeypatch.setattr(streaming, "_upload_live_part", interrupt_once)
    client, stats = fake_client(), streaming.TransferStats()
    await asyncio.wait_for(streaming.stream_local_video(client, 1, result, stats=stats), 15)
    assert failed
    requests = [call.args[0] for call in client.invoke.call_args_list]
    filenames = [request.media.attributes[1].file_name for request in requests]
    assert len(requests) >= 3 and len(set(filenames)) == len(requests)
    assert stats.completed_parts == len(requests)
    decoded_frames = []
    for index, request in enumerate(requests, 1):
        assert filenames[index - 1] == f"Course_part_{index:02d}.mp4"
        uploaded = [rpc for rpc in telegram[0] if rpc.file_id == request.media.file.id]
        output = tmp_path / f"delivered-{index}.mp4"
        output.write_bytes(b"".join(rpc.bytes for rpc in sorted(uploaded, key=lambda rpc: rpc.file_part)))
        frames = subprocess.run([ffmpeg, "-v", "error", "-xerror", "-i", str(output),
                                 "-map", "0:v", "-f", "framemd5", "-"], check=True, capture_output=True).stdout.decode()
        decoded_frames.extend(line.split(",")[-1].strip() for line in frames.splitlines()
                              if line and not line.startswith("#"))
    frames = subprocess.run([ffmpeg, "-v", "error", "-i", str(source), "-map", "0:v",
                             "-f", "framemd5", "-"], check=True, capture_output=True).stdout.decode()
    original_frames = [line.split(",")[-1].strip() for line in frames.splitlines()
                       if line and not line.startswith("#")]
    assert decoded_frames == original_frames
    assert all(session.stop.await_count == 1 for session in telegram[1])


@pytest.mark.asyncio
async def test_vod_hls_streams_directly_and_splits_without_disk_download(tmp_path, monkeypatch, telegram):
    ffmpeg = ffmpeg_executable()
    playlist = tmp_path / "index.m3u8"
    subprocess.run([ffmpeg, "-y", "-v", "error", "-f", "lavfi", "-i",
                    "testsrc2=size=320x240:rate=25:duration=4", "-f", "lavfi", "-i",
                    "sine=duration=4", "-c:v", "libx264", "-g", "25", "-sc_threshold", "0",
                    "-c:a", "aac", "-shortest", "-f", "hls", "-hls_time", "1",
                    "-hls_list_size", "0", "-hls_segment_filename", str(tmp_path / "seg%02d.ts"),
                    str(playlist)], check=True, capture_output=True)
    files = {path.name: path for path in tmp_path.iterdir()}

    async def serve(request):
        if request.match_info["name"] not in files:
            raise web.HTTPNotFound()
        return web.FileResponse(files[request.match_info["name"]])

    app = web.Application()
    app.router.add_get("/{name}", serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    info = {"title": "VOD course", "duration": 4, "formats": [{
        "url": f"http://127.0.0.1:{port}/index.m3u8", "protocol": "m3u8_native",
        "ext": "mp4", "height": 1080, "width": 1920, "vcodec": "avc1", "acodec": "mp4a"}]}
    monkeypatch.setattr(streaming.shutil, "which", lambda _: ffmpeg)
    monkeypatch.setattr(config, "VIDEO_PART_SIZE_BYTES", 80_000)
    monkeypatch.setattr(config, "DOWNLOAD_STORAGE_LIMIT_BYTES", 1)
    client = fake_client()
    try:
        await asyncio.wait_for(streaming.stream_video(client, 1, streaming.select_streams(info, "1080")), 20)
    finally:
        await runner.cleanup()
    assert client.invoke.await_count > 1
    for index, call in enumerate(client.invoke.call_args_list, 1):
        file_id = call.args[0].media.file.id
        data = b"".join(r.bytes for r in sorted(telegram[0], key=lambda r: r.file_part)
                        if r.file_id == file_id)
        output = tmp_path / f"hls-part{index}.mp4"
        output.write_bytes(data)
        subprocess.run([ffmpeg, "-v", "error", "-xerror", "-i", str(output),
                        "-map", "0:v", "-map", "0:a", "-f", "null", "-"],
                       check=True, capture_output=True)
