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
    file, actual = await streaming.upload_stream(fake_client(), reader, "test.mp4")
    assert actual == size and file.parts == math.ceil(size / PART_SIZE)
    assert b"".join(rpc.bytes for rpc in sorted(calls, key=lambda rpc: rpc.file_part)) == payload
    assert all(rpc.file_total_parts == -1 for rpc in calls[:-1])
    assert calls[-1].file_total_parts == file.parts
    assert len(calls[-1].bytes) == size % PART_SIZE
    assert all(s.stop.await_count == 1 for s in sessions)


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
    await streaming.upload_stream(fake_client(), reader, "test.mp4")
    await task
    assert all(len(rpc.bytes) == PART_SIZE for rpc in telegram[0][:-1])


@pytest.mark.asyncio
async def test_limit_stops_upload_without_finalizing(monkeypatch, telegram):
    monkeypatch.setattr(config, "MAX_FILE_SIZE", PART_SIZE + 1)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE + 2))
    reader.feed_eof()
    with pytest.raises(ValueError, match="size limit"):
        await streaming.upload_stream(fake_client(), reader, "test.mp4")
    assert not any(rpc.file_total_parts > 0 for rpc in telegram[0])
    assert all(s.stop.await_count == 1 for s in telegram[1])


@pytest.mark.asyncio
async def test_failed_chunk_interrupts_stalled_source(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=stopped, invoke=AsyncMock(side_effect=RuntimeError("chunk failed")))))
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE))  # Source now stalls, without EOF.
    with pytest.raises(RuntimeError, match="chunk failed"):
        await asyncio.wait_for(streaming.upload_stream(fake_client(), reader, "test.mp4"), 2)
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

    task = asyncio.create_task(streaming.upload_stream(fake_client(), EndlessReader(), "test.mp4"))
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
    parts_sent, bytes_sent = 0, 0
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
        return True

    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(), stop=AsyncMock(), invoke=invoke)))
    file, total = await streaming.upload_stream(fake_client(), LargeReader(), "large.mp4")
    assert total == bytes_sent == size
    assert file.parts == size // PART_SIZE
    assert parts_sent == file.parts + 1  # final empty EOF part


@pytest.mark.asyncio
async def test_startup_failure_closes_sessions(monkeypatch):
    stopped = AsyncMock()
    monkeypatch.setattr(streaming, "Session", Mock(return_value=SimpleNamespace(
        start=AsyncMock(side_effect=OSError("offline")), stop=stopped)))
    with pytest.raises(OSError, match="offline"):
        await streaming.upload_stream(fake_client(), asyncio.StreamReader(), "test.mp4")
    assert stopped.await_count == config.UPLOAD_WORKERS


@pytest.mark.asyncio
async def test_source_timeout_closes_sessions(monkeypatch, telegram):
    monkeypatch.setattr(config, "STREAM_READ_TIMEOUT", 0.01)
    with pytest.raises(TimeoutError):
        await streaming.upload_stream(fake_client(), asyncio.StreamReader(), "test.mp4")
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
    task = asyncio.create_task(streaming.upload_stream(fake_client(), reader, "test.mp4"))
    try:
        await asyncio.wait_for(fast_completed.wait(), 2)
        assert counts[0] == 0 and counts[1] >= 8
        assert all(1 <= peak <= 2 for peak in peaks)
    finally:
        release.set()
        await asyncio.wait_for(task, 2)


@pytest.mark.asyncio
async def test_media_session_never_has_overlapping_mtproto_requests(monkeypatch):
    """A Pyrogram Session must serialize its request/response map.

    This regression covers the real failure mode where 16 sessions with an
    eight-request depth triggered BadMsgNotification(33) and a KeyError after
    the source had already finished streaming.
    """
    monkeypatch.setattr(config, "UPLOAD_ADAPTIVE", True)
    monkeypatch.setattr(config, "UPLOAD_WORKERS", 2)
    monkeypatch.setattr(config, "UPLOAD_MAX_WORKERS", 2)
    # Deliberately retain old, unsafe environment values.  The uploader must
    # still use only one request per Session.
    monkeypatch.setattr(config, "UPLOAD_REQUESTS_PER_CONNECTION", 8)
    monkeypatch.setattr(config, "UPLOAD_MAX_REQUESTS_PER_CONNECTION", 8)
    active, peak = {}, {}

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
                await asyncio.sleep(0.002)
                return True
            finally:
                active[self.index] -= 1

    monkeypatch.setattr(streaming, "Session", OrderedSession)
    reader = asyncio.StreamReader()
    reader.feed_data(media_bytes(PART_SIZE * 20 + 7))
    reader.feed_eof()
    stats = streaming.TransferStats()
    _, sent = await asyncio.wait_for(streaming.upload_stream(fake_client(), reader, "ordered.mp4", stats=stats), 3)
    assert sent == PART_SIZE * 20 + 7
    assert stats.request_capacity == 2
    assert peak and all(value == 1 for value in peak.values())


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
        fake_client(), Reader(), "test.mp4", progress=slow_progress), 2)
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
    task = asyncio.create_task(streaming.upload_stream(fake_client(), reader, "test.mp4", stats=stats))
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
    await asyncio.wait_for(streaming.upload_stream(fake_client(), reader, "retry.mp4", stats=stats), 3)
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
    await asyncio.wait_for(streaming.upload_stream(client, reader, "flood.mp4", stats=stats), 3)
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
    await asyncio.wait_for(streaming.upload_stream(client, first, "first.mp4"), 4)
    profile = client._bot_upload_profile
    assert profile.setting[0] == 2
    # Observe the warm start itself, without a later adaptive trial changing
    # the final window before the assertion.
    monkeypatch.setattr(config, "UPLOAD_TUNE_INTERVAL", 10)
    second = asyncio.StreamReader()
    second.feed_data(media_bytes(PART_SIZE * 4))
    second.feed_eof()
    stats = streaming.TransferStats()
    await asyncio.wait_for(streaming.upload_stream(client, second, "second.mp4", stats=stats), 2)
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
        assert plan.output_quality.replace("_", " · ") in message.message
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
    client.invoke.side_effect = [object(), RuntimeError("SendMedia failed")]
    with pytest.raises(RuntimeError, match=r"after 1 part\(s\) were delivered"):
        await streaming.stream_local_video(client, 1, result, stats=stats)
    assert stats.completed_parts == 1
    assert client.invoke.await_count == 2
    first = client.invoke.call_args_list[0].args[0]
    assert first.media.attributes[1].file_name == "Course_part_01.mp4"
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
