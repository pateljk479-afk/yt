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


def test_selection_degrades_only_for_telegram_limit_and_respects_height(monkeypatch):
    monkeypatch.setattr(config, "MAX_FILE_SIZE", 1000 * 1024 * 1024)
    assert streaming.select_streams(metadata(), "max").formats[0]["height"] == 720
    assert streaming.select_streams(metadata(), "720").formats[0]["height"] == 720
    with pytest.raises(streaming.UnsupportedStream):
        streaming.select_streams(metadata(), "480")


@pytest.mark.parametrize("update", [{"is_live": True}, {"formats": []},
    {"formats": [{**metadata()["formats"][0], "protocol": "m3u8_native"}]}])
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
        # 4 queued chunks + 8 requests in flight + one producer read.
        assert 8 <= reads <= 13
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
