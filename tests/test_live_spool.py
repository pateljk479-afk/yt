import asyncio
import errno
import threading
from unittest.mock import patch

import pytest

import config
from live_spool import BLOCK_SIZE, LivePartSpool
from transfer_runtime import TransferStageError


def source(data):
    reader = asyncio.StreamReader()
    reader.index = 1
    reader.feed_data(data)
    reader.feed_eof()
    return reader


@pytest.mark.asyncio
async def test_readers_wait_for_growth_and_recovery_unblocks_backpressure(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STREAM_BUFFER_BYTES", BLOCK_SIZE)
    payload = b"a" * BLOCK_SIZE + b"b" * BLOCK_SIZE + b"tail"
    async with LivePartSpool(source(payload), len(payload)) as spool:
        first = await asyncio.wait_for(spool.readexactly(BLOCK_SIZE), 1)
        assert first == b"a" * BLOCK_SIZE
        # No more upload reads: recovery must release producer backpressure.
        assert await asyncio.wait_for(spool.finish_for_recovery(), 1) == len(payload)
        assert spool.done and spool.failure is None
        with open(spool.path, "rb") as file:
            assert file.read() == payload
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_storage_limit_enforced_before_writing_an_oversized_block(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    async with LivePartSpool(source(b"too large"), 4) as spool:
        with pytest.raises(ValueError, match="storage limit"):
            await spool.readexactly(4)
        assert spool.written == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_disk_full_is_terminal_and_cannot_trigger_a_cdn_replay(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    async with LivePartSpool(source(b"payload"), 100) as spool:
        with patch.object(spool.writer, "write", side_effect=OSError(errno.ENOSPC, "full")):
            with pytest.raises(TransferStageError, match="storage write failed"):
                await spool.readexactly(1)
        assert spool.written == 0
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_source_stall_has_deadline_and_releases_waiting_consumer(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STREAM_READ_TIMEOUT", 0.02)
    reader = asyncio.StreamReader()
    reader.index = 1
    async with LivePartSpool(reader, 100) as spool:
        with pytest.raises(TimeoutError, match="Live video source timed out"):
            await asyncio.wait_for(spool.readexactly(1), 1)
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_cancellation_during_source_wait_closes_files_and_producer(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    entered = asyncio.Event()
    owned = []
    async def transfer():
        reader = asyncio.StreamReader()
        reader.index = 1
        async with LivePartSpool(reader, 100) as spool:
            owned.append(spool)
            entered.set()
            await spool.readexactly(1)
    task = asyncio.create_task(transfer())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert owned[0].producer.done() and owned[0].reader.closed and owned[0].writer.closed
    assert not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_cancelled_transfer_waits_for_outstanding_disk_write_before_closing(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    writing, release = threading.Event(), threading.Event()
    owned = []
    async def transfer():
        async with LivePartSpool(source(b"payload"), 100) as spool:
            owned.append(spool)
            original = spool.writer.write
            def delayed_write(block):
                writing.set()
                release.wait(2)
                assert not spool.writer.closed
                return original(block)
            with patch.object(spool.writer, "write", side_effect=delayed_write):
                await spool.readexactly(1)
    task = asyncio.create_task(transfer())
    try:
        assert await asyncio.to_thread(writing.wait, 1)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done() and not owned[0].writer.closed
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 1)
    assert owned[0].writer.closed and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_disk_io_cleanup_deadline_releases_job_and_defers_safe_disposal(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "TRANSFER_CLEANUP_TIMEOUT", 0.01)
    writing, release = threading.Event(), threading.Event()
    disposed = asyncio.Event()
    owned = []
    async def transfer():
        async with LivePartSpool(source(b"payload"), 100) as spool:
            owned.append(spool)
            original_write, original_dispose = spool.writer.write, spool._dispose
            def delayed_write(block):
                writing.set()
                release.wait(2)
                assert not spool.writer.closed
                return original_write(block)
            def dispose():
                original_dispose()
                disposed.set()
            spool.writer.write, spool._dispose = delayed_write, dispose
            await spool.readexactly(1)
    task = asyncio.create_task(transfer())
    try:
        assert await asyncio.to_thread(writing.wait, 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert not disposed.is_set() and not owned[0].writer.closed
    finally:
        release.set()
    await asyncio.wait_for(disposed.wait(), 1)
    assert owned[0].writer.closed and not list(tmp_path.iterdir())


@pytest.mark.asyncio
async def test_backpressure_bounds_source_run_ahead_without_upload_consumer(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DOWNLOAD_DIR", str(tmp_path))
    monkeypatch.setattr(config, "STREAM_BUFFER_BYTES", BLOCK_SIZE)
    class Endless:
        index = 1
        calls = 0
        async def readexactly(self, count):
            self.calls += 1
            return b"x" * count
    incoming = Endless()
    async with LivePartSpool(incoming, BLOCK_SIZE * 10) as spool:
        async with spool.condition:
            await asyncio.wait_for(spool.condition.wait_for(lambda: spool.written >= BLOCK_SIZE), 1)
        assert incoming.calls == 1
        assert spool.written - spool.consumed <= spool.window
    assert not list(tmp_path.iterdir())
