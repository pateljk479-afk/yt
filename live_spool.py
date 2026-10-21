"""A single bounded recovery file that can be read while it is being written."""
import asyncio
import logging
import os
import tempfile

import config
from storage import cleanup_stream_part
from transfer_runtime import bounded, TransferStageError

logger = logging.getLogger(__name__)
BLOCK_SIZE = 2 * 1024 * 1024


class LivePartSpool:
    """One source owner; upload retries never read the CDN a second time.

    The upload consumes a growing disk file immediately. The producer pauses
    when it gets a bounded distance ahead. On transport exhaustion that pause
    can be released to finish this part for a known-size recovery upload.
    """
    def __init__(self, part, limit):
        if limit <= 0:
            raise ValueError("No storage available for a recovery part")
        self.part, self.limit = part, limit
        self.written = self.consumed = 0
        self.done = False
        self.failure = None
        self.condition = asyncio.Condition()
        self.window = max(BLOCK_SIZE, config.STREAM_BUFFER_BYTES)
        self.recovering = False
        self.io_tasks = set()
        self.path = self.writer = self.reader = self.producer = None

    async def __aenter__(self):
        try:
            handle = tempfile.NamedTemporaryFile(mode="w+b", buffering=0,
                prefix="stream_part_", suffix=".mp4", dir=config.DOWNLOAD_DIR, delete=False)
        except OSError as exc:
            raise TransferStageError("Cannot create the bounded video recovery file: " + str(exc)) from exc
        self.writer, self.path = handle, handle.name
        try:
            self.reader = open(self.path, "rb", buffering=0)
            self.producer = asyncio.create_task(self._produce())
        except BaseException:
            handle.close()
            cleanup_stream_part(self.path)
            raise
        return self

    async def _io(self, operation, *args):
        # Cancelling an asyncio.to_thread await does not stop its OS I/O.
        # Keep ownership until that operation really finishes before closing
        # or unlinking its file, including during upload cancellation.
        task = asyncio.create_task(asyncio.to_thread(operation, *args))
        self.io_tasks.add(task)
        def completed(future):
            self.io_tasks.discard(future)
            if not future.cancelled():
                future.exception()
        task.add_done_callback(completed)
        return await asyncio.shield(task)

    async def _produce(self):
        try:
            while True:
                async with self.condition:
                    await self.condition.wait_for(lambda: self.recovering or
                        self.written - self.consumed < self.window)
                try:
                    block = await bounded(self.part.readexactly(BLOCK_SIZE),
                                          config.STREAM_READ_TIMEOUT, "Live video source")
                    final = False
                except asyncio.IncompleteReadError as exc:
                    block, final = exc.partial, True
                if self.written + len(block) > self.limit:
                    raise ValueError("Playable video part exceeds the safe storage limit")
                if block:
                    try:
                        count = await self._io(self.writer.write, block)
                    except OSError as exc:
                        raise TransferStageError("Video recovery storage write failed: " + str(exc)) from exc
                    if count != len(block):
                        raise TransferStageError("Short write to the video recovery file")
                    async with self.condition:
                        self.written += count
                        self.condition.notify_all()
                if final:
                    if self.written == 0:
                        raise ValueError("Cannot upload an empty playable video part")
                    logger.info("Live part %d source complete: %d bytes; recovery file retained until upload ends",
                                self.part.index, self.written)
                    return
        except BaseException as exc:
            self.failure = exc
            if isinstance(exc, asyncio.CancelledError):
                raise
            logger.warning("Live part %d source failed: %s: %s", self.part.index, type(exc).__name__, exc)
        finally:
            async with self.condition:
                self.done = True
                self.condition.notify_all()

    async def readexactly(self, count):
        if count <= 0:
            return b""
        if count > self.window:
            raise ValueError("Read request exceeds the bounded recovery window")
        async with self.condition:
            await self.condition.wait_for(lambda: self.failure is not None or self.done or
                self.written - self.consumed >= count)
            if self.failure is not None:
                raise self.failure
            available = min(count, self.written - self.consumed)
        block = await self._io(self.reader.read, available)
        if len(block) != available:
            raise OSError("Recovery file ended before its confirmed written bytes")
        async with self.condition:
            self.consumed += len(block)
            self.condition.notify_all()
        if len(block) != count:
            raise asyncio.IncompleteReadError(block, count)
        return block

    async def finish_for_recovery(self):
        async with self.condition:
            self.recovering = True
            self.condition.notify_all()
        await self.producer
        if self.failure is not None:
            raise self.failure
        return self.written

    async def __aexit__(self, *args):
        if self.producer is not None:
            self.producer.cancel()
            await asyncio.gather(self.producer, return_exceptions=True)
        pending = set(self.io_tasks)
        if pending:
            _, pending = await asyncio.wait(pending, timeout=config.TRANSFER_CLEANUP_TIMEOUT)
        if pending:
            # An OS-level disk stall cannot be cancelled safely by asyncio.
            # Release the job slot at its deadline, but retain file ownership
            # and dispose only when the outstanding OS operations finish.
            logger.warning("Recovery file I/O still pending after cleanup deadline; disposal deferred")
            async def dispose_later():
                await asyncio.gather(*pending, return_exceptions=True)
                self._dispose()
            asyncio.create_task(dispose_later())
            return
        self._dispose()

    def _dispose(self):
        for handle in (self.writer, self.reader):
            if handle is not None and not handle.closed:
                try:
                    handle.close()
                except OSError:
                    logger.warning("Could not close video recovery handle", exc_info=True)
        try:
            cleanup_stream_part(self.path)
        except OSError:
            logger.warning("Could not remove closed video recovery file %s", os.path.basename(self.path),
                           exc_info=True)
