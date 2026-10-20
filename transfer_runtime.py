"""Deadlines and explicit retry budgets for external transfer operations."""
import asyncio
import logging
import time

from pyrogram.errors import FloodWait, InternalServerError, RPCError

import config

logger = logging.getLogger(__name__)
RETRY_BACKOFF = 2


class TransferStageError(RuntimeError):
    """A stage exhausted its budget; do not restart the entire media upload."""


async def bounded(awaitable, timeout, label):
    try:
        return await asyncio.wait_for(awaitable, timeout)
    except asyncio.TimeoutError as exc:
        raise TimeoutError(f"{label} timed out after {timeout:g}s") from exc


async def retry_stage(label, operation, timeout):
    """Initial attempt plus two retries. Cancellation is never retried.

    The caller owns idempotency: publication must reuse its SendMedia request,
    and chunk retries must reuse the same file ID, index and payload.
    """
    for attempt in range(3):
        started = time.monotonic()
        logger.info("%s attempt=%d/3 deadline=%.1fs", label, attempt + 1, timeout)
        try:
            result = await bounded(operation(), timeout, label)
            logger.info("%s finished in %.2fs", label, time.monotonic() - started)
            return result
        except Exception as exc:
            logger.warning("%s attempt=%d/3 failed after %.2fs: %s: %s",
                           label, attempt + 1, time.monotonic() - started, type(exc).__name__, exc)
            # Permission, invalid media, etc. cannot be repaired by resending.
            permanent = isinstance(exc, RPCError) and not isinstance(exc, (FloodWait, InternalServerError))
            delay = exc.value if isinstance(exc, FloodWait) else RETRY_BACKOFF * (attempt + 1)
            if permanent or attempt == 2 or delay > timeout:
                raise TransferStageError(f"{label} failed ({attempt + 1} attempt(s)): {exc}") from exc
            logger.info("%s retry=%d/2 in %.1fs", label, attempt + 1, delay)
            await asyncio.sleep(delay)


async def close_media_sessions(sessions, label):
    """Stop every owned session; a stuck stop must not hold the playlist slot."""
    async def close(session):
        try:
            await bounded(session.stop(), config.TRANSFER_CLEANUP_TIMEOUT, label + " session cleanup")
        except Exception:
            logger.warning("%s session cleanup failed; closing transport", label, exc_info=True)
            connection = getattr(session, "connection", None)
            if connection:
                try:
                    await bounded(connection.close(), config.TRANSFER_CLEANUP_TIMEOUT, label + " transport close")
                except Exception:
                    logger.warning("%s transport close failed", label, exc_info=True)
            for name in ("ping_task", "recv_task"):
                task = getattr(session, name, None)
                if isinstance(task, asyncio.Task):
                    task.cancel()
    await asyncio.gather(*(close(session) for session in sessions))


async def best_effort_status(operation):
    """Telegram UI updates never decide the outcome of a media transfer."""
    try:
        return await bounded(operation, config.TRANSFER_STATUS_TIMEOUT, "Telegram status update")
    except Exception:
        logger.warning("Telegram status update unavailable", exc_info=True)

