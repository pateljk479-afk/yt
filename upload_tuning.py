"""Probe upload concurrency while keeping the fastest observed stable setting."""
from concurrent.futures import ThreadPoolExecutor
import logging

import pyrogram

import config

logger = logging.getLogger(__name__)


def configure_crypto_workers():
    """Call before connecting clients; Pyrogram otherwise shares one worker."""
    workers = max(1, min(config.CRYPTO_WORKERS, 8))
    current = pyrogram.crypto_executor
    if getattr(current, "_bot_crypto_workers", None) == workers:
        return workers
    replacement = ThreadPoolExecutor(workers, thread_name_prefix="TelegramCrypto")
    replacement._bot_crypto_workers = workers
    pyrogram.crypto_executor = replacement
    current.shutdown(wait=False)
    logger.info("Telegram encryption pool: %d workers", workers)
    return workers


class AdaptiveUploadWindow:
    """Compare two samples per trial; stop probing when extra work stops helping.

    Only a saturated upload with queued source bytes is a valid capacity trial.
    A source bottleneck must not cause speculative connection growth. Already
    queued requests finish normally when the window shrinks.
    """

    def __init__(self, workers, depth, max_workers, max_depth, enabled=True):
        self.enabled = enabled
        self.levels = list(dict.fromkeys([
            (workers, depth), (max(workers, max_workers), depth),
            (max(workers, max_workers), max(depth, max_depth))]))
        self.level = 0
        self.baseline = None
        self.samples = []
        self.stopped = not enabled
        self.reason = "fixed" if not enabled else "initial"

    @property
    def workers(self):
        return self.levels[self.level][0]

    @property
    def depth(self):
        return self.levels[self.level][1]

    @property
    def capacity(self):
        return self.workers * self.depth

    def observe(self, rate, backlog, saturated, congested=False):
        before = (self.workers, self.depth)
        if not self.enabled:
            return False
        if congested:
            if self.level:
                self.level -= 1
            else:
                self.levels[0] = (max(1, self.workers // 2), self.depth)
            self.stopped = True
            self.samples.clear()
            self.reason = "retry/throttle backoff"
        elif not self.stopped:
            if not backlog or not saturated or rate <= 0:
                self.samples.clear()
                return False
            self.samples.append(rate)
            if len(self.samples) < 2:
                return False
            average = sum(self.samples) / len(self.samples)
            self.samples.clear()
            if self.baseline is not None and average < self.baseline * 1.10:
                self.level = max(0, self.level - 1)
                self.stopped = True
                self.reason = "extra concurrency did not improve throughput"
            else:
                self.baseline = average
                if self.level + 1 < len(self.levels):
                    self.level += 1
                    self.reason = "testing higher concurrency"
                else:
                    self.stopped = True
                    self.reason = "maximum concurrency reached"
        return before != (self.workers, self.depth)
