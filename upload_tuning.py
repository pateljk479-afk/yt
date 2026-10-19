"""Probe upload concurrency while keeping the fastest observed stable setting."""
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from dataclasses import dataclass

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
    """Compare two samples per trial; retry capacity checks after a cooldown.

    Only a saturated upload with queued source bytes is a valid capacity trial.
    A source bottleneck must not cause speculative connection growth. Already
    queued requests finish normally when the window shrinks.
    """

    def __init__(self, workers, depth, max_workers, max_depth, enabled=True,
                 recovery_seconds=30, start=None):
        self.enabled = enabled
        self.levels = list(dict.fromkeys([
            (workers, depth), (max(workers, max_workers), depth),
            (max(workers, max_workers), max(depth, max_depth))]))
        self.level = 0
        if start in self.levels:
            self.level = self.levels.index(start)
        self.confirmed = None
        self.confirmed_rate = 0
        self.recovery_seconds = recovery_seconds
        self.resume_at = 0
        self.floor = (max(1, min(workers, 2)), depth)
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

    def observe(self, rate, backlog, saturated, congested=False, now=None):
        now = time.monotonic() if now is None else now
        before = (self.workers, self.depth)
        if not self.enabled:
            return False
        if congested:
            if self.level:
                self.level -= 1
            else:
                # Preserve the original levels so recovery can restore them.
                reduced = (max(self.floor[0], self.workers // 2), self.depth)
                if reduced != self.levels[0]:
                    self.levels.insert(0, reduced)
            self.stopped = True
            self.resume_at = now + self.recovery_seconds
            self.baseline = None
            self.samples.clear()
            self.reason = "retry/throttle backoff"
        elif self.enabled:
            if self.stopped:
                if now < self.resume_at or not backlog or not saturated:
                    return False
                self.stopped = False
                self.baseline = None
                self.samples.clear()
                self.reason = "recovering upload capacity"
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
                self.resume_at = now + self.recovery_seconds
                self.reason = "extra concurrency did not improve throughput"
            else:
                self.baseline = average
                self.confirmed = (self.workers, self.depth)
                self.confirmed_rate = average
                if self.level + 1 < len(self.levels):
                    self.level += 1
                    self.reason = "testing higher concurrency"
                else:
                    self.stopped = True
                    self.resume_at = now + self.recovery_seconds
                    self.reason = "maximum concurrency reached"
        return before != (self.workers, self.depth)


@dataclass(frozen=True)
class UploadProfile:
    dc_id: int
    test_mode: bool
    setting: tuple
    rate: float
    saved_at: float


def previous_upload_setting(client, dc_id, test_mode, workers, depth, max_workers, max_depth):
    profile = getattr(client, "_bot_upload_profile", None)
    if (isinstance(profile, UploadProfile) and profile.dc_id == dc_id
            and profile.test_mode == test_mode and time.monotonic() - profile.saved_at < 1800
            and workers <= profile.setting[0] <= max_workers
            and depth <= profile.setting[1] <= max_depth):
        return profile.setting
    return None


def remember_upload_setting(client, dc_id, test_mode, control, size, throttled=False):
    if throttled:
        # Never carry a throttled window into the next video.
        client._bot_upload_profile = None
    elif control.enabled and control.confirmed and control.confirmed_rate > 0 and size >= 16 * 1024 * 1024:
        client._bot_upload_profile = UploadProfile(dc_id, test_mode, control.confirmed,
                                                  control.confirmed_rate, time.monotonic())
