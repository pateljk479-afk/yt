"""One FIFO playlist worker; waiting jobs do not own transfer tasks or files."""
import asyncio
import logging
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Awaitable, Callable

logger = logging.getLogger(__name__)


@dataclass
class PlaylistJob:
    chat_id: int
    title: str
    entries: list[dict]
    quality: str
    runner: Callable[["PlaylistJob"], Awaitable[None]] = field(repr=False)
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    state: str = "queued"
    successful: int = 0
    failed: int = 0
    current: int = 0
    current_title: str = ""
    stage: str = "Waiting"
    created_at: float = field(default_factory=time.time)
    task: asyncio.Task | None = field(default=None, repr=False)
    total: int = field(init=False)

    def __post_init__(self):
        self.entries = [dict(entry) for entry in self.entries]
        self.total = len(self.entries)

    @property
    def completed(self):
        return self.successful + self.failed


class PlaylistQueue:
    def __init__(self):
        self.pending = deque()
        self.active = None
        self.history = deque(maxlen=20)
        self.worker = None

    def enqueue(self, job: PlaylistJob):
        self.pending.append(job)
        logger.info("Playlist queued job=%s chat=%s videos=%s", job.id, job.chat_id, job.total)
        if self.worker is None or self.worker.done():
            self.worker = asyncio.create_task(self._run())
        return job

    def visible(self, chat_id):
        return ([self.active] if self.active and self.active.chat_id == chat_id else []) + [
            job for job in self.pending if job.chat_id == chat_id
        ]

    def cancel(self, job_id, chat_id):
        """Request cancellation without awaiting cleanup in the Telegram handler."""
        if self.active and self.active.id == job_id and self.active.chat_id == chat_id:
            job = self.active
            if job.state != "cancelling":
                job.state = "cancelling"
                job.stage = "Stopping transfer and cleaning up"
                # Only cancel once: a second cancellation could interrupt cleanup.
                if job.task:
                    job.task.cancel()
            return job
        for job in self.pending:
            if job.id == job_id and job.chat_id == chat_id:
                self.pending.remove(job)
                job.state = "cancelled"
                job.stage = "Cancelled before starting"
                self.history.append(job)
                job.entries.clear()
                job.runner = None
                logger.info("Waiting playlist cancelled job=%s chat=%s", job.id, chat_id)
                return job
        return None

    async def _run(self):
        while self.pending:
            job = self.pending.popleft()
            self.active = job
            job.state = "running"
            job.stage = "Starting"
            logger.info("Playlist started job=%s chat=%s", job.id, job.chat_id)
            job.task = asyncio.create_task(job.runner(job))
            try:
                await job.task
                job.state = "cancelled" if job.state == "cancelling" else "completed"
            except asyncio.CancelledError:
                job.state = "cancelled"
            except Exception:
                job.state = "failed"
                job.stage = "Playlist worker failed; see server logs"
                logger.exception("Playlist failed job=%s chat=%s", job.id, job.chat_id)
            finally:
                job.task = None
                # Release captured client/message and entry lists from history.
                job.entries.clear()
                job.runner = None
                self.history.append(job)
                logger.info("Playlist finished job=%s state=%s sent=%s failed=%s total=%s",
                            job.id, job.state, job.successful, job.failed, job.total)
                self.active = None

    async def close(self):
        self.pending.clear()
        if self.active:
            self.cancel(self.active.id, self.active.chat_id)
        if self.worker:
            await self.worker


playlist_queue = PlaylistQueue()
