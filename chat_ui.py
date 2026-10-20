"""Readable video collections and short-lived transfer notices."""
import asyncio
import hashlib
import html
import logging
import math
from dataclasses import dataclass, field

from resolution_options import resolution_label
from transfer_runtime import retry_stage
from uploader import human_readable_size, human_readable_time
import config

logger = logging.getLogger(__name__)
NOTICE_LIFETIME = 300
_deletions = {}
_delete_tasks = set()
_running_deletions = {}


def _notice_key(message):
    chat_id = getattr(getattr(message, "chat", None), "id", None)
    message_id = getattr(message, "id", None)
    if type(chat_id) is int and type(message_id) is int and message_id > 0:
        return chat_id, message_id
    return None


def cancel_notice_cleanup(message):
    key = _notice_key(message)
    handle = _deletions.pop(key, None)
    if handle:
        handle.cancel()
    task = _running_deletions.pop(key, None)
    if task:
        task.cancel()


def expire_notice(message):
    """Only call for bot-owned, terminal TEXT status messages, never media."""
    key = _notice_key(message)
    if key is None or getattr(message, "media", None):
        return
    cancel_notice_cleanup(message)
    async def delete():
        try:
            await retry_stage("Delete temporary transfer notice", message.delete, config.TRANSFER_STATUS_TIMEOUT)
        except Exception:
            logger.warning("Could not delete transfer notice chat=%s message=%s", *key, exc_info=True)
    def expired():
        _deletions.pop(key, None)
        task = asyncio.create_task(delete())
        _delete_tasks.add(task)
        _running_deletions[key] = task
        task.add_done_callback(_delete_tasks.discard)
        def finished(done):
            if _running_deletions.get(key) is done:
                _running_deletions.pop(key, None)
        task.add_done_callback(finished)
    _deletions[key] = asyncio.get_running_loop().call_later(NOTICE_LIFETIME, expired)


async def close_notice_cleanup():
    for handle in _deletions.values():
        handle.cancel()
    _deletions.clear()
    for task in tuple(_delete_tasks):
        task.cancel()
    await asyncio.gather(*_delete_tasks, return_exceptions=True)
    _running_deletions.clear()


def sent_message_id(sent, random_id=None):
    direct = getattr(sent, "id", None)
    if type(direct) is int and direct > 0:
        return direct
    updates = getattr(sent, "updates", None)
    if not isinstance(updates, (tuple, list)):
        return None
    # Telegram can return several updates; match our request ID first.
    for update in updates:
        if random_id is not None and getattr(update, "random_id", None) == random_id:
            value = getattr(update, "id", None)
            if type(value) is int and value > 0:
                return value
    for update in updates:
        message = getattr(update, "message", None)
        value = getattr(message, "id", None)
        if type(value) is int and value > 0 and getattr(message, "media", None):
            return value
    return None


@dataclass
class VideoCollection:
    chat_id: int
    source: str
    root: int = None
    resolutions: dict = field(default_factory=dict)

    @property
    def tag(self):
        digest = hashlib.sha256(f"{self.chat_id}:{self.source}".encode()).hexdigest()[:8]
        return "course_" + digest

    def quality_key(self, quality):
        return quality.removeprefix("best_") or "source"

    def reply_to(self, quality):
        return self.resolutions.get(self.quality_key(quality), self.root)

    def record(self, sent, quality, random_id=None):
        message_id = sent_message_id(sent, random_id)
        if message_id is not None:
            self.root = self.root or message_id
            self.resolutions.setdefault(self.quality_key(quality), message_id)

    def forget(self, message_id):
        self.resolutions = {key: value for key, value in self.resolutions.items() if value != message_id}
        if self.root == message_id:
            self.root = next(iter(self.resolutions.values()), None)

    def tags(self, quality):
        return f"#{self.tag}  #{self.tag}_{self.quality_key(quality)}"


def video_caption(title, quality, size, duration, collection=None, index=None, start=0):
    text = f"🎬 <b>{html.escape(str(title)[:240])}</b>\n"
    text += f"🎞 <b>{html.escape(resolution_label(quality))}</b>"
    if index is not None:
        text += f" · <b>Part {index}</b>"
    text += (f"\n\n⏱ {human_readable_time(math.ceil(duration))}"
             f"  ·  💾 {human_readable_size(size)}")
    if index is not None:
        text += f"\n🕒 {human_readable_time(int(start))} – {human_readable_time(math.ceil(start + duration))}"
    if collection:
        text += "\n\n" + collection.tags(quality)
    return text
