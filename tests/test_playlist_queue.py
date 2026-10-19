import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from playlist_queue import PlaylistJob, PlaylistQueue


def job(title, runner, chat=1):
    return PlaylistJob(chat, title, [{"id": title}], "720", runner)


@pytest.mark.asyncio
async def test_fifo_has_no_waiting_limit_and_never_interleaves():
    queue = PlaylistQueue()
    started = asyncio.Event()
    release = asyncio.Event()
    order = []

    async def run(item):
        order.append(item.title)
        if item.title == "first":
            started.set()
            await release.wait()

    queue.enqueue(job("first", run))
    await started.wait()
    for index in range(150):
        queue.enqueue(job(str(index), run))
    assert len(queue.visible(1)) == 151
    assert order == ["first"]
    release.set()
    await queue.worker
    assert order == ["first"] + [str(index) for index in range(150)]
    assert queue.visible(1) == []


@pytest.mark.asyncio
async def test_cancel_waiting_job_and_reject_other_chat():
    queue = PlaylistQueue()
    release = asyncio.Event()
    started = asyncio.Event()

    async def run(item):
        started.set()
        await release.wait()

    active = queue.enqueue(job("first", run))
    await started.wait()
    waiting_runner = AsyncMock()
    waiting = queue.enqueue(job("second", waiting_runner))
    assert queue.cancel(waiting.id, 2) is None
    assert queue.visible(2) == []
    assert queue.cancel(waiting.id, 1).state == "cancelled"
    assert queue.cancel(waiting.id, 1) is None
    release.set()
    await queue.worker
    waiting_runner.assert_not_awaited()
    assert active.state == "completed"


@pytest.mark.asyncio
async def test_active_cancel_waits_for_cleanup_even_when_cancelled_twice():
    queue = PlaylistQueue()
    started = asyncio.Event()
    cleaning = asyncio.Event()
    release_cleanup = asyncio.Event()
    next_started = asyncio.Event()

    async def run(item):
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await release_cleanup.wait()

    active = queue.enqueue(job("first", run))
    queue.enqueue(job("second", lambda item: next_runner(item)))
    async def next_runner(item):
        next_started.set()
    await started.wait()
    assert queue.cancel(active.id, 1).state == "cancelling"
    await cleaning.wait()
    queue.cancel(active.id, 1)
    await asyncio.sleep(0)
    assert not next_started.is_set()
    release_cleanup.set()
    await queue.worker
    assert active.state == "cancelled"
    assert next_started.is_set()


@pytest.mark.asyncio
async def test_worker_failure_does_not_block_next_job():
    queue = PlaylistQueue()
    async def fail(item):
        raise RuntimeError("failed playlist")
    first = queue.enqueue(job("first", fail))
    second_runner = AsyncMock()
    second = queue.enqueue(job("second", second_runner))
    await queue.worker
    assert first.state == "failed"
    assert second.state == "completed"
    second_runner.assert_awaited_once()


@pytest.mark.asyncio
async def test_playlist_reports_success_and_failure_counts():
    import bot
    item = PlaylistJob(1, "stats", [{"id": "one"}, {"id": "two"}], "720", AsyncMock())
    with patch.object(bot.config, "PREFETCH_PLAYLIST", False), \
         patch.object(bot, "prepare_download", new=AsyncMock(side_effect=[RuntimeError("bad"), None])):
        await bot.process_playlist_downloads(AsyncMock(), AsyncMock(), 1, item.entries, "720", job=item)
    assert item.failed == 2
    assert item.completed == 2
    assert item.current == 2


@pytest.mark.asyncio
async def test_repeated_quality_callback_enqueues_selection_only_once():
    import re
    from types import SimpleNamespace
    import bot

    session = bot.session_manager.create_playlist_session(1, "queue test", "url", [{"id": "one"}])
    session.select_all()
    callback = SimpleNamespace(
        matches=[re.match(r"^pl_q:(.+):(.+)", f"pl_q:{session.session_id}:720")],
        message=SimpleNamespace(chat=SimpleNamespace(id=1), edit_text=AsyncMock()),
        answer=AsyncMock(),
    )
    with patch.object(bot.playlist_queue, "enqueue") as enqueue:
        await asyncio.gather(bot.callback_pl_start_download(AsyncMock(), callback),
                             bot.callback_pl_start_download(AsyncMock(), callback))
    enqueue.assert_called_once()
    assert enqueue.call_args.args[0].total == 1


@pytest.mark.asyncio
async def test_cancelled_thread_download_stops_at_hook_and_preserves_cancellation():
    import threading
    import bot

    started = threading.Event()
    invoke_hook = threading.Event()
    hook_finished = threading.Event()
    def download(url, quality, hook, height):
        started.set()
        invoke_hook.wait(3)
        try:
            hook({"status": "downloading"})
        finally:
            hook_finished.set()

    with patch.object(bot.config, "STREAM_VIDEO_UPLOADS", False), \
         patch.object(bot, "download_semaphore", asyncio.Semaphore(1)), \
         patch.object(bot, "download_media", side_effect=download):
        task = asyncio.create_task(bot.prepare_download(AsyncMock(), "url", "audio"))
        assert await asyncio.to_thread(started.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        invoke_hook.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert hook_finished.is_set()


def test_queue_view_paginates_and_escapes_playlist_titles():
    import bot
    queue = PlaylistQueue()
    for index in range(20):
        queue.pending.append(job("<unsafe>" + str(index), AsyncMock()))
    queue.pending.append(job("another chat", AsyncMock(), chat=2))
    with patch.object(bot, "playlist_queue", queue):
        text, keyboard = bot.playlist_queue_view(1)
        assert "20 active/waiting" in text
        assert "Page 1/4" in text
        assert "&lt;unsafe&gt;" in text
        assert "another chat" not in text
        assert len(keyboard.inline_keyboard) == 7
        text, keyboard = bot.playlist_queue_view(1, 999)
        assert "Page 4/4" in text
