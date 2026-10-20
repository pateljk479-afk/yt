import asyncio
import re
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

import bot
import config
import streaming
from downloader import analyze_video_qualities
from keyboards import build_multi_resolution_keyboard, build_resolution_mode_keyboard
from playlist_queue import PlaylistJob
from resolution_options import ordered_qualities
from tests.test_streaming import metadata as source_metadata


def playlist():
    session = bot.session_manager.create_playlist_session(
        1, "Course <one>", "url", [{"id": "one"}, {"id": "two"}])
    session.select_all()
    return session


def video():
    return bot.session_manager.create_video_session(1, "url", analyze_video_qualities(source_metadata()))


def callback(data, chat=1):
    patterns = {
        "rq_mode": r"^rq_mode:(pl|vid):([^:]+):(single|multi)$",
        "rq_tg": r"^rq_tg:(pl|vid):([^:]+):([^:]+)$",
        "rq_go": r"^rq_go:(pl|vid):([^:]+)$",
        "rq_back": r"^rq_back:(pl|vid):([^:]+)$",
        "pl_dl": r"^pl_dl:(.+)$",
        "pl_q": r"^pl_q:(.+):(.+)$",
    }
    return SimpleNamespace(matches=[re.match(patterns[data.split(":")[0]], data)],
                           message=SimpleNamespace(chat=SimpleNamespace(id=chat), edit_text=AsyncMock()),
                           answer=AsyncMock())


def buttons(markup):
    return [button for row in markup.inline_keyboard for button in row]


@pytest.mark.asyncio
async def test_playlist_download_first_asks_resolution_mode():
    session = playlist()
    cb = callback(f"pl_dl:{session.session_id}")
    await bot.callback_pl_download_prompt(None, cb)
    options = buttons(cb.message.edit_text.call_args.kwargs["reply_markup"])
    assert [item.text for item in options[:2]] == ["🎬 Single Resolution", "☑️ Multiple Resolutions"]
    assert session.resolution_mode == "choice"


@pytest.mark.asyncio
async def test_video_request_first_asks_mode_and_keeps_analyzed_choices():
    status = SimpleNamespace(edit_text=AsyncMock(), id=10)
    message = SimpleNamespace(reply_text=AsyncMock(return_value=status), chat=SimpleNamespace(id=1))
    with patch.object(bot, "extract_info", return_value=source_metadata()):
        await bot.handle_video_request(None, message, "https://youtu.be/one")
    options = buttons(status.edit_text.call_args.kwargs["reply_markup"])
    assert options[0].text == "🎬 Single Resolution"
    assert options[1].text == "☑️ Multiple Resolutions"
    sid = options[0].callback_data.split(":")[2]
    session = bot.session_manager.get_video_session(sid)
    assert session.resolution_mode == "choice"
    assert session.analysis.max_quality.height == 1080


@pytest.mark.asyncio
async def test_switch_back_to_single_queues_only_one_quality():
    session = playlist()
    session.resolution_mode = "multi"
    session.selected_qualities.update(["max", "720"])
    await bot.callback_resolution_mode(None, callback(f"rq_mode:pl:{session.session_id}:single"))
    with patch.object(bot.playlist_queue, "enqueue") as enqueue:
        await bot.callback_pl_start_download(None, callback(f"pl_q:{session.session_id}:720"))
    job = enqueue.call_args.args[0]
    assert job.qualities == ("720",)
    with patch.object(bot, "process_playlist_downloads", new=AsyncMock()) as process:
        await job.runner(job)
    assert process.call_args.args[4] == "720"


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["pl", "vid"])
async def test_modes_toggles_and_back_preserve_checked_qualities(kind):
    session = playlist() if kind == "pl" else video()
    sid = session.session_id
    cb = callback(f"rq_mode:{kind}:{sid}:multi")
    await bot.callback_resolution_mode(None, cb)
    assert session.resolution_mode == "multi"
    assert session.selected_qualities == set()
    await bot.callback_resolution_toggle(None, callback(f"rq_tg:{kind}:{sid}:720"))
    await bot.callback_resolution_toggle(None, callback(f"rq_tg:{kind}:{sid}:max"))
    assert ordered_qualities(session.selected_qualities) == ("max", "720")
    options = buttons(build_multi_resolution_keyboard(kind, session))
    assert any(item.text == "✅ Best" for item in options)
    assert all(len(item.callback_data.encode()) <= 64 for item in options)
    await bot.callback_resolution_toggle(None, callback(f"rq_tg:{kind}:{sid}:720"))
    assert session.selected_qualities == {"max"}
    await bot.callback_resolution_back(None, callback(f"rq_back:{kind}:{sid}"))
    assert session.resolution_mode == "choice"
    cb = callback(f"rq_mode:{kind}:{sid}:single")
    await bot.callback_resolution_mode(None, cb)
    assert session.resolution_mode == "single"
    assert session.selected_qualities == {"max"}
    assert any(item.callback_data.startswith("pl_q:" if kind == "pl" else "vid:")
               for item in buttons(cb.message.edit_text.call_args.kwargs["reply_markup"]))


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["empty", "audio", "injected", "wrong_chat", "expired", "single"])
async def test_invalid_multiple_selection_never_enqueues(invalid):
    session = playlist()
    session.resolution_mode = "multi"
    if invalid == "audio":
        await bot.callback_resolution_toggle(None, callback(f"rq_tg:pl:{session.session_id}:audio"))
    elif invalid == "injected":
        await bot.callback_resolution_toggle(None, callback(f"rq_tg:pl:{session.session_id}:999999"))
    elif invalid != "empty":
        session.selected_qualities.add("720")
    if invalid == "expired":
        session.created_at = time.time() - config.SESSION_TTL_SECONDS - 10
    if invalid == "single":
        session.resolution_mode = "single"
    cb = callback(f"rq_go:pl:{session.session_id}", chat=2 if invalid == "wrong_chat" else 1)
    with patch.object(bot.playlist_queue, "enqueue") as enqueue:
        await bot.callback_resolution_start(None, cb)
    enqueue.assert_not_called()
    assert cb.answer.call_args.kwargs["show_alert"]
    assert bot.session_manager.get_playlist_session(session.session_id) is session


def test_video_options_only_include_analyzed_resolutions_and_state_is_independent():
    one, two = video(), video()
    one.selected_qualities.add("720")
    assert two.selected_qualities == set()
    choices = [item.callback_data.split(":")[-1] for item in buttons(build_multi_resolution_keyboard("vid", one))
               if item.callback_data.startswith("rq_tg:")]
    assert choices == ["max", "1080", "720", "480", "360", "240", "144"]
    assert "2160" not in choices
    assert len(buttons(build_resolution_mode_keyboard("vid", one.session_id))) == 3


@pytest.mark.asyncio
async def test_multi_start_is_once_and_queued_choices_are_frozen():
    session = playlist()
    session.resolution_mode = "multi"
    session.selected_qualities.update(["720", "max", "1080"])
    cb = callback(f"rq_go:pl:{session.session_id}")
    with patch.object(bot.playlist_queue, "enqueue") as enqueue:
        await asyncio.gather(bot.callback_resolution_start(None, cb), bot.callback_resolution_start(None, cb))
    enqueue.assert_called_once()
    job = enqueue.call_args.args[0]
    assert job.qualities == ("max", "1080", "720")
    assert job.quality == "Best, 1080p, 720p"
    assert job.total == 2
    session.selected_qualities.clear()
    assert job.qualities == ("max", "1080", "720")
    with patch.object(bot, "process_playlist_downloads", new=AsyncMock()) as run:
        await job.runner(job)
    assert run.call_args.args[4] == ("max", "1080", "720")


@pytest.mark.asyncio
async def test_single_video_multi_start_spawns_once():
    session = video()
    session.resolution_mode = "multi"
    session.selected_qualities.update(["max", "720"])
    cb = callback(f"rq_go:vid:{session.session_id}")
    tasks = []

    def capture(coro):
        tasks.append(coro)

    with patch.object(bot.asyncio, "create_task", side_effect=capture), \
         patch.object(bot, "process_multiple_resolutions", new=AsyncMock()) as process:
        await bot.callback_resolution_start(None, cb)
        await bot.callback_resolution_start(None, cb)
    assert len(tasks) == 1
    await tasks[0]
    assert process.call_args.args[-1] == ("max", "720")


@pytest.mark.asyncio
@pytest.mark.parametrize("prefetch", [False, True])
async def test_thirty_videos_three_qualities_order_metadata_reuse_and_bounded_work(prefetch, monkeypatch):
    monkeypatch.setattr(config, "PREFETCH_PLAYLIST", prefetch)
    formats = source_metadata()
    formats["formats"].insert(0, {**formats["formats"][0], "height": 2160, "url": "https://cdn.example/4k"})
    selected = ("max", "1080", "720")
    entries = [{"id": str(i), "title": str(i)} for i in range(30)]
    prepared_keys, delivered, cleanups, active, peak = [], [], [], 0, 0
    infos = {}

    async def prepare(status, url, key, metadata=None):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        info = infos.setdefault(url, {**formats, "title": url})
        if key == "max":
            assert metadata is None
        else:
            assert metadata is info
        prepared_keys.append((url, key))
        await asyncio.sleep(0)
        active -= 1
        return streaming.select_streams(info, key)

    async def transfer(client, status, chat, url, key, prepared_result=None, filename_quality=None):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        assert filename_quality == key
        delivered.append((url, key))
        await asyncio.sleep(0)
        active -= 1
        return True

    client = SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock()))
    status = AsyncMock()
    job = PlaylistJob(1, "Course", entries, "Best, 1080p, 720p", AsyncMock(), qualities=selected)
    with patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", side_effect=transfer), \
         patch.object(bot, "safe_cleanup", side_effect=cleanups.append):
        await bot.process_playlist_downloads(client, status, 1, entries, selected, job)
    expected = [(f"https://www.youtube.com/watch?v={i}", key) for i in range(30) for key in selected]
    assert delivered == prepared_keys == expected
    assert peak == 1 and len(cleanups) == 90
    assert job.successful == 30 and job.failed == 0
    assert job.successful_variants == 90
    assert job.stage == "Completed" and job.current == 30
    assert "Versions delivered: 90" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
async def test_best_same_as_1080_is_sent_once_and_failed_quality_does_not_skip_later_video():
    selected = ("max", "1080", "720", "360")
    status = AsyncMock()
    client = SimpleNamespace(send_message=AsyncMock(side_effect=lambda *args, **kwargs: AsyncMock()))

    async def prepare(message, url, key, metadata=None):
        if key == "360":
            raise ValueError("No available stream under requested height")
        return streaming.select_streams(metadata or source_metadata(), key)

    job = PlaylistJob(1, "Course", [{"id": "one"}, {"id": "two"}], "multiple", AsyncMock(), qualities=selected)
    with patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", new=AsyncMock(return_value=True)) as send, \
         patch.object(bot, "safe_cleanup") as cleanup:
        await bot.process_multiple_resolutions(client, status, 1, job.entries, selected, job)
    assert [call.args[4] for call in send.call_args_list] == ["max", "720", "max", "720"]
    assert job.skipped_variants == 2 and job.failed_variants == 2 and job.successful_variants == 4
    assert job.failed == 2 and job.completed == 2
    assert cleanup.call_count == 6


@pytest.mark.asyncio
async def test_cancel_stops_before_next_quality_and_releases_owned_result():
    entered = asyncio.Event()

    async def block(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    prepared = streaming.select_streams(source_metadata(), "max")
    with patch.object(bot, "prepare_download", new=AsyncMock(return_value=prepared)) as prepare, \
         patch.object(bot, "process_single_download", side_effect=block), \
         patch.object(bot, "safe_cleanup") as cleanup:
        task = asyncio.create_task(bot.process_multiple_resolutions(
            SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock())), AsyncMock(), 1,
            [{"id": "one"}, {"id": "two"}], ("max", "720")))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    prepare.assert_awaited_once()
    cleanup.assert_called_once_with(None)


@pytest.mark.asyncio
@pytest.mark.parametrize("key,actual,tag", [("max", 1080, "best_1080p"), ("1080", 720, "720p")])
async def test_stream_filename_uses_actual_height_and_best_label(key, actual, tag):
    plan = streaming.select_streams(source_metadata(), str(actual))
    with patch.object(bot, "prepare_download", new=AsyncMock(return_value=plan)), \
         patch.object(bot, "stream_video", new=AsyncMock()) as sender:
        assert await bot.process_single_download(AsyncMock(), AsyncMock(), 1, "url", key, filename_quality=key)
    assert sender.call_args.args[2].output_quality == tag


@pytest.mark.asyncio
async def test_prepare_reuses_stream_metadata_without_extracting_again():
    info = source_metadata()
    with patch.object(bot, "prepare_stream") as extract, patch.object(bot, "download_media") as disk:
        result = await bot.prepare_download(AsyncMock(), "url", "720", metadata=info)
    extract.assert_not_called()
    disk.assert_not_called()
    assert result.formats[0]["height"] == 720 and result.info is info


@pytest.mark.asyncio
async def test_failed_best_does_not_cause_1080_to_be_skipped():
    async def prepare(message, url, key, metadata=None):
        return streaming.select_streams(metadata or source_metadata(), key)

    status = AsyncMock()
    with patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", new=AsyncMock(side_effect=[False, True, True])) as send:
        await bot.process_multiple_resolutions(
            SimpleNamespace(send_message=AsyncMock(return_value=AsyncMock())), status, 1,
            [{"id": "one"}], ("max", "1080", "720"))
    assert [call.args[4] for call in send.call_args_list] == ["max", "1080", "720"]
    assert "Versions delivered: 2" in status.edit_text.call_args.args[0]
    assert "Failed versions: 1" in status.edit_text.call_args.args[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1000, 3 * 1024**3])
async def test_disk_fallback_names_and_splitter_keep_resolution_label(size):
    result = SimpleNamespace(temp_dir=None, file_path="course.mp4", title="Course", duration=20,
                             width=1280, height=720, file_size=size, is_audio=False, thumbnail_path=None)
    client = AsyncMock()
    with patch.object(bot, "prepare_download", new=AsyncMock(return_value=result)), \
         patch.object(bot, "verify_media_integrity", return_value=True), \
         patch.object(bot, "stream_local_video", new=AsyncMock()) as splitter:
        assert await bot.process_single_download(client, AsyncMock(), 1, "url", "1080", filename_quality="1080")
    if size <= bot.video_part_limit():
        assert client.send_video.call_args.kwargs["file_name"] == "Course_720p.mp4"
        assert "720p" in client.send_video.call_args.kwargs["caption"]
        splitter.assert_not_awaited()
    else:
        splitter.assert_awaited_once()
        assert splitter.call_args.args[2].output_quality == "720p"
        client.send_video.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_stream_propagates_quality_tag_to_playable_parts():
    result = SimpleNamespace(title="Course", duration=20, width=1920, height=1080,
                             file_path="course.mp4", file_size=3 * 1024**3, output_quality="1080p")
    with patch.object(streaming, "_stream_video", new=AsyncMock()) as send:
        await streaming.stream_local_video(None, 1, result)
    assert send.call_args.args[2].output_quality == "1080p"
