import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pyrogram import raw, enums
from pyrogram.parser import Parser
from pyrogram.errors import MessageIdInvalid

import bot
import chat_ui
import streaming
import transfer_runtime
from chat_ui import VideoCollection, video_caption
from resolution_options import nominal_height, resolution_label, resolution_tag


@pytest.mark.parametrize("height, expected", [
    (1078, 1080), (718, 720), (478, 480), (1082, 1080), (2158, 2160),
    (960, 960), (540, 540), (1040, 1040), (0, 0), (None, 0)])
def test_standard_resolution_labels_for_slightly_cropped_encodes(height, expected):
    assert nominal_height(height) == expected


def test_best_label_capitalization_and_filename_tag():
    assert resolution_tag("max", 1078) == "best_1080p"
    assert resolution_tag("720", 718) == "720p"
    assert resolution_label("best_1080p") == "Best · 1080p"
    assert resolution_tag("1080", 960) == "960p"


def test_clean_caption_keeps_title_quality_part_timeline_and_searchable_groups():
    group = VideoCollection(1, "https://youtube.com/watch?v=course")
    caption = video_caption("Spring Boot & JPA", "best_1080p", 1024**3, 120,
                            group, 2, 3600)
    assert "Spring Boot &amp; JPA" in caption
    assert "Best · 1080p" in caption and "Part 2" in caption
    assert "01:00:00 – 01:02:00" in caption
    assert group.tags("1080p") in caption
    assert group.tags("1080p") == group.tags("best_1080p")
    assert "1078p" not in caption


def test_nominal_tier_selection_never_modifies_actual_pixel_dimensions():
    info = {"title": "Cropped", "formats": [{"height": 1082, "width": 1920,
            "url": "https://cdn.example/file", "protocol": "https", "ext": "mp4",
            "vcodec": "avc1", "acodec": "mp4a"}]}
    plan = streaming.select_streams(info, "1080")
    assert plan.formats[0]["height"] == 1082
    assert resolution_tag("1080", plan.formats[0]["height"]) == "1080p"


@pytest.mark.asyncio
async def test_reply_groups_parts_by_quality_and_resolutions_by_course():
    group = VideoCollection(1, "course-one")
    client = SimpleNamespace(parser=Parser(None), resolve_peer=AsyncMock(return_value=raw.types.InputPeerSelf()),
                             rnd_id=Mock(side_effect=range(100, 200)), invoke=AsyncMock())
    requests = []
    async def send(request, **kwargs):
        requests.append(request)
        return SimpleNamespace(updates=[raw.types.UpdateMessageID(id=len(requests), random_id=request.random_id)])
    client.invoke.side_effect = send
    plan = streaming.StreamPlan({"title": "Course"}, [{"height": 1078, "width": 1920}], 100,
                                output_quality="best_1080p", collection=group)
    for key, part in [("best_1080p", 1), ("best_1080p", 2), ("720p", 1), ("720p", 2)]:
        plan.output_quality = key
        await streaming._publish_video_part(client, 1, plan,
            raw.types.InputFileBig(id=9, parts=1, name="course.mp4"), "course.mp4", 100, 60, index=part)
    assert [request.reply_to_msg_id for request in requests] == [None, 1, 1, 3]
    assert group.resolutions == {"1080p": 1, "720p": 3}
    assert requests[0].media.attributes[0].h == 1078
    assert all("#" + group.tag in request.message for request in requests)
    assert "Best · 1080p" in requests[0].message and "720p" in requests[2].message
    assert VideoCollection(1, "course-two").reply_to("720p") is None
    assert VideoCollection(1, "course-two").tag != group.tag
    assert VideoCollection(2, "course-one").tag != group.tag


@pytest.mark.asyncio
async def test_deleted_first_part_does_not_prevent_publication(monkeypatch):
    monkeypatch.setattr(transfer_runtime, "RETRY_BACKOFF", 0)
    group = VideoCollection(1, "course", root=5, resolutions={"1080p": 5})
    client = SimpleNamespace(parser=Parser(None), resolve_peer=AsyncMock(return_value=raw.types.InputPeerSelf()),
                             rnd_id=Mock(return_value=99), invoke=AsyncMock())
    replies = []
    async def send(request, **kwargs):
        replies.append(request.reply_to_msg_id)
        if len(replies) == 1:
            raise MessageIdInvalid()
        return SimpleNamespace(updates=[raw.types.UpdateMessageID(id=10, random_id=99)])
    client.invoke.side_effect = send
    plan = streaming.StreamPlan({"title": "Course"}, [{"height": 1080}], 100,
                                output_quality="1080p", collection=group)
    await streaming._publish_video_part(client, 1, plan,
        raw.types.InputFileBig(id=9, parts=1, name="course.mp4"), "course.mp4", 100, 60, index=2)
    assert replies == [5, None]
    assert group.root == 10 and group.resolutions["1080p"] == 10
    requests = [call.args[0] for call in client.invoke.call_args_list]
    assert requests[0] is requests[1] and requests[1].random_id == 99


def notice(message_id=10, media=None):
    return SimpleNamespace(chat=SimpleNamespace(id=1), id=message_id, media=media, delete=AsyncMock(return_value=True))


@pytest.mark.asyncio
async def test_terminal_notice_is_deleted_after_five_minutes_and_only_it_is_deleted():
    message, video = notice(), notice(11, enums.MessageMediaType.VIDEO)
    loop = asyncio.get_running_loop()
    handle = Mock()
    with patch.object(loop, "call_later", return_value=handle) as timer:
        chat_ui.expire_notice(message)
        assert timer.call_args.args[0] == 300
        fire = timer.call_args.args[1]
        chat_ui.expire_notice(video)
        assert timer.call_count == 1
    fire()
    await asyncio.sleep(0.02)
    message.delete.assert_awaited_once()
    video.delete.assert_not_awaited()
    await chat_ui.close_notice_cleanup()


@pytest.mark.asyncio
async def test_reusing_status_cancels_old_deletion_and_shutdown_releases_timers():
    message = notice()
    loop = asyncio.get_running_loop()
    first, second = Mock(), Mock()
    with patch.object(loop, "call_later", side_effect=[first, second]):
        chat_ui.expire_notice(message)
        chat_ui.expire_notice(message)
    first.cancel.assert_called_once()
    chat_ui.cancel_notice_cleanup(message)
    second.cancel.assert_called_once()
    await chat_ui.close_notice_cleanup()
    assert not chat_ui._deletions and not chat_ui._delete_tasks
    message.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_reusing_notice_at_expiration_cancels_inflight_deletion():
    message = notice()
    loop = asyncio.get_running_loop()
    with patch.object(loop, "call_later", return_value=Mock()) as timer:
        chat_ui.expire_notice(message)
        fire = timer.call_args.args[1]
    fire()
    chat_ui.cancel_notice_cleanup(message)
    await asyncio.sleep(0.02)
    message.delete.assert_not_awaited()
    await chat_ui.close_notice_cleanup()


@pytest.mark.asyncio
async def test_transfer_failure_schedules_terminal_status_cleanup_but_not_media():
    status = notice()
    status.edit_text = AsyncMock()
    plan = streaming.StreamPlan({"title": "Course"}, [{"height": 1078}], 100)
    with patch.object(bot, "stream_video", new=AsyncMock(side_effect=RuntimeError("failed"))), \
         patch.object(bot, "expire_notice") as expire:
        assert not await bot.process_single_download(AsyncMock(), status, 1, "url", "max", prepared_result=plan)
    expire.assert_called_once_with(status)


@pytest.mark.asyncio
async def test_duplicate_resolution_is_removed_without_a_skip_warning():
    statuses = []
    def make_status(*args, **kwargs):
        message = notice(len(statuses) + 1)
        message.edit_text = AsyncMock()
        statuses.append(message)
        return message
    client = SimpleNamespace(send_message=AsyncMock(side_effect=make_status))
    info = {"title": "Course", "formats": [{"height": 1078, "width": 1920,
            "url": "https://cdn.example/file", "protocol": "https", "ext": "mp4",
            "vcodec": "avc1", "acodec": "mp4a"}]}
    async def prepare(status, url, key, metadata=None):
        return streaming.select_streams(info, key)
    with patch.object(bot, "prepare_download", side_effect=prepare), \
         patch.object(bot, "process_single_download", new=AsyncMock(return_value=True)) as send, \
         patch.object(bot, "expire_notice"):
        await bot.process_multiple_resolutions(client, AsyncMock(), 1, [{"id": "course"}], ("max", "1080"))
    assert send.await_count == 1
    statuses[1].delete.assert_awaited_once()
    statuses[1].edit_text.assert_not_awaited()
