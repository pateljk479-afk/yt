import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pyrogram import raw

import ordered_session
from ordered_session import OrderedMediaSession, MediaTransportError


def session_with_transport(monkeypatch, send):
    session = OrderedMediaSession(None, 2, bytes(256), False, is_media=True)
    counter = iter(range(1, 100))
    session.msg_factory = lambda data: SimpleNamespace(msg_id=next(counter))
    session.connection = SimpleNamespace(send=send)
    def pack(message, *args):
        # Concurrent, unordered encryption would finish message 2 before 1.
        time.sleep(0.02 if message.msg_id == 1 else 0.001)
        return message.msg_id
    monkeypatch.setattr(ordered_session.mtproto, "pack", pack)
    return session


def query():
    return raw.functions.upload.SaveBigFilePart(
        file_id=1, file_part=0, file_total_parts=1, bytes=b"payload")


@pytest.mark.asyncio
async def test_wire_is_ordered_but_replies_are_pipelined_and_may_arrive_out_of_order(monkeypatch):
    written, all_written = [], asyncio.Event()
    async def write(payload):
        written.append(payload)
        if len(written) == 8:
            all_written.set()
    session = session_with_transport(monkeypatch, write)
    tasks = [asyncio.create_task(session.send(query(), timeout=2)) for _ in range(8)]
    try:
        await asyncio.wait_for(all_written.wait(), 1)
        # All eight requests reached the socket before any reply arrived.
        assert written == list(range(1, 9))
        assert len(session.results) == 8 and not any(task.done() for task in tasks)
        for msg_id in reversed(written):
            result = session.results[msg_id]
            result.value = True
            result.event.set()
        assert await asyncio.gather(*tasks) == [True] * 8
        assert not session.results
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_reply_timeout_and_cancel_remove_pending_results_without_poisoning_wire(monkeypatch):
    sent = asyncio.Event()
    async def write(payload):
        sent.set()
    session = session_with_transport(monkeypatch, write)
    with pytest.raises(TimeoutError, match="timed out"):
        await session.send(query(), timeout=0.01)
    assert not session.results and not session._wire_failed
    sent.clear()
    task = asyncio.create_task(session.send(query()))
    await sent.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not session.results and not session._wire_failed


@pytest.mark.asyncio
async def test_cancelled_packet_write_cannot_leave_a_sequence_gap_on_reused_transport(monkeypatch):
    writing = asyncio.Event()
    async def interrupted_write(payload):
        writing.set()
        await asyncio.Event().wait()
    session = session_with_transport(monkeypatch, interrupted_write)
    task = asyncio.create_task(session.send(query()))
    await writing.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(MediaTransportError, match="fresh session"):
        await session.send(query())
    assert not session.results


@pytest.mark.asyncio
async def test_rejected_sequence_retires_transport_without_losing_result_cleanup(monkeypatch):
    async def write(msg_id):
        result = session.results[msg_id]
        result.value = raw.types.BadMsgNotification(bad_msg_id=msg_id, bad_msg_seqno=1, error_code=33)
        result.event.set()
    session = session_with_transport(monkeypatch, write)
    response = await session.send(query())
    assert isinstance(response, raw.types.BadMsgNotification) and not session.results
    with pytest.raises(MediaTransportError, match="fresh session"):
        await session.send(query())


@pytest.mark.asyncio
async def test_salt_recovery_resends_through_ordered_writer_and_acks_need_no_result(monkeypatch):
    sent = []
    async def write(msg_id):
        sent.append(msg_id)
        result = session.results.get(msg_id)
        if result is not None:
            result.value = (raw.types.BadServerSalt(bad_msg_id=msg_id,
                bad_msg_seqno=1, error_code=48, new_server_salt=123)
                if msg_id == 1 else True)
            result.event.set()
    session = session_with_transport(monkeypatch, write)
    assert await session.send(query()) is True
    assert session.salt == 123 and sent == [1, 2]
    await session.send(raw.types.MsgsAck(msg_ids=[1]), wait_response=False)
    assert sent == [1, 2, 3] and not session.results


@pytest.mark.asyncio
async def test_rpc_validation_error_is_preserved_for_permanent_failure_handling(monkeypatch):
    from pyrogram.errors import FilePartsInvalid
    async def write(msg_id):
        result = session.results[msg_id]
        result.value = raw.types.RpcError(error_code=400, error_message="FILE_PARTS_INVALID")
        result.event.set()
    session = session_with_transport(monkeypatch, write)
    with pytest.raises(FilePartsInvalid):
        await session.send(query())
    assert not session.results


@pytest.mark.asyncio
async def test_disconnect_wakes_pending_rpcs_and_never_reopens_a_socket(monkeypatch):
    sent = asyncio.Event()
    async def write(msg_id):
        sent.set()
    session = session_with_transport(monkeypatch, write)
    task = asyncio.create_task(session.send(query(), timeout=30))
    await sent.wait()
    session.start = AsyncMock()
    await session.restart()
    with pytest.raises(MediaTransportError, match="connection closed"):
        await asyncio.wait_for(task, 0.5)
    session.start.assert_not_awaited()
    assert session._wire_failed and not session.results


@pytest.mark.asyncio
@pytest.mark.parametrize("method,args", [("ping_worker", ()), ("handle_packet", (b"packet",))])
async def test_retired_background_workers_finish_without_unhandled_errors(monkeypatch, method, args):
    session = session_with_transport(monkeypatch, AsyncMock())
    monkeypatch.setattr(ordered_session.Session, method,
                        AsyncMock(side_effect=MediaTransportError("retired")))
    await getattr(session, method)(*args)
