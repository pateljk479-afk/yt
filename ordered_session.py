"""Pipelined Pyrogram media sessions with ordered MTProto wire writes.

Pyrogram 2.0.106 constructs a message before offloading encryption. Concurrent
send calls can therefore finish encryption in a different order. Serialize
message numbering, encryption and socket writes, but wait for replies outside
the lock so network latency does not serialize the entire upload.
"""
import asyncio
import logging

import pyrogram
from pyrogram import raw
from pyrogram.crypto import mtproto
from pyrogram.errors import RPCError
from pyrogram.session import Session
from pyrogram.session.session import Result

logger = logging.getLogger(__name__)


class MediaTransportError(RuntimeError):
    """Owner must replace this session; Pyrogram must not reconnect it secretly."""


class OrderedMediaSession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._wire_lock = asyncio.Lock()
        self._wire_failed = False

    def _retire(self, reason):
        self._wire_failed = True
        for result in tuple(self.results.values()):
            if result.value is None:
                result.value = MediaTransportError(reason)
                result.event.set()

    async def restart(self):
        # recv_worker schedules this on disconnection. An automatic reconnect
        # could reopen sockets after the upload owner has already cleaned up.
        # Attachment recovery owns replacement, so retire instead.
        self._retire("Media connection closed; a fresh owned session is required")

    async def ping_worker(self):
        try:
            await super().ping_worker()
        except MediaTransportError:
            # The uploader owns reconnection; finish this background worker
            # cleanly so Session.stop can still close its connection.
            logger.debug("Retired media session stopped its keepalive worker")

    async def handle_packet(self, packet):
        try:
            await super().handle_packet(packet)
        except MediaTransportError:
            # An ACK write can race with disconnection. Pending upload calls
            # were already notified by _retire; do not leak a packet task.
            logger.debug("Retired media session stopped its ACK worker")

    async def send(self, data, wait_response=True, timeout=Session.WAIT_TIMEOUT):
        async with self._wire_lock:
            if self._wire_failed:
                raise MediaTransportError("Media transport needs a fresh session after an interrupted write")
            message = self.msg_factory(data)
            msg_id = message.msg_id
            result = Result() if wait_response else None
            if result is not None:
                self.results[msg_id] = result
            try:
                payload = await self.loop.run_in_executor(
                    pyrogram.crypto_executor, mtproto.pack, message, self.salt,
                    self.session_id, self.auth_key, self.auth_key_id)
                await self.connection.send(payload)
            except BaseException as exc:
                # A cancelled/partial write may consume a sequence number
                # without delivering it. Do not send later messages through
                # this transport; staged-part recovery opens fresh sessions.
                self._retire("Media transport write was interrupted; a fresh session is required")
                self.results.pop(msg_id, None)
                if isinstance(exc, OSError):
                    # Session.start retries OSError internally without a
                    # budget. Use our own error so its caller owns recovery.
                    raise MediaTransportError(str(exc)) from exc
                raise

        if result is None:
            return None
        try:
            try:
                await asyncio.wait_for(result.event.wait(), timeout)
            except asyncio.TimeoutError:
                raise TimeoutError("Request timed out") from None
            response = result.value
        finally:
            # Keep a local Result reference. Cancellation or a late response
            # must never make a dictionary lookup raise KeyError.
            self.results.pop(msg_id, None)
        if response is None:
            raise TimeoutError("Request timed out")
        if isinstance(response, MediaTransportError):
            raise response
        if isinstance(response, raw.types.RpcError):
            query = data.query if isinstance(data, (raw.functions.InvokeWithoutUpdates,
                                                    raw.functions.InvokeWithTakeout)) else data
            RPCError.raise_it(response, type(query))
        if isinstance(response, raw.types.BadServerSalt):
            self.salt = response.new_server_salt
            return await self.send(data, wait_response, timeout)
        if isinstance(response, raw.types.BadMsgNotification):
            self._retire("Telegram rejected this transport's sequence; a fresh session is required")
            logger.warning("Media transport rejected MTProto message %s: error=%d",
                           msg_id, response.error_code)
        return response
