"""Opt-in live Telegram protocol/byte-integrity check; posts no chat messages.

Run from the repository: python tools/check_streamed_upload.py
Uses API_ID/API_HASH/BOT_TOKEN from the existing environment/.env, a fresh
in-memory session, and about 15 MiB of unposted test documents. It validates
unknown-length RPC finalization and downloads each resulting document to check
its SHA-256. This checks protocol correctness, not hosting upload bandwidth.
"""
import asyncio
import hashlib
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyrogram import Client, raw

import config
import streaming
from upload_tuning import configure_crypto_workers


async def check(client, size):
    payload = (bytes(range(256)) * ((size + 255) // 256))[:size]
    first_ack = asyncio.Event()
    acknowledged, requests = set(), []
    original_session = streaming.Session

    class ObservedSession(original_session):
        async def invoke(self, rpc, *args, **kwargs):
            if isinstance(rpc, raw.functions.upload.SaveBigFilePart):
                assert rpc.bytes, "Empty live file part"
                if rpc.file_total_parts != -1:
                    assert set(range(rpc.file_total_parts - 1)) <= acknowledged
                requests.append((rpc.file_part, rpc.file_total_parts, len(rpc.bytes)))
            response = await super().invoke(rpc, *args, **kwargs)
            if isinstance(rpc, raw.functions.upload.SaveBigFilePart) and response is True:
                acknowledged.add(rpc.file_part)
                if rpc.file_total_parts == -1:
                    assert not reader.eof or reader.overlapped
                    reader.overlapped = reader.overlapped or not reader.eof
                    first_ack.set()
            return response

    class Source:
        offset = 0
        eof = False
        overlapped = False

        async def readexactly(self, count):
            # EOF cannot arrive until at least one real streamed RPC has been
            # acknowledged. Serial download-then-upload would deadlock here.
            if self.offset >= 2 * streaming.PART_SIZE and not self.overlapped:
                await asyncio.wait_for(first_ack.wait(), config.TRANSFER_STAGE_TIMEOUT)
            block = payload[self.offset:self.offset + count]
            self.offset += len(block)
            if len(block) < count:
                self.eof = True
                raise asyncio.IncompleteReadError(block, count)
            return block

    reader = Source()
    streaming.Session = ObservedSession
    stats = streaming.TransferStats()
    try:
        file, actual = await streaming.upload_stream(client, reader, "stream-protocol-check.bin",
            estimated_size=size, stats=stats, check_header=False)
    finally:
        streaming.Session = original_session
    assert actual == size and reader.offset == size and reader.overlapped
    assert requests[-1][1] == file.parts and requests[-1][2] > 0
    assert all(total == -1 for _, total, _ in requests[:-1])
    # UploadMedia creates a reusable file reference without sending a message.
    media = await asyncio.wait_for(client.invoke(raw.functions.messages.UploadMedia(
        peer=raw.types.InputPeerSelf(),
        media=raw.types.InputMediaUploadedDocument(file=file, mime_type="application/octet-stream",
            attributes=[raw.types.DocumentAttributeFilename(file_name="stream-protocol-check.bin")]))),
        config.TRANSFER_STAGE_TIMEOUT)
    document = media.document
    assert document.size == size
    location = raw.types.InputDocumentFileLocation(id=document.id, access_hash=document.access_hash,
                                                   file_reference=document.file_reference, thumb_size="")
    digest, offset = hashlib.sha256(), 0
    while offset < size:
        response = await asyncio.wait_for(client.invoke(raw.functions.upload.GetFile(
            location=location, offset=offset, limit=streaming.PART_SIZE, cdn_supported=False)),
            config.TRANSFER_STAGE_TIMEOUT)
        block = response.bytes
        assert block and len(block) <= size - offset
        digest.update(block)
        offset += len(block)
    assert digest.digest() == hashlib.sha256(payload).digest()
    print(f"PASS bytes={size} parts={file.parts} overlap=True SHA256=verified retries={stats.retry_events}",
          flush=True)


async def main():
    if config.validate_config():
        raise RuntimeError("Configure API_ID, API_HASH and BOT_TOKEN before the live check")
    configure_crypto_workers()
    client = Client("stream-protocol-check", api_id=config.API_ID, api_hash=config.API_HASH,
                    bot_token=config.BOT_TOKEN, in_memory=True, no_updates=True, workers=1)
    try:
        await asyncio.wait_for(client.start(), 60)
        for size in (streaming.PART_SIZE * 4, streaming.PART_SIZE * 2 + 17,
                     streaming.PART_SIZE * 24 + 123):
            await asyncio.wait_for(check(client, size), 180)
    finally:
        if client.is_connected:
            await asyncio.wait_for(client.stop(), 15)


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    asyncio.run(main())
