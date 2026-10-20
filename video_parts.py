"""Split a fragmented MP4 pipe into playable bounded uploads, without staging.

Only ISO BMFF metadata is buffered. mdat payloads pass through in small reads.
FFmpeg must emit keyframe fragments and default-base-is-moof addressing.
Container structure: https://www.w3.org/TR/mse-byte-stream-format-isobmff/
"""
import asyncio
import logging
import math
import struct
from dataclasses import dataclass

logger = logging.getLogger(__name__)
MAX_METADATA = 4 * 1024 * 1024


def plan_parts(estimate, limit):
    """Minimum estimated count, balanced payload; runtime byte checks are final."""
    count = max(1, math.ceil(estimate / limit)) if estimate else 1
    # A nominally small or unknown video gets the full ceiling. Otherwise an
    # underestimate would needlessly turn it into many tiny attachments.
    return count, min(limit, math.ceil(estimate * 1.02 / count)) if count > 1 else limit


def _boxes(data, start=0, end=None):
    end = len(data) if end is None else end
    while start < end:
        if end - start < 8:
            raise ValueError("Truncated MP4 box header")
        size, kind = struct.unpack_from(">I4s", data, start)
        header = 8
        if size == 1:
            if end - start < 16:
                raise ValueError("Truncated extended MP4 box header")
            size, header = struct.unpack_from(">Q", data, start + 8)[0], 16
        if size < header or start + size > end:
            raise ValueError("Invalid MP4 metadata box length")
        yield kind, start + header, start + size
        start += size


def _uint(data, offset):
    if offset + 4 > len(data):
        raise ValueError("Truncated MP4 metadata")
    return struct.unpack_from(">I", data, offset)[0]


def _tracks(moov):
    tracks, defaults = {}, {}
    for kind, begin, end in _boxes(moov):
        if kind != b"moov":
            continue
        for child, a, b in _boxes(moov, begin, end):
            if child == b"trak":
                track_id = timescale = None
                video = False
                for field, c, d in _boxes(moov, a, b):
                    if field == b"tkhd":
                        track_id = _uint(moov, c + (20 if moov[c] == 1 else 12))
                    if field == b"mdia":
                        for item, e, f in _boxes(moov, c, d):
                            if item == b"mdhd":
                                timescale = _uint(moov, e + (20 if moov[e] == 1 else 12))
                            elif item == b"hdlr":
                                video = moov[e + 8:e + 12] == b"vide"
                if not track_id or not timescale:
                    raise ValueError("Missing MP4 track timing")
                tracks[track_id] = (timescale, video)
            elif child == b"mvex":
                for item, c, d in _boxes(moov, a, b):
                    if item == b"trex":
                        defaults[_uint(moov, c + 4)] = (_uint(moov, c + 12), _uint(moov, c + 20))
    if not tracks or not any(video for _, video in tracks.values()):
        raise ValueError("No video track in streamed MP4")
    return tracks, defaults


@dataclass
class Fragment:
    moof: bytes
    mdat_header: bytes
    payload: int
    clocks: list
    start: float
    end: float
    keyframe: bool

    @property
    def size(self):
        return len(self.moof) + len(self.mdat_header) + self.payload


def _fragment(moof, header, payload, tracks, defaults):
    clocks, starts, ends = [], [], []
    keyframe = False
    for kind, a, b in _boxes(moof):
        if kind != b"moof":
            continue
        for child, c, d in _boxes(moof, a, b):
            if child != b"traf":
                continue
            fields = list(_boxes(moof, c, d))
            tfhd = next((e for k, e, f in fields if k == b"tfhd"), None)
            tfdt = next((e for k, e, f in fields if k == b"tfdt"), None)
            if tfhd is None or tfdt is None:
                raise ValueError("Missing MP4 fragment timing")
            flags = _uint(moof, tfhd) & 0xFFFFFF
            if flags & 1 or not flags & 0x020000:
                raise ValueError("MP4 fragments require relative data offsets")
            track_id = _uint(moof, tfhd + 4)
            if track_id not in tracks:
                raise ValueError("Unknown MP4 fragment track")
            scale, video = tracks[track_id]
            duration_default, sample_flags = defaults.get(track_id, (0, 0))
            pos = tfhd + 8
            if flags & 2:
                pos += 4
            if flags & 8:
                duration_default, pos = _uint(moof, pos), pos + 4
            if flags & 16:
                pos += 4
            if flags & 32:
                sample_flags = _uint(moof, pos)
            width = 8 if moof[tfdt] == 1 else 4
            decode = int.from_bytes(moof[tfdt + 4:tfdt + 4 + width], "big")
            clocks.append((tfdt + 4, width, decode, scale))
            ticks, first = 0, True
            for item, e, f in fields:
                if item != b"trun":
                    continue
                run_flags = _uint(moof, e) & 0xFFFFFF
                count, pos = _uint(moof, e + 4), e + 8
                if run_flags & 1:
                    pos += 4
                first_flags = sample_flags
                if run_flags & 4:
                    first_flags, pos = _uint(moof, pos), pos + 4
                stride = 4 * sum(bool(run_flags & flag) for flag in (256, 512, 1024, 2048))
                if pos + count * stride > f:
                    raise ValueError("Truncated MP4 sample table")
                if count and first:
                    first_pos = pos + (4 if run_flags & 256 else 0) + (4 if run_flags & 512 else 0)
                    if run_flags & 1024:
                        first_flags = _uint(moof, first_pos)
                    if video:
                        keyframe = not bool(first_flags & 0x10000)
                    first = False
                if run_flags & 256:
                    ticks += sum(_uint(moof, pos + i * stride) for i in range(count))
                else:
                    if count and not duration_default:
                        raise ValueError("Unknown MP4 sample duration")
                    ticks += count * duration_default
            starts.append(decode / scale)
            ends.append((decode + ticks) / scale)
    if not clocks:
        raise ValueError("Empty MP4 fragment")
    return Fragment(moof, header, payload, clocks, min(starts), max(ends), keyframe)


class FragmentedVideo:
    """One source pipe, one active part reader, and at most one metadata lookahead."""

    def __init__(self, reader, limit, estimated_size=0):
        self.reader, self.limit = reader, limit
        self.estimated_count, self.target = plan_parts(estimated_size, limit)
        self.init = b""
        self.tracks, self.defaults = {}, {}
        self.next_fragment = None
        self.eof = False
        self.index = 0
        self.source_bytes = 0
        self.active = None

    async def _header(self):
        try:
            header = await self.reader.readexactly(8)
        except asyncio.IncompleteReadError as exc:
            if exc.partial:
                raise ValueError("Truncated MP4 box") from exc
            return None
        size, kind = struct.unpack(">I4s", header)
        if size == 1:
            header += await self.reader.readexactly(8)
            size = struct.unpack(">Q", header[8:])[0]
        if size < len(header):
            raise ValueError("Unbounded or invalid streamed MP4 box")
        self.source_bytes += len(header)
        return kind, header, size - len(header)

    async def _metadata(self, header, size):
        if size + len(header) > MAX_METADATA:
            raise ValueError("MP4 metadata exceeds the bounded parsing limit")
        body = await self.reader.readexactly(size)
        self.source_bytes += len(body)
        return header + body

    async def start(self):
        while True:
            box = await self._header()
            if box is None:
                raise ValueError("Missing streamed MP4 initialization")
            kind, header, size = box
            if kind not in (b"ftyp", b"moov", b"free"):
                raise ValueError("Unexpected streamed MP4 initialization")
            data = await self._metadata(header, size)
            if kind != b"free":
                self.init += data
            if len(self.init) > MAX_METADATA:
                raise ValueError("MP4 initialization exceeds the parsing limit")
            if kind == b"moov":
                if self.init[4:8] != b"ftyp":
                    raise ValueError("Missing MP4 file type")
                self.tracks, self.defaults = _tracks(data)
                await self._advance()
                return self

    async def _advance(self):
        while True:
            box = await self._header()
            if box is None:
                self.eof, self.next_fragment = True, None
                return
            kind, header, size = box
            if kind in (b"mfra", b"free", b"sidx"):
                # Global seek indexes contain offsets for the original stream.
                # They are unnecessary for independently initialized fragments.
                await self._metadata(header, size)
                continue
            if kind != b"moof":
                raise ValueError("Expected an MP4 movie fragment")
            moof = await self._metadata(header, size)
            box = await self._header()
            if box is None or box[0] != b"mdat":
                raise ValueError("Missing MP4 fragment payload")
            _, header, size = box
            self.next_fragment = _fragment(moof, header, size, self.tracks, self.defaults)
            return

    def next_part(self):
        if self.active is not None and not self.active.done:
            raise RuntimeError("Drain the current video part before starting another")
        if self.eof:
            return None
        self.index += 1
        self.active = VideoPart(self, self.index)
        return self.active


class VideoPart:
    """StreamReader-compatible bounded view of complete keyframe fragments."""

    def __init__(self, source, index):
        self.source, self.index = source, index
        self.pending = source.init
        self.payload_left = 0
        self.size = len(self.pending)
        self.fragments = 0
        self.start = self.end = None
        self.origin = 0
        self.done = False

    @property
    def duration(self):
        return max(0, (self.end or 0) - (self.start or 0))

    async def readexactly(self, count):
        blocks, total = [], 0
        while total < count:
            try:
                block = await self._read(count - total)
            except asyncio.IncompleteReadError as exc:
                # A truncated source is NOT the normal EOF of a complete part.
                raise ValueError("Truncated MP4 fragment payload or metadata") from exc
            if not block:
                raise asyncio.IncompleteReadError(b"".join(blocks), count)
            blocks.append(block)
            total += len(block)
        return b"".join(blocks)

    async def _read(self, count):
        if self.pending:
            block, self.pending = self.pending[:count], self.pending[count:]
            return block
        if self.done:
            return b""
        if self.payload_left:
            block = await self.source.reader.readexactly(min(count, self.payload_left))
            self.payload_left -= len(block)
            self.source.source_bytes += len(block)
            if not self.payload_left:
                await self.source._advance()
            return block
        fragment = self.source.next_fragment
        if fragment is None:
            if not self.fragments:
                raise ValueError("Streamed video contains no media fragments")
            self.done = True
            return b""
        if len(self.source.init) + fragment.size > self.source.limit:
            raise ValueError("One video keyframe interval exceeds the part limit; cannot split without re-encoding")
        if self.fragments and fragment.keyframe and self.size + fragment.size > self.source.target:
            logger.debug("Video part %d cut before keyframe at %.3fs; size=%d target=%d",
                         self.index, fragment.start, self.size, self.source.target)
            self.done = True
            return b""
        if self.size + fragment.size > self.source.limit:
            raise ValueError("Cannot split at a safe keyframe within the video part limit")
        if not self.fragments:
            if not fragment.keyframe:
                raise ValueError("A video part must start at a keyframe")
            self.start = fragment.start
            self.origin = fragment.start if self.index > 1 else 0
        moof = bytearray(fragment.moof)
        for offset, width, decode, scale in fragment.clocks:
            adjusted = max(0, decode - round(self.origin * scale))
            moof[offset:offset + width] = adjusted.to_bytes(width, "big")
        self.pending = bytes(moof) + fragment.mdat_header
        self.payload_left = fragment.payload
        self.size += fragment.size
        self.fragments += 1
        self.end = max(self.end or 0, fragment.end)
        return await self._read(count)
