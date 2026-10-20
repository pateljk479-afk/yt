import asyncio
import math
import struct

import pytest

from video_parts import FragmentedVideo, plan_parts


def box(kind, payload):
    return struct.pack(">I4s", len(payload) + 8, kind) + payload


def initialization():
    tkhd = box(b"tkhd", bytes(12) + struct.pack(">I", 1))
    mdhd = box(b"mdhd", bytes(12) + struct.pack(">I", 1000))
    hdlr = box(b"hdlr", bytes(8) + b"vide")
    trak = box(b"trak", tkhd + box(b"mdia", mdhd + hdlr))
    trex = box(b"trex", struct.pack(">6I", 0, 1, 1, 1000, 0, 0))
    return box(b"ftyp", b"isom" + bytes(8)) + box(b"moov", trak + box(b"mvex", trex))


def fragment(index, size=128, keyframe=True):
    tfhd = box(b"tfhd", struct.pack(">4I", 0x020028, 1, 1000, 0))
    tfdt = box(b"tfdt", struct.pack(">II", 0, index * 1000))
    trun = box(b"trun", struct.pack(">III", 4, 1, 0 if keyframe else 0x10000))
    return box(b"moof", box(b"traf", tfhd + tfdt + trun)), struct.pack(">I4s", size + 8, b"mdat")


def reader(data):
    result = asyncio.StreamReader()
    result.feed_data(data)
    result.feed_eof()
    return result


async def drain(part, chunk=97):
    blocks = []
    while True:
        try:
            blocks.append(await part.readexactly(chunk))
        except asyncio.IncompleteReadError as exc:
            blocks.append(exc.partial)
            return b"".join(blocks)


@pytest.mark.parametrize("gib,count", [(1.65, 1), (2, 2), (2.2, 2), (2.5, 2), (3.3, 2), (7, 5), (7.5, 5)])
def test_balanced_plan_keeps_quality_and_fewest_estimated_parts(gib, count):
    limit = 1700 * 1024 * 1024
    parts, target = plan_parts(int(gib * 1024**3), limit)
    assert parts == count and target <= limit
    assert math.ceil(gib * 1024**3 / limit) == parts


@pytest.mark.asyncio
@pytest.mark.parametrize("estimate", [0, 300, 10000])
async def test_split_has_init_every_part_and_rebased_timing(estimate):
    init = initialization()
    fragments = [fragment(i) for i in range(7)]
    raw = init + b"".join(moof + header + bytes([i]) * 128 for i, (moof, header) in enumerate(fragments))
    limit = len(init) + len(fragments[0][0]) * 2 + (8 + 128) * 2
    source = await FragmentedVideo(reader(raw), limit, estimate).start()
    chunks, durations = [], []
    while (part := source.next_part()) is not None:
        chunk = await drain(part)
        assert chunk.startswith(init) and len(chunk) <= limit
        assert len(chunk) == part.size
        first_tfdt = chunk.index(b"tfdt") + 8
        assert struct.unpack_from(">I", chunk, first_tfdt)[0] == 0
        chunks.append(chunk)
        durations.append(part.duration)
    assert len(chunks) >= 4
    assert sum(durations) == 7
    # Every source fragment payload appears exactly once across the outputs.
    for i in range(7):
        assert sum(chunk.count(bytes([i]) * 128) for chunk in chunks) == 1


@pytest.mark.asyncio
async def test_oversize_gop_and_non_keyframe_are_explicit_errors():
    moof, header = fragment(0)
    source = await FragmentedVideo(reader(initialization() + moof + header + bytes(128)), 100).start()
    with pytest.raises(ValueError, match="keyframe interval"):
        await drain(source.next_part())
    moof, header = fragment(0, keyframe=False)
    source = await FragmentedVideo(reader(initialization() + moof + header + bytes(128)), 1000).start()
    with pytest.raises(ValueError, match="start at a keyframe"):
        await drain(source.next_part())


@pytest.mark.asyncio
async def test_non_keyframe_fragments_stay_with_their_keyframe():
    init = initialization()
    flags = [True, False, False, True, False, False]
    fragments = [fragment(i, keyframe=flag) for i, flag in enumerate(flags)]
    raw = init + b"".join(moof + header + bytes([i]) * 128
                          for i, (moof, header) in enumerate(fragments))
    limit = len(init) + 3 * (len(fragments[0][0]) + 8 + 128)
    source = await FragmentedVideo(reader(raw), limit).start()
    parts = []
    while (part := source.next_part()) is not None:
        parts.append(await drain(part))
    assert len(parts) == 2
    assert all(data.count(b"moof") == 3 for data in parts)


@pytest.mark.asyncio
async def test_metadata_and_truncation_cannot_be_published():
    with pytest.raises(ValueError, match="parsing limit"):
        await FragmentedVideo(reader(struct.pack(">I4s", 100_000_000, b"ftyp")), 1000).start()
    moof, header = fragment(0)
    source = await FragmentedVideo(reader(initialization() + moof + header + bytes(127)), 1000).start()
    with pytest.raises(ValueError, match="Truncated"):
        await drain(source.next_part())


@pytest.mark.asyncio
async def test_seven_gib_source_is_five_parts_with_bounded_reads():
    """Generate payload lazily: regression for a source much larger than host RAM."""
    size = 7 * 1024**3
    fragment_size = 8 * 1024**2
    total_fragments = size // fragment_size

    class LazyMovie:
        def __init__(self):
            self.pending = initialization()
            self.payload = self.index = self.max_read = 0

        async def readexactly(self, count):
            self.max_read = max(self.max_read, count)
            blocks, length = [], 0
            while length < count:
                if self.pending:
                    block, self.pending = self.pending[:count - length], self.pending[count - length:]
                elif self.payload:
                    take = min(count - length, self.payload)
                    block = bytes(take)
                    self.payload -= take
                elif self.index < total_fragments:
                    moof, header = fragment(self.index, fragment_size)
                    self.pending = moof + header
                    self.payload = fragment_size
                    self.index += 1
                    continue
                else:
                    raise asyncio.IncompleteReadError(b"".join(blocks), count)
                blocks.append(block)
                length += len(block)
            return b"".join(blocks)

    lazy = LazyMovie()
    limit = 1700 * 1024**2
    source = await FragmentedVideo(lazy, limit, size).start()
    sizes = []
    while (part := source.next_part()) is not None:
        total = 0
        while True:
            try:
                total += len(await part.readexactly(512 * 1024))
            except asyncio.IncompleteReadError as exc:
                total += len(exc.partial)
                break
        sizes.append(total)
    assert len(sizes) == 5
    assert all(value <= limit for value in sizes)
    assert sum(sizes) > size  # repeated init + fragment metadata
    assert lazy.max_read <= 512 * 1024
