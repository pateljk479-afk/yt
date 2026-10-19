import asyncio
from types import SimpleNamespace

import aiohttp
from aiohttp import web
import pytest
import pytest_asyncio

import config
from direct_download import DirectRangeRelay


@pytest_asyncio.fixture
async def cdn():
    payload = bytes(range(256)) * 14000
    state = {"requests": [], "active": 0, "peak": 0, "ignore": False, "bad_range": False,
             "short": False, "fail_once": False}

    async def serve(request):
        value = request.headers.get("Range", "")
        state["requests"].append((value, dict(request.headers)))
        if state["ignore"]:
            return web.Response(body=payload)
        start, end = map(int, value.removeprefix("bytes=").split("-"))
        state["active"] += 1
        state["peak"] = max(state["peak"], state["active"])
        try:
            # Deliberately finish later ranges first to test byte ordering.
            await asyncio.sleep(0.01 if start % 2 else 0.03)
            data = payload[start:end + 1]
            if state["short"] and end > 0:
                data = data[:-1]
            if state["fail_once"] and end > 0:
                state["fail_once"] = False
                data = data[:-1]
            header = f"bytes {start}-{end}/{len(payload)}"
            if state["bad_range"] and end > 0:
                header = f"bytes 0-{end}/{len(payload)}"
            return web.Response(status=206, headers={"Content-Range": header}, body=data)
        finally:
            state["active"] -= 1

    app = web.Application()
    app.router.add_get("/source", serve)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/source", payload, state
    finally:
        await runner.cleanup()


def plan(url):
    return SimpleNamespace(info={"http_headers": {"User-Agent": "range-test", "Proxy-Authorization": "secret"}},
                           formats=[{"url": url}])


@pytest.mark.asyncio
async def test_ranges_parallel_order_seek_and_direct_proxy_bypass(cdn, monkeypatch):
    url, payload, state = cdn
    monkeypatch.setattr(config, "STREAM_DOWNLOAD_CHUNK_BYTES", 256 * 1024)
    monkeypatch.setenv("http_proxy", "http://127.0.0.1:1")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:1")
    async with DirectRangeRelay(plan(url)) as relay:
        assert relay.urls[0] != url
        async with aiohttp.ClientSession(trust_env=False) as client:
            async with client.get(relay.urls[0], headers={"Range": "bytes=100-3000000"}) as response:
                assert response.status == 206
                assert await response.read() == payload[100:3000001]
            async with client.get(relay.urls[0], headers={"Range": "bytes=-100"}) as response:
                assert await response.read() == payload[-100:]
            async with client.head(relay.urls[0]) as response:
                assert response.content_length == len(payload)
            async with client.get(relay.urls[0], headers={"Range": "bytes=99999999-"}) as response:
                assert response.status == 416
        assert relay.failure is None
        assert state["peak"] > 1
        assert state["peak"] <= config.STREAM_DOWNLOAD_WORKERS
    assert all("Proxy-Authorization" not in headers for _, headers in state["requests"])
    assert all(headers["User-Agent"] == "range-test" for _, headers in state["requests"])


@pytest.mark.asyncio
async def test_ignore_range_server_falls_back_without_buffering_body(cdn):
    url, _, state = cdn
    state["ignore"] = True
    async with DirectRangeRelay(plan(url)) as relay:
        assert relay.urls == [url]
        assert not relay.routes


@pytest.mark.asyncio
async def test_short_range_retries_same_bytes(cdn):
    url, payload, state = cdn
    state["fail_once"] = True
    async with DirectRangeRelay(plan(url)) as relay:
        async with aiohttp.ClientSession() as client:
            async with client.get(relay.urls[0], headers={"Range": "bytes=100-200"}) as response:
                assert await response.read() == payload[100:201]
        assert relay.failure is None
    assert sum(value == "bytes=100-200" for value, _ in state["requests"]) == 2


@pytest.mark.asyncio
async def test_wrong_range_cannot_publish_corrupt_bytes(cdn):
    url, _, state = cdn
    state["bad_range"] = True
    async with DirectRangeRelay(plan(url)) as relay:
        async with aiohttp.ClientSession() as client:
            with pytest.raises(aiohttp.ClientPayloadError):
                async with client.get(relay.urls[0], headers={"Range": "bytes=100-200"}) as response:
                    await response.read()
        assert relay.failure is not None


@pytest.mark.asyncio
async def test_relay_cancels_a_disconnected_reader(cdn, monkeypatch):
    url, _, state = cdn
    monkeypatch.setattr(config, "STREAM_DOWNLOAD_CHUNK_BYTES", 256 * 1024)
    async with DirectRangeRelay(plan(url)) as relay:
        async with aiohttp.ClientSession() as client:
            async with client.get(relay.urls[0]) as response:
                await response.content.readexactly(1)
        await asyncio.sleep(0.15)
    assert state["active"] == 0

