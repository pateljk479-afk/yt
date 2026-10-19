"""Bounded parallel CDN range reads behind a seekable loopback FFmpeg input."""
import asyncio
import logging
import re
import secrets
from collections import deque

import aiohttp
from aiohttp import web
from yarl import URL

import config

logger = logging.getLogger(__name__)


def media_headers(info, fmt):
    headers = {**(info.get("http_headers") or {}), **(fmt.get("http_headers") or {})}
    return {k: str(v) for k, v in headers.items()
            if k.lower() not in {"proxy-authorization", "proxy-connection", "range",
                                 "host", "connection", "content-length", "accept-encoding"}
            and "\r" not in str(k) + str(v) and "\n" not in str(k) + str(v)}


class DirectRangeRelay:
    """Download a few small ranges concurrently, deliver them in byte order.

    This does not use a paid egress proxy: the HTTP client connects directly to
    the CDN. The local server exists only to give FFmpeg seekable parallel input.
    Backpressure bounds pending data instead of filling the server's disk.
    """

    def __init__(self, plan):
        self.plan = plan
        self.workers = max(1, min(config.STREAM_DOWNLOAD_WORKERS, 8))
        self.chunk_size = max(256 * 1024, min(config.STREAM_DOWNLOAD_CHUNK_BYTES, 4 * 1024 * 1024))
        self.urls = [f["url"] for f in plan.formats]
        self.routes = {}
        self.client = self.runner = None
        self.failure = None
        self.network_bytes = 0
        self.token = secrets.token_urlsafe(24)

    async def __aenter__(self):
        self.client = aiohttp.ClientSession(
            trust_env=False, auto_decompress=False, headers={"Accept-Encoding": "identity"},
            timeout=aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=30),
            connector=aiohttp.TCPConnector(limit=self.workers * len(self.plan.formats)))
        try:
            if not config.STREAM_PARALLEL_DOWNLOADS:
                return self
            probes = await asyncio.gather(*(self._probe(i, f) for i, f in enumerate(self.plan.formats)))
            self.routes = {str(i): route for i, route in enumerate(probes) if route}
            if self.routes:
                app = web.Application()
                app.router.add_route("GET", "/{token}/{index}", self._serve)
                app.router.add_route("HEAD", "/{token}/{index}", self._serve)
                self.runner = web.AppRunner(app, access_log=None, shutdown_timeout=1)
                await self.runner.setup()
                site = web.TCPSite(self.runner, "127.0.0.1", 0)
                await site.start()
                port = site._server.sockets[0].getsockname()[1]
                for index in self.routes:
                    self.urls[int(index)] = f"http://127.0.0.1:{port}/{self.token}/{index}"
                logger.info("Direct CDN range downloads: %d connections/input, %d KiB ranges, %d inputs",
                            self.workers, self.chunk_size // 1024, len(self.routes))
            return self
        except BaseException:
            await self.__aexit__(None, None, None)
            raise

    async def __aexit__(self, *args):
        if self.runner:
            await self.runner.cleanup()
        if self.client:
            await self.client.close()

    async def _probe(self, index, fmt):
        headers = media_headers(self.plan.info, fmt)
        try:
            async with self.client.get(URL(fmt["url"], encoded=True),
                                       headers={**headers, "Range": "bytes=0-0"}) as response:
                if response.status != 206:
                    # Ignore-range servers stay on direct FFmpeg input. Never
                    # buffer their entire 200 response or retry through a proxy.
                    logger.info("Input %d cannot use HTTP ranges (status %d); keeping direct FFmpeg input",
                                index, response.status)
                    return None
                match = re.fullmatch(r"bytes 0-0/(\d+)", response.headers.get("Content-Range", ""))
                if not match or int(match[1]) <= 0:
                    return None
                byte = await response.content.readexactly(1)
                self.network_bytes += len(byte)
                return {"url": fmt["url"], "headers": headers, "size": int(match[1])}
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
            logger.info("Input %d range probe unavailable; keeping direct FFmpeg input", index)
            return None

    async def _fetch(self, route, start, end):
        expected = end - start + 1
        for attempt in range(3):
            try:
                async with self.client.get(URL(route["url"], encoded=True), headers={
                    **route["headers"], "Range": f"bytes={start}-{end}"}) as response:
                    if response.status != 206:
                        raise RuntimeError(f"Direct CDN range request failed (HTTP {response.status})")
                    if response.headers.get("Content-Range") != f"bytes {start}-{end}/{route['size']}":
                        raise RuntimeError("CDN returned a different byte range; refusing corrupt video")
                    if response.headers.get("Content-Encoding", "identity") != "identity":
                        raise RuntimeError("CDN returned an encoded byte range")
                    try:
                        data = await response.content.readexactly(expected + 1)
                    except asyncio.IncompleteReadError as exc:
                        data = exc.partial
                    if len(data) != expected:
                        raise OSError("Direct CDN range was incomplete")
                    self.network_bytes += len(data)
                    return data
            except (aiohttp.ClientError, OSError, asyncio.TimeoutError):
                if attempt == 2:
                    raise RuntimeError("Direct CDN range download failed after retries") from None
                await asyncio.sleep(0.25 * (attempt + 1))

    async def _serve(self, request):
        if request.match_info["token"] != self.token:
            raise web.HTTPNotFound()
        route = self.routes.get(request.match_info["index"])
        if not route:
            raise web.HTTPNotFound()
        size = route["size"]
        start, end = 0, size - 1
        requested_range = request.headers.get("Range")
        if requested_range:
            match = re.fullmatch(r"bytes=(\d*)-(\d*)", requested_range)
            if not match or not any(match.groups()):
                raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{size}"})
            if match[1]:
                start = int(match[1])
                end = min(int(match[2]), end) if match[2] else end
            else:
                start = max(0, size - int(match[2]))
            if start > end or start >= size:
                raise web.HTTPRequestRangeNotSatisfiable(headers={"Content-Range": f"bytes */{size}"})
        headers = {"Content-Length": str(end - start + 1), "Accept-Ranges": "bytes",
                   "Content-Type": "application/octet-stream"}
        if requested_range:
            headers["Content-Range"] = f"bytes {start}-{end}/{size}"
        response = web.StreamResponse(status=206 if requested_range else 200, headers=headers)
        await response.prepare(request)
        if request.method == "HEAD":
            return response
        pending = deque()
        next_start = start

        def schedule():
            nonlocal next_start
            while len(pending) < self.workers and next_start <= end:
                last = min(end, next_start + self.chunk_size - 1)
                pending.append(asyncio.create_task(self._fetch(route, next_start, last)))
                next_start = last + 1

        try:
            schedule()
            while pending:
                # Keep the current task in the deque until its result is read,
                # so cancellation also retrieves its exception.
                data = await pending[0]
                pending.popleft()
                await response.write(data)
                schedule()
            await response.write_eof()
        except (ConnectionResetError, asyncio.CancelledError):
            # FFmpeg closes and reopens input when seeking. That is normal.
            if request.transport:
                request.transport.close()
        except Exception as exc:
            self.failure = RuntimeError(str(exc) if isinstance(exc, RuntimeError)
                                        else "Direct CDN download failed")
            logger.warning("Direct CDN range transfer failed")
            if request.transport:
                request.transport.close()
        finally:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        return response
