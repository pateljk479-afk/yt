import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from web import create_web_app


class WebAppTestCase(AioHTTPTestCase):
    async def get_application(self):
        return create_web_app()

    async def test_root_endpoint(self):
        resp = await self.client.request("GET", "/")
        assert resp.status == 200
        data = await resp.json()
        assert data.get("status") == "online"

    async def test_health_endpoint(self):
        resp = await self.client.request("GET", "/health")
        assert resp.status == 200
        data = await resp.json()
        assert data.get("status") == "ok"
        assert "uptime_seconds" in data
        assert "max_file_size_bytes" in data
