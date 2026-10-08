import time
import logging
from aiohttp import web

import config

logger = logging.getLogger(__name__)

start_time = time.time()


async def health_check(request: web.Request) -> web.Response:
    """HTTP endpoint for health checks (e.g. Render, Koyeb, Railway)."""
    uptime_seconds = int(time.time() - start_time)
    return web.json_response(
        {
            "status": "ok",
            "service": "YouTube-Telegram-Downloader",
            "uptime_seconds": uptime_seconds,
            "aria2c_available": config.ARIA2C_AVAILABLE,
            "max_file_size_bytes": config.MAX_FILE_SIZE,
        }
    )


async def root_handler(request: web.Request) -> web.Response:
    """Root HTTP landing page."""
    return web.json_response(
        {
            "message": "YouTube Telegram Downloader Bot is running smoothly.",
            "status": "online",
        }
    )


def create_web_app() -> web.Application:
    """Create aiohttp web application."""
    app = web.Application()
    app.router.add_get("/", root_handler)
    app.router.add_get("/health", health_check)
    return app


async def start_web_server():
    """Start background web server on configured port."""
    if not config.ENABLE_WEB_SERVER:
        logger.info("Web server disabled by config.")
        return None

    app = create_web_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, config.HOST, config.PORT)
    await site.start()
    logger.info("Web health check server running at http://%s:%d", config.HOST, config.PORT)
    return runner
