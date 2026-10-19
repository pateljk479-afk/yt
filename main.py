import os
import sys
import asyncio
import logging
from pyrogram import idle

import config
from bot import app
from web import start_web_server
from proxy_check import check_proxy
from downloader import get_js_runtimes
from storage import cleanup_stale_downloads

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("main")


async def main():
    logger.info("==================================================")
    logger.info(" Starting YouTube Telegram Downloader Service ")
    logger.info("==================================================")

    # 1. Validate configuration
    config_errors = config.validate_config()
    if config_errors:
        logger.error("Configuration validation failed:")
        for err in config_errors:
            logger.error("  - %s", err)
        logger.error("\nPlease configure .env file or environment variables before running.")
        logger.error("Refer to .env.example and README.md for setup instructions.")
        sys.exit(1)

    logger.info("Performance & Environment settings:")
    logger.info("  - Telegram Max File Size: %d MB", config.MAX_FILE_SIZE // (1024 * 1024))
    logger.info("  - Video transfers: %s", "direct streaming (no media files on disk)"
                if config.STREAM_VIDEO_UPLOADS else "download to disk before upload")
    logger.info("  - Max Concurrent Downloads: %d", config.MAX_CONCURRENT_DOWNLOADS)
    logger.info("  - Fragment Parallel Streams: %d", config.CONCURRENT_FRAGMENT_DOWNLOADS)
    logger.info("  - aria2c Accelerated Downloader: %s", "Enabled" if config.ARIA2C_AVAILABLE else "Disabled (yt-dlp native multithread)")
    if config.COOKIES_FILE and os.path.exists(config.COOKIES_FILE):
        logger.info("  - YouTube Cookies: Active (%s, %.1f KB)", os.path.basename(config.COOKIES_FILE), os.path.getsize(config.COOKIES_FILE) / 1024)
    else:
        logger.info("  - YouTube Cookies: None configured (optional)")
    if getattr(config, "YOUTUBE_PROXIES", None):
        logger.info("  - YouTube Egress Proxy: Enabled (%d proxies configured in pool)", len(config.YOUTUBE_PROXIES))
        for idx, p in enumerate(config.YOUTUBE_PROXIES, 1):
            p_status = await asyncio.to_thread(check_proxy, p)
            logger.info("    [Proxy %d/%d] %s", idx, len(config.YOUTUBE_PROXIES), p_status)
    elif config.YOUTUBE_PROXY:
        logger.info("  - YouTube Egress Proxy: Enabled")
        proxy_status = await asyncio.to_thread(check_proxy, config.YOUTUBE_PROXY)
        logger.info("YouTube proxy check: %s", proxy_status)
    logger.info("  - Web Port (Render): %d", config.PORT)
    runtimes = get_js_runtimes()
    if runtimes:
        logger.info("  - YouTube JS runtime: Deno at %s", runtimes["deno"]["path"])
    else:
        logger.warning("YouTube JS runtime missing: install requirements.txt before downloading videos")

    # Ensure downloads directory exists
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)
    removed = await asyncio.to_thread(cleanup_stale_downloads)
    logger.info("Startup cleanup removed %d abandoned download folders", removed)

    # 2. Start lightweight HTTP health check server for Render hosting
    web_runner = None
    if config.ENABLE_WEB_SERVER:
        try:
            web_runner = await start_web_server()
        except Exception as e:
            logger.warning("Could not start web health check server: %s", e)

    # Clear any stale webhooks to ensure Telegram delivers updates via MTProto
    try:
        import urllib.request
        import json
        webhook_del_url = f"https://api.telegram.org/bot{config.BOT_TOKEN}/deleteWebhook"
        req = urllib.request.Request(webhook_del_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
            logger.info("Telegram webhook status reset: %s", data.get("description", "OK"))
    except Exception as e:
        logger.debug("Webhook reset check: %s", e)

    # 3. Start Pyrogram MTProto Bot Client
    logger.info("Connecting Telegram Bot client...")
    await app.start()
    bot_info = await app.get_me()
    logger.info("Bot successfully authenticated as @%s (ID: %s)", bot_info.username, bot_info.id)
    logger.info("==================================================")
    logger.info("🚀 Server marked as running and ready!")
    logger.info("   Bot username : @%s", bot_info.username)
    logger.info("   Send /start to your bot on Telegram to begin.")
    logger.info("==================================================")

    # 4. Heartbeat task to keep logs active and monitor uptime
    async def heartbeat():
        minute_counter = 0
        while True:
            await asyncio.sleep(300)  # every 5 minutes
            minute_counter += 5
            logger.info("💓 [HEARTBEAT] Bot @%s is active & listening (uptime: %d min)", bot_info.username, minute_counter)

    heartbeat_task = asyncio.create_task(heartbeat())

    # 5. Idle loop
    try:
        await idle()
    finally:
        heartbeat_task.cancel()
        logger.info("Shutting down service...")
        await app.stop()
        if web_runner:
            await web_runner.cleanup()
        # Worker threads may still be writing during shutdown. Clean only
        # bot-owned abandoned directories on next startup, never the whole root.
        logger.info("Shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        app.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Service exited.")
