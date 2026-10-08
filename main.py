import os
import sys
import asyncio
import logging
from pyrogram import idle

import config
from bot import app
from web import start_web_server
from uploader import safe_cleanup

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
    logger.info("  - Max Concurrent Downloads: %d", config.MAX_CONCURRENT_DOWNLOADS)
    logger.info("  - Fragment Parallel Streams: %d", config.CONCURRENT_FRAGMENT_DOWNLOADS)
    logger.info("  - aria2c Accelerated Downloader: %s", "Enabled" if config.ARIA2C_AVAILABLE else "Disabled (yt-dlp native multithread)")
    logger.info("  - Web Port (Render): %d", config.PORT)

    # Ensure downloads directory exists
    os.makedirs(config.DOWNLOAD_DIR, exist_ok=True)

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
        safe_cleanup(config.DOWNLOAD_DIR)
        logger.info("Shutdown completed cleanly.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Service exited.")
