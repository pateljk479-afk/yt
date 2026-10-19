# ⚡ High-Speed YouTube & Playlist Downloader Telegram Bot

A production-ready Telegram Bot built with Python, Pyrogram (MTProto), and yt-dlp to download YouTube videos and playlists at maximum network speeds (saturating up to 600 Mbps connections), specifically engineered to handle Telegram's 2 GB file limits, preserve original video titles & thumbnails, provide 10-per-page interactive playlist selection, and run efficiently on modest hardware (2 GB RAM, 4 CPU @ 1.5 GHz) or cloud platforms like Render.

---

## 🌟 Key Features

1. **⚡ Maximum Speed Optimization (600 Mbps Tuned)**
   - **Video streaming without disk copies**: direct CDN inputs are merged by FFmpeg and uploaded immediately in 512 KiB Telegram chunks, with bounded memory and parallel upload connections.
   - **Regular downloader acceleration**: audio and manifest-only sources use yt-dlp fragment downloads and 10 MB HTTP chunks.
   - **C-accelerated MTProto encryption**: Integrates `TgCrypto` for lightning-fast Telegram chunk uploads.
   - **Aria2c integration**: Automatically detects and uses `aria2c` with 16 parallel connections if available on the system.
   - **Memory-bounded concurrency**: Protected by an `asyncio.Semaphore` queue to fully utilize 4 CPU cores while preventing Out-Of-Memory (OOM) errors on 2 GB RAM servers.

2. **🛡️ Smart 2 GB Limit Protection & Automatic Quality Degradation**
   - Telegram MTProto restricts regular bot file uploads to **2 GB (~2000 MB)**.
   - Format bitrates and stream sizes are evaluated before presenting choices.
   - **Automatic Degradation**: If 4K is available but exceeds 2 GB (e.g., 2.3 GB), 4K is **filtered out** from the choices. The bot automatically presents **2K (1440p)** or 1080p as the **⭐ Max Quality** option.
   - Quality options show clear estimated file sizes (e.g., `⭐ Max Quality (2K ~ 1.6 GB)`, `1080p (~850 MB)`, `720p (~450 MB)`).

3. **📋 Interactive 10-Per-Page Playlist Navigation & Multi-Selection**
   - Fast metadata extraction without downloading all videos upfront.
   - Displays **10 videos per page** with dynamic checkbox status (`✅` / `⬜`).
   - Clean navigation buttons: `⬅️ Prev`, `📄 Page X/Y`, `Next ➡️`.
   - Batch selection actions:
     - `✅ Select All`: Selects every video in the playlist at once.
     - `⬜ Deselect All`: Clears selection.
   - `📥 Download Selected (N)`: Triggers quality selection and downloads only the chosen videos sequentially.

4. **🎞️ Original Titles, Thumbnails & Streamable MP4 Container**
   - **Original Title**: Preserves the exact title from YouTube as the video file name (safely sanitized against filesystem illegal characters).
   - **Original Thumbnail**: Extracts and converts the video thumbnail to standard JPEG format (scaled to max 320x320) so the Telegram player displays it crisp and clear.
   - **Streamable MP4**: the streaming path produces fragmented MP4 with the initialization metadata at the front, without re-encoding. The disk path uses `-movflags +faststart`.
   - **Integrity Validation**: streamed videos require an MP4 header, successful FFmpeg completion and a duration check before publishing. Disk downloads use container header and `ffprobe` inspection.

5. **☁️ Cloud-Ready & Render Deployment**
   - Built-in lightweight `aiohttp` web server responding on `GET /` and `GET /health` to satisfy Render's port binding and HTTP health check requirements.
   - Direct execution via `python main.py` — **no Docker needed**.

---

## 📁 Repository Structure

```
.
├── main.py                 # Application entry point & service lifecycle
├── config.py               # Environment variables, tuning parameters, and limits
├── bot.py                  # Pyrogram bot client, commands, and callback query handlers
├── downloader.py           # yt-dlp integration, 2GB filtering, size estimation & downloads
├── streaming.py            # Direct FFmpeg muxing and bounded Telegram streamed uploads
├── keyboards.py            # Telegram inline keyboards (quality selection & playlist pagination)
├── session_manager.py      # In-memory session state for playlist pagination & selections
├── uploader.py             # Progress formatting, JPEG thumbnail conversion & MP4 validation
├── web.py                  # Aiohttp HTTP health check server for Render hosting
├── requirements.txt        # Python package dependencies
├── .env.example            # Sample configuration file
├── .gitignore              # Files and directories excluded from git
└── tests/                  # Comprehensive automated test suite (33 tests)
    ├── test_downloader.py      # 2GB degradation, audio size calculation & sanitization
    ├── test_uploader.py        # Progress bars, media integrity & thumbnail conversion
    ├── test_session_manager.py # 10-per-page pagination, toggle, select all
    ├── test_keyboards.py       # Inline keyboard layout & 64-byte callback validation
    ├── test_bot_handlers.py    # URL recognition & playlist detection
    ├── test_web.py             # Web server health endpoints
    └── test_edge_cases.py      # Boundary values, unicode emojis, huge files
```

---

## ⚙️ Configuration & Environment Variables

Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env
```

| Variable | Description | Default |
|---|---|---|
| `API_ID` | Telegram API ID from [my.telegram.org](https://my.telegram.org) | *Required* |
| `API_HASH` | Telegram API Hash from [my.telegram.org](https://my.telegram.org) | *Required* |
| `BOT_TOKEN` | Telegram Bot Token from [@BotFather](https://t.me/BotFather) | *Required* |
| `MAX_FILE_SIZE` | Max file size in bytes (Telegram 2GB ceiling) | `2097152000` (2000 MB) |
| `DOWNLOAD_DIR` | Directory for temporary video and thumbnail files | `./downloads` |
| `CONCURRENT_FRAGMENT_DOWNLOADS` | Number of parallel fragment downloads | `16` |
| `MAX_CONCURRENT_DOWNLOADS` | Max simultaneous video downloads | `3` |
| `YOUTUBE_PROXY` | Optional `http://user:pass@host:port` or `socks5://host:port` egress proxy for YouTube | *(unset)* |
| `PORT` | HTTP port for cloud health check (Render `$PORT`) | `8080` |
| `ENABLE_WEB_SERVER` | Enable health check web server | `true` |

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- Python 3.10+
- FFmpeg installed and available in system PATH:
  - **Ubuntu / Debian**: `sudo apt update && sudo apt install -y ffmpeg`
  - **macOS**: `brew install ffmpeg`
  - **Windows**: Download from [gyan.dev](https://www.gyan.dev/ffmpeg/builds/) or install via `winget install Gyan.FFmpeg`
- *(Optional for extra boost)* `aria2c`:
  - **Ubuntu / Debian**: `sudo apt install -y aria2`
  - **macOS**: `brew install aria2`

### 2. Install Dependencies
```bash
python -m pip install -r requirements.txt
```

### 3. Run the Bot Directly
```bash
python main.py
```

---

## 💬 Bot Commands & Usage

| Command | Usage | Description |
|---|---|---|
| `/start` | `/start` | Welcome message and bot feature overview |
| `/help` | `/help` | Detailed guide on downloading videos and playlists |
| `/video` | `/video <url>` | Analyze video, show qualities under 2GB, and download |
| `/playlist` | `/playlist <url>` | Open paginated 10-per-page selection interface |

*Note: You can also simply paste any YouTube video or playlist link directly into the chat!*

### User Flow Example (Single Video):
1. User sends `/video https://www.youtube.com/watch?v=...`
2. Bot analyzes stream formats and size limits.
3. If 4K is > 2GB (e.g. 2.3 GB), it is excluded. 2K (1.6 GB) is selected as **⭐ Max Quality**.
4. User selects `⭐ Max Quality (2K ~ 1.6 GB)` or `1080p (~850 MB)`.
5. Bot streams the download with real-time speed & progress updates.
6. Bot uploads the streamable MP4 with original YouTube title and thumbnail.

### User Flow Example (Playlist):
1. User sends `/playlist https://www.youtube.com/playlist?list=...`
2. Bot extracts playlist entries and displays Page 1 (10 videos with `⬜` checkmarks).
3. User navigates with `Next ➡️` / `⬅️ Prev` or clicks `✅ Select All`.
4. User clicks `📥 Download Selected (N)`.
5. User selects desired quality.
6. Bot downloads, merges and uploads each video together, preserving playlist order.
   The completed video appears as one Telegram message after successful transfer.
   Optional `PREFETCH_PLAYLIST=true` prepares the next video's metadata; streamed
   media is only fetched when its transfer starts. For audio and manifest-only
   sources, prefetch downloads the next file to disk. Progress-bar edits are at
   least five seconds apart (`PROGRESS_UPDATE_INTERVAL`, minimum 5).

---

## 🌐 Deploying to Render (Free or Paid)

1. **Push repository to GitHub**:
   ```bash
   git init
   git add .
   git commit -m "Initial commit: YouTube Telegram Downloader Bot"
   git remote add origin https://github.com/<your-username>/<your-repo>.git
   git push -u origin main
   ```
2. **Create New Web Service on Render**:
   - Go to [dashboard.render.com](https://dashboard.render.com) -> **New** -> **Web Service**.
   - Connect your GitHub repository.
   - **Environment**: Python
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python main.py`
3. **Set Environment Variables in Render**:
   - `API_ID`: Your Telegram API ID
   - `API_HASH`: Your Telegram API Hash
   - `BOT_TOKEN`: Your Telegram Bot Token
   - `PYTHON_VERSION`: `3.10.11`
4. Render automatically supplies `$PORT` and hits `GET /health` to keep the service healthy!

---

## 🧪 Running the Test Suite

### Minimizing paid YouTube proxy traffic

`STREAM_VIDEO_UPLOADS=true` is the default. For normal on-demand HTTP MP4/M4A
streams the flow is **YouTube CDN → FFmpeg stream copy → Telegram chunk upload**.
No complete input, merged output, or video chunk files are written to disk.
This allows a 1.5 GB video to transfer on a server with a 2 GB disk without
reserving another 1.5 GB for merging. This uses Telegram's documented
[streamed-upload protocol](https://core.telegram.org/api/files#streamed-uploads),
including unknown part counts until EOF; Telegram assembles one final video.

Uploads retain at most twice `UPLOAD_WORKERS` chunks of 512 KiB (4 MiB with
the default 4 workers), plus subprocess, network and FFmpeg buffers. A slower
Telegram connection pauses reading, which also pauses FFmpeg and the CDN input.
Chunks retry up to five times while their bytes remain in memory. Failed chunks,
source timeouts and cancellation stop the transfer and close the upload sessions.
Incomplete or failed FFmpeg output is never published to the chat. Actual byte
counts still enforce `MAX_FILE_SIZE` and the standard 2000 MiB upload ceiling.
Transfer speed depends on CDN and Telegram throughput as well as server bandwidth.

FFmpeg must be installed and on PATH. `STREAM_READ_TIMEOUT=90` bounds a stalled
source; `UPLOAD_WORKERS=4` controls upload parallelism (clamped to 1–16).
Set `STREAM_VIDEO_UPLOADS=false` to use the regular disk path.
Audio conversion and sources without complete HTTP MP4/M4A resources (such as
manifest-only formats) also use that path. Its temporary storage is bounded by
`DOWNLOAD_STORAGE_LIMIT_MB` (default 1536 MiB) and `MIN_FREE_DISK_MB=128`.
It still needs room for the downloaded inputs and merged/converted output.
Failed folders are removed immediately; startup removes only abandoned `yt_dl_*`
folders inside `DOWNLOAD_DIR`, not cookies, sessions or other server files.

With `YOUTUBE_PROXY` configured, YouTube metadata uses the proxy, but video,
audio and thumbnails ALWAYS use direct server connections. The streaming
FFmpeg process explicitly disables HTTP proxies and removes inherited proxy
environment variables; only complete HTTP resources are accepted as inputs.
For the regular disk path, native yt-dlp downloading is used instead of aria2
so request routing is enforced. Logs report direct routing. Telegram uploads
do not use the YouTube proxy.

Media proxy fallback has been removed entirely. Any old
`YOUTUBE_MEDIA_PROXY_FALLBACK` environment value is ignored, even `true`.
If YouTube rejects a direct media request, the download fails without retrying
through the paid proxy. Metadata still consumes proxy data.
Live streams requiring networked FFmpeg are rejected while the proxy is enabled,
because that downloader bypasses per-request routing. Local merging is unaffected.
Upload the updated Python files and restart the server to apply these changes.

Run all automated unit and integration tests with `pytest`:

```bash
python -m pytest -v
```

All 27 test cases test:
- 2 GB file degradation and max quality determination
- Audio size estimation from bitrates and formats
- Filename sanitization against filesystem forbidden characters
- Telegram 64-byte callback_data compliance
- 10-per-page playlist pagination and multi-video selection logic
- Web health check server endpoints
- MP4 container verification & JPEG thumbnail conversion
- Boundary conditions and edge cases (emojis, unicode, huge recordings)
