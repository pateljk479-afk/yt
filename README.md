# ⚡ High-Speed YouTube & Playlist Downloader Telegram Bot

A production-ready Telegram Bot built with Python, Pyrogram (MTProto), and yt-dlp to download YouTube videos and playlists at maximum network speeds (saturating up to 600 Mbps connections), specifically engineered to handle Telegram's 2 GB file limits, preserve original video titles & thumbnails, provide 10-per-page interactive playlist selection, and run efficiently on modest hardware (2 GB RAM, 4 CPU @ 1.5 GHz) or cloud platforms like Render.

---

## 🌟 Key Features

1. **⚡ Maximum Speed Optimization (600 Mbps Tuned)**
   - **Simultaneous video download and upload**: direct CDN inputs are merged by FFmpeg and uploaded in 512 KiB Telegram chunks while one bounded recovery file is written alongside the upload.
   - **Regular downloader acceleration**: audio and unsupported fragment-only sources use yt-dlp fragment downloads and 10 MB HTTP chunks.
   - **C-accelerated MTProto encryption**: Integrates `TgCrypto` for lightning-fast Telegram chunk uploads.
   - **Aria2c integration**: Automatically detects and uses `aria2c` with 16 parallel connections if available on the system.
   - **Memory-bounded concurrency**: Protected by an `asyncio.Semaphore` queue to fully utilize 4 CPU cores while preventing Out-Of-Memory (OOM) errors on 2 GB RAM servers.

2. **📦 Automatic Playable Video Parts at Original Quality**
   - The source video can exceed Telegram's per-attachment limit.
   - Format bitrates and stream sizes are evaluated before presenting choices.
   - **No size-based downgrade**: Max Quality selects the highest available resolution. Large videos arrive as `Title_part_01.mp4`, `Title_part_02.mp4`, etc.
   - Parts default to at most **1500 MiB** each, balanced from the size estimate. Cuts follow keyframes, preserving encoded video and audio without re-encoding.
   - Source download remains direct and overlaps Telegram upload. The current part has one bounded recovery copy, deleted when its upload ends. A whole source video is never held in RAM or staged on disk.
   - Quality options show clear estimated file sizes (e.g., `⭐ Max Quality (2K ~ 1.6 GB)`, `1080p (~850 MB)`, `720p (~450 MB)`).

3. **📋 Interactive 10-Per-Page Playlist Navigation & Multi-Selection**
   - Fast metadata extraction without downloading all videos upfront.
   - Displays **10 videos per page** with dynamic checkbox status (`✅` / `⬜`).
   - Clean navigation buttons: `⬅️ Prev`, `📄 Page X/Y`, `Next ➡️`.
   - Batch selection actions:
     - `✅ Select All`: Selects every video in the playlist at once.
     - `⬜ Deselect All`: Clears selection.
   - `📥 Download Selected (N)`: Triggers quality selection and downloads only the chosen videos sequentially.
   - Choose **Single Resolution** for the existing flow, or **Multiple Resolutions** to check several qualities and press Start.
   - Multiple mode delivers Best, then selected heights from highest to lowest, for each video before moving to the next.

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
├── downloader.py           # yt-dlp integration, quality/size estimation & disk fallbacks
├── streaming.py            # Direct FFmpeg muxing and bounded Telegram streamed uploads
├── video_parts.py          # Bounded playable MP4 parts with keyframe cuts and rebased clocks
├── direct_download.py      # Parallel CDN ranges through a seekable loopback input
├── keyboards.py            # Telegram inline keyboards (quality selection & playlist pagination)
├── resolution_options.py   # Resolution tiers, readable labels & stream identity
├── chat_ui.py              # Video reply collections, captions & five-minute notice cleanup
├── session_manager.py      # In-memory session state for playlist pagination & selections
├── uploader.py             # Progress formatting, JPEG thumbnail conversion & MP4 validation
├── web.py                  # Aiohttp HTTP health check server for Render hosting
├── requirements.txt        # Python package dependencies
├── .env.example            # Sample configuration file
├── .gitignore              # Files and directories excluded from git
└── tests/                  # Comprehensive automated test suite (33 tests)
    ├── test_downloader.py      # Quality preservation, audio sizing & sanitization
    ├── test_video_parts.py     # Large logical streams, split boundaries, clocks & integrity
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
| `VIDEO_PART_SIZE_MB` | Hard playable-part ceiling in MiB (maximum 1700); one current part is staged then deleted | `1500` |
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
| `/video` | `/video <url>` | Analyze all qualities, transfer and auto-split large videos |
| `/playlist` | `/playlist <url>` | Open paginated 10-per-page selection interface |
| `/queue` | `/queue` or `/queue 2` | Show active/waiting playlists, processed/sent/failed counts, current video, and cancel buttons |
| `/cancelqueue` | `/cancelqueue <job-id>` | Cancel a waiting or running playlist in this chat |

*Note: You can also simply paste any YouTube video or playlist link directly into the chat!*

After selecting videos and quality, each playlist joins one FIFO queue. There is
no configured limit on waiting playlists (available server memory is the practical
limit). Only one playlist runs at a time; the next starts automatically after
completion, failure, or cancellation cleanup. `/queuestatus` and `/playlistqueue`
are aliases for `/queue`. Refresh the queue view to see updated progress. Queue
views are paginated, and each chat sees/cancels only its own jobs. Cancelling an
active job stops remaining items and requests transfer cancellation; an existing
worker thread or prefetched item must finish cleanup before the next job starts.
Already uploaded videos remain in Telegram. The queue is in memory and is cleared
when the bot restarts; the bot drains the active job before a normal shutdown.

### User Flow Example (Single Video):
1. User sends `/video https://www.youtube.com/watch?v=...`
2. Bot analyzes stream formats and size limits.
3. Even if 4K is 7 GB, it remains the **⭐ Max Quality** option.
4. User chooses Single Resolution and a quality, or Multiple Resolutions and several qualities.
5. Bot streams the download with real-time speed & progress updates.
6. Bot publishes the MP4 or numbered playable parts with title, thumbnail and source time range.

### User Flow Example (Playlist):
1. User sends `/playlist https://www.youtube.com/playlist?list=...`
2. Bot extracts playlist entries and displays Page 1 (10 videos with `⬜` checkmarks).
3. User navigates with `Next ➡️` / `⬅️ Prev` or clicks `✅ Select All`.
4. User clicks `📥 Download Selected (N)`.
5. User chooses Single Resolution for one quality, or Multiple Resolutions, checks Best / 1080p / 720p / other heights, then presses Start.
6. Bot downloads, merges and uploads each video together, preserving playlist order.
   Each version appears as a video or numbered playable parts.
   Optional `PREFETCH_PLAYLIST=true` prepares the next video's metadata; streamed
   media is only fetched when its transfer starts. For audio and manifest-only
   sources, prefetch downloads the next file to disk. Progress-bar edits are at
   least five seconds apart (`PROGRESS_UPDATE_INTERVAL`, minimum 5).

### Multiple-resolution downloads

The choice is available for single videos and after playlist video selection.
Single Resolution retains the previous quality/audio buttons.
Multiple Resolutions has checkboxes, a selected-count Start button, Back and
Cancel. Best is first, followed by 2160p, 1440p, 1080p, 720p, 480p, 360p,
240p and 144p. Single-video choices follow the existing format analysis; playlist
choices are common target heights because individual videos may differ.

For example, a 30-video playlist with Best, 1080p and 720p sends video 1's Best
version (all parts), then its 1080p version (all parts), then its 720p version,
before starting video 2. Qualities are fetched from the source, without encoding
lower-resolution copies locally. Supported streaming variants reuse extracted
metadata within the current video; the bot does not stage several full videos
or raise simultaneous transfer limits. Multi mode processes variants sequentially
even when `PREFETCH_PLAYLIST=true`.

Multi-mode filenames label the actual result:

```
Spring Boot course_best_2160p.mp4
Spring Boot course_1080p_part_01.mp4
Spring Boot course_1080p_part_02.mp4
Spring Boot course_720p.mp4
```

Captions show the same quality label. The part-size limit and keyframe-based
splitter apply independently to each version. The available quality tier is used
in filenames, so a requested 1080p version that resolves to 720p is named 720p.
Slightly cropped encodes such as 1078/718 pixels use the familiar 1080p/720p
labels; substantially different heights such as 960p retain their actual height.
The original pixel dimensions and encoded media stay intact. Best captions read
`Best · 1080p`, with `best_1080p` in filenames. A source is never lowered merely to
fit the per-attachment size ceiling.

If Best selects the same stream as 1080p, that stream is uploaded once, under
its Best filename; the duplicate placeholder is deleted immediately and the
skip is recorded in `/logs` and queue counts. Likewise,
different requested heights resolving to an already-delivered stream are
skipped. Duplicate detection runs before upload; for streaming inputs it
requires no duplicate media download. A failed version is not marked delivered,
so another selected option resolving to that stream can still be attempted.
Disk-only fallbacks identify duplicates after preparation and may require the
additional disk download.

The playlist remains one FIFO job. `/queue` shows its qualities, current version,
video completion counts, delivered/failed versions and duplicate skips. A video
is complete only when every selected version succeeded or was already delivered.
A failed version does not prevent trying later qualities/videos. Cancellation
stops further variants and drains current cleanup; already delivered media stays
available. Repeated Start taps consume the session once and cannot enqueue a
second job. The same in-memory session and queue restart limitations still apply.

Videos now carry compact title/quality/part captions plus two searchable tags:
one for the course and one for that course's resolution. Every later part replies
to the first video of its resolution; the first video of another resolution replies
to the course's original video. Tapping the reply preview navigates back to the
matching first part. This uses native Telegram replies instead of separate folder
messages and keeps each part available immediately. Private bot chats do not have
per-course folders; albums would require holding all parts until the batch is ready.
If a reply target is deleted, publication can proceed without that reference.

Bot-owned transfer errors, cancellations and completion summaries automatically
delete five minutes after their terminal update. Successful progress placeholders
and duplicate notices are removed immediately. Media messages and user commands
are never scheduled for this cleanup; a reused active status cancels its old timer.
Deletion uses bounded retries and logs failures without disrupting transfers.
Timers run in the bot process and are cancelled at shutdown; notices left by an
earlier deployment are not retroactively deleted after a restart.

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
including unknown transport-chunk counts until EOF. Each playable MP4 part gets
its own Telegram file identity; transport chunks are not separate chat files.

### Large videos without quality reduction

The source size is analyzed before transfer (exact CDN range sizes replace
approximate metadata when available). `VIDEO_PART_SIZE_MB=1500` sets the hard
per-attachment ceiling in MiB, also bounded by `MAX_FILE_SIZE`. A 7–7.5 GiB video
typically needs five parts; a 2–2.5 GiB video needs two. The initial count is the
minimum estimate divided by the hard ceiling, rounded up. Targets balance bytes
across those parts with up to 2% muxing room, without adding a part purely for
that reserve. Unknown sizes use the full ceiling, and actual bytes always override estimates. Variable
bitrate, estimates and keyframe placement can change the final part count or
duration. Part boundaries follow video keyframes rather than exact hour marks.

One FFmpeg process copies the selected encoded streams into fragmented MP4.
`video_parts.py` reads only bounded metadata, repeats the initialization for
each attachment, and rebases fragment clocks for independent playback while
preserving audio/video timing. Media payload passes through in 512 KiB reads.
The 128 MiB producer queue and existing parallel Telegram windows remain active;
neither the full video nor a full 1500 MiB part is held in RAM. The current
completed part is briefly staged on disk to obtain the exact positive Telegram
file-part count required by `upload.saveBigFilePart`, then removed immediately
after it is sent.
An 8 MiB FFmpeg fragment-size target also bounds normal muxer read-ahead. No new
Python dependency, transcoding or repeated download is required.

Parts arrive immediately after their upload completes, with filenames such as
`Spring Boot course_part_01.mp4`, original quality, per-part duration and original
timeline range in captions. Small videos retain their original filename. Logs
record the initial plan and each part's byte count, time range and publication.
Progress resets its byte/speed counters for each numbered part. If a later part
fails, already sent parts stay available and the error reports the partial
delivery. Within the running job, source/FFmpeg failures get two automatic
retries. The stream is reconstructed from the start with bounded memory;
confirmed parts are read and discarded rather than uploaded or posted again.
The original size plan and part boundaries are checked before continuing.
This can require re-downloading earlier bytes; it does not require staging a
full file. Recovery across a bot restart is not implemented.

The splitter rejects malformed/truncated fragments and never publishes a
part that starts without a keyframe. The last part additionally requires a
successful FFmpeg exit and full-source duration check. Extremely long keyframe
intervals that cannot fit the hard limit fail explicitly rather than degrading
quality. Audio conversion and fragment-only formats without a playable HTTP/HLS
URL may still use the disk fallback; that path requires enough local space.

The direct downloader requests small HTTP ranges in parallel (8 connections
and 2 MiB ranges per input by default), preserving byte order and supporting
FFmpeg seeks. This avoids depending on one unbounded CDN request. A loopback
HTTP input connects FFmpeg to these reads; CDN traffic itself is always direct.
Range responses and byte counts are validated before forwarding. Sources that
ignore Range stay on direct FFmpeg input. No paid proxy fallback is used.

`STREAM_BUFFER_MB=128` bounds the upload source queue. Uploads start with
`UPLOAD_WORKERS=8` independent MTProto connections and
`UPLOAD_REQUESTS_PER_CONNECTION=8` outstanding requests per connection
(64 chunks / 32 MiB in flight). Replies overlap to hide network latency.
`ordered_session.py` holds a per-session lock from message numbering through
encryption and the socket write, then releases it before waiting for the reply.
This prevents encryption completion from reordering MTProto sequence numbers
without forcing each chunk to wait for the previous reply. Cancellation or
failure during a wire write retires that transport; late replies and response
timeouts remove their result entries safely. A disconnected media transport
wakes its pending requests and retires; Pyrogram cannot reopen its sockets in
an unowned background reconnect after the upload has been cleaned up.
`UPLOAD_ADAPTIVE=true` backs off on retries/throttling and can expand a lower
configured starting window after sustained delivery, up to
`UPLOAD_MAX_WORKERS=8` connections and `UPLOAD_MAX_REQUESTS_PER_CONNECTION=8`
outstanding requests each. Extra media sessions handshake in the background
while ready sessions continue sending chunks. Throughput trials exclude
handshake and partial-window intervals, so connection setup cannot falsely
reject a faster setting. Logs report ready connections and usable capacity,
and final draining updates the in-flight count as individual requests finish.
Existing environment overrides are respected: installations explicitly using
`UPLOAD_WORKERS=4` / `UPLOAD_REQUESTS_PER_CONNECTION=4` should set both to `8`
to use the full starting window. The ordered transport, terminal-chunk barrier,
bounded recovery file and two-retry recovery policy remain enabled.

The live path follows [Telegram's streamed-upload protocol](https://core.telegram.org/api/files#streamed-uploads):
all uploads with an unknown length use `SaveBigFilePart`, including streams
smaller than 10 MiB. Intermediate RPCs use `file_total_parts=-1`. A one-chunk
lookahead retains the final nonempty 512 KiB chunk (or shorter tail), waits for
all previous RPCs to be acknowledged across every session, then sends that
terminal chunk with the exact final count. An aligned EOF therefore needs no
empty payload. Estimates affect progress and video splitting, never the
protocol's final count. Known-size recovery uploads continue to use the
standard positive-count protocol and the correct small-file MD5 when needed.

`live_spool.py` starts a single source producer and a concurrent reader over a
growing unbuffered recovery file. A condition wakes readers when bytes become
available or a source error occurs. Source run-ahead is bounded by the configured
window plus one 2 MiB write block; upload RAM buffering remains bounded
separately. The producer checks the attachment limit before every write. Disk
failures are terminal rather than triggering repeated CDN downloads. On upload
transport exhaustion, the same producer finishes just the current part, then
at most two known-size attempts use fresh sessions and smaller windows. This
recovery path never restarts the CDN source. Source/FFmpeg failure recovery
remains separate and can require replaying the source as described above.

For an opt-in real Telegram check, run `python tools/check_streamed_upload.py`
with the bot's existing credentials configured. It uses a separate in-memory
session and about 15 MiB of unposted test documents. It checks aligned/partial
EOF, a file above 10 MiB, actual ACKs before source EOF, final part counts and
SHA-256 after downloading the resulting documents. It sends no chat messages
and measures protocol correctness rather than hosting network bandwidth.
Only saturated transfers with queued bytes are expanded. Each setting is
measured over two `UPLOAD_TUNE_INTERVAL=5` second samples. A trial must improve
throughput by at least 10% to be kept; otherwise the previous setting is restored.
Capacity checks resume after `UPLOAD_RECOVERY_SECONDS=30`, so an early slowdown
cannot permanently lock a long video into a small window. Temporary connection
errors retry their own chunk without reducing all upload connections. Real
FloodWait pauses new requests for the required duration and reduces concurrency;
recovery retains the original configured levels and a minimum of two connections
(or one when configured with one). In-flight requests finish normally as the
window shrinks. A confirmed setting from a large completed transfer is reused
for the next file on the same client/DC, within configured limits, for up to
30 minutes. Throttled settings are discarded, and no video bytes are cached.

`CRYPTO_WORKERS=4` replaces Pyrogram's shared single-worker encryption pool
before connecting the bot, allowing packet encryption/decryption to run on
multiple threads. TgCrypto remains required for native cryptographic acceleration.
FFmpeg continues stream-copying without re-encoding or changing quality.
Free connection slots take the next chunk; slow connections do not hold up
healthy ones. Sessions start gradually, and both staging and upload status
edits run outside the data path. These buffers plus CDN ranges and subprocess/network/FFmpeg buffers
use RAM, not disk. Once the buffer fills, backpressure pauses downloading.
Chunks get an initial attempt and at most two retries while their bytes remain
in memory. Failed chunks, source timeouts and cancellation close the upload sessions.
If chunk recovery is exhausted, the current recovery attachment can be retried
twice with fresh sessions, fewer connections and a smaller request depth,
without downloading its bytes again. Exact total-parts validation, no empty
EOF requests and publication idempotency remain enforced with pipelining.
Incomplete current parts are not published; previously completed parts survive
a later error. Actual bytes enforce limits per attachment, not per source video.
Transfer speed depends on CDN and Telegram throughput as well as server bandwidth.

Uploading all bytes and publishing the Telegram message are separate stages.
After upload, the chat shows **Publishing to Telegram** and the logs record
each publication attempt, deadline, elapsed time and result. Final publication
uses `TRANSFER_STAGE_TIMEOUT=60` seconds per attempt and at most two retries
(three attempts total, with 2s and 4s backoff). All publication retries use the
same uploaded file and `SendMedia.random_id`, preventing duplicate messages
when Telegram accepted a request but its response was lost. Pyrogram's hidden
retry/FloodWait loops are disabled for these requests. Permanent RPC errors
fail immediately; a FloodWait longer than the stage budget is reported instead
of blocking the playlist for minutes or hours. An exhausted publication/chunk
budget is not multiplied by restarting the entire video.

Media-session startup and chunk requests also have stage deadlines; thumbnail
preparation/upload has a total `THUMBNAIL_TIMEOUT=20` seconds and can be skipped.
`TRANSFER_CLEANUP_TIMEOUT=10` bounds session, FFmpeg and relay cleanup, with
transport closure attempted if a session stop fails. Status sends/edits/deletes
use `TRANSFER_STATUS_TIMEOUT=10` and cannot turn a successfully published part
into a failed transfer. Cancellation stops retries. After recovery is exhausted,
the failed variant is reported and the playlist continues; confirmed parts stay
in the chat. These deadlines cover asynchronous network/subprocess waits while
the bot's event loop is running, not a crashed process or a suspended host.

FFmpeg must be installed and on PATH. `STREAM_READ_TIMEOUT=90` bounds a stalled
source; `UPLOAD_WORKERS=8` controls initial upload parallelism (clamped to 1–8).
`STREAM_PARALLEL_DOWNLOADS=false` disables parallel ranges for troubleshooting;
`STREAM_DOWNLOAD_WORKERS` (1–8) and `STREAM_DOWNLOAD_CHUNK_KB` (256–4096) tune
CDN reads. `STREAM_BUFFER_MB` is clamped to 1–256, and request depth to 1–8
per ordered MTProto connection. More outstanding requests
cannot overcome an upstream CDN or Telegram route bandwidth limit.
At the default maximum, queued MP4 bytes, upload chunk payloads and two inputs'
CDN ranges account for about 192 MiB, plus encryption copies, FFmpeg, sockets
and the application. This is a bounded use of the available RAM, not an attempt
to fill it. `UPLOAD_ADAPTIVE=false` retains a fixed upload window. Crypto worker
threads are clamped to 1–8. Existing `UPLOAD_WORKERS`, `STREAM_BUFFER_MB` and
download settings still apply. An explicitly configured request depth of `1`
disables pipelining; use `4` initially and `8` for the adaptive ceiling.
`/logs` records every adaptive
window change and any rejected MTProto chunk before it is retried.
Set `STREAM_VIDEO_UPLOADS=false` to use the regular disk path.
Audio conversion and unsupported fragment-list-only sources use that path.
Direct MP4/WebM resources and VOD HLS can use the streaming splitter. Disk storage is bounded by
`DOWNLOAD_STORAGE_LIMIT_MB` (default 1536 MiB) and `MIN_FREE_DISK_MB=128`.
It still needs room for the downloaded inputs and merged/converted output.
Failed folders are removed immediately; startup removes only abandoned `yt_dl_*`
folders inside `DOWNLOAD_DIR`, not cookies, sessions or other server files.

`/speedtest` now uploads a 16 MiB memory probe through the same parallel
Telegram uploader used for videos, excluding connection startup/shutdown.
While a transfer runs, it shows live feed/delivery rates and queued bytes
instead of starting competing network probes. Cloudflare download/upload
and Telegram upload measure different paths; fast download bandwidth does
not imply equal Telegram upload bandwidth. `/logs` includes feed speed,
acknowledged Telegram delivery speed, queue usage, active requests and retries
so a source bottleneck can be distinguished from a Telegram bottleneck.

Runtime logs are explicitly attached to the hosting console even when the bot
has already registered its in-memory `/logs` handler. Each video has a transfer
ID. Startup/session timings, CDN range capability and speed, produced/ACKed
bytes, buffer occupancy, request window, retries/FloodWait, recovery decisions,
FFmpeg exit status, final delivery and cleanup are logged. A `waiting=source`
or `waiting=Telegram` hint describes which side lacks capacity; it is not proof
of a particular remote network failure. `/speedtest` during an active transfer
also shows that hint and the active connection count.

`LOG_LEVEL=INFO` is the default; `DEBUG` adds application range diagnostics
without enabling Pyrogram wire dumps. `LOG_FILE=bot.log` keeps a rotating
2 MiB file and two backups (about 6 MiB maximum); set it empty to disable file
logging. Console and `/logs` remain active. CDN signed URLs, cookie values and
proxy credentials are not included in the new transfer diagnostics.

With `YOUTUBE_PROXY` configured, YouTube metadata uses the proxy, but video,
audio and thumbnails ALWAYS use direct server connections. The streaming
FFmpeg process explicitly disables HTTP proxies and removes inherited proxy
environment variables; supported HTTP resources and VOD HLS read directly.
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
- Original-quality selection and automatic playable-part limits
- Audio size estimation from bitrates and formats
- Filename sanitization against filesystem forbidden characters
- Telegram 64-byte callback_data compliance
- 10-per-page playlist pagination and multi-video selection logic
- Web health check server endpoints
- MP4 container verification & JPEG thumbnail conversion
- Boundary conditions and edge cases (emojis, unicode, huge recordings)
