"""Use the paid proxy for YouTube details only, never media transfers."""

import logging
from urllib.parse import urlsplit

from yt_dlp.networking import Request
from yt_dlp.networking.exceptions import HTTPError, TransportError

logger = logging.getLogger(__name__)


def install_media_routing(ydl):
    """Wrap this downloader only; never change process-wide proxy settings.

    Metadata retains its proxy/cookies. CDN requests preserve headers, cookies,
    range and method, but explicitly override *all* proxy sources for direct use.
    All requests during media downloading are direct, including nonstandard
    CDN hosts. There is deliberately no paid media fallback or opt-in switch.
    """
    original_urlopen = ydl.urlopen
    downloading = False
    logged_direct = False

    def urlopen(request):
        url = request if isinstance(request, str) else (
            request.url if isinstance(request, Request) else request.get_full_url())
        host = (urlsplit(url).hostname or "").lower()
        metadata = any(host == domain or host.endswith("." + domain)
                       for domain in ("youtube.com", "youtube-nocookie.com",
                                      "google.com", "googleapis.com"))
        if metadata and not downloading:
            return original_urlopen(request)
        if isinstance(request, str):
            request = Request(request)
        elif not isinstance(request, Request):
            from yt_dlp.compat import urllib_req_to_req
            request = urllib_req_to_req(request)
        direct = request.copy()
        direct.proxies = {"all": None, "http": None, "https": None}
        nonlocal logged_direct
        try:
            response = original_urlopen(direct)
        except (HTTPError, TransportError):
            logger.warning("Direct request failed; paid media proxy fallback is forbidden.")
            raise
        if not logged_direct:
            logger.info("Download route: server direct; paid proxy is for YouTube details only.")
            logged_direct = True
        return response

    ydl.urlopen = urlopen

    # Live HLS and section downloads may force networked FFmpeg despite the
    # native option. Refuse those rather than silently bypass bandwidth rules.
    if hasattr(ydl, "dl"):
        original_dl = ydl.dl

        def dl(filename, info, *args, **kwargs):
            nonlocal downloading
            from yt_dlp.downloader import get_suitable_downloader
            from yt_dlp.downloader.external import ExternalFD
            from yt_dlp.utils import DownloadError
            downloader = get_suitable_downloader(info.copy(), ydl.params)
            if downloader and issubclass(downloader, ExternalFD):
                raise DownloadError(
                    "This stream requires an external network downloader, which cannot "
                    "enforce metadata-only proxy bandwidth limits. Choose a non-live video.")
            previous = downloading
            downloading = True
            try:
                return original_dl(filename, info, *args, **kwargs)
            finally:
                downloading = previous

        ydl.dl = dl
