from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.request import Request as UrllibRequest

import pytest
from yt_dlp.networking import Request
from yt_dlp.networking.exceptions import HTTPError, TransportError

import config
from downloader import build_ydl_options
from media_routing import install_media_routing


def http_error(status=403):
    return HTTPError(SimpleNamespace(status=status, reason="Rejected", close=Mock()))


def test_metadata_keeps_proxy_and_media_is_direct():
    original = Mock(return_value=object())
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    metadata = Request("https://www.youtube.com/youtubei/v1/player")
    ydl.urlopen(metadata)
    assert original.call_args.args[0] is metadata
    media = Request("https://rr1.googlevideo.com/videoplayback", headers={"Range": "bytes=0-4095"},
                    proxies={"https": "http://proxy"})
    ydl.urlopen(media)
    sent = original.call_args.args[0]
    assert sent.proxies == {"all": None, "http": None, "https": None}
    assert sent.headers["Range"] == "bytes=0-4095"
    assert media.proxies == {"https": "http://proxy"}


@pytest.mark.parametrize("error", [http_error(), TransportError("connection failed")])
def test_failed_direct_requests_never_fall_back(error):
    original = Mock(side_effect=error)
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    request = Request("https://rr1.googlevideo.com/videoplayback")
    for _ in range(2):
        with pytest.raises(type(error)):
            ydl.urlopen(request)
    assert original.call_count == 2
    assert all(call.args[0].proxies["https"] is None for call in original.call_args_list)


def test_strict_mode_never_falls_back():
    original = Mock(side_effect=http_error())
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    with pytest.raises(HTTPError):
        ydl.urlopen("https://rr1.googlevideo.com/videoplayback")
    assert original.call_count == 1
    assert original.call_args.args[0].proxies["https"] is None


def test_missing_media_does_not_spend_proxy_data():
    original = Mock(side_effect=http_error(404))
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    with pytest.raises(HTTPError):
        ydl.urlopen("https://rr1.googlevideo.com/videoplayback")
    assert original.call_count == 1


def test_host_matching_and_urllib_conversion():
    original = Mock()
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    ydl.urlopen("https://googlevideo.com.attacker.test/videoplayback")
    assert original.call_args.args[0].proxies["https"] is None
    ydl.urlopen(UrllibRequest("https://rr1.googlevideo.com/videoplayback", headers={"Range": "bytes=0-1"}))
    assert original.call_args.args[0].proxies["https"] is None
    assert original.call_args.args[0].headers["Range"] == "bytes=0-1"
    ydl.urlopen("https://i.ytimg.com/vi/id/default.jpg")
    assert original.call_args.args[0].proxies["https"] is None


def test_parallel_fragments_are_always_direct():
    original = Mock(return_value=object())
    ydl = SimpleNamespace(urlopen=original)
    install_media_routing(ydl)
    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(ydl.urlopen, [Request("https://rr1.googlevideo.com/videoplayback")] * 16))
    assert all(call.args[0].proxies["https"] is None for call in original.call_args_list)
    assert original.call_count == 16


def test_proxy_uses_native_downloader_even_when_aria2_available(tmp_path):
    with patch.object(config, "YOUTUBE_PROXY", "http://proxy"), \
         patch.object(config, "ARIA2C_AVAILABLE", True), \
         patch("downloader.get_cookiefile_path", return_value=None):
        opts = build_ydl_options(str(tmp_path), "best")
    assert opts["external_downloader"] == {"default": "native"}
    assert opts["hls_prefer_native"] is True


def test_external_live_downloader_cannot_bypass_routing():
    from yt_dlp.utils import DownloadError
    ydl = SimpleNamespace(urlopen=Mock(), dl=Mock(),
                          params={"external_downloader": {"default": "native"}})
    original_dl = ydl.dl
    install_media_routing(ydl)
    with pytest.raises(DownloadError, match="bandwidth limits"):
        ydl.dl("live.mp4", {"url": "https://rr1.googlevideo.com/live.m3u8",
                            "protocol": "m3u8_native", "is_live": True})
    original_dl.assert_not_called()
    ydl.dl("video.mp4", {"url": "https://rr1.googlevideo.com/videoplayback", "protocol": "https"})
    original_dl.assert_called_once()


def test_download_stage_forces_direct_even_for_youtube_host_and_restores_metadata():
    original = Mock(return_value=object())
    ydl = SimpleNamespace(urlopen=original, params={})
    ydl.dl = lambda *args, **kwargs: ydl.urlopen("https://www.youtube.com/videoplayback")
    install_media_routing(ydl)
    ydl.dl("video.mp4", {"url": "https://www.youtube.com/videoplayback", "protocol": "https"})
    assert original.call_args.args[0].proxies["https"] is None
    ydl.urlopen("https://www.youtube.com/youtubei/v1/player")
    assert original.call_args.args[0] == "https://www.youtube.com/youtubei/v1/player"
