from unittest.mock import Mock

import pytest
import config
import proxy_check


def test_offline_session_recovery_preserves_credentials_and_options(monkeypatch):
    original = "http://account-session-ytplaylist3-type-residential-mode-sticky:pass%40word@gate.busyip.com:18080"
    monkeypatch.setattr(config, "YOUTUBE_PROXY", original)
    probe = Mock(side_effect=[
        "CONNECT HTTP 403; X-Proxy-Error: sticky_device_offline",
        "CONNECT HTTP 403; X-Proxy-Error: sticky_device_offline",
        "CONNECT HTTP 200",
    ])
    monkeypatch.setattr(proxy_check, "check_proxy", probe)
    operation = Mock(side_effect=[RuntimeError("Tunnel connection failed: 403; proxy refused"), {"ok": True}])
    opts = {"proxy": original, "cookiefile": "cookies.txt", "format": "best"}
    assert proxy_check.run_with_proxy_recovery(operation, opts) == {"ok": True}
    replacement = original.replace("ytplaylist3", "ytplaylist5")
    assert config.YOUTUBE_PROXY == replacement
    assert operation.call_args.args[0] == {**opts, "proxy": replacement}
    assert opts["proxy"] == original


def test_proxy_authentication_failure_does_not_rotate(monkeypatch):
    original = "http://account-session-s1:secret@gate.busyip.com:18080"
    monkeypatch.setattr(config, "YOUTUBE_PROXY", original)
    monkeypatch.setattr(proxy_check, "check_proxy", Mock(return_value="CONNECT HTTP 403; X-Proxy-Error: invalid_credentials"))
    operation = Mock(side_effect=RuntimeError("proxy tunnel 403"))
    with pytest.raises(RuntimeError, match="proxy tunnel 403"):
        proxy_check.run_with_proxy_recovery(operation, {"proxy": original})
    assert operation.call_count == 1
    assert config.YOUTUBE_PROXY == original


def test_offline_recovery_is_bounded(monkeypatch):
    original = "http://account-session-s1:secret@gate.busyip.com:18080"
    monkeypatch.setattr(config, "YOUTUBE_PROXY", original)
    probe = Mock(return_value="CONNECT HTTP 403; X-Proxy-Error: sticky_device_offline")
    monkeypatch.setattr(proxy_check, "check_proxy", probe)
    operation = Mock(side_effect=RuntimeError("proxy tunnel 403"))
    with pytest.raises(proxy_check.ProxyRecoveryError, match="after 5"):
        proxy_check.run_with_proxy_recovery(operation, {"proxy": original})
    assert probe.call_count == 6  # Diagnose old exit, then try five replacements.
    assert "session-s6" in probe.call_args.args[0]


def test_parallel_worker_reuses_recovered_session(monkeypatch):
    original = "http://account-session-s1:secret@gate.busyip.com:18080"
    replacement = original.replace("session-s1", "session-s2")
    monkeypatch.setattr(config, "YOUTUBE_PROXY", replacement)
    probe = Mock()
    monkeypatch.setattr(proxy_check, "check_proxy", probe)
    operation = Mock(side_effect=[RuntimeError("proxy tunnel 403"), "success"])
    assert proxy_check.run_with_proxy_recovery(operation, {"proxy": original}) == "success"
    assert operation.call_args.args[0]["proxy"] == replacement
    probe.assert_not_called()


def test_multi_proxy_pool_rotates_to_healthy_proxy_on_403_tunnel(monkeypatch):
    p1 = "http://user-session-s1:pass@gate.busyip.com:18080"
    p2 = "http://user-session-s2:pass@gate.busyip.com:18080"
    p3 = "http://user-session-s3:pass@gate.busyip.com:18080"
    pool = [p1, p2, p3]
    monkeypatch.setattr(config, "YOUTUBE_PROXIES", pool)
    monkeypatch.setattr(config, "YOUTUBE_PROXY", p1)

    probe = Mock(side_effect=[
        "CONNECT HTTP 403; X-Proxy-Error: sticky_device_offline",  # check p2
        "CONNECT HTTP 200",                                        # check p3
    ])
    monkeypatch.setattr(proxy_check, "check_proxy", probe)
    # Operation fails with Tunnel connection failed on p1, then succeeds on p3
    operation = Mock(side_effect=[
        RuntimeError("('Unable to connect to proxy', OSError('Tunnel connection failed: 403 Forbidden'))"),
        {"entries": [{"id": "vid1"}]},
    ])

    result = proxy_check.run_with_proxy_recovery(operation, {"proxy": p1})
    assert result == {"entries": [{"id": "vid1"}]}
    assert config.YOUTUBE_PROXY == p3
    assert operation.call_args.args[0]["proxy"] == p3


def test_multi_proxy_pool_exhaustion_raises_recovery_error(monkeypatch):
    p1 = "http://user-session-s1:pass@gate.busyip.com:18080"
    p2 = "http://user-session-s2:pass@gate.busyip.com:18080"
    pool = [p1, p2]
    monkeypatch.setattr(config, "YOUTUBE_PROXIES", pool)
    monkeypatch.setattr(config, "YOUTUBE_PROXY", p1)

    probe = Mock(return_value="CONNECT HTTP 403; X-Proxy-Error: sticky_device_offline")
    monkeypatch.setattr(proxy_check, "check_proxy", probe)
    operation = Mock(side_effect=RuntimeError("Unable to connect to proxy: Tunnel connection failed"))

    with pytest.raises(proxy_check.ProxyRecoveryError, match="(All 2 configured proxies failed|No online BusyIP sticky exit)"):
        proxy_check.run_with_proxy_recovery(operation, {"proxy": p1})


def test_parse_proxy_list_handles_commas_and_newlines():
    raw = """
    http://user-s1:pass@gate.busyip.com:18080,
    http://user-s2:pass@gate.busyip.com:18080;
    http://user-s3:pass@gate.busyip.com:18080
    """
    parsed = config.parse_proxy_list(raw)
    assert parsed == [
        "http://user-s1:pass@gate.busyip.com:18080",
        "http://user-s2:pass@gate.busyip.com:18080",
        "http://user-s3:pass@gate.busyip.com:18080",
    ]
    assert config.parse_proxy_list("") == []

