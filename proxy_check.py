"""Read the HTTP proxy's CONNECT result without logging credentials."""

import base64
import re
import socket
import ssl
import time
import logging
import threading
from urllib.parse import unquote, urlsplit

logger = logging.getLogger(__name__)
_session_lock = threading.Lock()
MAX_SESSION_RETRIES = 5


class ProxyRecoveryError(RuntimeError):
    pass


def _next_session(proxy_url: str) -> str:
    """Increment only BusyIP's session marker, preserving all credentials."""
    proxy = urlsplit(proxy_url)
    if proxy.hostname != "gate.busyip.com":
        raise ProxyRecoveryError("Automatic session recovery supports BusyIP proxies only.")
    credentials, separator, endpoint = proxy.netloc.rpartition("@")
    username, colon, password = credentials.partition(":")
    match = re.search(r"-session-([A-Za-z0-9]+)(?=-|$)", username)
    if not separator or not match:
        raise ProxyRecoveryError("BusyIP proxy needs a session marker for automatic recovery.")
    session = match.group(1)
    suffix = re.search(r"\d+$", session)
    next_session = (session[:suffix.start()] + str(int(suffix.group()) + 1)) if suffix else session + "1"
    username = username[:match.start(1)] + next_session + username[match.end(1):]
    return proxy._replace(netloc=username + colon + password + "@" + endpoint).geturl()


def run_with_proxy_recovery(operation, options: dict):
    """Retry an operation only when BusyIP confirms its sticky exit is offline.

    New selections are shared across workers for the lifetime of the process.
    The hosting-panel value remains the starting session after a restart.
    """
    import config

    options = dict(options)
    rotations = 0
    while True:
        try:
            return operation(options)
        except Exception as exc:
            failed_proxy = options.get("proxy", "")
            message = str(exc).lower()
            proxy_failure = "sticky_device_offline" in message or (
                "proxy" in message and ("403" in message or "tunnel connection failed" in message)
            ) or "playlists that require authentication may not extract correctly" in message
            if not failed_proxy or urlsplit(failed_proxy).hostname != "gate.busyip.com" or not proxy_failure:
                raise
            with _session_lock:
                # Another download may already have recovered this session.
                if config.YOUTUBE_PROXY and config.YOUTUBE_PROXY != failed_proxy:
                    options["proxy"] = config.YOUTUBE_PROXY
                    rotations += 1
                    if rotations > MAX_SESSION_RETRIES:
                        raise ProxyRecoveryError("Proxy recovery retry limit reached.") from exc
                    continue
                status = check_proxy(failed_proxy)
                if "sticky_device_offline" not in status:
                    # A 403 can also mean bad credentials, exhausted credit or
                    # denied access. Changing session does not fix those errors.
                    raise
                while rotations < MAX_SESSION_RETRIES:
                    failed_proxy = _next_session(failed_proxy)
                    rotations += 1
                    logger.warning("BusyIP sticky exit offline; testing next session (%d/%d)", rotations, MAX_SESSION_RETRIES)
                    status = check_proxy(failed_proxy)
                    if status == "CONNECT HTTP 200":
                        config.YOUTUBE_PROXY = failed_proxy
                        options["proxy"] = failed_proxy
                        logger.info("BusyIP replacement session connected; retrying YouTube request")
                        break
                    if "sticky_device_offline" not in status:
                        raise ProxyRecoveryError("Replacement proxy session failed: " + status) from exc
                else:
                    raise ProxyRecoveryError("No online BusyIP sticky exit found after 5 session retries.") from exc


def check_proxy(proxy_url: str) -> str:
    if not proxy_url:
        return "No YouTube proxy configured"
    try:
        proxy = urlsplit(proxy_url)
        if proxy.scheme not in ("http", "https"):
            return "CONNECT diagnostic supports HTTP/HTTPS proxies only"
        if not proxy.hostname:
            return "Invalid proxy URL: missing hostname"
        username = unquote(proxy.username or "")
        password = unquote(proxy.password or "")
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        headers = [
            "CONNECT www.youtube.com:443 HTTP/1.1",
            "Host: www.youtube.com:443",
        ]
        if username or password:
            headers.append(f"Proxy-Authorization: Basic {auth}")
        deadline = time.monotonic() + 15
        with socket.create_connection(
            (proxy.hostname, proxy.port or (443 if proxy.scheme == "https" else 80)),
            timeout=10,
        ) as raw_socket:
            conn = raw_socket
            if proxy.scheme == "https":
                conn = ssl.create_default_context().wrap_socket(raw_socket, server_hostname=proxy.hostname)
            try:
                conn.sendall(("\r\n".join(headers) + "\r\n\r\n").encode())
                data = b""
                # Some gateways use CRCRLF rather than CRLF. Normalize it for
                # parsing while limiting both response size and elapsed time.
                while len(data) < 8192:
                    conn.settimeout(max(0.1, deadline - time.monotonic()))
                    chunk = conn.recv(min(1024, 8192 - len(data)))
                    if not chunk:
                        break
                    data += chunk
                    if b"\n\n" in data.replace(b"\r", b""):
                        break
                    if time.monotonic() >= deadline:
                        break
            finally:
                if conn is not raw_socket:
                    conn.close()
        lines = data.decode("utf-8", errors="replace").replace("\r", "").split("\n")
        status = re.match(r"HTTP/\d(?:\.\d)? (\d{3})", lines[0])
        if not status:
            return "Proxy returned an invalid CONNECT response"
        code = status.group(1)
        details = []
        for line in lines[1:]:
            if not line:
                break
            key, _, value = line.partition(":")
            if key.lower() in ("x-proxy-error", "x-proxy-message"):
                # Redact even if a gateway unexpectedly echoes credentials.
                for secret in (proxy_url, username, password, auth):
                    if secret:
                        value = value.replace(secret, "[redacted]")
                value = "".join(c for c in value if c.isprintable()).strip()[:400]
                details.append(f"{key}: {value}")
        return f"CONNECT HTTP {code}" + ("; " + "; ".join(details) if details else "")
    except Exception as exc:
        # Exception text can contain a credential-bearing URL; emit type only.
        return f"CONNECT diagnostic failed ({type(exc).__name__})"


if __name__ == "__main__":
    import config
    print(check_proxy(config.YOUTUBE_PROXY))
