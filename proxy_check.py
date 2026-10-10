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


def _redact_proxy(proxy_url: str) -> str:
    """Safely redact credentials from a proxy URL for logging."""
    if not proxy_url:
        return ""
    try:
        parsed = urlsplit(proxy_url)
        username = parsed.username or ""
        password = parsed.password or ""
        redacted = proxy_url
        if username:
            redacted = redacted.replace(username, "[user]")
        if password:
            redacted = redacted.replace(password, "[redacted]")
        return redacted
    except Exception:
        return "[proxy]"


def is_proxy_failure(exc: Exception) -> bool:
    """Detect if an exception is caused by proxy tunnel or connection failure."""
    message = str(exc).lower()
    return (
        "sticky_device_offline" in message
        or "tunnel connection failed" in message
        or "unable to connect to proxy" in message
        or "proxyerror" in message
        or "proxy error" in message
        or "proxy refused" in message
        or "cannot connect to proxy" in message
        or "connection refused" in message
        or "failed to establish a new connection" in message
        or ("proxy" in message and ("403" in message or "407" in message or "502" in message or "503" in message or "504" in message or "tunnel" in message))
        or "playlists that require authentication may not extract correctly" in message
    )


def run_with_proxy_recovery(operation, options: dict):
    """Retry an operation across multiple configured proxies or with BusyIP session recovery.

    If a proxy fails (tunnel 403, offline, etc.), it tests and rotates through
    all available proxies in YOUTUBE_PROXIES until a healthy one is found.
    New selections are shared across workers for the lifetime of the process.
    """
    import config

    options = dict(options)
    tried_proxies = set()
    initial_proxy = options.get("proxy", "")
    if initial_proxy:
        tried_proxies.add(initial_proxy)

    session_rotations = 0

    while True:
        try:
            return operation(options)
        except Exception as exc:
            failed_proxy = options.get("proxy", "")
            if not failed_proxy or not is_proxy_failure(exc):
                raise

            with _session_lock:
                # 1. Check if another concurrent worker has already switched to a new proxy
                if config.YOUTUBE_PROXY and config.YOUTUBE_PROXY != failed_proxy and config.YOUTUBE_PROXY not in tried_proxies:
                    options["proxy"] = config.YOUTUBE_PROXY
                    tried_proxies.add(config.YOUTUBE_PROXY)
                    logger.info("Using proxy %s recovered by another worker", _redact_proxy(config.YOUTUBE_PROXY))
                    continue

                # 2. Check if we have multiple proxies in the configured pool
                pool = getattr(config, "YOUTUBE_PROXIES", [])
                if not pool and config.YOUTUBE_PROXY:
                    pool = [config.YOUTUBE_PROXY]

                candidate_found = False
                if len(pool) > 1:
                    # Find candidate proxies in the pool that haven't been tried yet
                    start_idx = (pool.index(failed_proxy) + 1) if failed_proxy in pool else 0
                    for offset in range(len(pool)):
                        candidate = pool[(start_idx + offset) % len(pool)]
                        if candidate in tried_proxies:
                            continue
                        tried_proxies.add(candidate)
                        logger.warning("Proxy %s failed; testing candidate from pool: %s", _redact_proxy(failed_proxy), _redact_proxy(candidate))
                        status = check_proxy(candidate)
                        if status == "CONNECT HTTP 200" or status.startswith("CONNECT diagnostic"):
                            config.YOUTUBE_PROXY = candidate
                            options["proxy"] = candidate
                            logger.info("Switching to healthy pool proxy: %s", _redact_proxy(candidate))
                            candidate_found = True
                            break
                        else:
                            logger.warning("Pool proxy %s returned %s; continuing search...", _redact_proxy(candidate), status)

                    # If diagnostic didn't return 200 for any, but there are unattempted pool candidates, try next directly
                    if not candidate_found and len(tried_proxies) < len(pool):
                        for offset in range(len(pool)):
                            candidate = pool[(start_idx + offset) % len(pool)]
                            if candidate not in tried_proxies:
                                tried_proxies.add(candidate)
                                config.YOUTUBE_PROXY = candidate
                                options["proxy"] = candidate
                                logger.info("Retrying with next pool proxy directly: %s", _redact_proxy(candidate))
                                candidate_found = True
                                break

                    if candidate_found:
                        continue

                # 3. BusyIP sticky exit recovery (if using BusyIP proxy)
                if urlsplit(failed_proxy).hostname == "gate.busyip.com":
                    status = check_proxy(failed_proxy)
                    if "sticky_device_offline" in status:
                        while session_rotations < MAX_SESSION_RETRIES:
                            failed_proxy = _next_session(failed_proxy)
                            session_rotations += 1
                            logger.warning(
                                "BusyIP sticky exit offline; testing next session (%d/%d)",
                                session_rotations,
                                MAX_SESSION_RETRIES,
                            )
                            status = check_proxy(failed_proxy)
                            if status == "CONNECT HTTP 200":
                                config.YOUTUBE_PROXY = failed_proxy
                                options["proxy"] = failed_proxy
                                logger.info("BusyIP replacement session connected; retrying YouTube request")
                                candidate_found = True
                                break
                            if "sticky_device_offline" not in status:
                                raise ProxyRecoveryError("Replacement proxy session failed: " + status) from exc
                        if not candidate_found:
                            raise ProxyRecoveryError("No online BusyIP sticky exit found after 5 session retries.") from exc
                        continue
                    else:
                        # Non-recoverable BusyIP error (e.g. invalid credentials) with single proxy
                        if len(pool) <= 1:
                            raise

                # 4. If all pool proxies were tried and exhausted
                if len(pool) > 1 and len(tried_proxies) >= len(pool):
                    raise ProxyRecoveryError(f"All {len(pool)} configured proxies failed.") from exc

                raise


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
