"""HTTP page fetcher with timeout, compression support, and transient failure retries."""

import gzip
import io
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib

from extractor.models import FetchResult

DEFAULT_USER_AGENT = "WebLinkExtractor/1.0"
MAX_BODY_BYTES = 10 * 1024 * 1024  # 10 MB limit to prevent memory exhaustion & zip bombs
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}


def fetch_page(
    url: str,
    timeout: float = 10.0,
    user_agent: str = DEFAULT_USER_AGENT,
    retries: int = 0,
    retry_delay: float = 0.5,
) -> FetchResult:
    """Fetch a single page and return its HTML or an informative error.

    Requirements addressed:
    - Every request has a configurable timeout.
    - Resolves final_url following redirects.
    - Automatic retries for transient errors only (500/502/503/504, timeouts, connection resets).
    - Never retries non-transient errors (4xx, 501, 505, DNS NXDOMAIN, SSL errors, invalid URLs).
    - Captures failures without halting the pipeline.
    """
    # 1. Validate basic URL format before attempting network I/O (never retried)
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc:
            return FetchResult(
                url=url,
                final_url=url,
                status=None,
                html=None,
                error="Invalid URL",
            )
    except Exception:
        return FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error="Invalid URL",
        )

    # 2. Build HTTP request with specified User-Agent and Accept-Encoding
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
        },
    )

    for attempt in range(retries + 1):
        try:
            # urllib.request.urlopen automatically follows HTTP redirects (301, 302, 307, 308)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                final_url = response.geturl()
                status = getattr(response, "status", response.getcode())

                # Check Content-Type header
                content_type = response.headers.get_content_type().lower()
                if content_type not in ("text/html", "application/xhtml+xml"):
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        html=None,
                        error=f"Non-HTML content ({content_type})",
                    )

                # Read response body with maximum size guard
                raw_bytes = response.read(MAX_BODY_BYTES + 1)
                if len(raw_bytes) > MAX_BODY_BYTES:
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        html=None,
                        error="Response exceeded maximum allowed size (10 MB)",
                    )

                # Decompress if encoded (gzip/deflate) with safety size check
                content_encoding = (response.headers.get("Content-Encoding") or "").strip().lower()
                if content_encoding == "gzip":
                    try:
                        raw_bytes = gzip.decompress(raw_bytes)
                        if len(raw_bytes) > MAX_BODY_BYTES:
                            return FetchResult(
                                url=url,
                                final_url=final_url,
                                status=status,
                                html=None,
                                error="Decompressed content exceeded maximum allowed size (10 MB)",
                            )
                    except Exception as dec_err:
                        return FetchResult(
                            url=url,
                            final_url=final_url,
                            status=status,
                            html=None,
                            error=f"Gzip decompression error: {dec_err}",
                        )
                elif content_encoding == "deflate":
                    try:
                        try:
                            raw_bytes = zlib.decompress(raw_bytes)
                        except Exception:
                            raw_bytes = zlib.decompress(raw_bytes, -zlib.MAX_WBITS)
                        if len(raw_bytes) > MAX_BODY_BYTES:
                            return FetchResult(
                                url=url,
                                final_url=final_url,
                                status=status,
                                html=None,
                                error="Decompressed content exceeded maximum allowed size (10 MB)",
                            )
                    except Exception as dec_err:
                        return FetchResult(
                            url=url,
                            final_url=final_url,
                            status=status,
                            html=None,
                            error=f"Deflate decompression error: {dec_err}",
                        )

                charset = response.headers.get_content_charset() or "utf-8"
                try:
                    html = raw_bytes.decode(charset, errors="replace")
                except (LookupError, UnicodeDecodeError):
                    # Fallback to UTF-8 with character replacement on unknown/invalid charset
                    html = raw_bytes.decode("utf-8", errors="replace")

                return FetchResult(
                    url=url,
                    final_url=final_url,
                    status=status,
                    html=html,
                    error=None,
                )

        except urllib.error.HTTPError as err:
            final_url = err.geturl() or url
            status_code = err.code
            err.close()

            # Retry strictly on transient 5xx statuses (500, 502, 503, 504), never on 501, 505 or 4xx
            if status_code in TRANSIENT_HTTP_STATUSES and attempt < retries:
                time.sleep(retry_delay)
                continue

            return FetchResult(
                url=url,
                final_url=final_url,
                status=status_code,
                html=None,
                error=f"HTTP {status_code}",
            )

        except (TimeoutError, socket.timeout):
            # Native socket timeout: transient failure
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            return FetchResult(
                url=url,
                final_url=url,
                status=None,
                html=None,
                error="Request timed out",
            )

        except urllib.error.URLError as err:
            reason_str = str(err.reason).lower()
            is_timeout = isinstance(err.reason, (socket.timeout, TimeoutError)) or "timed out" in reason_str
            is_connection_reset = isinstance(err.reason, (ConnectionResetError, ConnectionRefusedError))

            # Only retry transient timeouts or connection resets/refusals. Never retry DNS NXDOMAIN or SSL errors!
            if (is_timeout or is_connection_reset) and attempt < retries:
                time.sleep(retry_delay)
                continue

            error_msg = "Request timed out" if is_timeout else f"Connection failed: {err.reason}"
            return FetchResult(
                url=url,
                final_url=url,
                status=None,
                html=None,
                error=error_msg,
            )

        except Exception as err:
            # Non-network unexpected errors (e.g. system bugs) are never retried
            return FetchResult(
                url=url,
                final_url=url,
                status=None,
                html=None,
                error=str(err),
            )

    return FetchResult(url=url, final_url=url, error="Exceeded max retries")
