"""HTTP page fetcher with timeout, maximum body size limit, and transient failure retries."""

import gzip
import time
import urllib.error
import urllib.parse
import urllib.request

from extractor.models import FetchResult

DEFAULT_USER_AGENT = "WebLinkExtractor/1.0"
MAX_BODY_BYTES = 10 * 1024 * 1024  # 10 MB limit
TRANSIENT_HTTP_STATUSES = {500, 502, 503, 504}


def _decompress_body(raw_bytes: bytes, encoding: str) -> bytes:
    if encoding == "gzip":
        return gzip.decompress(raw_bytes)
    return raw_bytes


def fetch_page(
    url: str,
    timeout: float = 10.0,
    user_agent: str = DEFAULT_USER_AGENT,
    retries: int = 0,
    retry_delay: float = 0.5,
) -> FetchResult:  # pyright: ignore[reportReturnType]
    """Fetch a single page and return its HTML or an informative error."""

    # 1. Validate basic URL format before attempting network I/O (never retried)
    try:
        parsed = urllib.parse.urlsplit(url)
        is_valid = parsed.scheme.lower() in ("http", "https") and bool(parsed.netloc)

    except ValueError:
        is_valid = False

    if not is_valid:
        return FetchResult(url=url, final_url=url, error="Invalid URL")

    # 2. Build HTTP request with specified User-Agent and Accept-Encoding
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip",
        },
    )

    for attempt in range(retries + 1):
        try:
            # urllib.request.urlopen automatically follows HTTP redirects (301, 302, 307, 308)
            with urllib.request.urlopen(req, timeout=timeout) as response:
                final_url = response.geturl()
                status = response.status

                # Non-200 responses (e.g. 201, 203) produce an error record
                if status != 200:
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        error=f"HTTP {status}",
                    )

                # Check Content-Type header
                content_type = response.headers.get_content_type().lower()
                if content_type not in ("text/html", "application/xhtml+xml"):
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        error=f"Non-HTML content ({content_type})",
                    )

                # Read response body and enforce maximum size cap (10 MB)
                raw_bytes = response.read(MAX_BODY_BYTES + 1)
                if len(raw_bytes) > MAX_BODY_BYTES:
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        error="Response exceeded maximum allowed size (10 MB)",
                    )

                # Decompress if encoded
                raw_encoding = response.headers.get("Content-Encoding")
                encoding = (
                    raw_encoding.strip().lower()
                    if isinstance(raw_encoding, str)
                    else ""
                )
                if encoding not in ("", "identity", "gzip"):
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        error=f"Unsupported Content-Encoding ({encoding})",
                    )
                raw_bytes = _decompress_body(raw_bytes, encoding)

                # Check maximum allowed size limit after decompression
                if len(raw_bytes) > MAX_BODY_BYTES:
                    return FetchResult(
                        url=url,
                        final_url=final_url,
                        status=status,
                        error="Response exceeded maximum allowed size (10 MB)",
                    )

                charset = response.headers.get_content_charset() or "utf-8"
                try:
                    html = raw_bytes.decode(charset, errors="replace")
                except LookupError:
                    html = raw_bytes.decode("utf-8", errors="replace")

                return FetchResult(
                    url=url,
                    final_url=final_url,
                    status=status,
                    html=html,
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
                error=f"HTTP {status_code}",
            )

        except (TimeoutError, ConnectionError) as err:
            # Native socket timeout or connection reset (including during read())
            if attempt < retries:
                time.sleep(retry_delay)
                continue
            error_msg = (
                "Request timed out"
                if isinstance(err, TimeoutError)
                else f"Connection error: {err}"
            )
            return FetchResult(
                url=url,
                final_url=url,
                error=error_msg,
            )

        except urllib.error.URLError as err:
            is_timeout = isinstance(err.reason, TimeoutError)
            is_conn_error = isinstance(err.reason, ConnectionError)

            # Only retry transient timeouts or connection resets/refusals. Never retry DNS NXDOMAIN or SSL errors!
            if (is_timeout or is_conn_error) and attempt < retries:
                time.sleep(retry_delay)
                continue

            error_msg = (
                "Request timed out"
                if is_timeout
                else f"Connection failed: {err.reason}"
            )
            return FetchResult(
                url=url,
                final_url=url,
                error=error_msg,
            )

        except Exception as err:
            # Non-network unexpected errors are never retried
            err_msg = f"{type(err).__name__}: {err}" if str(err) else type(err).__name__
            return FetchResult(
                url=url,
                final_url=url,
                error=err_msg,
            )
