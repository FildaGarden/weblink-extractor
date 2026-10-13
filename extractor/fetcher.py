"""HTTP page fetcher with timeout and comprehensive failure handling."""

import socket
import urllib.error
import urllib.parse
import urllib.request

from extractor.models import FetchResult

DEFAULT_USER_AGENT = "WebLinkExtractor/1.0"


def fetch_page(
    url: str,
    timeout: float = 10.0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> FetchResult:
    """Fetch a single page and return its HTML or an informative error.

    Requirements addressed:
    - Every request has a configurable timeout.
    - Resolves final_url following redirects.
    - Captures failures (invalid URL, DNS errors, timeouts, non-200 responses,
      non-HTML content) without ever raising an unhandled exception or stopping the run.
    """
    # 1. Validate basic URL format before attempting network I/O
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

    # 2. Build HTTP request with specified User-Agent
    req = urllib.request.Request(
        url,
        headers={"User-Agent": user_agent},
    )

    try:
        # urllib.request.urlopen automatically follows HTTP redirects (301, 302, 307, 308)
        with urllib.request.urlopen(req, timeout=timeout) as response:
            final_url = response.geturl()
            status = getattr(response, "status", response.getcode())

            # Check HTTP status (in case non-200 was somehow returned without raising HTTPError)
            if status != 200:
                return FetchResult(
                    url=url,
                    final_url=final_url,
                    status=status,
                    html=None,
                    error=f"HTTP {status}",
                )

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

            # Read and decode body
            raw_bytes = response.read()
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
        # Server returned a 4xx or 5xx HTTP response
        final_url = err.geturl() or url
        status_code = err.code
        err.close()
        return FetchResult(
            url=url,
            final_url=final_url,
            status=status_code,
            html=None,
            error=f"HTTP {status_code}",
        )

    except (TimeoutError, socket.timeout):
        # Native timeout
        return FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error="Request timed out",
        )

    except urllib.error.URLError as err:
        # Check if underlying reason was a socket timeout
        reason_str = str(err.reason).lower()
        if isinstance(err.reason, (socket.timeout, TimeoutError)) or "timed out" in reason_str:
            return FetchResult(
                url=url,
                final_url=url,
                status=None,
                html=None,
                error="Request timed out",
            )
        return FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error=f"Connection failed: {err.reason}",
        )

    except Exception as err:
        # Ultimate fallback: ensure no network glitch or unhandled exception halts the pipeline
        return FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error=str(err),
        )
