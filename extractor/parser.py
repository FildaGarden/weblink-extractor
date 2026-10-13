"""HTML parsing and URL normalization logic."""

from html.parser import HTMLParser
from urllib.parse import urldefrag, urljoin, urlsplit, urlunsplit


class _AnchorParser(HTMLParser):
    """Event-driven HTML parser extracting raw href attributes from <a> tags."""

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() == "a":
            for attr_name, attr_value in attrs:
                if attr_name.lower() == "href" and attr_value is not None:
                    self.hrefs.append(attr_value)
                    break


def _is_valid_netloc(netloc: str) -> bool:
    """Validate that the network location (host[:port]) is syntactically sound."""
    if not netloc or any(char in netloc for char in (" ", "\t", "\r", "\n")):
        return False
    return True


def normalize_link(href: str, base_url: str) -> str | None:
    """Normalize and validate a raw href against the base URL.

    Rules applied:
    1. Strips leading and trailing whitespace.
    2. Resolves relative URLs against base_url (final URL after redirects).
    3. Strips URL fragments (#hash).
    4. Enforces 'http' or 'https' scheme (rejects mailto:, javascript:, ftp:, etc.).
    5. Validates host / netloc presence and absence of whitespace.

    Returns the clean absolute URL string, or None if the link is invalid or non-HTTP.
    """
    raw = href.strip()
    if not raw:
        return None

    try:
        # Resolve relative link against final URL
        abs_url = urljoin(base_url, raw)

        # Strip fragment (#section)
        defragged_url, _ = urldefrag(abs_url)

        # Parse URL components
        parsed = urlsplit(defragged_url)

        # Enforce scheme
        if parsed.scheme.lower() not in ("http", "https"):
            return None

        # Validate network location (host)
        if not _is_valid_netloc(parsed.netloc):
            return None

        return urlunsplit(parsed)
    except ValueError:
        # Discard syntactically malformed URLs
        return None


def extract_links(html: str, base_url: str) -> list[str]:
    """Extract all valid, normalized hyperlinks from HTML.

    - Resolves relative links to absolute URLs based on base_url.
    - Strips URL fragments.
    - Keeps only http and https URLs.
    - Discards invalid/malformed hrefs and non-HTTP protocols.
    - Removes duplicates within the page while strictly preserving original order.
    """
    if not html:
        return []

    parser = _AnchorParser()
    try:
        parser.feed(html)
    except Exception:
        # html.parser is very forgiving, but if feeding fails on corrupt content,
        # return whatever hrefs were found so far without crashing.
        pass

    results: list[str] = []
    seen: set[str] = set()

    for raw_href in parser.hrefs:
        clean_url = normalize_link(raw_href, base_url)
        if clean_url is not None and clean_url not in seen:
            seen.add(clean_url)
            results.append(clean_url)

    return results
