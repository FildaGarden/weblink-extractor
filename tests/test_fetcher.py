"""Unit tests for HTTP page fetcher (offline, mocked)."""

import socket
import unittest
from unittest.mock import MagicMock, patch
import urllib.error

from extractor.fetcher import fetch_page


class TestPageFetcher(unittest.TestCase):
    """Test suite for HTTP fetch operations and error tolerance."""

    @patch("urllib.request.urlopen")
    def test_fetch_success(self, mock_urlopen: MagicMock) -> None:
        """Successful fetch returns 200 status and decoded HTML."""
        mock_response = MagicMock()
        mock_response.geturl.return_value = "https://example.com/final"
        mock_response.status = 200
        mock_response.headers.get_content_type.return_value = "text/html"
        mock_response.headers.get_content_charset.return_value = "utf-8"
        mock_response.read.return_value = b"<html><body>Hello</body></html>"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = fetch_page("https://example.com/start", timeout=5.0)

        self.assertEqual(result.url, "https://example.com/start")
        self.assertEqual(result.final_url, "https://example.com/final")
        self.assertEqual(result.status, 200)
        self.assertEqual(result.html, "<html><body>Hello</body></html>")
        self.assertIsNone(result.error)

    @patch("urllib.request.urlopen")
    def test_fetch_http_404(self, mock_urlopen: MagicMock) -> None:
        """HTTP 404 error is caught and produces error 'HTTP 404'."""
        err = urllib.error.HTTPError(
            url="https://example.com/missing",
            code=404,
            msg="Not Found",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        mock_urlopen.side_effect = err

        result = fetch_page("https://example.com/missing")
        err.close()

        self.assertEqual(result.status, 404)
        self.assertEqual(result.final_url, "https://example.com/missing")
        self.assertEqual(result.error, "HTTP 404")
        self.assertIsNone(result.html)

    @patch("urllib.request.urlopen")
    def test_fetch_http_500(self, mock_urlopen: MagicMock) -> None:
        """HTTP 500 server error is caught and produces error 'HTTP 500'."""
        err = urllib.error.HTTPError(
            url="https://example.com/error",
            code=500,
            msg="Internal Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        mock_urlopen.side_effect = err

        result = fetch_page("https://example.com/error")
        err.close()

        self.assertEqual(result.status, 500)
        self.assertEqual(result.error, "HTTP 500")
        self.assertIsNone(result.html)

    @patch("urllib.request.urlopen")
    def test_fetch_timeout(self, mock_urlopen: MagicMock) -> None:
        """Timeouts produce error 'Request timed out'."""
        mock_urlopen.side_effect = TimeoutError("Connection timed out")

        result = fetch_page("https://example.com/slow", timeout=1.0)

        self.assertIsNone(result.status)
        self.assertEqual(result.error, "Request timed out")
        self.assertIsNone(result.html)

    @patch("urllib.request.urlopen")
    def test_fetch_unreachable_host(self, mock_urlopen: MagicMock) -> None:
        """Unreachable host produces connection error."""
        gaierr = socket.gaierror(-2, "Name or service not known")
        mock_urlopen.side_effect = urllib.error.URLError(gaierr)

        result = fetch_page("https://this-domain-does-not-exist.invalid")

        self.assertIsNone(result.status)
        self.assertIn("Connection failed", result.error or "")
        self.assertIsNone(result.html)

    @patch("urllib.request.urlopen")
    def test_fetch_non_html_content(self, mock_urlopen: MagicMock) -> None:
        """Non-HTML content (e.g. image/png) returns status 200 with informative error."""
        mock_response = MagicMock()
        mock_response.geturl.return_value = "https://example.com/image.png"
        mock_response.status = 200
        mock_response.headers.get_content_type.return_value = "image/png"
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = fetch_page("https://example.com/image.png")

        self.assertEqual(result.status, 200)
        self.assertEqual(result.final_url, "https://example.com/image.png")
        self.assertEqual(result.error, "Non-HTML content (image/png)")
        self.assertIsNone(result.html)

    def test_fetch_invalid_url_syntax(self) -> None:
        """Invalid URL format fails immediately without making network calls."""
        for invalid in ["not a url", "ftp://example.com", "://invalid", ""]:
            result = fetch_page(invalid)
            self.assertIsNone(result.status)
            self.assertEqual(result.error, "Invalid URL")
            self.assertIsNone(result.html)


if __name__ == "__main__":
    unittest.main()
