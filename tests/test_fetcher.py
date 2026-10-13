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
    def test_fetch_non_200_2xx_status(self, mock_urlopen: MagicMock) -> None:
        """Non-200 2xx responses (e.g. 203) produce an error record."""
        mock_response = MagicMock()
        mock_response.geturl.return_value = "https://example.com/203"
        mock_response.status = 203
        mock_urlopen.return_value.__enter__.return_value = mock_response

        result = fetch_page("https://example.com/203")

        self.assertEqual(result.status, 203)
        self.assertEqual(result.error, "HTTP 203")
        self.assertIsNone(result.html)

    @patch("urllib.request.urlopen")
    def test_fetch_retry_on_connection_reset_during_read(self, mock_urlopen: MagicMock) -> None:
        """ConnectionResetError during body read is retried and succeeds."""
        mock_err_resp = MagicMock()
        mock_err_resp.status = 200
        mock_err_resp.geturl.return_value = "https://example.com/reset"
        mock_err_resp.headers.get_content_type.return_value = "text/html"
        mock_err_resp.read.side_effect = ConnectionResetError("Connection reset by peer")

        mock_ok_resp = MagicMock()
        mock_ok_resp.status = 200
        mock_ok_resp.geturl.return_value = "https://example.com/reset"
        mock_ok_resp.headers.get_content_type.return_value = "text/html"
        mock_ok_resp.headers.get_content_charset.return_value = "utf-8"
        mock_ok_resp.headers.get.return_value = None
        mock_ok_resp.read.return_value = b"<html><body>OK</body></html>"

        cm1 = MagicMock()
        cm1.__enter__.return_value = mock_err_resp
        cm2 = MagicMock()
        cm2.__enter__.return_value = mock_ok_resp

        mock_urlopen.side_effect = [cm1, cm2]

        result = fetch_page("https://example.com/reset", retries=1, retry_delay=0.01)

        self.assertEqual(result.status, 200)
        self.assertEqual(result.html, "<html><body>OK</body></html>")
        self.assertEqual(mock_urlopen.call_count, 2)

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
    def test_fetch_urlerror_timeout(self, mock_urlopen: MagicMock) -> None:
        """URLError wrapping socket.timeout produces error 'Request timed out'."""
        mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("timed out"))

        result = fetch_page("https://example.com/slow_socket", timeout=1.0)

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

    @patch("urllib.request.urlopen")
    def test_fetch_retry_transient_failure_success(self, mock_urlopen: MagicMock) -> None:
        """Transient 500 error is retried and succeeds on second attempt."""
        err = urllib.error.HTTPError(
            url="https://example.com/retry",
            code=500,
            msg="Internal Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        mock_response = MagicMock()
        mock_response.geturl.return_value = "https://example.com/retry"
        mock_response.status = 200
        mock_response.headers.get_content_type.return_value = "text/html"
        mock_response.headers.get_content_charset.return_value = "utf-8"
        mock_response.headers.get.return_value = None
        mock_response.read.return_value = b"<html><body>Recovered</body></html>"

        # Attempt 1: 500 error; Attempt 2: success context manager
        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_response
        mock_urlopen.side_effect = [err, mock_cm]

        result = fetch_page("https://example.com/retry", retries=1, retry_delay=0.01)
        err.close()

        self.assertEqual(result.status, 200)
        self.assertEqual(result.html, "<html><body>Recovered</body></html>")
        self.assertIsNone(result.error)
        self.assertEqual(mock_urlopen.call_count, 2)

    @patch("urllib.request.urlopen")
    def test_fetch_no_retry_on_501_not_implemented(self, mock_urlopen: MagicMock) -> None:
        """HTTP 501 Not Implemented is a permanent error and must not be retried."""
        err = urllib.error.HTTPError(
            url="https://example.com/501",
            code=501,
            msg="Not Implemented",
            hdrs=None,  # type: ignore[arg-type]
            fp=None,
        )
        mock_urlopen.side_effect = err

        result = fetch_page("https://example.com/501", retries=3, retry_delay=0.01)
        err.close()

        self.assertEqual(result.status, 501)
        self.assertEqual(result.error, "HTTP 501")
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch("urllib.request.urlopen")
    def test_fetch_no_retry_on_dns_nxdomain(self, mock_urlopen: MagicMock) -> None:
        """DNS resolution failures are permanent and must not be retried."""
        gaierr = socket.gaierror(-2, "Name or service not known")
        mock_urlopen.side_effect = urllib.error.URLError(gaierr)

        result = fetch_page("https://this-domain-does-not-exist.invalid", retries=3, retry_delay=0.01)

        self.assertIsNone(result.status)
        self.assertIn("Connection failed", result.error or "")
        self.assertEqual(mock_urlopen.call_count, 1)

    @patch("urllib.request.urlopen")
    def test_fetch_max_body_size_exceeded(self, mock_urlopen: MagicMock) -> None:
        """Responses exceeding the 10 MB safety cap return an informative error."""
        from extractor.fetcher import MAX_BODY_BYTES

        mock_response = MagicMock()
        mock_response.geturl.return_value = "https://example.com/huge"
        mock_response.status = 200
        mock_response.headers.get_content_type.return_value = "text/html"
        mock_response.headers.get_content_charset.return_value = "utf-8"
        mock_response.headers.get.return_value = None
        # Return 1 byte more than MAX_BODY_BYTES
        mock_response.read.return_value = b"x" * (MAX_BODY_BYTES + 1)

        mock_cm = MagicMock()
        mock_cm.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_cm

        result = fetch_page("https://example.com/huge")

        self.assertEqual(result.status, 200)
        self.assertIsNone(result.html)
        self.assertIn("maximum allowed size", result.error or "")


if __name__ == "__main__":
    unittest.main()
