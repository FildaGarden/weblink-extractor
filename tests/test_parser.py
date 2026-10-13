"""Unit tests for HTML link extraction and URL normalization."""

import unittest

from extractor.parser import extract_links, normalize_link


class TestLinkExtractor(unittest.TestCase):
    """Test suite covering all requirements for link extraction."""

    def setUp(self) -> None:
        self.base_url = "https://example.com/docs/index.html"

    def test_relative_links_resolution(self) -> None:
        """Relative links must resolve against the page's final URL."""
        html = """
        <html>
            <body>
                <a href="/root-path">Root</a>
                <a href="guide.html">Sibling</a>
                <a href="../about.html">Parent</a>
                <a href="//cdn.example.com/asset">Protocol Relative</a>
            </body>
        </html>
        """
        links = extract_links(html, self.base_url)
        expected = [
            "https://example.com/root-path",
            "https://example.com/docs/guide.html",
            "https://example.com/about.html",
            "https://cdn.example.com/asset",
        ]
        self.assertEqual(links, expected)

    def test_strip_fragments(self) -> None:
        """URL fragments (#section) must be stripped."""
        html = """
        <a href="https://example.com/docs/page#intro">Intro</a>
        <a href="/faq#question-1">FAQ</a>
        <a href="#in-page-anchor">Anchor</a>
        """
        links = extract_links(html, self.base_url)
        expected = [
            "https://example.com/docs/page",
            "https://example.com/faq",
            "https://example.com/docs/index.html",
        ]
        self.assertEqual(links, expected)

    def test_filter_non_http_protocols(self) -> None:
        """Only http and https links are kept. mailto, javascript, ftp, etc. are discarded."""
        html = """
        <a href="mailto:support@example.com">Email</a>
        <a href="javascript:void(0);">Click</a>
        <a href="javascript:alert('xss')">Alert</a>
        <a href="tel:+123456789">Call</a>
        <a href="ftp://files.example.com/file.zip">FTP</a>
        <a href="https://example.com/valid">Valid</a>
        <a href="http://example.com/also-valid">Also Valid</a>
        """
        links = extract_links(html, self.base_url)
        expected = [
            "https://example.com/valid",
            "http://example.com/also-valid",
        ]
        self.assertEqual(links, expected)

    def test_malformed_hrefs(self) -> None:
        """Malformed, empty, or whitespace-only hrefs must be discarded gracefully."""
        html = """
        <a>No href</a>
        <a href="">Empty href</a>
        <a href="   ">Whitespace href</a>
        <a href="http://">Incomplete HTTP</a>
        <a href="http:// bad host ">Invalid host with spaces</a>
        <a href="https://valid.com">Valid</a>
        """
        links = extract_links(html, self.base_url)
        self.assertEqual(links, ["https://valid.com"])

    def test_duplicate_removal_preserving_order(self) -> None:
        """Duplicates within a page must be removed while keeping original first-seen order."""
        html = """
        <a href="/first">First</a>
        <a href="/second">Second</a>
        <a href="/first#with-fragment">First duplicate</a>
        <a href="https://example.com/first">First duplicate absolute</a>
        <a href="/third">Third</a>
        <a href="/second">Second duplicate</a>
        """
        links = extract_links(html, "https://example.com/")
        expected = [
            "https://example.com/first",
            "https://example.com/second",
            "https://example.com/third",
        ]
        self.assertEqual(links, expected)

    def test_malformed_html(self) -> None:
        """Unclosed or broken HTML tags should not crash the parser."""
        html = """
        <div>
            <a href="/broken1">Broken 1
            <a href="/broken2"
            <p>Some text</p>
            <a href="/broken3">Broken 3</a>
        </div>
        """
        links = extract_links(html, "https://example.com/")
        self.assertIn("https://example.com/broken1", links)
        self.assertIn("https://example.com/broken3", links)

    def test_empty_or_none_input(self) -> None:
        """Empty HTML returns empty list."""
        self.assertEqual(extract_links("", self.base_url), [])
        self.assertEqual(extract_links(None, self.base_url), [])  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
