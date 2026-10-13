"""End-to-end integration tests for the extraction pipeline against a local test server."""

import http.server
import json
from pathlib import Path
import tempfile
import threading
import unittest

from extractor.pipeline import read_input_urls, run_pipeline


class _LocalTestHTTPHandler(http.server.BaseHTTPRequestHandler):
    """Local test HTTP server handler serving test pages and failure conditions."""

    def do_GET(self) -> None:
        if self.path == "/page1":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = (
                "<html><body>"
                "<a href='/page2'>Page 2</a>"
                "<a href='https://external.org/link'>External</a>"
                "<a href='/page2#fragment'>Duplicate Page 2</a>"
                "<a href='mailto:info@test.com'>Email</a>"
                "<a href='javascript:void(0)'>JS</a>"
                "</body></html>"
            )
            self.wfile.write(body.encode("utf-8"))

        elif self.path == "/redirect":
            # Redirect to /target
            self.send_response(302)
            self.send_header("Location", "/target")
            self.end_headers()

        elif self.path == "/target":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            body = "<html><body><a href='relative/endpoint'>Relative</a></body></html>"
            self.wfile.write(body.encode("utf-8"))

        elif self.path == "/notfound":
            self.send_response(404)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"Not Found")

        elif self.path == "/image":
            self.send_response(200)
            self.send_header("Content-Type", "image/png")
            self.end_headers()
            self.wfile.write(b"\x89PNG\r\n\x1a\nfake-png-binary-data")

        else:
            self.send_response(500)
            self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        """Suppress standard HTTP server logging to stderr during test runs."""
        pass


class TestPipelineIntegration(unittest.TestCase):
    """End-to-end integration test verifying the full producer-consumer pipeline."""

    @classmethod
    def setUpClass(cls) -> None:
        # Start a local HTTP server on a random available port (0)
        cls.server = http.server.HTTPServer(("127.0.0.1", 0), _LocalTestHTTPHandler)
        cls.port = cls.server.server_port
        cls.base_url = f"http://127.0.0.1:{cls.port}"

        cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.server_thread.join()

    def test_read_input_urls(self) -> None:
        """Verify comments and blank lines are ignored when reading input file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = Path(tmpdir) / "test_urls.txt"
            file_path.write_text(
                "# Header comment\n"
                "\n"
                "https://example.com/one\n"
                "   # Indented comment\n"
                "https://example.com/two   \n"
                "\n",
                encoding="utf-8",
            )
            urls = read_input_urls(str(file_path))
            self.assertEqual(urls, ["https://example.com/one", "https://example.com/two"])

    def test_full_pipeline_end_to_end(self) -> None:
        """Run complete pipeline against local HTTP endpoints and verify output JSONL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.txt"
            output_file = Path(tmpdir) / "output.jsonl"

            # Create input list with valid pages, redirects, 404s, binary content, and invalid lines
            urls = [
                f"{self.base_url}/page1",
                f"{self.base_url}/redirect",
                f"{self.base_url}/notfound",
                f"{self.base_url}/image",
                "not a url",
            ]
            content = "# Test urls\n\n" + "\n".join(urls) + "\n\n# Trailing comment\n"
            input_file.write_text(content, encoding="utf-8")

            # Execute pipeline
            run_pipeline(
                input_path=str(input_file),
                output_path=str(output_file),
                concurrency=3,
                timeout=5.0,
                queue_size=5,
            )

            self.assertTrue(output_file.exists())

            # Read and parse JSONL records
            records = []
            with open(output_file, "r", encoding="utf-8") as f:
                for line in f:
                    records.append(json.loads(line.strip()))

            # Must have exactly one record per input URL
            self.assertEqual(len(records), len(urls))

            # Index records by original input URL for verification
            by_url = {rec["url"]: rec for rec in records}

            # 1. /page1: 200 OK, relative links resolved, duplicates/mailto/js stripped
            page1_rec = by_url[f"{self.base_url}/page1"]
            self.assertEqual(page1_rec["status"], 200)
            self.assertEqual(page1_rec["final_url"], f"{self.base_url}/page1")
            self.assertIsNone(page1_rec["error"])
            self.assertEqual(
                page1_rec["links"],
                [f"{self.base_url}/page2", "https://external.org/link"],
            )

            # 2. /redirect: 302 -> /target, relative link resolved against /target
            redirect_rec = by_url[f"{self.base_url}/redirect"]
            self.assertEqual(redirect_rec["status"], 200)
            self.assertEqual(redirect_rec["final_url"], f"{self.base_url}/target")
            self.assertIsNone(redirect_rec["error"])
            self.assertEqual(
                redirect_rec["links"],
                [f"{self.base_url}/relative/endpoint"],
            )

            # 3. /notfound: 404 HTTP Error
            notfound_rec = by_url[f"{self.base_url}/notfound"]
            self.assertEqual(notfound_rec["status"], 404)
            self.assertEqual(notfound_rec["error"], "HTTP 404")
            self.assertEqual(notfound_rec["links"], [])

            # 4. /image: Non-HTML content
            image_rec = by_url[f"{self.base_url}/image"]
            self.assertEqual(image_rec["status"], 200)
            self.assertEqual(image_rec["error"], "Non-HTML content (image/png)")
            self.assertEqual(image_rec["links"], [])

            # 5. 'not a url': Invalid URL syntax
            invalid_rec = by_url["not a url"]
            self.assertIsNone(invalid_rec["status"])
            self.assertEqual(invalid_rec["error"], "Invalid URL")
            self.assertEqual(invalid_rec["links"], [])

    def test_output_file_is_overwritten_not_appended(self) -> None:
        """The output file must be overwritten on each run, not appended to."""
        with tempfile.TemporaryDirectory() as tmpdir:
            input_file = Path(tmpdir) / "input.txt"
            output_file = Path(tmpdir) / "output.jsonl"

            # Pre-populate output file with dummy content
            output_file.write_text('{"dummy": "old_record"}\n{"dummy": "old_record2"}\n')

            # Run with single URL
            input_file.write_text(f"{self.base_url}/page1\n")
            run_pipeline(
                input_path=str(input_file),
                output_path=str(output_file),
                concurrency=1,
                timeout=5.0,
                queue_size=2,
            )

            # File must now contain only the new record, not the old ones
            with open(output_file, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]

            self.assertEqual(len(lines), 1)
            record = json.loads(lines[0])
            self.assertEqual(record["url"], f"{self.base_url}/page1")


if __name__ == "__main__":
    unittest.main()
