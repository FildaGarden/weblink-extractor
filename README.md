# Web Link Extractor

A robust, concurrent command-line web crawler pipeline in Python that fetches web pages, handles redirects, extracts normalized hyperlinks from HTML documents, and streams structured records to a JSON Lines (`.jsonl`) file.

Built with **zero external dependencies**, relying exclusively on Python 3.11+ standard library modules.

---

## Architecture Overview

The program implements a concurrent **Producer-Consumer pipeline** connected by a bounded, thread-safe queue with backpressure:

```
[ Input File (urls.txt) ]
          │
          ▼
┌─────────────────────────────────────────┐
│ Producer Coordinator                    │
│ • Reads valid lines (skips '#' & blanks)│
│ • Manages ThreadPoolExecutor            │
│ • Limits in-flight I/O via Semaphore    │
└───────────────────┬─────────────────────┘
                    │
                    ▼  (up to --concurrency fetches)
┌─────────────────────────────────────────┐
│ Worker Threads (urllib.request)         │
│ • Follows HTTP redirects                │
│ • Enforces request timeout              │
│ • Validates text/html Content-Type      │
│ • Handles HTTP 4xx/5xx, DNS, timeouts   │
└───────────────────┬─────────────────────┘
                    │
                    ▼  blocks on put() when full (Backpressure)
┌─────────────────────────────────────────┐
│ Bounded Queue (queue.Queue, default 10) │
└───────────────────┬─────────────────────┘
                    │
                    ▼  consumes FetchResult
┌─────────────────────────────────────────┐
│ Consumer Thread                         │
│ • Streaming HTMLParser (SAX-like)       │
│ • Resolves relative links vs final_url  │
│ • Strips #fragments & non-HTTP links    │
│ • Deduplicates preserving order         │
│ • Streams JSON records to disk (flush)  │
└───────────────────┬─────────────────────┘
                    │
                    ▼
[ Output File (links.jsonl) ]
```

---

## Installation & Requirements

- **Python Version**: Python 3.11 or higher
- **Dependencies**: None. Uses only standard library modules (`urllib`, `html.parser`, `concurrent.futures`, `queue`, `threading`, `json`, `argparse`, `dataclasses`, `unittest`).

No `pip install` or virtual environment activation is strictly required. Clone and run immediately.

---

## Usage

Run the program as a Python module:

```bash
python -m extractor --input urls.txt --output links.jsonl
```

### Command-Line Arguments

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--input` | `-i` | *(required)* | Path to text file with URLs (one per line). Blank lines and `#` comments are ignored. |
| `--output` | `-o` | *(required)* | Path to output `.jsonl` file. Overwritten on each run. |
| `--concurrency` | `-c` | `5` | Maximum number of concurrent HTTP fetch workers. |
| `--timeout` | `-t` | `10.0` | Request timeout in seconds for each network fetch. |
| `--queue-size` | `-q` | `10` | Maximum size of the bounded queue between fetcher and consumer. |

### Example Run

Using the provided `sample_urls.txt`:

```bash
python -m extractor --input sample_urls.txt --output links.jsonl --concurrency 5 --timeout 10
```

### Output Format

Each line of the output file is an independent JSON object:

```json
{"url": "https://example.com", "final_url": "https://example.com/", "status": 200, "links": ["https://www.iana.org/domains/example"], "error": null}
{"url": "https://httpbin.org/status/404", "final_url": "https://httpbin.org/status/404", "status": 404, "links": [], "error": "HTTP 404"}
{"url": "https://httpbin.org/image/png", "final_url": "https://httpbin.org/image/png", "status": 200, "links": [], "error": "Non-HTML content (image/png)"}
{"url": "not a url", "final_url": "not a url", "status": null, "links": [], "error": "Invalid URL"}
```

---

## Design Decisions

### 1. Concurrency Model: Threading + Semaphore over Asyncio

We chose multi-threading (`ThreadPoolExecutor` + `threading.Semaphore`) rather than `asyncio` for several technical reasons:

1. **GIL Release on Network I/O**: In Python, low-level socket operations (`urllib.request`) release the Global Interpreter Lock (GIL). Multiple worker threads can perform blocking network reads and wait on OS sockets genuinely in parallel.
2. **CPU and I/O Separation**: HTML parsing is CPU-bound. In a naive single-threaded `asyncio` event loop, parsing large HTML strings blocks the event loop thread, starving concurrent download coroutines unless offloaded to an executor. With our threading model, fetch workers handle network I/O concurrently while the consumer thread parses HTML in parallel without event loop contention.
3. **Deterministic Backpressure**: Python's `queue.Queue` provides thread-safe, blocking synchronization out of the box without the complexity of coroutine scheduling, event loops, or async task cancellation.
4. **Architectural Simplicity**: The Producer-Consumer pattern with a bounded queue is straightforward, predictable, and easy to audit line-by-line.

### 2. Full Queue Handling (Backpressure Strategy)

**Decision**: When the queue reaches `--queue-size` (default 10), `page_queue.put()` blocks the worker thread until the consumer reads an item.

**Rationale**:
- **Zero Data Loss**: Dropping results or rejecting pages when the queue is full would compromise data collection integrity.
- **Bounded Memory Footprint**: If reading thousands of URLs while network throughput outpaces parsing speed, an unbounded queue would cause memory usage to explode. Bounding the queue ensures that at most `queue_size` fetched HTML bodies are held in RAM simultaneously.
- **Natural Pipeline Regulation**: Because each worker releases its semaphore permit only *after* successfully enqueuing its result, blocked workers keep the semaphore occupied. This naturally halts the producer coordinator from submitting new network requests until consumer capacity frees up.

### 3. Link Extraction & Normalization

- **Streaming Parser**: We subclass standard library `html.parser.HTMLParser` rather than using heavy DOM libraries (such as BeautifulSoup). `HTMLParser` is an event-driven SAX-style parser that streams through HTML tokens and captures `<a href>` attributes on the fly without building an in-memory document tree.
- **Relative URL Resolution**: Relative paths (`/path`, `sub/page`, `../parent`) are resolved against the page's final URL (after redirects) using `urllib.parse.urljoin`.
- **Filtering & Defragging**:
  - Fragments (`#heading`) are stripped with `urllib.parse.urldefrag`.
  - Only `http` and `https` schemes with valid network locations are retained. Schemes like `mailto:`, `javascript:`, `ftp:`, and `tel:` are cleanly discarded.
- **Deduplication**: Duplicates within each page are removed while preserving first-seen order using an auxiliary `set()` membership check.

### 4. Fault Tolerance & Resilience

A failure on one URL must never abort the pipeline:
- **HTTP 4xx / 5xx**: Caught via `urllib.error.HTTPError`, recording the status code (e.g. `HTTP 404`) and final redirected URL.
- **Timeouts**: Caught via `TimeoutError` and socket timeout, recording `"Request timed out"`.
- **DNS / Unreachable Hosts**: Caught via `urllib.error.URLError`, recording `"Connection failed: <reason>"`.
- **Non-HTML Content**: Validated before parsing; returns status code and `"Non-HTML content (<content_type>)"` without downloading binary payloads.
- **Malformed Input**: Validated prior to network invocation; returns `"Invalid URL"`.

### 5. Deterministic Shutdown

Shutdown is strictly deterministic:
1. The producer coordinator submits all URL fetch tasks and waits for the thread pool to finish.
2. The coordinator places a sentinel object (`_SENTINEL`) onto `page_queue`.
3. The consumer processes all remaining queued items, encounters `_SENTINEL`, flushes and closes the output file, and terminates.
4. The main thread joins the coordinator thread. No orphaned threads, processes, or leaked file handles remain.

---

## Testing

The test suite is fully self-contained, runs offline with **zero network access**, and requires only standard library `unittest`:

```bash
python -m unittest discover tests -v
```

### Test Coverage

1. **`tests/test_parser.py`**:
   - Relative URL resolution against base URL.
   - Fragment stripping (`#section`).
   - Filtering non-HTTP schemes (`mailto:`, `javascript:`, `ftp:`).
   - Malformed hrefs (spaces, empty strings, invalid host syntax).
   - Duplicate link removal while maintaining original appearance order.
   - Resilient parsing on unclosed/corrupt HTML.
2. **`tests/test_fetcher.py`**:
   - Successful 200 OK fetch with HTML decoding.
   - HTTP 404 and 500 error handling.
   - Request timeout handling.
   - Connection/DNS failure handling.
   - Non-HTML MIME type detection (`image/png`).
   - Malformed URL syntax handling.
3. **`tests/test_pipeline.py`**:
   - End-to-end integration test against a local `http.server.HTTPServer` on loopback (`127.0.0.1`).
   - Verifies full pipeline execution with redirects, 404s, non-HTML responses, and invalid lines.
   - Verifies output file overwrite behavior.

---

## What I Would Do With More Time

1. **Transient Failure Retries**:
   - Implement exponential backoff with jitter for transient errors (connection drops, timeouts, HTTP 500/502/503/504).
   - Strict avoidance of retries on deterministic client errors (HTTP 4xx).
2. **Per-Host Rate Limiting**:
   - Implement domain-level token bucket or polite concurrency limits (e.g. max 1 concurrent request per target host) to prevent server throttling or accidental DoS.
3. **Execution Summary Line**:
   - Print a clean summary upon pipeline completion: total URLs processed, successful fetches, failed fetches, total unique links extracted, and elapsed time.
4. **HTTP Connection Pooling & HTTP/2**:
   - In production environments hitting the same domain frequently, integrating `requests.Session` or `httpx` with persistent connection pooling (keep-alive) and HTTP/2 would significantly cut TLS handshake latency.
5. **Containerization**:
   - Provide a multi-stage `Dockerfile` (based on `python:3.12-slim`) for containerized batch execution.
