# Web Link Extractor

A command-line link extractor in Python 3.11+ that fetches web pages concurrently, resolves redirects, normalizes and deduplicates hyperlinks from HTML, and writes JSON Lines (`.jsonl`) records.

Implemented with **zero external dependencies**, relying exclusively on Python standard library modules.

---

## Architecture

The program uses a **Producer-Consumer pipeline** based on a classic **Worker Pool**:

```
[ Input File (urls.txt) ]
          │
          ▼  (lazily streamed by feeder thread)
┌──────────────────────────────────────────────┐
│ Bounded URL Queue (url_queue, maxsize=Q)     │
└──────────────────────┬───────────────────────┘
                       │
                       ▼  (concurrent fetches by N workers)
┌──────────────────────────────────────────────┐
│ Worker Threads (urllib.request)              │
│ • Pull URLs, follow HTTP redirects           │
│ • Enforce 10 MB maximum body size cap        │
│ • Enforce socket timeout                     │
│ • Retry transient errors (5xx, resets)       │
│ • Catch failures (4xx, DNS, non-HTML)        │
└──────────────────────┬───────────────────────┘
                       │
                       ▼  blocks on put() when full (Backpressure)
┌──────────────────────────────────────────────┐
│ Bounded Page Queue (page_queue, maxsize=Q)   │
└──────────────────────┬───────────────────────┘
                       │
                       ▼  consumes FetchResult
┌──────────────────────────────────────────────┐
│ Consumer (Main Thread)                       │
│ • Event-driven HTMLParser (<a> tags only)    │
│ • Resolves relative links vs final_url       │
│ • Strips #fragments and non-HTTP protocols   │
│ • Deduplicates preserving first-seen order   │
│ • Streams JSONL records to disk (flush)      │
└──────────────────────┬───────────────────────┘
                       │
                       ▼
[ Output File (links.jsonl) ]
```

---

## Requirements & Installation

- **Python**: 3.11 or higher
- **Dependencies**: None (uses only `urllib`, `html.parser`, `queue`, `threading`, `json`, `argparse`, `dataclasses`, `unittest`).

No virtual environment or `pip install` required. Clone and run directly:

```bash
python -m extractor --input sample_urls.txt --output links.jsonl
```

---

## CLI Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--input` | `-i` | *(required)* | Path to text file with URLs (one per line). |
| `--output` | `-o` | *(required)* | Path to output `.jsonl` file (overwritten on each run). |
| `--concurrency` | `-c` | `5` | Maximum concurrent fetch workers. |
| `--timeout` | `-t` | `10.0` | Socket timeout in seconds per network request. |
| `--queue-size` | `-q` | `10` | Maximum capacity of bounded queues. |
| `--retries` | `-r` | `0` | Retries for transient failures (timeouts, 500/502/503/504, connection resets). Never retries 4xx or permanent DNS errors. |

### Example

```bash
python -m extractor -i sample_urls.txt -o links.jsonl -c 5 -t 10 -r 2
```

Outputs one JSON object per input URL to `links.jsonl`, and prints an execution summary to `stderr`:
```
Summary: 3 URLs ok, 5 URLs failed, 256 total links found
```

---

## Design Decisions

### Concurrency Model
A classic **Worker Pool with bounded queues** was chosen because:
1. **Zero dependencies**: `urllib.request` is synchronous and blocking. Using `asyncio` would either require third-party libraries (`httpx`, `aiohttp`) or wrapping blocking calls in thread executors.
2. **I/O and CPU overlap**: Network socket calls release Python's Global Interpreter Lock (GIL), allowing worker threads to perform HTTP I/O concurrently. Pure Python HTML parsing does not release the GIL, but it runs in the consumer while worker threads are suspended waiting on network sockets.
3. **Simplicity & Intuitiveness**: A feeder thread, $N$ worker threads, and a consumer thread connected by bounded `queue.Queue` instances mirrors standard goroutine/channel patterns in Go and `BlockingQueue` patterns in Java.

### Backpressure & Memory Bounds
- **Queue saturation**: Both `url_queue` and `page_queue` are bounded to `--queue-size`. If workers fall behind, `url_queue.put()` blocks the feeder thread, halting disk reads. If the consumer falls behind, `page_queue.put()` blocks worker threads, halting network fetches.
- **Memory footprint**: The maximum number of response bodies in memory at any instant is strictly bounded by `queue_size + concurrency + 1` (buffered in queue + held by blocked workers + actively parsed by consumer).
- **Deadlock protection**: All worker and feeder queue operations poll with a short timeout against a `stop_event`. If the consumer encounters a fatal disk error, all background threads unblock and join cleanly without hanging.

### Link Normalization & Resilience
- **Resolution**: Relative links are resolved against `final_url` (after redirects) via `urllib.parse.urljoin`.
- **Filtering**: Fragments (`#section`) are stripped; only `http` and `https` schemes with non-empty hostnames are kept. `mailto:`, `javascript:`, `ftp:`, and empty links are discarded.
- **Deduplication**: Duplicates within a page are removed in $O(1)$ time per link using a `seen` set while preserving the original order of appearance.
- **Per-Page Error Isolation**: If HTML parsing or processing fails on a corrupt page, an error record is emitted and the consumer carries on. A failure on one page never aborts the pipeline run.
- **Output Ordering**: Output records are emitted in completion order (as fetches finish), not input file order. Duplicate input lines are processed as distinct records.

### Retries Strategy
- **Transient failures only**: Retries are restricted to status codes `500, 502, 503, 504`, socket timeouts, and socket connection resets.
- **Never retried**: Client errors (`4xx`), permanent server errors (`501 Not Implemented`, `505`), DNS resolution errors (`gaierror`), SSL certificate verification errors, and invalid URL formats are never retried.
- **Worst-case runtime**: Nominal execution time per URL under retries is approximately bounded by `timeout * (retries + 1) + retry_delay * retries` (subject to per-socket timeout semantics as noted in Known Limitations).

---

## Known Limitations

1. **Socket Timeout vs. Deadline**: `urllib`'s `timeout` is a per-socket-operation timeout (connect/read), not an overall deadline. A slow server streaming bytes intermittently could exceed the nominal timeout.
2. **Body Size Safety Cap**: Responses are capped at 10 MB (`MAX_BODY_BYTES = 10 MB`) before parsing to prevent excessive memory usage. Files exceeding 10 MB produce an explicit error record.
3. **HTML `<base href>`**: Relative URLs are resolved against `final_url`. A document containing `<base href="...">` would override this base, which is currently not handled.
4. **Politeness & robots.txt**: Not implemented within the scope of this single-file pipeline.

---

## Testing

Run the test suite offline with zero network access:

```bash
python -W error -m unittest discover tests -v
```

### Coverage (28 tests)
- **`tests/test_parser.py` (7 tests)**: Relative link resolution, fragment stripping, protocol filtering (`mailto:`, `javascript:`), malformed hrefs, in-page duplicate removal, unclosed HTML tags, empty input.
- **`tests/test_fetcher.py` (14 tests)**: 200 OK, non-200 2xx status (e.g. 203), HTTP 404, HTTP 500, native & socket timeouts, connection/DNS failure, non-HTML MIME types, 10 MB size limit, retries on transient 5xx and connection resets during read, no retries on 4xx, no retries on permanent 501, no retries on DNS NXDOMAIN.
- **`tests/test_pipeline.py` (7 tests)**: End-to-end integration against a local loopback `http.server.HTTPServer` with redirects, 404s, binary content, invalid URLs, file overwrite behavior, summary stats, non-UTF8 input resilience, corrupt page isolation, consumer disk write error propagation, and consumer failure deadlock protection.

---

## What I Would Do With More Time

1. **Content Compression**: Add support for `Accept-Encoding: gzip, deflate` with streaming decompression to reduce network transfer bandwidth.
2. **Per-Host Rate Limiting**: Token-bucket rate limiting per domain to prevent overwhelming target hosts.
3. **Streaming Response Parser**: Parse tokens directly from the incoming socket stream without buffering the entire body into RAM.
4. **Connection Pooling & HTTP/2**: Integrate `httpx` or `urllib3` with connection reuse (keep-alive) to eliminate repeated TLS handshakes when querying the same host multiple times.
5. **Advanced Retry Backoff**: Replace fixed 0.5s delay with exponential backoff (`retry_delay * 2**attempt`), randomized jitter, and honoring server `Retry-After` response headers.
