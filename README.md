# Web Link Extractor

A command-line link extractor in Python 3.11+ that fetches web pages concurrently, resolves redirects, normalizes and deduplicates hyperlinks from HTML, and writes JSON Lines (`.jsonl`) records.

Implemented with **zero external dependencies**, relying exclusively on Python standard library modules.

---

## Architecture

The program uses a **Producer-Consumer pipeline** connected by a bounded, thread-safe queue:

```
[ Input File (urls.txt) ]
          │
          ▼
┌──────────────────────────────────────────────┐
│ Producer Coordinator                         │
│ • Lazily streams URLs (skips '#' & blanks)   │
│ • Throttles task submission via Semaphore    │
│ • Runs ThreadPoolExecutor (max_workers=N)    │
└──────────────────────┬───────────────────────┘
                       │
                       ▼  (concurrent fetches)
┌──────────────────────────────────────────────┐
│ Worker Threads (urllib.request)              │
│ • Follows HTTP redirects                     │
│ • Enforces 10 MB maximum body size limit     │
│ • Decompresses gzip and deflate              │
│ • Enforces socket timeout                    │
│ • Retries transient errors (5xx, timeouts)   │
│ • Catches failures (4xx, DNS, non-HTML)      │
└──────────────────────┬───────────────────────┘
                       │
                       ▼  blocks on put() when full (Backpressure)
┌──────────────────────────────────────────────┐
│ Bounded Queue (queue.Queue, default 10)      │
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
- **Dependencies**: None (uses only `urllib`, `html.parser`, `concurrent.futures`, `queue`, `threading`, `json`, `argparse`, `dataclasses`, `unittest`).

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
| `--queue-size` | `-q` | `10` | Maximum capacity of the bounded queue. |
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
Threads (`ThreadPoolExecutor` + `queue.Queue`) were chosen because:
1. **Zero dependencies**: `urllib.request` is synchronous and blocking. Using `asyncio` would either require third-party libraries (`httpx`, `aiohttp`) or wrapping blocking calls in `run_in_executor`, which uses a thread pool anyway.
2. **I/O and CPU overlap**: Network socket calls release Python's Global Interpreter Lock (GIL), allowing worker threads to perform HTTP I/O concurrently. Pure Python HTML parsing does not release the GIL, but it runs while other threads are suspended waiting on network sockets. For CPU-bound parsing at extreme scale, parsing would be offloaded to a process pool.
3. **Asyncio trade-off**: `asyncio` with `httpx` is also a valid architectural choice, but threading provides native synchronization with `queue.Queue` with no event loop overhead or external packages.

### Backpressure & Memory Bounds
- **Queue saturation**: When the queue reaches `--queue-size`, `page_queue.put()` blocks the worker thread until the consumer reads an item.
- **Task dispatch throttling**: `ThreadPoolExecutor` has an unbounded task queue. The input file is streamed lazily line-by-line (`iter_input_urls`), and we use `threading.Semaphore(concurrency)` in the coordinator loop so that at most `concurrency` tasks are in-flight in the executor at once. Because the semaphore is released only *after* `page_queue.put()` succeeds, full queue backpressure halts the coordinator from reading further URLs from disk.
- **Memory footprint**: The maximum number of response bodies in memory at any instant is bounded by `queue_size + concurrency + 1` (buffered in queue + held by blocked workers + actively parsed by consumer).
- **Deadlock protection**: Worker `put()` calls poll with a short timeout against a `stop_event`. If the consumer encounters a fatal disk error, the coordinator and workers unblock and terminate cleanly instead of deadlocking.

### Link Normalization & Resilience
- **Resolution**: Relative links are resolved against `final_url` (after redirects) via `urllib.parse.urljoin`.
- **Filtering**: Fragments (`#section`) are stripped; only `http` and `https` schemes with non-empty hostnames are kept. `mailto:`, `javascript:`, `ftp:`, and empty links are discarded.
- **Deduplication**: Duplicates within a page are removed in $O(1)$ time per link using a `seen` set while preserving the original order of appearance.
- **Per-Page Error Isolation**: If HTML parsing or processing fails on a corrupt page, an error record is emitted and the consumer carries on. A failure on one page never aborts the pipeline run.
- **Output Ordering**: Output records are emitted in completion order (as fetches finish), not input file order. Duplicate input lines are processed as distinct records.

### Retries Strategy
- **Transient failures only**: Retries are restricted to status codes `500, 502, 503, 504`, socket timeouts, and socket connection resets.
- **Never retried**: Client errors (`4xx`), permanent server errors (`501 Not Implemented`, `505`), DNS resolution errors (`gaierror`), SSL certificate verification errors, and invalid URL formats are never retried.
- **Worst-case runtime**: Maximum execution time per URL under retries is bounded by `timeout * (retries + 1) + retry_delay * retries`.

---

## Known Limitations

1. **Socket Timeout vs. Deadline**: `urllib`'s `timeout` is a per-socket-operation timeout (connect/read), not an overall deadline. A slow server streaming bytes intermittently could exceed the nominal timeout.
2. **Body Size Safety Cap**: Responses are capped at 10 MB (`MAX_BODY_BYTES = 10 MB`) before parsing to prevent memory exhaustion and decompression bombs. Files exceeding 10 MB produce an explicit error record.
3. **HTML `<base href>`**: Relative URLs are resolved against `final_url`. A document containing `<base href="...">` would override this base, which is currently not handled.
4. **Politeness & robots.txt**: Not implemented within the scope of this single-file pipeline.

---

## Testing

Run the test suite offline with zero network access:

```bash
python -W error -m unittest discover tests -v
```

### Coverage (24 tests)
- **`tests/test_parser.py`**: Relative link resolution, fragment stripping, protocol filtering (`mailto:`, `javascript:`), malformed hrefs, in-page duplicate removal, unclosed HTML tags.
- **`tests/test_fetcher.py`**: 200 OK, HTTP 404, HTTP 500, timeouts, connection/DNS failure, non-HTML MIME types, gzip/deflate decompression, 10 MB size limit, retries on 5xx, no retries on 4xx, no retries on 501, no retries on DNS NXDOMAIN.
- **`tests/test_pipeline.py`**: End-to-end integration against a local loopback `http.server.HTTPServer` with redirects, 404s, binary content, invalid URLs, file overwrite behavior, summary stats, corrupt page isolation, and consumer failure deadlock protection.

---

## What I Would Do With More Time

1. **Per-Host Rate Limiting**: Token-bucket rate limiting per domain to prevent overwhelming target hosts.
2. **Streaming Response Parser**: Parse tokens directly from the incoming socket stream without buffering the entire body into RAM.
3. **Connection Pooling & HTTP/2**: Integrate `httpx` or `urllib3` with connection reuse (keep-alive) to eliminate repeated TLS handshakes when querying the same host multiple times.
4. **Containerization**: Provide a lightweight `Dockerfile` (`python:3.12-alpine`) for reproducible container deployment.
