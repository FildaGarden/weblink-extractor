"""Producer-consumer pipeline coordinating concurrent fetching and HTML parsing."""

from collections.abc import Iterator
import queue
import threading
from typing import Any

from extractor.fetcher import fetch_page
from extractor.models import ExtractionResult, FetchResult, PipelineStats
from extractor.parser import extract_links

_SENTINEL = object()


def iter_input_urls(input_path: str) -> Iterator[str]:
    """Lazily read URLs from input file line by line, ignoring blank lines and '#' comments."""
    with open(input_path, "r", encoding="utf-8", errors="replace") as file:
        for line in file:
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                yield stripped


def read_input_urls(input_path: str) -> list[str]:
    """Eager helper returning list of URLs from input file."""
    return list(iter_input_urls(input_path))


def _put_queue(q: queue.Queue, item: Any, stop_event: threading.Event) -> None:
    """Put an item into a bounded queue without hanging if stop_event is signaled."""
    while not stop_event.is_set():
        try:
            q.put(item, timeout=0.1)
            break
        except queue.Full:
            continue


def _fetch_worker(
    url_queue: queue.Queue,
    page_queue: queue.Queue,
    timeout: float,
    retries: int,
    stop_event: threading.Event,
) -> None:
    """Worker thread: pulls URLs from url_queue, fetches pages, and puts results into page_queue."""
    while not stop_event.is_set():
        try:
            url = url_queue.get(timeout=0.1)
        except queue.Empty:
            continue

        if url is _SENTINEL:
            break

        try:
            result = fetch_page(url, timeout=timeout, retries=retries)
        except Exception as err:
            result = FetchResult(url=url, final_url=url, error=str(err))

        _put_queue(page_queue, result, stop_event)


def _consumer_loop(
    output_path: str,
    page_queue: queue.Queue,
    stop_event: threading.Event,
) -> PipelineStats:
    """Consumer loop: reads fetched pages from page_queue, parses links, and writes JSONL."""
    urls_ok = 0
    urls_failed = 0
    total_links = 0

    with open(output_path, "w", encoding="utf-8") as out_file:
        while not stop_event.is_set():
            try:
                item = page_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            if item is _SENTINEL:
                break

            links, error = [], item.error
            if error is None:
                try:
                    links = extract_links(item.html or "", item.final_url)
                    urls_ok += 1
                    total_links += len(links)
                except Exception as exc:
                    error = f"Extraction error: {exc}"
                    urls_failed += 1
            else:
                urls_failed += 1

            record = ExtractionResult(
                url=item.url,
                final_url=item.final_url,
                status=item.status,
                links=links,
                error=error,
            )

            out_file.write(record.to_json() + "\n")
            out_file.flush()

    return PipelineStats(
        urls_ok=urls_ok,
        urls_failed=urls_failed,
        total_links=total_links,
    )


def run_pipeline(
    input_path: str,
    output_path: str,
    concurrency: int = 5,
    timeout: float = 10.0,
    queue_size: int = 10,
    retries: int = 0,
) -> PipelineStats:
    """Execute the full web link extraction pipeline and return summary stats.

    Uses a classic Worker Pool architecture:
    1. Feeder thread streams URLs from file into bounded url_queue (backpressure on file reading).
    2. Concurrency worker threads pull URLs, fetch pages, and push results to bounded page_queue.
    3. Consumer (calling thread) parses HTML, streams JSONL records to disk, and tracks statistics.
    4. Deterministic shutdown via sentinel tokens and stop_event guarantees zero orphan threads.
    """
    url_queue: queue.Queue = queue.Queue(maxsize=queue_size)
    page_queue: queue.Queue = queue.Queue(maxsize=queue_size)
    stop_event = threading.Event()

    # 1. Start worker threads
    workers = [
        threading.Thread(
            target=_fetch_worker,
            args=(url_queue, page_queue, timeout, retries, stop_event),
            name=f"Fetcher-{i}",
        )
        for i in range(concurrency)
    ]
    for w in workers:
        w.start()

    # 2. Feeder thread reads URLs from file into url_queue, then coordinates worker shutdown
    feeder_errors: list[Exception] = []

    def _feeder() -> None:
        try:
            for url in iter_input_urls(input_path):
                if stop_event.is_set():
                    break
                _put_queue(url_queue, url, stop_event)
        except Exception as err:
            feeder_errors.append(err)
        finally:
            # Signal all workers to stop
            for _ in workers:
                _put_queue(url_queue, _SENTINEL, stop_event)
            # Wait for all workers to finish putting results
            for w in workers:
                w.join()
            # Signal consumer to stop
            _put_queue(page_queue, _SENTINEL, stop_event)

    feeder_thread = threading.Thread(target=_feeder, name="URLFeeder")
    feeder_thread.start()

    try:
        # Consumer processes pages in the calling thread and returns statistics
        stats = _consumer_loop(output_path, page_queue, stop_event)
    except Exception:
        stop_event.set()
        raise
    finally:
        stop_event.set()
        feeder_thread.join()
        for w in workers:
            w.join()

    # If file reading encountered an error, propagate it
    if feeder_errors:
        raise feeder_errors[0]

    return stats
