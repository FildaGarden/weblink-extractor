"""Producer-consumer pipeline coordinating concurrent fetching and HTML parsing."""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from typing import Any

from extractor.fetcher import fetch_page
from extractor.models import ExtractionResult, FetchResult, PipelineStats
from extractor.parser import extract_links

# Sentinel token signaling consumer termination
_SENTINEL = object()


def iter_input_urls(input_path: str) -> Iterator[str]:
    """Lazily read URLs from input file line by line, ignoring blank lines and '#' comments."""
    with open(input_path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            yield stripped


def read_input_urls(input_path: str) -> list[str]:
    """Eager helper returning list of URLs from input file."""
    return list(iter_input_urls(input_path))


def _put_queue(
    page_queue: queue.Queue,
    item: Any,
    stop_event: threading.Event,
    timeout: float = 0.2,
) -> bool:
    """Put an item into the bounded queue, polling stop_event to avoid hanging if consumer stops."""
    while not stop_event.is_set():
        try:
            page_queue.put(item, timeout=timeout)
            return True
        except queue.Full:
            continue
    return False


def _producer_worker(
    url: str,
    timeout: float,
    retries: int,
    page_queue: queue.Queue,
    semaphore: threading.Semaphore,
    stop_event: threading.Event,
) -> None:
    """Worker task executed in ThreadPoolExecutor for a single URL fetch."""
    try:
        if stop_event.is_set():
            return
        result = fetch_page(url, timeout=timeout, retries=retries)
        _put_queue(page_queue, result, stop_event)
    except Exception as err:
        fallback_result = FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error=str(err),
        )
        _put_queue(page_queue, fallback_result, stop_event)
    finally:
        semaphore.release()


def _producer_thread_target(
    url_stream: Iterator[str],
    concurrency: int,
    timeout: float,
    retries: int,
    page_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    """Producer coordinator: reads URLs lazily and submits fetches bounded by concurrency semaphore.

    The semaphore throttles task dispatch against the ThreadPoolExecutor's unbounded queue.
    """
    semaphore = threading.Semaphore(concurrency)
    try:
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="Fetcher") as executor:
            for url in url_stream:
                if stop_event.is_set():
                    break
                # Acquire with timeout to observe stop_event promptly
                while not stop_event.is_set():
                    if semaphore.acquire(timeout=0.2):
                        executor.submit(
                            _producer_worker,
                            url,
                            timeout,
                            retries,
                            page_queue,
                            semaphore,
                            stop_event,
                        )
                        break
    finally:
        # Enqueue sentinel signaling consumer shutdown with deadlock-free polling
        _put_queue(page_queue, _SENTINEL, stop_event)


def _consumer_loop(output_path: str, page_queue: queue.Queue) -> PipelineStats:
    """Consumer loop: parses HTML from fetched pages, writes JSON lines, and computes summary stats.

    Overwrites output_path upon starting. Each item is strictly isolated: a failure parsing or
    handling one page never halts or aborts the entire pipeline run.
    """
    urls_ok = 0
    urls_failed = 0
    total_links = 0

    with open(output_path, "w", encoding="utf-8") as out_file:
        while True:
            item = page_queue.get()
            if item is _SENTINEL:
                break

            try:
                if not isinstance(item, FetchResult):
                    continue

                if item.error is not None:
                    # Page fetch failed or was non-HTML
                    urls_failed += 1
                    record = ExtractionResult(
                        url=item.url,
                        final_url=item.final_url,
                        status=item.status,
                        links=[],
                        error=item.error,
                    )
                else:
                    # Parse HTML links; catch any corrupt/unhandled parse failure per item
                    try:
                        links = extract_links(item.html or "", item.final_url)
                        urls_ok += 1
                        total_links += len(links)
                        record = ExtractionResult(
                            url=item.url,
                            final_url=item.final_url,
                            status=item.status,
                            links=links,
                            error=None,
                        )
                    except Exception as parse_err:
                        urls_failed += 1
                        record = ExtractionResult(
                            url=item.url,
                            final_url=item.final_url,
                            status=item.status,
                            links=[],
                            error=f"Extraction error: {parse_err}",
                        )

                out_file.write(record.to_json() + "\n")
                out_file.flush()
            except Exception:
                # Guarantee consumer loop never dies on unexpected record handling
                urls_failed += 1

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

    1. Streams URLs lazily from input_path.
    2. Runs producer thread managing concurrent HTTP fetches throttled by semaphore.
    3. Buffers fetched pages in a bounded queue (queue_size) for natural backpressure.
    4. Runs consumer in the calling thread to parse HTML, stream output JSON lines, and collect stats.
    5. Exits cleanly when all URLs are processed, leaving no hanging threads.
    """
    url_stream = iter_input_urls(input_path)

    # Bounded queue between producer and consumer
    page_queue: queue.Queue = queue.Queue(maxsize=queue_size)
    stop_event = threading.Event()

    # Start producer in a separate background thread
    producer_thread = threading.Thread(
        target=_producer_thread_target,
        args=(url_stream, concurrency, timeout, retries, page_queue, stop_event),
        name="ProducerCoordinator",
    )
    producer_thread.start()

    try:
        # Consumer processes pages in the calling thread and returns statistics
        stats = _consumer_loop(output_path, page_queue)
        return stats
    except Exception:
        stop_event.set()
        raise
    finally:
        stop_event.set()
        # Ensure producer thread is fully terminated before returning
        producer_thread.join()
