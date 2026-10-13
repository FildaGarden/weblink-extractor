"""Producer-consumer pipeline coordinating concurrent fetching and HTML parsing."""

from concurrent.futures import ThreadPoolExecutor
import queue
import threading
from typing import Iterable

from extractor.fetcher import fetch_page
from extractor.models import ExtractionResult, FetchResult
from extractor.parser import extract_links

# Sentinel token signaling consumer termination
_SENTINEL = object()


def read_input_urls(input_path: str) -> list[str]:
    """Read URLs from input file, ignoring blank lines and lines starting with '#'.

    Strips leading and trailing whitespace from each valid line.
    """
    urls: list[str] = []
    with open(input_path, "r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            urls.append(stripped)
    return urls


def _producer_worker(
    url: str,
    timeout: float,
    page_queue: queue.Queue,
    semaphore: threading.Semaphore,
    stop_event: threading.Event,
) -> None:
    """Worker task executed in ThreadPoolExecutor for a single URL fetch."""
    try:
        if stop_event.is_set():
            return
        result = fetch_page(url, timeout=timeout)
        # Put with timeout in loop to allow clean unblocking if consumer fails
        while not stop_event.is_set():
            try:
                page_queue.put(result, timeout=0.2)
                break
            except queue.Full:
                continue
    except Exception as err:
        fallback_result = FetchResult(
            url=url,
            final_url=url,
            status=None,
            html=None,
            error=str(err),
        )
        while not stop_event.is_set():
            try:
                page_queue.put(fallback_result, timeout=0.2)
                break
            except queue.Full:
                continue
    finally:
        semaphore.release()


def _producer_thread_target(
    urls: Iterable[str],
    concurrency: int,
    timeout: float,
    page_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    """Producer coordination thread: submits fetches with concurrency limit.

    Guarantees sentinel enqueuing in finally block to ensure consumer shutdown,
    and responds promptly to stop_event if consumer terminates unexpectedly.
    """
    semaphore = threading.Semaphore(concurrency)
    try:
        with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="Fetcher") as executor:
            for url in urls:
                if stop_event.is_set():
                    break
                # Acquire with timeout to observe stop_event promptly
                while not stop_event.is_set():
                    if semaphore.acquire(timeout=0.2):
                        executor.submit(
                            _producer_worker,
                            url,
                            timeout,
                            page_queue,
                            semaphore,
                            stop_event,
                        )
                        break
    finally:
        if not stop_event.is_set():
            page_queue.put(_SENTINEL)


def _consumer_loop(output_path: str, page_queue: queue.Queue) -> None:
    """Consumer loop: parses HTML from fetched pages and writes JSON lines.

    Overwrites output_path upon starting. Flushes each record to disk.
    """
    with open(output_path, "w", encoding="utf-8") as out_file:
        while True:
            item = page_queue.get()
            if item is _SENTINEL:
                page_queue.task_done()
                break

            try:
                assert isinstance(item, FetchResult)
                if item.error is not None:
                    # Page fetch failed or was non-HTML
                    record = ExtractionResult(
                        url=item.url,
                        final_url=item.final_url,
                        status=item.status,
                        links=[],
                        error=item.error,
                    )
                else:
                    # Successful fetch: extract and normalize links
                    links = extract_links(item.html or "", item.final_url)
                    record = ExtractionResult(
                        url=item.url,
                        final_url=item.final_url,
                        status=item.status,
                        links=links,
                        error=None,
                    )

                out_file.write(record.to_json() + "\n")
                out_file.flush()
            finally:
                page_queue.task_done()


def run_pipeline(
    input_path: str,
    output_path: str,
    concurrency: int = 5,
    timeout: float = 10.0,
    queue_size: int = 10,
) -> None:
    """Execute the full web link extraction pipeline.

    1. Reads URLs from input_path.
    2. Starts a producer thread that manages concurrent HTTP fetches up to concurrency.
    3. Buffers fetched pages in a bounded queue (queue_size) for natural backpressure.
    4. Runs consumer in the current thread to parse HTML and stream output JSON lines.
    5. Exits cleanly when all URLs are processed, leaving no hanging threads.
    """
    urls = read_input_urls(input_path)

    # Bounded queue between producer and consumer
    page_queue: queue.Queue = queue.Queue(maxsize=queue_size)
    stop_event = threading.Event()

    # Start producer in a separate background thread
    producer_thread = threading.Thread(
        target=_producer_thread_target,
        args=(urls, concurrency, timeout, page_queue, stop_event),
        name="ProducerCoordinator",
    )
    producer_thread.start()

    try:
        # Consumer processes pages in the calling thread
        _consumer_loop(output_path, page_queue)
    except Exception:
        stop_event.set()
        raise
    finally:
        stop_event.set()
        # Ensure producer thread is fully terminated before returning
        producer_thread.join()
