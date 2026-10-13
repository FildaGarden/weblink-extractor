"""Command-line interface argument parsing and application entry point."""

import argparse
from pathlib import Path
import sys

from extractor.pipeline import run_pipeline


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """Parse and validate command-line arguments."""
    parser = argparse.ArgumentParser(
        prog="python -m extractor",
        description="Web Link Extractor: Concurrent web page fetcher and hyperlink extractor.",
    )

    parser.add_argument(
        "--input",
        "-i",
        required=True,
        type=str,
        help="Path to input text file containing URLs (one per line).",
    )

    parser.add_argument(
        "--output",
        "-o",
        required=True,
        type=str,
        help="Path to output JSONL file to overwrite with extracted links.",
    )

    parser.add_argument(
        "--concurrency",
        "-c",
        type=int,
        default=5,
        help="Maximum number of concurrent HTTP fetch workers (default: 5).",
    )

    parser.add_argument(
        "--timeout",
        "-t",
        type=float,
        default=10.0,
        help="HTTP request timeout in seconds (default: 10.0).",
    )

    parser.add_argument(
        "--queue-size",
        "-q",
        type=int,
        default=10,
        help="Maximum bounded queue size between fetcher and parser (default: 10).",
    )

    parsed = parser.parse_args(args)

    if parsed.concurrency < 1:
        parser.error("--concurrency must be an integer greater than or equal to 1.")

    if parsed.timeout <= 0:
        parser.error("--timeout must be a positive number greater than 0.")

    if parsed.queue_size < 1:
        parser.error("--queue-size must be an integer greater than or equal to 1.")

    return parsed


def main(argv: list[str] | None = None) -> int:
    """Main execution function invoked by python -m extractor."""
    args = parse_args(argv)

    input_path = Path(args.input)
    if not input_path.is_file():
        print(f"Error: Input file '{args.input}' does not exist or is not a file.", file=sys.stderr)
        return 1

    try:
        run_pipeline(
            input_path=str(input_path),
            output_path=args.output,
            concurrency=args.concurrency,
            timeout=args.timeout,
            queue_size=args.queue_size,
        )
        return 0
    except KeyboardInterrupt:
        print("\nExecution interrupted by user.", file=sys.stderr)
        return 130
    except Exception as err:
        print(f"Fatal error during pipeline execution: {err}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
