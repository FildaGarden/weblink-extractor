"""Entry point for executing extractor module via python -m extractor."""

import sys
from extractor.cli import main

if __name__ == "__main__":
    sys.exit(main())
