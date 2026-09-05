#!/usr/bin/env python3
"""See README.md for workloads, measurement boundaries, and output files."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from profiling import main

if __name__ == "__main__":
    main()
