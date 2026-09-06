#!/usr/bin/env python3
"""Compare disabled, Fixed-R K=32 and Succession K=32 at R=W=1,2,3."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent / "_support"))
from replication_counts import main

if __name__ == "__main__":
    main()
