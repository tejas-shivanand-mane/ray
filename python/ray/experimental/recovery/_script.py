"""Execute a script normally, preserving argv and sibling imports."""

import runpy
import sys
from pathlib import Path


if __name__ == "__main__":
    script = Path(sys.argv[1]).resolve()
    sys.argv = [str(script), *sys.argv[2:]]
    sys.path.insert(0, str(script.parent))
    runpy.run_path(str(script), run_name="__main__")
