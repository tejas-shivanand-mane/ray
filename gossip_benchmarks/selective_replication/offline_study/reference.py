"""Load one shared instance of the repository's existing planner."""

import importlib.util
import sys
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "_selective_offline_planner", Path(__file__).resolve().parents[1] / "planner.py"
)
planner = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = planner
_spec.loader.exec_module(planner)
