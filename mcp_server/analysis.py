"""Thin re-export layer over scripts/analyze_heap_parquet.py.

Keeps one source of truth for the analysis logic — if the script evolves,
the MCP server picks up changes automatically.
"""

import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).resolve().parent.parent / "scripts")
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)

from analyze_heap_parquet import (  # noqa: E402
    ParquetResolver,
    WasteFinding,
    classify_severity,
    format_bytes,
    query,
    try_query,
    run_summary,
    run_top_types,
    run_category_breakdown,
    run_byte_array_distribution,
    run_large_byte_arrays,
    run_waste_analysis,
    ALL_CHECKS,
)
