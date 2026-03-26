# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Zac Policzer

"""MCP server for HeapDumpStarDiver — JVM heap dump analysis.

Tools:
  Session lifecycle:
    - convert_heap_dump   Convert HPROF -> Parquet and open a session
    - open_session        Open a session against existing Parquet files
    - list_sessions       Show all sessions and their status
    - close_session       Close a session's DuckDB connection (keep files)
    - cleanup_session     Close connection and delete Parquet files

  Analysis:
    - list_parquet_files  Discover available tables with row counts and schemas
    - query_heap          Run arbitrary DuckDB SQL against Parquet files
    - analyze_heap        Automated waste detection and heap profiling
"""

import asyncio
import json
import os
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .analysis import (
    ParquetResolver,
    format_bytes,
    run_summary,
    run_top_types,
    run_category_breakdown,
    run_byte_array_distribution,
    run_large_byte_arrays,
    run_waste_analysis,
)
from .heap_state import manager

DEFAULT_PAGE_SIZE = 1000

mcp = FastMCP(
    name="HeapDumpStarDiver",
    instructions=(
        "JVM heap dump analyzer. Sessions track state between calls.\n\n"
        "Quick start:\n"
        "  convert_heap_dump(hprof_path) -> opens a session automatically\n"
        "  open_session(parquet_dir)     -> resume from existing Parquet files\n\n"
        "Once a session is open:\n"
        "  list_parquet_files()  -> see available tables\n"
        "  query_heap(sql)       -> run DuckDB SQL against Parquet files\n"
        "  analyze_heap()        -> automated waste detection\n\n"
        "When done:\n"
        "  close_session(id)   -> close connection, keep files for later\n"
        "  cleanup_session(id) -> close connection AND delete files\n\n"
        "If only one session is open, session_id can be omitted from query tools.\n"
        "Use list_sessions() to see all active sessions."
    ),
)


def _json(obj) -> str:
    """Serialize to compact JSON."""
    return json.dumps(obj, indent=2, default=str)


# ---------------------------------------------------------------------------
# Resources — reference material agents can read on demand
# ---------------------------------------------------------------------------

@mcp.resource(
    "heapdump://guides/setup",
    name="Setup Guide",
    description="How to build HeapDumpStarDiver and install MCP server dependencies",
    mime_type="text/markdown",
)
def guide_setup() -> str:
    return """\
# Setup Guide

## Build the HeapDumpStarDiver binary

The Rust binary converts HPROF heap dumps to Parquet files. It must be built before
`convert_heap_dump` can be used.

```bash
# 1. Clone the jvm-hprof dependency (relative to the repo root)
mkdir -p ../../bitbucket
git clone https://bitbucket.org/ZacAttack/jvm-hprof-rs-li-hackweek.git ../../bitbucket/jvm-hprof-rs-li-hackweek

# 2. Build the binary
cargo build --release
```

The binary is output to `target/release/HeapDumpStarDiver`.

## Install MCP server Python dependencies

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

## Binary resolution

The MCP server looks for the binary in this order:
1. `HEAP_DUMP_STAR_DIVER_BINARY_OVERRIDE` environment variable
2. `target/release/HeapDumpStarDiver` relative to the repo root
3. `HeapDumpStarDiver` on PATH
"""


@mcp.resource(
    "heapdump://guides/sql-examples",
    name="SQL Examples",
    description="Example DuckDB SQL queries for common heap dump analysis tasks",
    mime_type="text/markdown",
)
def guide_sql_examples() -> str:
    return """\
# SQL Examples for query_heap

All queries use DuckDB syntax. Reference Parquet files with `read_parquet('pattern')`.
Robo mode files use `_chunk*.parquet` suffixes and bare UInt64 IDs for references.

## Top types by object count
```sql
SELECT type_name, COUNT(*) as cnt
FROM read_parquet('_object_index_chunk*.parquet')
GROUP BY type_name ORDER BY cnt DESC LIMIT 20
```

## Look up what type an object ID belongs to
```sql
SELECT * FROM read_parquet('_object_index_chunk*.parquet')
WHERE obj_id = 12345678
```

## Thread stack analysis
```sql
SELECT sf.class_name, sf.method_name, COUNT(*) as appearances
FROM '_stack_traces.parquet' st, UNNEST(st.frame_ids) AS t(fid)
JOIN '_stack_frames.parquet' sf ON sf.frame_id = t.fid
GROUP BY sf.class_name, sf.method_name
ORDER BY appearances DESC LIMIT 10
```

## Duplicate strings with waste estimate
```sql
WITH str_bytes AS (
    SELECT s.obj_id, s.value as byte_id,
           md5(CAST(b.values AS VARCHAR)) as hash, len(b.values) as len
    FROM read_parquet('java.lang.String_*_chunk*.parquet') s
    JOIN read_parquet('_primitive_arrays_byte_chunk*.parquet') b ON s.value = b.obj_id
)
SELECT hash, COUNT(*) as dups, MIN(len) as str_len
FROM str_bytes GROUP BY hash HAVING COUNT(*) > 1
ORDER BY dups * str_len DESC LIMIT 20
```

## GC roots by type
```sql
SELECT root_type, COUNT(*) as cnt
FROM read_parquet('_gc_roots_chunk*.parquet')
GROUP BY root_type ORDER BY cnt DESC
```

## Class hierarchy (find subclasses)
```sql
SELECT * FROM read_parquet('_class_hierarchy.parquet')
WHERE super_class_name = 'java.util.AbstractMap'
```

## Count objects of a specific class
```sql
SELECT COUNT(*) FROM read_parquet('com.example.MyClass_*_chunk*.parquet')
```

## Join instance fields to the object index for type resolution
```sql
SELECT i.*, idx.type_name as ref_type
FROM read_parquet('com.example.MyClass_*_chunk*.parquet') i
JOIN read_parquet('_object_index_chunk*.parquet') idx ON i.some_ref_field = idx.obj_id
LIMIT 20
```
"""


@mcp.resource(
    "heapdump://guides/waste-checks",
    name="Waste Checks Reference",
    description="What each waste analysis tier checks and what it detects",
    mime_type="text/markdown",
)
def guide_waste_checks() -> str:
    return """\
# Waste Analysis Checks

The `analyze_heap` tool runs waste checks controlled by the `waste_tier` parameter.
Higher tiers include all lower-tier checks plus additional ones.

| Tier | Check | What It Detects |
|------|-------|-----------------|
| 1 | Duplicate Strings | Strings with identical byte[] content |
| 1 | Bad Collections | Empty/single-element HashMap, ArrayList, LinkedList, TreeMap, ConcurrentHashMap |
| 1 | Bad Object Arrays | Zero-length, all-null, single-element, sparse (>70% null) |
| 1 | Bad Primitive Arrays | Zero-length, all-zero, single-element across all 8 primitive types |
| 1 | Boxed Primitives | Integer, Long, Double, etc. wrapper overhead |
| 2 | Collection Sizing | Sparse HashMaps (<33% utilized), oversized ArrayList backing arrays |
| 2 | Duplicate byte[] | Identical byte arrays (MD5 hash, arrays <=10KB) |
| 2 | Class Count | >20K classes suggests classloader leak |
| 2 | GC Roots | Root type breakdown (thread bloat, JNI leaks) |
| 2 | DirectByteBuffer | Off-heap capacity, empty buffers |
| 2 | Thread Stacks | Thread count and stack depth analysis |
| 3 | Duplicate Object Arrays | Same elements in same order |
| 3 | Estimated Shallow Size | Approximate heap usage by type |

## Severity levels

Severity is based on estimated waste bytes:
- **CRITICAL**: >100 MB
- **HIGH**: >10 MB
- **MEDIUM**: >1 MB
- **LOW**: >100 KB
- **INFO**: <=100 KB

## Recommendations

- Start with `waste_tier=1` for a fast scan (5 checks)
- Use `waste_tier=2` (default) for a thorough analysis (11 checks)
- Use `waste_tier=3` only when you need the expensive checks (13 checks, slower on large heaps)
"""


# ---------------------------------------------------------------------------
# Session lifecycle tools
# ---------------------------------------------------------------------------

@mcp.tool()
async def convert_heap_dump(
    hprof_path: str,
    session_id: str = "",
    flush_rows: int = 500_000,
) -> str:
    """Convert a JVM heap dump (HPROF) to Parquet files and open an analysis session.

    Always uses robo mode for LLM-optimized output.
    Output is written to <hprof_parent>/<session_id>/parquet/.

    Args:
        hprof_path: Absolute path to the .hprof file.
        session_id: Optional session name. Defaults to the HPROF filename stem.
        flush_rows: Rows to buffer before flushing. Lower = less RAM.
    """
    hprof = Path(hprof_path).resolve()
    if not hprof.is_file():
        return _json({"error": f"File not found: {hprof}"})

    if manager.rust_binary is None:
        return _json({
            "error": "HeapDumpStarDiver binary not found.",
            "hint": "Run `cargo build --release` in the repo root, or set "
                    "HEAP_DUMP_STAR_DIVER_BINARY_OVERRIDE to the binary path.",
        })

    # Determine session ID before creating directories
    sid = session_id or hprof.stem

    # Create a session-specific working directory next to the HPROF file.
    # The Rust binary writes to parquet/ relative to CWD, so we get:
    #   <hprof_parent>/<session_id>/parquet/
    work_dir = hprof.parent / sid
    work_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir = work_dir / "parquet"

    cmd = [
        str(manager.rust_binary),
        "-f", str(hprof),
        "dump-objects-to-parquet",
        "--flush-rows", str(flush_rows),
        "--robo-mode",
    ]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(work_dir),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()

    if proc.returncode != 0:
        return _json({
            "error": f"Conversion failed (exit {proc.returncode})",
            "stderr": stderr.decode(),
        })

    sess = manager.create_session(
        parquet_dir=parquet_dir,
        hprof_path=hprof,
        session_id=sid,
    )

    pq_files = sorted(parquet_dir.glob("*.parquet"))
    total_size = sum(f.stat().st_size for f in pq_files)

    return _json({
        "status": "ok",
        "session_id": sess.session_id,
        "parquet_dir": str(parquet_dir),
        "files_created": len(pq_files),
        "total_size": format_bytes(total_size),
        "rust_output": stdout.decode().strip() or stderr.decode().strip(),
    })


@mcp.tool()
def open_session(
    parquet_dir: str,
    session_id: str = "",
) -> str:
    """Open an analysis session against an existing Parquet directory.

    Use this to resume work on a previously converted heap dump.

    Args:
        parquet_dir: Path to directory containing .parquet files.
        session_id: Optional session name. Defaults to the directory name.
    """
    pdir = Path(parquet_dir).resolve()
    if not pdir.is_dir():
        return _json({"error": f"Directory not found: {pdir}"})

    pq_files = sorted(pdir.glob("*.parquet"))
    if not pq_files:
        return _json({"error": f"No .parquet files found in {pdir}"})

    sess = manager.create_session(
        parquet_dir=pdir,
        session_id=session_id or None,
    )

    return _json({
        "status": "ok",
        "session_id": sess.session_id,
        "parquet_dir": str(pdir),
        "files_available": len(pq_files),
    })


@mcp.tool()
def list_sessions() -> str:
    """List all heap dump sessions and their status."""
    sessions = manager.list_all()
    if not sessions:
        return _json({
            "sessions": [],
            "note": "No sessions. Use convert_heap_dump or open_session to start.",
        })
    return _json({"sessions": sessions})


@mcp.tool()
def close_session(session_id: str) -> str:
    """Close a session's DuckDB connection but keep Parquet files on disk.

    The session can be reopened later with open_session.

    Args:
        session_id: The session to close.
    """
    try:
        manager.close_session(session_id)
    except KeyError as e:
        return _json({"error": str(e)})
    return _json({
        "status": "ok",
        "session_id": session_id,
        "note": "Connection closed. Parquet files retained.",
    })


@mcp.tool()
def cleanup_session(session_id: str, confirm: bool = False) -> str:
    """Close a session and DELETE its Parquet files from disk.

    This is destructive — the Parquet files will be permanently removed.
    Set confirm=True to proceed.

    Args:
        session_id: The session to clean up.
        confirm: Must be True to proceed with file deletion.
    """
    if not confirm:
        return _json({
            "error": "Destructive operation — set confirm=True to delete Parquet files.",
            "session_id": session_id,
        })
    try:
        files_deleted, dir_path = manager.cleanup_session(session_id)
    except KeyError as e:
        return _json({"error": str(e)})

    return _json({
        "status": "ok",
        "session_id": session_id,
        "files_deleted": files_deleted,
        "directory": dir_path,
    })


# ---------------------------------------------------------------------------
# Analysis tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_parquet_files(session_id: str = "") -> str:
    """List available Parquet files with row counts and schema info.

    Args:
        session_id: Session to query. If omitted, uses the only active session.
    """
    try:
        sess = manager.get(session_id or None)
    except (KeyError, ValueError) as e:
        return _json({"error": str(e)})

    pdir = sess.parquet_dir
    pq_files = sorted(pdir.glob("*.parquet"))
    if not pq_files:
        return _json({"error": f"No .parquet files found in {pdir}"})

    con = sess.connection
    original_dir = os.getcwd()
    os.chdir(pdir)

    try:
        system_files = []
        class_files = []

        for pf in pq_files:
            name = pf.stem
            try:
                row_count = con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{pf.name}')"
                ).fetchone()[0]
                schema_rows = con.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{pf.name}')"
                ).fetchall()
                columns = [{"name": r[0], "type": r[1]} for r in schema_rows]
            except Exception:
                row_count = -1
                columns = []

            entry = {
                "file": pf.name,
                "row_count": row_count,
                "columns": columns,
            }

            if name.startswith("_"):
                system_files.append(entry)
            else:
                class_files.append(entry)

        class_files.sort(key=lambda x: x["row_count"], reverse=True)

        return _json({
            "session_id": sess.session_id,
            "parquet_dir": str(pdir),
            "total_files": len(pq_files),
            "system_files": system_files,
            "class_files": class_files,
        })
    finally:
        os.chdir(original_dir)


@mcp.tool()
def query_heap(
    sql: str,
    session_id: str = "",
    limit: int = DEFAULT_PAGE_SIZE,
    offset: int = 0,
) -> str:
    """Run a DuckDB SQL query against the heap dump Parquet files.

    Results are paginated. Use limit/offset to page through large result sets.
    Use read_parquet('filename_pattern') to reference tables. Example:
        SELECT * FROM read_parquet('java.lang.String_*.parquet')

    Args:
        sql: DuckDB SQL query.
        session_id: Session to query. If omitted, uses the only active session.
        limit: Max rows to return per page. Default 1000.
        offset: Number of rows to skip. Default 0.
    """
    try:
        sess = manager.get(session_id or None)
    except (KeyError, ValueError) as e:
        return _json({"error": str(e)})

    con = sess.connection
    original_dir = os.getcwd()
    os.chdir(sess.parquet_dir)

    try:
        # Wrap the user's query with pagination
        paginated_sql = f"SELECT * FROM ({sql}) LIMIT {limit + 1} OFFSET {offset}"
        result = con.execute(paginated_sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        has_more = len(rows) > limit
        if has_more:
            rows = rows[:limit]

        data = [dict(zip(columns, row)) for row in rows]

        response = {
            "session_id": sess.session_id,
            "columns": columns,
            "row_count": len(data),
            "offset": offset,
            "limit": limit,
            "has_more": has_more,
            "rows": data,
        }
        if has_more:
            response["next_offset"] = offset + limit
        return _json(response)
    except Exception as e:
        return _json({"error": str(e)})
    finally:
        os.chdir(original_dir)


@mcp.tool()
def analyze_heap(
    session_id: str = "",
    waste: bool = True,
    waste_tier: int = 2,
    top_n: int = 30,
) -> str:
    """Run automated heap analysis including waste detection.

    Args:
        session_id: Session to analyze. If omitted, uses the only active session.
        waste: Enable memory waste analysis.
        waste_tier: Depth: 1=fast (5 checks), 2=default (11 checks), 3=thorough (13 checks).
        top_n: Number of top types to show.
    """
    try:
        sess = manager.get(session_id or None)
    except (KeyError, ValueError) as e:
        return _json({"error": str(e)})

    con = sess.connection
    resolver = ParquetResolver(str(sess.parquet_dir))
    original_dir = os.getcwd()
    os.chdir(sess.parquet_dir)

    try:
        result = {
            "session_id": sess.session_id,
            "parquet_dir": str(sess.parquet_dir),
        }

        summary = run_summary(con, resolver)
        if summary:
            result["summary"] = summary[0]

        result["top_types"] = run_top_types(con, resolver, limit=top_n)
        result["categories"] = run_category_breakdown(con, resolver)
        result["byte_array_distribution"] = run_byte_array_distribution(con, resolver)
        result["large_byte_arrays"] = run_large_byte_arrays(con, resolver)

        if waste:
            findings = run_waste_analysis(con, resolver, max_tier=waste_tier)
            total_waste = sum(f.estimated_waste_bytes for f in findings)
            result["waste_findings"] = [
                {
                    "check_name": f.check_name,
                    "tier": f.tier,
                    "severity": f.severity,
                    "affected_count": f.affected_count,
                    "estimated_waste_bytes": f.estimated_waste_bytes,
                    "estimated_waste_human": format_bytes(f.estimated_waste_bytes),
                    "details": f.details,
                    "recommendation": f.recommendation,
                    "sub_findings": f.sub_findings,
                }
                for f in findings
            ]
            result["total_estimated_waste"] = format_bytes(total_waste)
            result["total_estimated_waste_bytes"] = total_waste

        return _json(result)
    except Exception as e:
        return _json({"error": str(e)})
    finally:
        os.chdir(original_dir)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
