---
name: heap-analysis
description: Analyze JVM heap dumps using HeapDumpStarDiver (HPROF-to-Parquet) and DuckDB. Automated waste detection, memory profiling, and triage.
---

# Heap Dump Analysis with HeapDumpStarDiver

Use the **heapdump MCP tools** for all heap dump analysis. Do not use bash-based workflows.

## Workflow

1. **Convert:** `convert_heap_dump(hprof_path="/path/to/dump.hprof")` — parses the HPROF file into Parquet and opens a named session
2. **Discover:** `list_parquet_files()` — see what tables and columns are available
3. **Analyze:** `analyze_heap()` — run automated waste detection (duplicate strings, bad collections, boxed primitives, etc.)
4. **Query:** `query_heap(sql="SELECT ...")` — ad-hoc DuckDB SQL against the Parquet files using `read_parquet('pattern')`
5. **Clean up:** `close_session(id)` to keep files, or `cleanup_session(id, confirm=True)` to delete them

## Session Management

- Sessions are named after the HPROF filename by default (e.g. `heap-dump-2024` from `heap-dump-2024.hprof`)
- If only one session is open, `session_id` can be omitted from all query tools
- Use `list_sessions()` to see all active sessions
- Multiple sessions can be open simultaneously for comparing heap dumps
- `query_heap` supports pagination via `limit` and `offset` parameters
- To resume a previous analysis, use `open_session(parquet_dir="/path/to/parquet")` instead of re-converting

## Prerequisites

If `convert_heap_dump` reports the binary is missing:

```bash
# Clone the jvm-hprof dependency (needed at ../../bitbucket/ relative to repo root)
mkdir -p ../../bitbucket
git clone https://bitbucket.org/ZacAttack/jvm-hprof-rs-li-hackweek.git ../../bitbucket/jvm-hprof-rs-li-hackweek

# Build the binary
cargo build --release
```

The MCP server also requires Python dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

## Waste Checks Reference

The `analyze_heap` tool runs these checks, controlled by `waste_tier`:

| Tier | Check | What It Detects |
|------|-------|-----------------|
| 1 | Duplicate Strings | Strings with identical byte[] content |
| 1 | Bad Collections | Empty/single-element HashMap, ArrayList, LinkedList, TreeMap, ConcurrentHashMap |
| 1 | Bad Object Arrays | Zero-length, all-null, single-element, sparse (>70% null) |
| 1 | Bad Primitive Arrays | Zero-length, all-zero, single-element across all 8 primitive types |
| 1 | Boxed Primitives | Integer, Long, Double, etc. wrapper overhead |
| 2 | Collection Sizing | Sparse HashMaps (<33% utilized), oversized ArrayList backing arrays |
| 2 | Duplicate byte[] | Identical byte arrays (MD5 hash, arrays ≤10KB) |
| 2 | Class Count | >20K classes suggests classloader leak |
| 2 | GC Roots | Root type breakdown (thread bloat, JNI leaks) |
| 2 | DirectByteBuffer | Off-heap capacity, empty buffers |
| 2 | Thread Stacks | Thread count and stack depth analysis |
| 3 | Duplicate Object Arrays | Same elements in same order |
| 3 | Estimated Shallow Size | Approximate heap usage by type |

## Example SQL for query_heap

Parquet files use robo mode conventions: chunked files (`_chunk*.parquet`), bare UInt64 IDs for references, and a separate `_object_index` for type lookups.

```sql
-- Top types by count
SELECT type_name, COUNT(*) as cnt
FROM read_parquet('_object_index_chunk*.parquet')
GROUP BY type_name ORDER BY cnt DESC LIMIT 20

-- Thread stack analysis
SELECT sf.class_name, sf.method_name, COUNT(*) as appearances
FROM '_stack_traces.parquet' st, UNNEST(st.frame_ids) AS t(fid)
JOIN '_stack_frames.parquet' sf ON sf.frame_id = t.fid
GROUP BY sf.class_name, sf.method_name
ORDER BY appearances DESC LIMIT 10

-- Duplicate strings with waste estimate
WITH str_bytes AS (
    SELECT s.obj_id, s.value as byte_id,
           md5(CAST(b.values AS VARCHAR)) as hash, len(b.values) as len
    FROM read_parquet('java.lang.String_*_chunk*.parquet') s
    JOIN read_parquet('_primitive_arrays_byte_chunk*.parquet') b ON s.value = b.obj_id
)
SELECT hash, COUNT(*) as dups, MIN(len) as str_len
FROM str_bytes GROUP BY hash HAVING COUNT(*) > 1
ORDER BY dups * str_len DESC LIMIT 20

-- Look up what type an object ID belongs to
SELECT * FROM read_parquet('_object_index_chunk*.parquet')
WHERE obj_id = 12345678

-- GC roots by type
SELECT root_type, COUNT(*) as cnt
FROM read_parquet('_gc_roots_chunk*.parquet')
GROUP BY root_type ORDER BY cnt DESC
```
