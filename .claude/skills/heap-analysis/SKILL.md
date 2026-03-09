---
name: heap-analysis
description: Analyze JVM heap dumps using HeapDumpStarDiver (HPROF-to-Parquet) and DuckDB. Automated waste detection, memory profiling, and triage.
---

# Heap Dump Analysis with HeapDumpStarDiver

**Argument:** `<path-to-hprof-file>` (required)

## Overview

This skill turns JVM heap dumps (.hprof) into queryable Parquet files using HeapDumpStarDiver, then runs automated analysis via DuckDB. It's designed for LLM-assisted heap dump triage — an AI agent can parse a multi-GB heap dump and surface findings in minutes.

## Step 1: Parse Heap Dump

```bash
HPROF_FILE="<path-to-hprof-file>"
HPROF_DIR="$(dirname "$HPROF_FILE")"
HPROF_NAME="$(basename "$HPROF_FILE")"

# Build if needed
cargo build --release

# Parse (robo mode recommended for LLM analysis)
cd "$HPROF_DIR"
time /path/to/HeapDumpStarDiver -f "$HPROF_NAME" dump-objects-to-parquet --robo-mode
```

**Output:** `$HPROF_DIR/parquet/` with per-class Parquet files, system files (`_object_index`, `_gc_roots`, `_stack_frames`, `_stack_traces`, etc.)

## Step 2: Automated Analysis

```bash
# Standard analysis (summary, top types, byte array distribution)
python3 scripts/analyze_heap_parquet.py "$HPROF_DIR/parquet"

# Memory waste analysis (duplicate strings, bad collections, boxed primitives, etc.)
python3 scripts/analyze_heap_parquet.py "$HPROF_DIR/parquet" --waste

# Tier control: 1=fast (5 checks), 2=default (10 checks), 3=thorough (12 checks)
python3 scripts/analyze_heap_parquet.py "$HPROF_DIR/parquet" --waste --waste-tier 3

# JSON output for programmatic consumption
python3 scripts/analyze_heap_parquet.py "$HPROF_DIR/parquet" --waste --json
```

### Waste Checks

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
| 3 | Duplicate Object Arrays | Same elements in same order |
| 3 | Estimated Shallow Size | Approximate heap usage by type |

## Step 3: Ad-hoc DuckDB Queries

```bash
cd "$HPROF_DIR/parquet"

# Top types by count
duckdb -c "SELECT type_name, COUNT(*) as cnt FROM read_parquet('_object_index_chunk*.parquet') GROUP BY type_name ORDER BY cnt DESC LIMIT 20;"

# Thread stacks (requires stack frame/trace parquet output)
duckdb -c "
SELECT sf.class_name, sf.method_name, COUNT(*) as appearances
FROM '_stack_traces.parquet' st, UNNEST(st.frame_ids) AS t(fid)
JOIN '_stack_frames.parquet' sf ON sf.frame_id = t.fid
GROUP BY sf.class_name, sf.method_name
ORDER BY appearances DESC LIMIT 10;
"

# Duplicate strings with waste estimate
duckdb -c "
WITH str_bytes AS (
    SELECT s.obj_id, s.value as byte_id,
           md5(CAST(b.values AS VARCHAR)) as hash, len(b.values) as len
    FROM read_parquet('java.lang.String_*_chunk*.parquet') s
    JOIN read_parquet('_primitive_arrays_byte_chunk*.parquet') b ON s.value = b.obj_id
)
SELECT hash, COUNT(*) as dups, MIN(len) as str_len
FROM str_bytes GROUP BY hash HAVING COUNT(*) > 1
ORDER BY dups * str_len DESC LIMIT 20;
"
```

## Requirements

- **Rust toolchain** (for building HeapDumpStarDiver)
- **Python 3** + `duckdb` (`pip install duckdb`)
- **jvm-hprof** crate (dependency, see Cargo.toml)

## Robo Mode vs Non-Robo Mode

| Aspect | Robo Mode | Non-Robo Mode |
|--------|-----------|---------------|
| Files | `_chunk0.parquet`, `_chunk1.parquet`, ... | Single file per class |
| References | Bare UInt64 IDs | STRUCT(id, type) |
| Object index | `_object_index_chunk*.parquet` | Not available |
| Use case | LLM/automated analysis | Human interactive exploration |

Always use `--robo-mode` for automated analysis.
