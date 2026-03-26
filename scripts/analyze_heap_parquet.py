#!/usr/bin/env python3
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 Zac Policzer
"""
Analyze JVM heap dump parquet files produced by HeapDumpStarDiver.

Supports both robo mode (_chunk* suffixed files, _object_index) and
non-robo mode (single files per class, STRUCT references).

Usage:
    python3 analyze_heap_parquet.py <parquet-dir> [--top N] [--json]
    python3 analyze_heap_parquet.py <parquet-dir> --waste [--waste-tier N]

Requires: duckdb (pip install duckdb)
"""

import argparse
import glob as globmod
import hashlib
import json
import os
import sys
from dataclasses import dataclass, field, asdict
from typing import List, Optional

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install duckdb", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Waste estimation constants (bytes, compressed oops / 64-bit JVM)
# ---------------------------------------------------------------------------
OBJECT_HEADER = 16          # mark word (8) + klass pointer (8, or 4 compressed)
ARRAY_HEADER = 16           # object header + length (4) + padding (4)
REF_SIZE = 4                # compressed oops
HASHMAP_ENTRY_SIZE = 32     # Node: header(16) + hash(4) + key(4) + value(4) + next(4)
HASHMAP_SHELL_SIZE = 48     # HashMap object itself
ARRAYLIST_SHELL_SIZE = 40   # ArrayList: header(16) + size(4) + elementData ref(4) + modCount(4) + pad
LINKEDLIST_SHELL_SIZE = 48  # header + first + last + size + modCount
LINKEDLIST_NODE_SIZE = 24   # Node: header(16) + item(4) + next(4) + prev(4) → 28 → pad to 32
TREEMAP_ENTRY_SIZE = 48     # Entry: header(16) + key(4) + value(4) + left(4) + right(4) + parent(4) + color(1) → pad
CHM_SHELL_SIZE = 64         # ConcurrentHashMap shell
CHM_NODE_SIZE = 32          # same as HashMap.Node


# ---------------------------------------------------------------------------
# WasteFinding dataclass
# ---------------------------------------------------------------------------
@dataclass
class WasteFinding:
    check_name: str
    tier: int
    severity: str  # CRITICAL, HIGH, MEDIUM, LOW, INFO
    affected_count: int
    estimated_waste_bytes: int
    details: str
    recommendation: str
    sub_findings: list = field(default_factory=list)

    def severity_rank(self):
        return {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}.get(self.severity, 5)


def classify_severity(waste_bytes):
    if waste_bytes > 100 * 1024 * 1024:
        return "CRITICAL"
    elif waste_bytes > 10 * 1024 * 1024:
        return "HIGH"
    elif waste_bytes > 1 * 1024 * 1024:
        return "MEDIUM"
    elif waste_bytes > 100 * 1024:
        return "LOW"
    return "INFO"


def format_bytes(n):
    if n >= 1024 ** 3:
        return f"{n / (1024 ** 3):.1f} GB"
    elif n >= 1024 ** 2:
        return f"{n / (1024 ** 2):.1f} MB"
    elif n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


# ---------------------------------------------------------------------------
# ParquetResolver — handles robo vs non-robo mode differences
# ---------------------------------------------------------------------------
class ParquetResolver:
    def __init__(self, parquet_dir):
        self.parquet_dir = parquet_dir
        # Detect mode: robo has _object_index_chunk*.parquet
        self.is_robo = bool(globmod.glob(os.path.join(parquet_dir, "_object_index_chunk*.parquet")))
        self._cache = {}

    def resolve(self, base_name):
        """Return a read_parquet glob expression for a class/system file, or None if missing."""
        if base_name in self._cache:
            return self._cache[base_name]

        if base_name.startswith("_"):
            # System file like _primitive_arrays_byte, _object_arrays, _gc_roots
            # Some system files are always single-file (stack frames/traces, class hierarchy)
            single_file = os.path.join(self.parquet_dir, f"{base_name}.parquet")
            if self.is_robo and not os.path.exists(single_file):
                pattern = f"{base_name}_chunk*.parquet"
            else:
                pattern = f"{base_name}.parquet"
        else:
            # Class file like java.util.HashMap
            if self.is_robo:
                pattern = f"{base_name}_*_chunk*.parquet"
            else:
                pattern = f"{base_name}_*.parquet"

        matches = globmod.glob(os.path.join(self.parquet_dir, pattern))
        if not matches:
            self._cache[base_name] = None
            return None

        # Return glob expression (relative — we cd into parquet_dir)
        expr = f"read_parquet('{pattern}')"
        self._cache[base_name] = expr
        return expr

    def ref_field(self, field_name):
        """Return the SQL expression to access a reference field's ID."""
        if self.is_robo:
            return field_name  # bare UInt64
        return f"{field_name}.id"  # STRUCT access

    def has_object_index(self):
        return self.is_robo


def try_query(con, sql):
    """Execute query, return [] on failure (missing files etc)."""
    try:
        result = con.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Original analysis functions (updated for dual-mode support)
# ---------------------------------------------------------------------------

def query(con, sql):
    """Execute a DuckDB query and return results as list of dicts."""
    result = con.execute(sql)
    columns = [desc[0] for desc in result.description]
    rows = result.fetchall()
    return [dict(zip(columns, row)) for row in rows]


def run_summary(con, resolver):
    """Total objects and unique classes."""
    if resolver.has_object_index():
        oi = resolver.resolve("_object_index")
        return query(con, f"""
            SELECT COUNT(*) as total_objects, COUNT(DISTINCT type_name) as unique_classes
            FROM {oi}
        """)
    else:
        # Non-robo: count parquet files as proxy for classes, sum objects across all files
        # We can count from _object_arrays + primitive arrays + class files
        # But a simpler approach: just report what we can
        return [{"total_objects": "N/A (non-robo)", "unique_classes": "N/A (non-robo)"}]


def run_top_types(con, resolver, limit=30):
    """Top types by object count."""
    if resolver.has_object_index():
        oi = resolver.resolve("_object_index")
        return query(con, f"""
            SELECT type_name, COUNT(*) as obj_count
            FROM {oi}
            GROUP BY type_name ORDER BY obj_count DESC LIMIT {limit}
        """)
    return []


def run_category_breakdown(con, resolver):
    """Category breakdown with counts."""
    if resolver.has_object_index():
        oi = resolver.resolve("_object_index")
        return query(con, f"""
            SELECT CASE
                WHEN type_name LIKE '%kafka%' OR type_name LIKE '%Kafka%' THEN 'Kafka'
                WHEN type_name LIKE '%MBean%' OR type_name LIKE '%javax.management%' OR type_name LIKE '%jmx%' THEN 'JMX/MBeans'
                WHEN type_name LIKE '%netty%' OR type_name LIKE '%Netty%' THEN 'Netty'
                WHEN type_name LIKE '%rocksdb%' OR type_name LIKE '%RocksDB%' THEN 'RocksDB'
                WHEN type_name LIKE '%grpc%' OR type_name LIKE '%Grpc%' THEN 'gRPC'
                WHEN type_name LIKE '%guava%' OR type_name LIKE '%google.common%' THEN 'Guava'
                WHEN type_name LIKE 'java.%' OR type_name LIKE 'sun.%' OR type_name LIKE 'jdk.%' THEN 'JDK'
                ELSE 'Other'
            END as category,
            COUNT(*) as obj_count
            FROM {oi}
            GROUP BY category ORDER BY obj_count DESC
        """)
    return []


def run_byte_array_distribution(con, resolver):
    """Byte array size distribution."""
    src = resolver.resolve("_primitive_arrays_byte")
    if not src:
        return [{"error": "No byte array parquet files found"}]
    try:
        return query(con, f"""
            SELECT CASE
                WHEN len(values) < 100 THEN '0-99'
                WHEN len(values) < 1000 THEN '100-999'
                WHEN len(values) < 10000 THEN '1K-10K'
                WHEN len(values) < 100000 THEN '10K-100K'
                ELSE '100K+'
            END as bucket,
            COUNT(*) as count,
            ROUND(SUM(len(values))/1048576.0, 2) as total_mb
            FROM {src}
            GROUP BY bucket ORDER BY total_mb DESC
        """)
    except Exception:
        return [{"error": "No byte array parquet files found"}]


def run_large_byte_arrays(con, resolver, min_bytes=102400):
    """Top large byte arrays (>100KB)."""
    src = resolver.resolve("_primitive_arrays_byte")
    if not src:
        return []
    try:
        return query(con, f"""
            SELECT obj_id, len(values) as size_bytes,
                   ROUND(len(values)/1024.0, 1) as size_kb
            FROM {src}
            WHERE len(values) > {min_bytes}
            ORDER BY size_bytes DESC LIMIT 20
        """)
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Waste Analysis Checks
# ---------------------------------------------------------------------------

def check_duplicate_strings(con, resolver, sample_pct=100):
    """Tier 1: Find duplicate strings by joining String → byte[] and hashing content."""
    str_src = resolver.resolve("java.lang.String")
    byte_src = resolver.resolve("_primitive_arrays_byte")
    if not str_src or not byte_src:
        return None

    val_ref = resolver.ref_field("value")

    # Check string count first for sampling
    str_count_rows = try_query(con, f"SELECT COUNT(*) as cnt FROM {str_src}")
    str_count = str_count_rows[0]["cnt"] if str_count_rows else 0

    sample_clause = ""
    if sample_pct < 100 or str_count > 5_000_000:
        pct = min(sample_pct, 20) if str_count > 5_000_000 else sample_pct
        sample_clause = f"USING SAMPLE {pct} PERCENT (bernoulli)"
        scale_factor = 100.0 / pct
    else:
        scale_factor = 1.0

    rows = try_query(con, f"""
        WITH string_bytes AS (
            SELECT s.obj_id, md5(CAST(b.values AS VARCHAR)) as hash, len(b.values) as str_len
            FROM (SELECT obj_id, {val_ref} as byte_id FROM {str_src} {sample_clause}) s
            JOIN {byte_src} b ON s.byte_id = b.obj_id
            WHERE b.obj_id != 0
        ),
        dups AS (
            SELECT hash, COUNT(*) as dup_count, MIN(str_len) as str_len
            FROM string_bytes
            GROUP BY hash HAVING COUNT(*) > 1
        )
        SELECT SUM(dup_count) as total_dup_strings,
               SUM((dup_count - 1) * str_len) as wasted_bytes,
               COUNT(*) as unique_dup_values,
               MAX(dup_count) as max_dups_single_value,
               MAX(str_len) as max_dup_str_len
        FROM dups
    """)

    if not rows or rows[0].get("total_dup_strings") is None:
        return None

    r = rows[0]
    total_dups = int((r["total_dup_strings"] or 0) * scale_factor)
    wasted = int((r["wasted_bytes"] or 0) * scale_factor)
    unique_vals = int((r["unique_dup_values"] or 0) * scale_factor)
    # Each duplicate String object also has overhead: header + fields
    string_obj_overhead = 40  # header(16) + hash(4) + hashIsZero(1) + coder(1) + value ref(4) + pad
    wasted_total = wasted + (total_dups - unique_vals) * string_obj_overhead

    # Get top duplicated strings by count
    top_dups = try_query(con, f"""
        WITH string_bytes AS (
            SELECT s.obj_id, md5(CAST(b.values AS VARCHAR)) as hash,
                   len(b.values) as str_len,
                   CASE WHEN b.values[1] = 0 THEN 'UTF-16' ELSE 'LATIN1' END as encoding
            FROM (SELECT obj_id, {val_ref} as byte_id FROM {str_src} {sample_clause}) s
            JOIN {byte_src} b ON s.byte_id = b.obj_id
            WHERE b.obj_id != 0
        )
        SELECT hash, COUNT(*) as dup_count, MIN(str_len) as str_len
        FROM string_bytes
        GROUP BY hash HAVING COUNT(*) > 1
        ORDER BY dup_count * str_len DESC LIMIT 10
    """)

    details = f"{total_dups:,} duplicate strings across {unique_vals:,} unique values"
    if r.get("max_dups_single_value"):
        details += f" (worst: {int(r['max_dups_single_value'] * scale_factor):,} copies)"
    if sample_pct < 100 or str_count > 5_000_000:
        details += f" [sampled {pct}%, scaled]"

    sub = []
    for d in (top_dups or []):
        sub.append(f"  hash={d['hash'][:8]}... count={d['dup_count']:,} len={d['str_len']} waste={format_bytes(d['dup_count'] * d['str_len'])}")

    return WasteFinding(
        check_name="Duplicate Strings",
        tier=1,
        severity=classify_severity(wasted_total),
        affected_count=total_dups,
        estimated_waste_bytes=wasted_total,
        details=details,
        recommendation="Intern frequently duplicated strings or use a string deduplication agent (-XX:+UseStringDeduplication with G1)",
        sub_findings=sub,
    )


def check_bad_collections(con, resolver):
    """Tier 1: Find empty and single-element collections."""
    findings = []

    # HashMap
    hm = resolver.resolve("java.util.HashMap")
    if hm:
        rows = try_query(con, f"""
            SELECT CASE WHEN size = 0 THEN 'empty' WHEN size = 1 THEN 'single' END as pattern,
                   COUNT(*) as count
            FROM {hm}
            WHERE size <= 1
            GROUP BY pattern
        """)
        empty = sum(r["count"] for r in rows if r["pattern"] == "empty")
        single = sum(r["count"] for r in rows if r["pattern"] == "single")
        # Empty HashMap: shell(48) + table array (at least 16 slots * 4 = 64) + array header = ~128 bytes, could be 0
        # Waste: empty = full object; single = HashMap overhead vs direct field
        waste = empty * (HASHMAP_SHELL_SIZE + ARRAY_HEADER + 16 * REF_SIZE) + single * (HASHMAP_SHELL_SIZE + ARRAY_HEADER + 16 * REF_SIZE + HASHMAP_ENTRY_SIZE - 2 * REF_SIZE)
        if empty + single > 0:
            findings.append(("HashMap", empty, single, waste))

    # ArrayList
    al = resolver.resolve("java.util.ArrayList")
    if al:
        rows = try_query(con, f"""
            SELECT CASE WHEN size = 0 THEN 'empty' WHEN size = 1 THEN 'single' END as pattern,
                   COUNT(*) as count
            FROM {al}
            WHERE size <= 1
            GROUP BY pattern
        """)
        empty = sum(r["count"] for r in rows if r["pattern"] == "empty")
        single = sum(r["count"] for r in rows if r["pattern"] == "single")
        waste = empty * (ARRAYLIST_SHELL_SIZE + ARRAY_HEADER + 10 * REF_SIZE) + single * (ARRAYLIST_SHELL_SIZE + ARRAY_HEADER + 10 * REF_SIZE - REF_SIZE)
        if empty + single > 0:
            findings.append(("ArrayList", empty, single, waste))

    # LinkedList
    ll = resolver.resolve("java.util.LinkedList")
    if ll:
        rows = try_query(con, f"""
            SELECT CASE WHEN size = 0 THEN 'empty' WHEN size = 1 THEN 'single' END as pattern,
                   COUNT(*) as count
            FROM {ll}
            WHERE size <= 1
            GROUP BY pattern
        """)
        empty = sum(r["count"] for r in rows if r["pattern"] == "empty")
        single = sum(r["count"] for r in rows if r["pattern"] == "single")
        waste = empty * LINKEDLIST_SHELL_SIZE + single * (LINKEDLIST_SHELL_SIZE + LINKEDLIST_NODE_SIZE - REF_SIZE)
        if empty + single > 0:
            findings.append(("LinkedList", empty, single, waste))

    # TreeMap
    tm = resolver.resolve("java.util.TreeMap")
    if tm:
        rows = try_query(con, f"""
            SELECT CASE WHEN size = 0 THEN 'empty' WHEN size = 1 THEN 'single' END as pattern,
                   COUNT(*) as count
            FROM {tm}
            WHERE size <= 1
            GROUP BY pattern
        """)
        empty = sum(r["count"] for r in rows if r["pattern"] == "empty")
        single = sum(r["count"] for r in rows if r["pattern"] == "single")
        waste = empty * 64 + single * (64 + TREEMAP_ENTRY_SIZE - 2 * REF_SIZE)
        if empty + single > 0:
            findings.append(("TreeMap", empty, single, waste))

    # ConcurrentHashMap (uses baseCount as size proxy)
    chm = resolver.resolve("java.util.concurrent.ConcurrentHashMap")
    if chm:
        rows = try_query(con, f"""
            SELECT CASE WHEN baseCount = 0 THEN 'empty' WHEN baseCount = 1 THEN 'single' END as pattern,
                   COUNT(*) as count
            FROM {chm}
            WHERE baseCount <= 1
            GROUP BY pattern
        """)
        empty = sum(r["count"] for r in rows if r["pattern"] == "empty")
        single = sum(r["count"] for r in rows if r["pattern"] == "single")
        waste = empty * (CHM_SHELL_SIZE + ARRAY_HEADER + 16 * REF_SIZE) + single * (CHM_SHELL_SIZE + ARRAY_HEADER + 16 * REF_SIZE + CHM_NODE_SIZE - 2 * REF_SIZE)
        if empty + single > 0:
            findings.append(("ConcurrentHashMap", empty, single, waste))

    if not findings:
        return None

    total_empty = sum(f[1] for f in findings)
    total_single = sum(f[2] for f in findings)
    total_waste = sum(f[3] for f in findings)
    total_affected = total_empty + total_single

    sub = []
    for name, emp, sing, w in findings:
        parts = []
        if emp > 0:
            parts.append(f"{emp:,} empty")
        if sing > 0:
            parts.append(f"{sing:,} single-element")
        sub.append(f"  {name}: {', '.join(parts)} ({format_bytes(w)})")

    return WasteFinding(
        check_name="Bad Collections (empty/single-element)",
        tier=1,
        severity=classify_severity(total_waste),
        affected_count=total_affected,
        estimated_waste_bytes=total_waste,
        details=f"{total_empty:,} empty + {total_single:,} single-element collections",
        recommendation="Replace empty collections with Collections.emptyMap/List/Set(); single-element with Collections.singletonMap/List/Set() or direct fields",
        sub_findings=sub,
    )


def check_bad_object_arrays(con, resolver):
    """Tier 1: Zero-length, all-null, single-element, sparse object arrays."""
    src = resolver.resolve("_object_arrays")
    if not src:
        return None

    rows = try_query(con, f"""
        SELECT
            CASE
                WHEN len(elements) = 0 THEN 'zero_length'
                WHEN list_count(elements, 0) = len(elements) THEN 'all_null'
                WHEN len(elements) = 1 THEN 'single_element'
                WHEN len(elements) > 3 AND CAST(list_count(elements, 0) AS DOUBLE) / len(elements) > 0.7 THEN 'sparse'
            END as pattern,
            COUNT(*) as count,
            SUM(len(elements)) as total_slots
        FROM {src}
        WHERE len(elements) = 0
           OR list_count(elements, 0) = len(elements)
           OR len(elements) = 1
           OR (len(elements) > 3 AND CAST(list_count(elements, 0) AS DOUBLE) / len(elements) > 0.7)
        GROUP BY pattern
        LIMIT 5000000
    """)

    if not rows:
        return None

    total_count = 0
    total_waste = 0
    sub = []
    for r in rows:
        pat = r["pattern"]
        cnt = r["count"]
        slots = r["total_slots"] or 0
        total_count += cnt
        if pat == "zero_length":
            waste = cnt * ARRAY_HEADER
            sub.append(f"  Zero-length: {cnt:,} arrays ({format_bytes(waste)})")
        elif pat == "all_null":
            waste = cnt * ARRAY_HEADER + slots * REF_SIZE
            sub.append(f"  All-null: {cnt:,} arrays, {slots:,} null slots ({format_bytes(waste)})")
        elif pat == "single_element":
            waste = cnt * (ARRAY_HEADER - REF_SIZE)  # array overhead beyond the single ref
            sub.append(f"  Single-element: {cnt:,} arrays ({format_bytes(waste)})")
        elif pat == "sparse":
            # Waste = null slots * ref_size
            null_slots = slots - cnt  # approximate non-null = ~count (rough)
            waste = int(slots * 0.7) * REF_SIZE  # ~70% null by threshold
            sub.append(f"  Sparse (>70% null): {cnt:,} arrays ({format_bytes(waste)})")
        else:
            waste = 0
        total_waste += waste

    return WasteFinding(
        check_name="Bad Object Arrays",
        tier=1,
        severity=classify_severity(total_waste),
        affected_count=total_count,
        estimated_waste_bytes=total_waste,
        details=f"{total_count:,} wasteful object arrays",
        recommendation="Use empty array constants (EMPTY_ARRAY), replace single-element arrays with direct references, compact sparse arrays",
        sub_findings=sub,
    )


def check_bad_primitive_arrays(con, resolver):
    """Tier 1: Zero-length, all-zero, single-element primitive arrays across all 8 types."""
    prim_types = {
        "boolean": 1, "byte": 1, "char": 2, "short": 2,
        "int": 4, "long": 8, "float": 4, "double": 8,
    }

    total_count = 0
    total_waste = 0
    sub = []

    for ptype, elem_size in prim_types.items():
        src = resolver.resolve(f"_primitive_arrays_{ptype}")
        if not src:
            continue

        rows = try_query(con, f"""
            SELECT
                CASE
                    WHEN len(values) = 0 THEN 'zero_length'
                    WHEN len(values) = 1 THEN 'single'
                    WHEN list_aggregate(values, 'min') = 0 AND list_aggregate(values, 'max') = 0 AND len(values) > 1 THEN 'all_zero'
                END as pattern,
                COUNT(*) as count,
                SUM(len(values) * {elem_size}) as data_bytes
            FROM {src}
            WHERE len(values) = 0
               OR len(values) = 1
               OR (len(values) > 1 AND list_aggregate(values, 'min') = 0 AND list_aggregate(values, 'max') = 0)
            GROUP BY pattern
        """)

        type_count = 0
        type_waste = 0
        for r in rows:
            if r["pattern"] is None:
                continue
            cnt = r["count"]
            data = r["data_bytes"] or 0
            type_count += cnt
            if r["pattern"] == "zero_length":
                type_waste += cnt * ARRAY_HEADER
            elif r["pattern"] == "all_zero":
                type_waste += data + cnt * ARRAY_HEADER
            elif r["pattern"] == "single":
                type_waste += cnt * (ARRAY_HEADER - elem_size)  # overhead beyond single value

        if type_count > 0:
            sub.append(f"  {ptype}[]: {type_count:,} wasteful ({format_bytes(type_waste)})")
            total_count += type_count
            total_waste += type_waste

    if total_count == 0:
        return None

    return WasteFinding(
        check_name="Bad Primitive Arrays",
        tier=1,
        severity=classify_severity(total_waste),
        affected_count=total_count,
        estimated_waste_bytes=total_waste,
        details=f"{total_count:,} wasteful primitive arrays (zero-length, single, all-zero)",
        recommendation="Replace zero-length with shared constants, avoid single-element arrays where a scalar field suffices, check all-zero arrays for uninitialized buffers",
        sub_findings=sub,
    )


def check_boxed_numbers(con, resolver):
    """Tier 1: Count boxed primitive wrappers."""
    wrappers = {
        "java.lang.Integer": 16,     # header(16) + value(4) + pad = 16
        "java.lang.Long": 24,        # header(16) + value(8)
        "java.lang.Short": 16,
        "java.lang.Byte": 16,
        "java.lang.Float": 16,
        "java.lang.Double": 24,
        "java.lang.Boolean": 16,
        "java.lang.Character": 16,
    }

    total_count = 0
    total_waste = 0
    sub = []

    for wtype, obj_size in wrappers.items():
        src = resolver.resolve(wtype)
        if not src:
            continue
        rows = try_query(con, f"SELECT COUNT(*) as cnt FROM {src}")
        if not rows:
            continue
        cnt = rows[0]["cnt"]
        if cnt == 0:
            continue
        # Waste = object overhead beyond the primitive value itself
        prim_size = obj_size - OBJECT_HEADER
        waste_per = OBJECT_HEADER  # the header IS the waste
        waste = cnt * waste_per
        total_count += cnt
        total_waste += waste
        short_name = wtype.split(".")[-1]
        sub.append(f"  {short_name}: {cnt:,} ({format_bytes(waste)})")

    if total_count == 0:
        return None

    return WasteFinding(
        check_name="Boxed Primitives",
        tier=1,
        severity=classify_severity(total_waste),
        affected_count=total_count,
        estimated_waste_bytes=total_waste,
        details=f"{total_count:,} boxed primitives (16-byte overhead each vs raw primitive)",
        recommendation="Use primitive types directly, IntArrayList/LongArrayList from fastutil/Eclipse Collections instead of List<Integer>/List<Long>",
        sub_findings=sub,
    )


def check_collection_sizing(con, resolver):
    """Tier 2: Sparse HashMaps (<33% utilized) and oversized ArrayList backing arrays."""
    findings = []
    total_count = 0
    total_waste = 0

    # Sparse HashMaps: join to table (object array) to check utilization
    hm = resolver.resolve("java.util.HashMap")
    oa = resolver.resolve("_object_arrays")
    if hm and oa:
        rows = try_query(con, f"""
            WITH hm_tables AS (
                SELECT h.obj_id, h.size, {resolver.ref_field('h.table')} as table_id
                FROM {hm} h
                WHERE h.size >= 2 AND {resolver.ref_field('h.table')} != 0
            )
            SELECT COUNT(*) as count,
                   SUM(len(a.elements) * {REF_SIZE}) as wasted_slot_bytes,
                   AVG(CAST(ht.size AS DOUBLE) / len(a.elements)) as avg_util
            FROM hm_tables ht
            JOIN {oa} a ON ht.table_id = a.obj_id
            WHERE len(a.elements) > 0
              AND CAST(ht.size AS DOUBLE) / len(a.elements) < 0.33
              AND len(a.elements) >= 16
        """)
        if rows and rows[0]["count"] and rows[0]["count"] > 0:
            cnt = rows[0]["count"]
            waste = rows[0]["wasted_slot_bytes"] or 0
            avg_util = rows[0]["avg_util"] or 0
            total_count += cnt
            total_waste += waste
            findings.append(f"  Sparse HashMaps (<33% full, >=16 slots): {cnt:,} (avg util: {avg_util:.1%}, wasted slots: {format_bytes(waste)})")

    # Oversized ArrayList backing arrays
    al = resolver.resolve("java.util.ArrayList")
    if al and oa:
        rows = try_query(con, f"""
            WITH al_data AS (
                SELECT a.size, {resolver.ref_field('a.elementData')} as arr_id
                FROM {al} a
                WHERE a.size >= 1 AND {resolver.ref_field('a.elementData')} != 0
            )
            SELECT COUNT(*) as count,
                   SUM((len(oa.elements) - ad.size) * {REF_SIZE}) as wasted_bytes
            FROM al_data ad
            JOIN {oa} oa ON ad.arr_id = oa.obj_id
            WHERE len(oa.elements) > ad.size * 2
              AND len(oa.elements) - ad.size > 8
        """)
        if rows and rows[0]["count"] and rows[0]["count"] > 0:
            cnt = rows[0]["count"]
            waste = rows[0]["wasted_bytes"] or 0
            total_count += cnt
            total_waste += waste
            findings.append(f"  Oversized ArrayList backing arrays (>2x needed, >8 spare): {cnt:,} ({format_bytes(waste)})")

    if total_count == 0:
        return None

    return WasteFinding(
        check_name="Collection Sizing Issues",
        tier=2,
        severity=classify_severity(total_waste),
        affected_count=total_count,
        estimated_waste_bytes=total_waste,
        details=f"{total_count:,} poorly-sized collections",
        recommendation="Use initial capacity hints: new HashMap<>(expectedSize) or new ArrayList<>(expectedSize); call trimToSize() after bulk adds",
        sub_findings=findings,
    )


def check_duplicate_byte_arrays(con, resolver):
    """Tier 2: Find duplicate byte arrays by MD5 hash."""
    src = resolver.resolve("_primitive_arrays_byte")
    if not src:
        return None

    rows = try_query(con, f"""
        WITH hashed AS (
            SELECT obj_id, md5(CAST(values AS VARCHAR)) as hash, len(values) as arr_len
            FROM {src}
            WHERE len(values) > 0 AND len(values) <= 10240
        ),
        dups AS (
            SELECT hash, COUNT(*) as dup_count, MIN(arr_len) as arr_len
            FROM hashed
            GROUP BY hash HAVING COUNT(*) > 1
        )
        SELECT SUM(dup_count) as total_dups,
               SUM((dup_count - 1) * arr_len) as wasted_bytes,
               COUNT(*) as unique_dup_values,
               MAX(dup_count) as max_dups
        FROM dups
    """)

    if not rows or rows[0].get("total_dups") is None:
        return None

    r = rows[0]
    total_dups = r["total_dups"] or 0
    wasted = r["wasted_bytes"] or 0
    # Add array header overhead per duplicate
    unique_vals = r["unique_dup_values"] or 0
    wasted_total = wasted + (total_dups - unique_vals) * ARRAY_HEADER

    if total_dups == 0:
        return None

    return WasteFinding(
        check_name="Duplicate byte[] Arrays",
        tier=2,
        severity=classify_severity(wasted_total),
        affected_count=total_dups,
        estimated_waste_bytes=wasted_total,
        details=f"{total_dups:,} duplicate byte arrays across {unique_vals:,} unique values (arrays <=10KB)",
        recommendation="Cache/intern frequently reused byte arrays; check for serialization producing identical buffers",
        sub_findings=[],
    )


def check_class_count(con, resolver):
    """Tier 2: Check for classloader leak (>20K classes)."""
    if resolver.has_object_index():
        oi = resolver.resolve("_object_index")
        rows = try_query(con, f"SELECT COUNT(DISTINCT type_name) as cls_count FROM {oi}")
    else:
        # Count parquet files as proxy (each file = one class roughly)
        pq_files = [f for f in os.listdir(".") if f.endswith(".parquet") and not f.startswith("_")]
        rows = [{"cls_count": len(pq_files)}]

    if not rows:
        return None

    cls_count = rows[0]["cls_count"]
    if isinstance(cls_count, str):
        return None

    if cls_count < 10000:
        return None

    severity = "INFO"
    if cls_count > 50000:
        severity = "HIGH"
    elif cls_count > 20000:
        severity = "MEDIUM"

    return WasteFinding(
        check_name="Class Count / Leak Detection",
        tier=2,
        severity=severity,
        affected_count=cls_count,
        estimated_waste_bytes=cls_count * 8192,  # ~8KB per class metadata estimate
        details=f"{cls_count:,} unique classes loaded",
        recommendation="If >20K, investigate classloader leaks (hot-deploy, OSGi, reflection-generated classes). Check for lambda/proxy class proliferation.",
        sub_findings=[],
    )


def check_gc_roots(con, resolver):
    """Tier 2: GC roots breakdown by type."""
    src = resolver.resolve("_gc_roots")
    if not src:
        return None

    rows = try_query(con, f"""
        SELECT root_type, COUNT(*) as count
        FROM {src}
        GROUP BY root_type
        ORDER BY count DESC
    """)

    if not rows:
        return None

    total = sum(r["count"] for r in rows)
    sub = [f"  {r['root_type']}: {r['count']:,}" for r in rows]

    # GC roots themselves aren't "waste" but high counts can indicate issues
    severity = "INFO"
    if total > 100000:
        severity = "MEDIUM"
    elif total > 50000:
        severity = "LOW"

    return WasteFinding(
        check_name="GC Roots Breakdown",
        tier=2,
        severity=severity,
        affected_count=total,
        estimated_waste_bytes=0,  # informational
        details=f"{total:,} GC roots across {len(rows)} root types",
        recommendation="High JavaStackFrame roots may indicate thread bloat. High JNI roots may indicate native resource leaks.",
        sub_findings=sub,
    )


def check_direct_byte_buffers(con, resolver):
    """Tier 2: DirectByteBuffer off-heap analysis."""
    src = resolver.resolve("java.nio.DirectByteBuffer")
    if not src:
        return None

    rows = try_query(con, f"""
        SELECT COUNT(*) as count,
               SUM(capacity) as total_capacity,
               SUM(CASE WHEN position = 0 AND "limit" = capacity THEN capacity ELSE 0 END) as untouched_bytes,
               COUNT(CASE WHEN capacity = 0 THEN 1 END) as empty_count,
               MAX(capacity) as max_capacity,
               AVG(capacity) as avg_capacity
        FROM {src}
    """)

    if not rows or not rows[0]["count"]:
        return None

    r = rows[0]
    count = r["count"]
    total_cap = r["total_capacity"] or 0
    untouched = r["untouched_bytes"] or 0
    empty = r["empty_count"] or 0

    if count == 0:
        return None

    # Waste = empty buffers + untouched capacity
    waste = empty * 64 + untouched  # 64 bytes for the DBB object itself

    sub = [
        f"  Total buffers: {count:,}",
        f"  Total capacity: {format_bytes(total_cap)} (off-heap)",
        f"  Empty buffers: {empty:,}",
        f"  Max single buffer: {format_bytes(r['max_capacity'] or 0)}",
        f"  Avg buffer size: {format_bytes(int(r['avg_capacity'] or 0))}",
    ]

    return WasteFinding(
        check_name="DirectByteBuffer Off-Heap",
        tier=2,
        severity=classify_severity(total_cap) if total_cap > 10 * 1024 * 1024 else "INFO",
        affected_count=count,
        estimated_waste_bytes=waste,
        details=f"{count:,} DirectByteBuffers, {format_bytes(total_cap)} total off-heap capacity",
        recommendation="Release unused DirectByteBuffers explicitly (sun.misc.Cleaner). Consider pooling for short-lived buffers.",
        sub_findings=sub,
    )


def check_duplicate_object_arrays(con, resolver):
    """Tier 3: Find duplicate object arrays (same elements in same order)."""
    src = resolver.resolve("_object_arrays")
    if not src:
        return None

    rows = try_query(con, f"""
        WITH hashed AS (
            SELECT obj_id, md5(CAST(elements AS VARCHAR)) as hash, len(elements) as arr_len
            FROM {src}
            WHERE len(elements) BETWEEN 1 AND 100
        ),
        dups AS (
            SELECT hash, COUNT(*) as dup_count, MIN(arr_len) as arr_len
            FROM hashed
            GROUP BY hash HAVING COUNT(*) > 1
        )
        SELECT SUM(dup_count) as total_dups,
               SUM((dup_count - 1) * arr_len * {REF_SIZE}) as wasted_bytes,
               COUNT(*) as unique_dup_values
        FROM dups
    """)

    if not rows or rows[0].get("total_dups") is None:
        return None

    r = rows[0]
    total_dups = r["total_dups"] or 0
    unique_vals = r["unique_dup_values"] or 0
    wasted = (r["wasted_bytes"] or 0) + (total_dups - unique_vals) * ARRAY_HEADER

    if total_dups == 0:
        return None

    return WasteFinding(
        check_name="Duplicate Object Arrays",
        tier=3,
        severity=classify_severity(wasted),
        affected_count=total_dups,
        estimated_waste_bytes=wasted,
        details=f"{total_dups:,} duplicate object arrays across {unique_vals:,} unique values (arrays 1-100 elements)",
        recommendation="Share immutable arrays or use flyweight pattern for identical element sequences",
        sub_findings=[],
    )


def check_estimated_shallow_size(con, resolver):
    """Tier 3: Approximate heap usage breakdown by type from parquet schema."""
    # Only works with object index (robo mode)
    if not resolver.has_object_index():
        return None

    oi = resolver.resolve("_object_index")
    rows = try_query(con, f"""
        SELECT type_name, COUNT(*) as count
        FROM {oi}
        GROUP BY type_name
        ORDER BY count DESC
        LIMIT 50
    """)

    if not rows:
        return None

    # Rough estimate: each object ~= OBJECT_HEADER + 32 bytes avg for fields
    AVG_OBJ_SIZE = OBJECT_HEADER + 32
    total_estimated = sum(r["count"] * AVG_OBJ_SIZE for r in rows)
    sub = [f"  {r['type_name']}: {r['count']:,} (~{format_bytes(r['count'] * AVG_OBJ_SIZE)})" for r in rows[:15]]

    return WasteFinding(
        check_name="Estimated Shallow Size (top 50 types)",
        tier=3,
        severity="INFO",
        affected_count=sum(r["count"] for r in rows),
        estimated_waste_bytes=0,  # informational
        details=f"Top 50 types estimated at ~{format_bytes(total_estimated)} (assuming avg {AVG_OBJ_SIZE}B per object)",
        recommendation="Use -XX:+PrintClassHistogram for exact shallow sizes. This is an approximation.",
        sub_findings=sub,
    )


def check_thread_stacks(con, resolver):
    """Tier 2: Thread count and stack depth analysis from _stack_traces and _stack_frames parquet."""
    traces_src = resolver.resolve("_stack_traces")
    frames_src = resolver.resolve("_stack_frames")
    if not traces_src:
        return None

    # Stack trace count (HPROF StackTrace records — includes allocation site traces)
    thread_rows = try_query(con, f"SELECT COUNT(*) as cnt FROM {traces_src}")
    if not thread_rows:
        return None
    trace_count = thread_rows[0]["cnt"]
    if trace_count == 0:
        return None

    # java.lang.Thread instance count and status breakdown (more accurate than stack traces)
    thread_src = resolver.resolve("java.lang.Thread")
    alive_count = 0
    total_thread_instances = 0
    status_breakdown = []
    if thread_src:
        # threadStatus is a JVM bitmask:
        # 0x0001=ALIVE, 0x0002=TERMINATED, 0x0004=RUNNABLE
        # 0x0010=WAITING_INDEFINITELY, 0x0020=WAITING_WITH_TIMEOUT
        # 0x0080=SLEEPING, 0x0100=IN_OBJECT_WAIT, 0x0200=PARKED, 0x0400=BLOCKED
        status_rows = try_query(con, f"""
            SELECT threadStatus, COUNT(*) as cnt
            FROM {thread_src}
            GROUP BY threadStatus ORDER BY cnt DESC
        """)
        if status_rows:
            for r in status_rows:
                s = r["threadStatus"]
                cnt = r["cnt"]
                total_thread_instances += cnt
                flags = []
                if s == 0:
                    flags.append("NEW")
                else:
                    if s & 0x0001: flags.append("ALIVE")
                    if s & 0x0002: flags.append("TERMINATED")
                    if s & 0x0004: flags.append("RUNNABLE")
                    if s & 0x0010: flags.append("WAITING")
                    if s & 0x0020: flags.append("TIMED_WAITING")
                    if s & 0x0080: flags.append("SLEEPING")
                    if s & 0x0100: flags.append("IN_OBJECT_WAIT")
                    if s & 0x0200: flags.append("PARKED")
                    if s & 0x0400: flags.append("BLOCKED")
                is_alive = bool(s & 0x0001) and not bool(s & 0x0002) if s != 0 else False
                if is_alive:
                    alive_count += cnt
                state_str = "|".join(flags) if flags else f"UNKNOWN({s})"
                status_breakdown.append((state_str, cnt))

    # Use alive thread count for severity if available, otherwise fall back to trace count
    effective_count = alive_count if alive_count > 0 else trace_count

    sub = []
    if total_thread_instances > 0:
        sub.append(f"java.lang.Thread instances: {total_thread_instances:,} (alive: {alive_count:,}, terminated: {total_thread_instances - alive_count:,})")
        sub.append(f"HPROF stack trace records: {trace_count:,}")
        sub.append("Thread status breakdown:")
        for state_str, cnt in status_breakdown:
            sub.append(f"  {cnt:>6}  {state_str}")
    else:
        sub.append(f"HPROF stack trace records: {trace_count:,}")

    # Stack depth distribution
    depth_rows = try_query(con, f"""
        SELECT
            CASE
                WHEN len(frame_ids) = 0 THEN '0 (empty)'
                WHEN len(frame_ids) <= 5 THEN '1-5'
                WHEN len(frame_ids) <= 20 THEN '6-20'
                WHEN len(frame_ids) <= 50 THEN '21-50'
                ELSE '50+'
            END as depth_bucket,
            COUNT(*) as cnt
        FROM {traces_src}
        GROUP BY depth_bucket ORDER BY cnt DESC
    """)
    if depth_rows:
        sub.append("Stack depth distribution:")
        for r in depth_rows:
            sub.append(f"  {r['depth_bucket']:>12}: {r['cnt']:,}")

    # Top thread pool classes (from stack frames)
    if frames_src:
        pool_rows = try_query(con, f"""
            SELECT class_name, COUNT(*) as cnt
            FROM {frames_src}
            WHERE class_name LIKE '%Thread%'
               OR class_name LIKE '%Pool%'
               OR class_name LIKE '%Executor%'
               OR class_name LIKE '%Worker%'
            GROUP BY class_name ORDER BY cnt DESC LIMIT 10
        """)
        if pool_rows:
            sub.append("Top thread-related classes in frames:")
            for r in pool_rows:
                sub.append(f"  {r['cnt']:>6}  {r['class_name']}")

    severity = "INFO"
    if effective_count > 5000:
        severity = "CRITICAL"
    elif effective_count > 2000:
        severity = "HIGH"
    elif effective_count > 1000:
        severity = "MEDIUM"

    details = f"{effective_count:,} alive threads"
    if total_thread_instances > 0:
        details += f" ({total_thread_instances:,} total Thread instances, {total_thread_instances - alive_count:,} terminated)"
    details += ". Classloader leak threshold is typically >1000."

    return WasteFinding(
        check_name="Thread Stacks",
        tier=2,
        severity=severity,
        affected_count=effective_count,
        estimated_waste_bytes=effective_count * 512 * 1024,  # ~512KB per thread stack
        details=details,
        recommendation="High thread counts increase memory overhead (~512KB stack per thread) and GC pressure. "
                        "Check for thread pool over-provisioning or unbounded thread creation.",
        sub_findings=sub,
    )


# ---------------------------------------------------------------------------
# Waste analysis orchestrator
# ---------------------------------------------------------------------------

ALL_CHECKS = [
    # (function, tier)
    (check_duplicate_strings, 1),
    (check_bad_collections, 1),
    (check_bad_object_arrays, 1),
    (check_bad_primitive_arrays, 1),
    (check_boxed_numbers, 1),
    (check_collection_sizing, 2),
    (check_duplicate_byte_arrays, 2),
    (check_class_count, 2),
    (check_gc_roots, 2),
    (check_direct_byte_buffers, 2),
    (check_thread_stacks, 2),
    (check_duplicate_object_arrays, 3),
    (check_estimated_shallow_size, 3),
]


def run_waste_analysis(con, resolver, max_tier=2, sample_pct=100):
    """Run all waste checks up to max_tier and return sorted findings."""
    findings = []

    for check_fn, tier in ALL_CHECKS:
        if tier > max_tier:
            continue
        print(f"  Running: {check_fn.__doc__.strip().split(chr(10))[0]}...", flush=True)
        try:
            if check_fn == check_duplicate_strings:
                result = check_fn(con, resolver, sample_pct=sample_pct)
            else:
                result = check_fn(con, resolver)
            if result:
                findings.append(result)
        except Exception as e:
            print(f"  WARNING: {check_fn.__name__} failed: {e}", file=sys.stderr)

    # Sort by estimated waste descending, then severity
    findings.sort(key=lambda f: (-f.estimated_waste_bytes, f.severity_rank()))
    return findings


def print_waste_report(findings):
    """Print the waste analysis report."""
    print("\n" + "=" * 60)
    print("MEMORY WASTE ANALYSIS")
    print("=" * 60)

    if not findings:
        print("\n  No waste patterns detected.")
        return

    # Summary table
    print("\n### Most Important Issues (sorted by estimated waste)")
    header = f"  {'#':>3}  | {'Check':<35} | {'Affected':>10} | {'Est. Waste':>10} | {'Severity':<8}"
    print(header)
    print(f"  {'---':>3}--+-{'-' * 35}-+-{'-' * 10}-+-{'-' * 10}-+-{'-' * 8}")

    for i, f in enumerate(findings, 1):
        waste_str = format_bytes(f.estimated_waste_bytes) if f.estimated_waste_bytes > 0 else "—"
        print(f"  {i:>3}  | {f.check_name:<35} | {f.affected_count:>10,} | {waste_str:>10} | {f.severity:<8}")

    total_waste = sum(f.estimated_waste_bytes for f in findings)
    print(f"\n  Total estimated waste: {format_bytes(total_waste)}")

    # Details per check
    print("\n### Check Details")
    for i, f in enumerate(findings, 1):
        print(f"\n  [{f.severity}] {i}. {f.check_name} (Tier {f.tier})")
        print(f"  {f.details}")
        if f.sub_findings:
            for sf in f.sub_findings:
                print(sf)
        print(f"  Recommendation: {f.recommendation}")


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def print_table(title, rows, columns=None):
    """Print a formatted table."""
    if not rows:
        print(f"\n### {title}")
        print("  (no data)")
        return
    if "error" in rows[0]:
        print(f"\n### {title}")
        print(f"  {rows[0]['error']}")
        return

    if columns is None:
        columns = list(rows[0].keys())

    widths = {col: len(col) for col in columns}
    for row in rows:
        for col in columns:
            val = str(row.get(col, ""))
            widths[col] = max(widths[col], len(val))

    print(f"\n### {title}")
    header = " | ".join(col.ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    print(f"  {header}")
    print(f"  {separator}")

    for row in rows:
        line = " | ".join(str(row.get(col, "")).ljust(widths[col]) for col in columns)
        print(f"  {line}")


def format_number(n):
    """Format number with commas."""
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Analyze HeapDumpStarDiver parquet output")
    parser.add_argument("parquet_dir", help="Path to parquet directory")
    parser.add_argument("--top", type=int, default=30, help="Number of top types to show (default 30)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--waste", action="store_true", help="Enable memory waste analysis")
    parser.add_argument("--waste-tier", type=int, default=2, choices=[1, 2, 3],
                        help="Max tier to run: 1=fast, 2=default, 3=thorough")
    parser.add_argument("--waste-sample-pct", type=int, default=100,
                        help="Sample percentage for expensive checks (default 100, use 20 for very large heaps)")
    args = parser.parse_args()

    parquet_dir = os.path.abspath(args.parquet_dir)
    if not os.path.isdir(parquet_dir):
        print(f"ERROR: Directory not found: {parquet_dir}", file=sys.stderr)
        sys.exit(1)

    # Detect mode
    resolver = ParquetResolver(parquet_dir)

    # Validate: need either _object_index (robo) or class parquet files (non-robo)
    if resolver.is_robo:
        oi = resolver.resolve("_object_index")
        if not oi:
            print(f"ERROR: No _object_index_chunk*.parquet files found in {parquet_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Mode: robo (chunked parquet)")
    else:
        pq_files = [f for f in os.listdir(parquet_dir) if f.endswith(".parquet")]
        if not pq_files:
            print(f"ERROR: No .parquet files found in {parquet_dir}", file=sys.stderr)
            sys.exit(1)
        print(f"Mode: non-robo (single-file parquet)")

    # Connect to DuckDB and set working directory
    con = duckdb.connect()
    original_dir = os.getcwd()
    os.chdir(parquet_dir)

    results = {}

    try:
        # Standard analysis
        print("=" * 60)
        print("HEAP DUMP ANALYSIS REPORT")
        print("=" * 60)
        print(f"Parquet directory: {parquet_dir}")

        # Parquet directory size
        total_size = sum(
            os.path.getsize(os.path.join(parquet_dir, f))
            for f in os.listdir(parquet_dir)
            if f.endswith(".parquet")
        )
        file_count = len([f for f in os.listdir(parquet_dir) if f.endswith(".parquet")])
        print(f"Parquet files: {file_count:,} files, {total_size / (1024**3):.2f} GB")

        # 1. Summary
        summary = run_summary(con, resolver)
        results["summary"] = summary
        if summary:
            s = summary[0]
            to = s.get("total_objects")
            uc = s.get("unique_classes")
            if isinstance(to, (int, float)):
                print(f"\nTotal objects: {format_number(to)}")
                print(f"Unique classes: {format_number(uc)}")
            else:
                print(f"\nNote: Object index not available (non-robo mode). Skipping summary counts.")

        # 2. Top types (robo only)
        if resolver.has_object_index():
            top_types = run_top_types(con, resolver, args.top)
            results["top_types"] = top_types
            print_table(f"Top {args.top} Types by Object Count", top_types)

            # 3. Category breakdown
            categories = run_category_breakdown(con, resolver)
            results["categories"] = categories
            total_objs = sum(r["obj_count"] for r in categories) if categories else 1
            for r in categories:
                r["pct"] = f"{r['obj_count'] / total_objs * 100:.1f}%"
            print_table("Category Breakdown", categories, ["category", "obj_count", "pct"])

        # 4. Byte array distribution
        byte_dist = run_byte_array_distribution(con, resolver)
        results["byte_arrays"] = byte_dist
        print_table("Byte Array Size Distribution", byte_dist)

        # 5. Large byte arrays
        large_bytes = run_large_byte_arrays(con, resolver)
        results["large_byte_arrays"] = large_bytes
        if large_bytes:
            print_table("Top Large Byte Arrays (>100KB)", large_bytes)

        # Waste analysis
        if args.waste:
            print("\n  Running waste analysis (tier 1-{})...".format(args.waste_tier))
            findings = run_waste_analysis(con, resolver, max_tier=args.waste_tier,
                                          sample_pct=args.waste_sample_pct)
            print_waste_report(findings)
            results["waste_findings"] = [asdict(f) for f in findings]

        print("\n" + "=" * 60)

    finally:
        os.chdir(original_dir)
        con.close()

    if args.json:
        def sanitize(obj):
            if isinstance(obj, (int, float, str, bool, type(None))):
                return obj
            return str(obj)

        clean = json.loads(json.dumps(results, default=sanitize))
        print("\n--- JSON OUTPUT ---")
        print(json.dumps(clean, indent=2))


if __name__ == "__main__":
    main()
