# HeapDumpStarDiver

This tool converts JVM heap dumps (HPROF binary format) into Apache Parquet files, enabling columnar analytical processing of heap objects. It parses every object instance, class definition, primitive array, and object reference from a heap dump and exports them as typed Parquet columns — one file per Java class.

It is also able to dump information classes on heap to your terminal, if you want a more high level/fast look at things.

This is an early stage project, and was made during a LinkedIn hackathon!  Any improvements or PR's are welcome!

## Dependencies

This project depends heavily on the work done by Marshall Pierce (thanks Marshall!).

We depend on a fork of the `jvm-hprof` crate located here: https://bitbucket.org/ZacAttack/jvm-hprof-rs-li-hackweek/src/master/

To build, you'll need to check out that repository at `../../bitbucket/jvm-hprof-rs-li-hackweek/` (relative to this project root), or update the path in `Cargo.toml`. (I'm thinking we'll at some point just vendor this dependency).

## Build & Run

```bash
# Build
cargo build --release

# Run (requires an HPROF file)
./target/release/HeapDumpStarDiver -f <path-to-heap-dump>.hprof <command>
```

## Commands

### dump-objects-to-parquet

The primary command. Exports all heap objects to Parquet files in a `parquet/` directory (created automatically). Produces one `.parquet` file per Java class, plus special files for primitive arrays, object arrays, static fields, and GC roots.

```
> ./target/release/HeapDumpStarDiver -f heap.hprof dump-objects-to-parquet

> ls parquet/
_gc_roots.parquet
_object_arrays.parquet
_primitive_arrays_byte.parquet
_primitive_arrays_int.parquet
_static_fields.parquet
java.lang.String.parquet
java.util.HashMap.parquet
java.util.HashMap.Node.parquet
...
```

Data is flushed to disk incrementally, so memory usage stays bounded even for very large heap dumps. You can tune the flush frequency with `--flush-rows` (default 500,000):

```bash
# Use less memory by flushing more often
./target/release/HeapDumpStarDiver -f heap.hprof dump-objects-to-parquet --flush-rows 100000
```

#### Robo Mode (`--robo-mode`)

Robo mode is an alternative output format optimized for speed at the expense of human readability. It's designed for LLM-assisted querying -- an LLM can easily follow bare object ID references and join across files, negating the readability loss while benefiting from the faster export.

```bash
./target/release/HeapDumpStarDiver -f heap.hprof dump-objects-to-parquet --robo-mode
```

**How it differs from default mode:**

| Aspect | Default Mode | Robo Mode |
|--------|-------------|-----------|
| Object references | `Struct { id: UInt64, type: Utf8 }` with resolved type names | Bare `UInt64` IDs (no type name resolution) |
| Type lookup | Embedded in every reference column | Separate `_object_index` file maps obj_id to type name |
| Class hierarchy | Not exported | `_class_hierarchy.parquet` with superclass relationships |
| Static fields | Includes `ref_type` column with resolved type names | Omits `ref_type` (use `_object_index` to resolve) |
| File naming | One file per class: `ClassName.parquet` | Chunked across 16 workers: `ClassName_chunk0.parquet` ... `ClassName_chunk15.parquet` |
| Speed | Slower (resolves type names for every object reference) | Faster (skips type resolution entirely) |

**Additional files produced in robo mode:**

- `_object_index_chunk{0-15}.parquet` -- Maps every object ID to its type name. Schema: `(obj_id: UInt64, type_name: Utf8)`. Use this to resolve the bare IDs in other files.
- `_class_hierarchy.parquet` -- Class inheritance tree. Schema: `(class_obj_id: UInt64, class_name: Utf8, super_class_obj_id: UInt64?, super_class_name: Utf8?)`.

**When to use robo mode:** When you're querying the heap dump programmatically (e.g. via Python/DuckDB/an LLM) and want the fastest possible export. The chunked output is trivially queryable -- tools like DuckDB and PyArrow can glob `parquet/ClassName_chunk*.parquet` to read all chunks as one table.

### dump-objects

Prints all heap objects to stdout in a human-readable format.

```
> ./target/release/HeapDumpStarDiver -f heap.hprof dump-objects

25789437384: byte[] = [0x35, 0x33, 0x39, 0x36, 0x34, ]

id 25789437408: java/lang/String
  - hashIsZero: boolean = false
  - hash: int = 0
  - coder: byte = 0
  - value = id 25789437384 (byte[])
```

### count-records

Tallies the top-level HPROF record types.

```
> ./target/release/HeapDumpStarDiver -f heap.hprof count-records

Utf8: 48206
LoadClass: 2079
StackFrame: 48
HeapDumpSegment: 14
StackTrace: 7
HeapDumpEnd: 1
```

## Configuring MCP For Agent Driven Analysis

HeapDumpStarDiver includes an MCP (Model Context Protocol) server that lets any compatible AI agent convert heap dumps, run SQL queries, and perform automated waste detection — no manual scripting required.

### Install

```bash
# 1. Build the Rust binary (see Dependencies above)
cargo build --release

# 2. Install the MCP server
pip install heapdump-stardiver-mcp
```

Or install from source:

```bash
python3 -m venv .venv
.venv/bin/pip install -e .
```

### Available Tools

| Tool | Description |
|------|-------------|
| `convert_heap_dump` | Convert an HPROF file to Parquet and open an analysis session |
| `open_session` | Open a session against existing Parquet files (resume previous work) |
| `list_sessions` | Show all active sessions |
| `close_session` | Close a session's DuckDB connection, keep files for later |
| `cleanup_session` | Close connection and delete Parquet files |
| `list_parquet_files` | List available tables with row counts and schemas |
| `query_heap` | Run DuckDB SQL against the Parquet files (paginated) |
| `analyze_heap` | Automated waste detection and heap profiling |

The server also exposes reference resources (`heapdump://guides/setup`, `heapdump://guides/sql-examples`, `heapdump://guides/waste-checks`) that agents can read on demand for setup help, query examples, and waste check documentation.

### Agent Configuration

Each agent needs a one-line config pointing at the `heapdump-stardiver-mcp` command. The format is the same across most agents — only the config file location differs.

| Agent | Config file |
|-------|------------|
| **Claude Code** | `.mcp.json` in repo root |
| **Claude Desktop** | `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) |
| **Cursor** | `.cursor/mcp.json` in repo root |
| **Windsurf** | `~/.windsurf/mcp.json` |
| **ChatGPT Desktop** | Settings UI → MCP servers |

Add the following to the appropriate config file:

```json
{
  "mcpServers": {
    "heapdump-stardiver": {
      "command": "heapdump-stardiver-mcp"
    }
  }
}
```

If installing from source instead of pip, point at the venv Python:

```json
{
  "mcpServers": {
    "heapdump-stardiver": {
      "command": ".venv/bin/python",
      "args": ["-m", "mcp_server.server"]
    }
  }
}
```

### Typical Agent Workflow

1. **Convert:** Agent calls `convert_heap_dump(hprof_path="/path/to/dump.hprof")` — converts HPROF to Parquet and opens a session named after the file
2. **Discover:** Agent calls `list_parquet_files()` — sees available tables and their schemas
3. **Analyze:** Agent calls `analyze_heap()` — gets automated waste detection results
4. **Query:** Agent calls `query_heap(sql="SELECT ...")` — runs ad-hoc DuckDB queries
5. **Clean up:** Agent calls `close_session(id)` to keep files, or `cleanup_session(id, confirm=True)` to delete them

Multiple sessions can be open simultaneously for comparing heap dumps across services or time periods.
