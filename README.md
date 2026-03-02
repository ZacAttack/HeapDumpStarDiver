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
