# HeapDumpStarDiver!

Is a useful tool which can be use to dump java heap dumps to parquet files.

This is a very early stage of this project.  So to get running some things:

This project depends heavily on the work done by Marshall Pierce (thanks Marshall!).

We're currently depending on a fork of the jvm-hprof crate located here: https://bitbucket.org/ZacAttack/jvm-hprof-rs-li-hackweek/src/master/

To get this to run, you'll need to check out that repository and add to this project with the command 

```
> cargo add --path <path>/jvm-hprof-rs-li-hackweek/
      Adding jvm-hprof (local) to dependencies
```

This is to get access to some of the utility functions in the example tools of the repo.

## Usage

```
> cargo run -- --help
Usage: HeapDumpStarDiver --file <FILE> [COMMAND]

Commands:
  dump-objects   Display Object (and other associated) heap dump subrecords
  count-records  Display the number of each of the top level hprof record types
  help           Print this message or the help of the given subcommand(s)

Options:
  -f, --file <FILE>  Heap dump file to read
  -h, --help         Print help
```

### dump-objects
```
> cargo run -- -f <filename>.hprof dump-objects
s
25789437384: byte[] = [0x35, 0x33, 0x39, 0x36, 0x34, ]

id 25789437408: java/lang/String
  - hashIsZero: boolean = false
  - hash: int = 0
  - coder: byte = 0
  - value = id 25789437384 (byte[])

25783024568: byte[] = [0x6B, 0x6F, 0x74, 0x6C, 0x69, 0x6E, 0x2D, 0x73, 0x74, 0x64, 0x6C, 0x69, 0x62, ]

id 25783024600: java/util/LinkedHashMap$Entry
  - after = id 25783024712 (java/util/LinkedHashMap$Entry)
  - before = id 25783024504 (java/util/LinkedHashMap$Entry)
  - next = null
  - value = id 25783024544 (java/lang/String)
  - key = id 25782924864 (java/util/jar/Attributes$Name)
  - hash: int = -342610999

id 25782843312: java/security/AccessControlContext
  - limitedContext = null
  - isLimited: boolean = false
  - isWrapped: boolean = false
  - parent = null
  - permissions = null
  - combiner = null
  - privilegedContext = null
  - isAuthorized: boolean = false
  - isPrivileged: boolean = false
  - context = id 25782843536 ([Ljava/security/ProtectionDomain;)
```

### count-records
```
> cargo run -- -f <filename>.hprof count-records
Utf8: 48206
LoadClass: 2079
StackFrame: 48
HeapDumpSegment: 14
StackTrace: 7
HeapDumpEnd: 1
CpuSamples: 0
EndThread: 0
AllocSites: 0
HeapSummary: 0
StartThread: 0
UnloadClass: 0
ControlSettings: 0
HeapDump: 0
```