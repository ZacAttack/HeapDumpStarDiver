HeapDumpStarDiver!

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