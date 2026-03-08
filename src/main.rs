mod commands;
mod hprof_index;
mod util;

use std::fs;
use jvm_hprof::parse_hprof;

fn main() {
    let app = clap::Command::new("Analyze Hprof")
        .arg(
            clap::Arg::new("file")
                .short('f')
                .long("file")
                .required(true)
                .value_name("FILE")
                .help("Heap dump file to read"),
        )
        .subcommand(clap::Command::new("dump-objects")
            .about("Display Object (and other associated) heap dump subrecords to stdout"))
        .subcommand(clap::Command::new("count-records")
            .about("Display the number of each of the top level hprof record types"))
        .subcommand(clap::Command::new("dump-objects-to-parquet")
            .about("Parses and dumps objects in the heap dump to parquet files")
            .arg(
                clap::Arg::new("flush-rows")
                    .long("flush-rows")
                    .value_name("N")
                    .value_parser(clap::value_parser!(usize))
                    .default_value("500000")
                    .help("Number of rows to accumulate before flushing to disk (lower = less memory)"),
            )
            .arg(
                clap::Arg::new("robo-mode")
                    .long("robo-mode")
                    .action(clap::ArgAction::SetTrue)
                    .help("LLM-optimized output: bare IDs for references, separate type index file. Faster parsing."),
            )
        );
    let matches = app.get_matches();

    let file_path = matches.get_one::<String>("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap_or_else(|_| panic!("Could not open file at path: {}", file_path));

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof = parse_hprof(&memmap[..]).unwrap();

    matches.subcommand().map(|(subcommand, sub_matches)| match subcommand {
        "dump-objects" => commands::dump_objects(&hprof),
        "count-records" => commands::count_records(&hprof),
        "dump-objects-to-parquet" => {
            let flush_rows = *sub_matches.get_one::<usize>("flush-rows").unwrap();
            let robo_mode = sub_matches.get_flag("robo-mode");
            commands::dump_objects_to_parquet(&hprof, flush_rows, robo_mode)
        }
        _ => panic!("Unknown subcommand"),
    });
}
