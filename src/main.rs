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
        );
    let matches = app.get_matches();

    let file_path = matches.get_one::<String>("file").expect("file must be specified");

    let file = fs::File::open(file_path).unwrap_or_else(|_| panic!("Could not open file at path: {}", file_path));

    let memmap = unsafe { memmap::MmapOptions::new().map(&file) }.unwrap();

    let hprof = parse_hprof(&memmap[..]).unwrap();

    matches.subcommand().map(|(subcommand, _)| match subcommand {
        "dump-objects" => commands::dump_objects(&hprof),
        "count-records" => commands::count_records(&hprof),
        "dump-objects-to-parquet" => commands::dump_objects_to_parquet(&hprof),
        _ => panic!("Unknown subcommand"),
    });
}
