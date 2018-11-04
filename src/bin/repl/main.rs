#[macro_use]
extern crate clap;
extern crate failure;
extern crate futures_executor;
extern crate heapsize;
extern crate locustdb;
extern crate nom;
extern crate rustyline;
extern crate time;
extern crate env_logger;
extern crate log;
extern crate sqlparser;

use failure::Fail;
use futures_executor::block_on;
use locustdb::unit_fmt::*;
use locustdb::LocustDB;
use time::precise_time_ns;
use clap::{Arg, App};

mod print_results;
mod fmt_table;

fn main() {
    env_logger::init();

    let mut options = locustdb::Options::default();
    let default_mem_limit_tables = format!("{}", options.mem_size_limit_tables / 1024 / 1024 / 1024);
    let default_readahead = format!("{}", options.readahead / 1024 / 1024);
    let help_threads = format!("Number of worker threads. [default: number of cores ({})]", options.threads);
    let matches = App::new("LocustDB")
        .version(crate_version!())
        .author("Clemens Winter <clemenswinter1@gmail.com>")
        .about("Massively parallel, high performance analytics database that will rapidly devour all of your data.")
        .arg(Arg::with_name("db-path")
            .help("Path to data directory")
            .long("db-path")
            .value_name("PATH")
            .takes_value(true))
        .arg(Arg::with_name("load")
            .help("Load .csv or .csv.gz files into the database")
            .long("load")
            .value_name("CSV_FILE")
            .multiple(true)
            .takes_value(true))
        .arg(Arg::with_name("table")
            .help("Name for the table populated with --load")
            .long("table")
            .value_name("NAME")
            .default_value("default")
            .takes_value(true))
        .arg(Arg::with_name("mem-limit-tables")
            .help("Limit for in-memory size of tables in GiB")
            .long("mem-limit-tables")
            .value_name("GB")
            .default_value(&default_mem_limit_tables)
            .takes_value(true))
        .arg(Arg::with_name("mem-lz4")
            .help("Keep data cached in memory lz4 encoded. Decreases memory usage and query speeds.")
            .long("mem-lz4"))
        .arg(Arg::with_name("partition-size")
            .help("Number of rows per partition when loading new data")
            .long("partition-size")
            .value_name("ROWS")
            .default_value("65536"))
        .arg(Arg::with_name("readahead")
            .help("How much data to load at a time when reading from disk during queries in MiB")
            .long("readahead")
            .value_name("MB")
            .default_value(&default_readahead))
        .arg(Arg::with_name("seq-disk-read")
            .help("Improves performance on HDD, can hurt performance on SSD.")
            .long("seq-disk-read"))
        .arg(Arg::with_name("threads")
            .help(&help_threads)
            .long("threads")
            .value_name("INTEGER")
            .takes_value(true))
        .arg(Arg::with_name("reduced-trips")
            .help("Set ingestion schema for select set of columns from nyc taxi ride dataset")
            .long("reduced-trips")
            .conflicts_with("trips"))
        .arg(Arg::with_name("trips")
            .help("Set ingestion schema for nyc taxi ride dataset")
            .long("trips")
            .conflicts_with("reduced-trips"))
        .get_matches();

    let files = matches.values_of("load").unwrap_or_default();
    let tablename = matches.value_of("table").unwrap();
    let partition_size = value_t!(matches, "partition-size", u32).unwrap() as usize;
    let reduced_nyc = matches.is_present("reduced-trips");
    let full_nyc = matches.is_present("trips");
    let db_path = matches.value_of("db-path");
    let file_count = files.len();

    if matches.is_present("db-path") && !cfg!(feature = "enable_rocksdb") {
        println!("WARNING: --db-path option passed, but RocksDB storage backend is not enabled in this build of LocustDB.");
    }

    options.db_path = db_path.map(|x| x.to_string());
    if let Some(t) = matches.value_of("threads") {
        options.threads = t.parse()
            .expect("Argument --threads must be a positive integer!");
    }
    options.mem_size_limit_tables = matches
        .value_of("mem-limit-tables").unwrap()
        .parse::<usize>()
        .map(|x| x * 1024 * 1024 * 1024)
        .expect("Argument --mem-limit-tables must be a positive integer!");
    options.readahead = matches
        .value_of("readahead").unwrap()
        .parse::<usize>()
        .map(|x| x * 1024 * 1024)
        .expect("Argument --readahead must be a positive integer!");
    options.mem_lz4 = matches.is_present("mem-lz4");
    if matches.is_present("seq-disk-read") {
        options.seq_disk_read = true;
        options.read_threads = 1;
    }

    if options.readahead > options.mem_size_limit_tables {
        println!("WARNING: `mem-limit-tables` should be at least as large as `readahead`");
    }

    let locustdb = locustdb::LocustDB::new(&options);

    let start_time = precise_time_ns();
    let mut loads = Vec::new();
    for file in files {
        let mut base_opts = if reduced_nyc {
            locustdb::nyc_taxi_data::ingest_reduced_file(&file, tablename)
        } else if full_nyc {
            locustdb::nyc_taxi_data::ingest_file(&file, tablename)
        } else {
            locustdb::LoadOptions::new(&file, &tablename)
        };
        let opts = base_opts.with_partition_size(partition_size);
        let load = locustdb.load_csv(opts);
        loads.push(load);
        if file_count < 4 {
            println!("Loading {} into table {}.", file, tablename);
        }
    }
    if file_count >= 4 {
        println!("Loading {} files into table {}.", file_count, tablename);
    }
    for l in loads {
        block_on(l)
            .expect("Ingestion crashed!")
            .expect("Failed to load file!");
    }
    if file_count > 0 {
        println!("Loaded data in {:.3}.", ns((precise_time_ns() - start_time) as usize));
    }

    table_stats(&locustdb);
    repl(&locustdb);
}

fn table_stats(locustdb: &LocustDB) {
    let stats = block_on(locustdb.table_stats()).expect("!?!");
    for table in stats {
        let size = table.batches_bytes + table.buffer_bytes;
        println!("\n# Table `{}` ({} rows, {}) #", &table.name, table.rows, bite(size));
        for &(ref columname, heapsize) in &table.size_per_column {
            println!("{}: {:.2}", columname, bite(heapsize));
        }
    }
}

fn repl(locustdb: &LocustDB) {
    let mut rl = rustyline::Editor::<()>::new();
    rl.load_history(".locustdb_history").ok();
    while let Ok(mut s) = rl.readline("locustdb> ") {
        if let Some('\n') = s.chars().next_back() {
            s.pop();
        }
        if let Some('\r') = s.chars().next_back() {
            s.pop();
        }
        if s.trim() == "" {
            continue;
        }
        if s == "exit" {
            break;
        }
        rl.add_history_entry(&s);
        if !s.ends_with(';') {
            s.push(';');
        }

        let mut print_trace = false;
        let mut explain = false;
        let mut show = vec![];
        let mut s: &str = &s;
        if s.starts_with(":memtree") {
            let depth = if s.starts_with(":memtree(") {
                let end = s.find(')').unwrap();
                s[9..end].parse::<usize>().expect("must pass integer to :memtree(x) command")
            } else { 2 };
            match block_on(locustdb.mem_tree(depth)) {
                Ok(trees) => for tree in trees {
                    println!("{}\n", &tree)
                },
                _ => println!("Error: Query execution was canceled!"),
            }
            continue;
        }
        if s.starts_with(":restore") {
            let start = precise_time_ns();
            match block_on(locustdb.bulk_load()) {
                Ok(trees) => {
                    println!("Restored DB from disk in {}",
                             ns((precise_time_ns() - start) as usize));
                    for tree in trees {
                        println!("{}\n", &tree)
                    }
                }
                _ => println!("Error: Query execution was canceled!"),
            }
            continue;
        }
        if s.starts_with(":explain") {
            explain = true;
            s = &s[9..];
        }
        if s.starts_with(":trace") {
            print_trace = true;
            s = &s[7..];
        }
        if s.starts_with(":show") {
            show = if s.starts_with(":show(") {
                let end = s.find(')').unwrap();
                let partition = s[6..end].parse::<usize>().expect("must pass integer to :show(x) command");
                s = &s[(end + 2)..];
                vec![partition]
            } else {
                s = &s[6..];
                vec![0]
            }
        }
        if s.starts_with(":recover") {
            locustdb.recover();
            continue;
        }
        if s.starts_with(":ast") {
            println!("{}", locustdb.ast(&s[5..]));
            continue;
        }

        let query = locustdb.run_query(s, explain, show);
        match block_on(query) {
            Ok((result, trace)) => {
                if print_trace {
                    trace.print();
                }
                match result {
                    Ok(output) => print_results::print_query_result(&output),
                    Err(mut fail) => print_error(&fail),
                }
            }
            _ => println!("Error: Query execution was canceled!"),
        }
    }
    rl.save_history(".locustdb_history").ok();
}

fn print_error(fail: &locustdb::QueryError) {
    println!("{}", fail);
    while let Some(cause) = fail.cause() {
        println!("{}", cause);
        if let Some(bt) = cause.backtrace() {
            println!("{}", bt);
        }
    }
}
