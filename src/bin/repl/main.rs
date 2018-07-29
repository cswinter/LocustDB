#[macro_use]
extern crate clap;
extern crate failure;
extern crate futures_executor;
extern crate heapsize;
extern crate locustdb;
extern crate nom;
extern crate rustyline;
extern crate time;

use failure::Fail;
use futures_executor::block_on;
use locustdb::unit_fmt::*;
use locustdb::LocustDB;
use time::precise_time_ns;
use clap::{Arg, App};

mod print_results;
mod fmt_table;

fn main() {
    let matches = App::new("LocustDB")
        .version(crate_version!())
        .author("Clemens Winter <clemenswinter1@gmail.com>")
        .about("Extremely fast analytics database")
        .arg(Arg::with_name("db-path")
            .help("Path to data directory")
            .default_value("ldb")
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
        .arg(Arg::with_name("partition-size")
            .help("Number of rows per partition when loading new data")
            .long("partition-size")
            .value_name("INTEGER")
            .default_value("65536"))
        .arg(Arg::with_name("reduced-nyc-taxi-rides")
            .help("Set ingestion schema to load select set of columns from the 1.46 billion taxi ride dataset")
            .long("reduced-nyc-taxi-rides"))
        .get_matches();

    if cfg!(feature = "nerf") {
        println!("NERFED!");
    }

    let files = matches.values_of("load").unwrap_or_default();
    let tablename = matches.value_of("table").unwrap();
    let partition_size = value_t!(matches, "partition-size", u32).unwrap() as usize;
    let reduced_nyc = matches.is_present("reduced-nyc-taxi-rides");
    let load = files.len() > 0;

    let locustdb = if cfg!(feature = "enable_rocksdb") {
        LocustDB::disk_backed("rocksdb")
    } else {
        LocustDB::memory_only()
    };

    let start_time = precise_time_ns();
    let mut loads = Vec::new();
    for file in files {
        let mut base_opts = if reduced_nyc {
            locustdb::nyc_taxi_data::ingest_file(&file, tablename)
        } else {
            locustdb::IngestFile::new(&file, &tablename)
        };
        let opts = base_opts.with_partition_size(partition_size);
        let load = locustdb.load_csv(opts);
        loads.push(load);
        println!("Loading {} into table {}.", file, tablename);
    }
    for l in loads {
        block_on(l)
            .expect("Ingestion crashed!")
            .expect("Failed to load file!");
    }
    if load {
        println!("Loaded data in {:.3}.", ns((precise_time_ns() - start_time) as usize));
    }

    table_stats(&locustdb);
    repl(&locustdb);
}

fn table_stats(locustdb: &LocustDB) {
    let stats = block_on(locustdb.table_stats()).expect("!?!");
    for table in stats {
        let size = table.batches_bytes + table.buffer_bytes;
        println!("\n# Table `{}` ({} rows, {:.2}) #", &table.name, table.rows, bite(size));
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
                    println!("Restored DB from disk in {:.2}",
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
                    Err(mut fail) => print_error(fail),
                }
            }
            _ => println!("Error: Query execution was canceled!"),
        }
    }
    rl.save_history(".locustdb_history").ok();
}

fn print_error(fail: locustdb::QueryError) {
    println!("{}", fail);
    while let Some(cause) = fail.cause() {
        println!("{}", cause);
        if let Some(bt) = cause.backtrace() {
            println!("{}", bt);
        }
    }
}