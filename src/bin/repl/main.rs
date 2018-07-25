extern crate failure;
extern crate futures_executor;
extern crate heapsize;
extern crate locustdb;
extern crate nom;
extern crate rustyline;
extern crate time;

use failure::Fail;
use futures_executor::block_on;
use locustdb::{LocustDB, TableStats};
use std::env;
use std::fs;
use time::precise_time_ns;

mod print_results;
mod fmt_table;

const LOAD_CHUNK_SIZE: usize = 1 << 16;

fn main() {
    #[cfg(feature = "nerf")]
    println!("NERFED!");

    let args: Vec<String> = env::args().collect();
    let filename = &args.get(1).expect("Specify data file as argument.");

    #[cfg(feature = "enable_rocksdb")]
    let locustdb = LocustDB::disk_backed("rocksdb");
    #[cfg(not(feature = "enable_rocksdb"))]
    let locustdb = LocustDB::memory_only();

    let start_time = precise_time_ns();
    println!("Loading {} into table trips.", filename);
    if filename == &"nyc" {
        let mut loads = Vec::new();
        for path in fs::read_dir("test_data/nyc-taxi-data").unwrap() {
            loads.push(locustdb.load_csv(
                locustdb::nyc_taxi_data::ingest_file(path.unwrap().path().to_str().unwrap(), "trips")
                    .with_chunk_size(1 << 20)));
        }
        for l in loads {
            let _ = block_on(l);
        }
    } else if filename == &"nyc100m" {
        let mut loads = Vec::new();
        for x in &["aa", "ab", "ac", "ad", "ae"] {
            let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
            loads.push(locustdb.load_csv(
                locustdb::nyc_taxi_data::ingest_file(&path, "trips")
                    .with_chunk_size(1 << 20)));
        }
        for l in loads {
            let _ = block_on(l);
        }
    } else if filename == &"load_from_db" {
        println!("Restoring data from db...");
    } else if filename == &"passenger_count" {
        let mut loads = Vec::new();
        for path in fs::read_dir("test_data/nyc-taxi-data").unwrap() {
            loads.push(locustdb.load_csv(
                locustdb::nyc_taxi_data::ingest_passenger_count(path.unwrap().path().to_str().unwrap(), "trips")
                    .with_chunk_size(1 << 20)));
        }
        for l in loads {
            let _ = block_on(l);
        }
    } else if filename == &"passenger_count100m" {
        let mut loads = Vec::new();
        for x in &["aa", "ab", "ac", "ad", "ae"] {
            let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
            loads.push(locustdb.load_csv(
                locustdb::nyc_taxi_data::ingest_passenger_count(&path, "trips")
                    .with_chunk_size(1 << 20)));
        }
        for l in loads {
            let _ = block_on(l);
        }
    } else {
        let ingestion_request = if filename.contains("nyc-taxi") {
            locustdb::nyc_taxi_data::ingest_file(filename, "trips")
                .with_chunk_size(LOAD_CHUNK_SIZE)
        } else {
            locustdb::IngestFile::new(filename, "trips")
                .with_chunk_size(LOAD_CHUNK_SIZE)
        };
        block_on(locustdb.load_csv(ingestion_request))
            .expect("Ingestion crashed!")
            .expect("Failed to load file!");
    }
    let table_stats = block_on(locustdb.table_stats()).expect("!?!");
    print_table_stats(&table_stats, start_time);
    repl(&locustdb);
}

fn print_table_stats(stats: &[TableStats], start_time: u64) {
    println!("Loaded data in {:.1} seconds.", (precise_time_ns() - start_time) / 1_000_000_000);
    for table in stats {
        let size = (table.batches_bytes + table.buffer_bytes) as f64 / 1024f64 / 1024f64;
        println!("\n# Table `{}` ({} rows, {:.2} MiB) #", &table.name, table.rows, size);
        for &(ref columname, heapsize) in &table.size_per_column {
            println!("{}: {:.2}MiB", columname, heapsize as f64 / 1024. / 1024.);
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
        if !s.ends_with(';') {
            s.push(';');
        }
        rl.add_history_entry(&s);

        let mut print_trace = false;
        let mut explain = false;
        let mut show = vec![];
        let mut s: &str = &s;
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
                println!("{}", s);
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
                    Err(mut fail) => {
                        println!("{}", fail);
                        while let Some(cause) = fail.cause() {
                            println!("{}", cause);
                            if let Some(bt) = cause.backtrace() {
                                println!("{}", bt);
                            }
                        }
                    }
                }
            }
            _ => println!("Error: Query execution was canceled!"),
        }
    }
    rl.save_history(".locustdb_history").ok();
}
