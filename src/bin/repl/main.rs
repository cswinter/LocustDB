extern crate locustdb;

extern crate rustyline;
extern crate heapsize;
extern crate time;
extern crate nom;
extern crate futures;
extern crate failure;

mod print_results;
mod fmt_table;

use std::env;

use futures::executor::block_on;
use locustdb::{LocustDB, TableStats};
use time::precise_time_ns;
use failure::Fail;

const LOAD_CHUNK_SIZE: usize = 1 << 16;

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args.get(1).expect("Specify data file as argument.");
    let locustdb = LocustDB::memory_only();
    println!("Loading {} into table default.", filename);
    let start_time = precise_time_ns();
    if filename == &"nyc100m" {
        let mut loads = Vec::new();
        for x in &["aa", "ab", "ac", "ad", "ae"] {
            let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
            loads.push(locustdb.load_csv(
                locustdb::nyc_taxi_data::ingest_file(&path, "test")
                    .with_chunk_size(1 << 20)));
        }
        for l in loads {
            let _ = block_on(l);
        }
    } else {
        let ingestion_request = if filename.contains("nyc-taxi-data") {
            locustdb::nyc_taxi_data::ingest_file(filename, "test")
                .with_chunk_size(LOAD_CHUNK_SIZE)
        } else {
            locustdb::IngestFile::new(filename, "default")
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
        let mut s: &str = &s;
        if s.starts_with(":explain") {
            explain = true;
            s = &s[9..];
        }
        if s.starts_with(":trace") {
            print_trace = true;
            s = &s[7..];
        }

        let query = locustdb.run_query(s, explain);
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
