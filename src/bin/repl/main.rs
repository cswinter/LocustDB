extern crate ruba;

extern crate rustyline;
extern crate heapsize;
extern crate time;
extern crate nom;
extern crate futures;

mod print_results;
mod fmt_table;

use std::env;

use futures::executor::block_on;
use ruba::{Ruba, TableStats};
use time::precise_time_ns;

const LOAD_CHUNK_SIZE: usize = 1 << 16;

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args.get(1).expect("Specify data file as argument.");
    let ruba = Ruba::memory_only();
    println!("Loading {} into table default.", filename);
    let start_time = precise_time_ns();
    let _ = block_on(ruba.load_csv(
        filename, "default", LOAD_CHUNK_SIZE,
        vec![
            ("Tolls_Amt".to_owned(), ruba::extractor::multiply_by_100),
            ("Tip_Amt".to_owned(), ruba::extractor::multiply_by_100),
            ("Fare_Amt".to_owned(), ruba::extractor::multiply_by_100),
            ("Total_Amt".to_owned(), ruba::extractor::multiply_by_100),
            ("Trip_Pickup_DateTime".to_owned(), ruba::extractor::date_time),
            ("Trip_Dropoff_DateTime".to_owned(), ruba::extractor::date_time),
        ]));
    let table_stats = block_on(ruba.table_stats()).expect("!?!");
    print_table_stats(&table_stats, start_time);
    repl(&ruba);
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

fn repl(ruba: &Ruba) {
    let mut rl = rustyline::Editor::<()>::new();
    rl.load_history(".ruba_history").ok();
    while let Ok(mut s) = rl.readline("ruba> ") {
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
        let mut s: &str = &s;
        if s.starts_with(":trace") {
            print_trace = true;
            s = &s[7..];
        }

        let query = ruba.run_query(s);
        match block_on(query) {
            Ok((result, trace)) => {
                if print_trace {
                    trace.print();
                }
                print_results::print_query_result(&result);
            }
            _ => println!("Error: Query execution was canceled!"),
        }
    }
    rl.save_history(".ruba_history").ok();
}
