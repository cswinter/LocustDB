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
use ruba::Ruba;

const LOAD_CHUNK_SIZE: usize = 200_000;

fn main() {
    let args: Vec<String> = env::args().collect();
    let filename = &args.get(1).expect("Specify data file as argument.");
    let ruba = Ruba::memory_only();
    let _ = block_on(ruba.load_csv(filename, "default", LOAD_CHUNK_SIZE));
    // print_ingestion_stats(&batches, columnarization_start_time);
    repl(ruba);
}

// TODO(clemens): Return ingestion stats from Ruba::load_csv or something
/*
fn print_ingestion_stats(batches: &Vec<Batch>, starttime: f64) {
    let bytes_in_ram: usize = batches.iter().map(|batch| batch.cols().heap_size_of_children()).sum();
    println!("Loaded data into {:.2} MiB in RAM in {} chunk(s) in {:.1} seconds.",
             bytes_in_ram as f64 / 1024f64 / 1024f64,
             batches.len(),
             precise_time_s() - starttime);

    println!("\n# Breakdown by column #");
    let mut column_sizes = HashMap::new();
    for batch in batches {
        for col in batch.cols() {
            let heapsize = col.heap_size_of_children();
            if let Some(size) = column_sizes.get_mut(col.name()) {
                *size += heapsize;
            }
            if !column_sizes.contains_key(col.name()) {
                column_sizes.insert(col.name().to_string(), heapsize);
            }
        }
    }
    for (columname, heapsize) in column_sizes {
        println!("{}: {:.2}MiB", columname, heapsize as f64 / 1024. / 1024.);
    }
}*/

fn repl(ruba: Ruba) {
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
        if s.chars().next_back() != Some(';') {
            s.push(';');
        }
        rl.add_history_entry(&s);
        let query = ruba.run_query(&s);
        match block_on(query) {
            Ok(result) => print_results::print_query_result(&result),
            _ => {}
        }
    }
    rl.save_history(".ruba_history").ok();
}
