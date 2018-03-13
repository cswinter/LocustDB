extern crate ruba;

use ruba::mem_store::csv_loader::ingest_file;
use ruba::parser::parse_query;


fn main() {
    let batches = ingest_file("test_data/green_tripdata_2017-06.csv", 16_384);
    let query = parse_query("select passenger_count, count(1) from test;".as_bytes()).to_result().unwrap();
    loop {
        let mut compiled_query = query.compile(&batches);
        compiled_query.run();
    }
}
