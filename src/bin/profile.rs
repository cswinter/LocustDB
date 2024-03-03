use locustdb::LocustDB;
use futures::executor::block_on;

fn main() {
    let locustdb = LocustDB::memory_only();
    let mut loads = Vec::new();
    for x in &["aa", "ab", "ac", "ad", "ae"] {
        let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
        loads.push(locustdb.load_csv(
            locustdb::nyc_taxi_data::ingest_reduced_file(&path, "test")
                .with_partition_size(1 << 20)));
    }
    for l in loads {
        let _ = block_on(l);
    }
    println!("Load completed");
    loop {
        let _ = block_on(locustdb.run_query("select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from test;", false, false, vec![]));
    }
}
