extern crate locustdb;
extern crate futures;

use locustdb::LocustDB;
use futures::executor::block_on;
use locustdb::nyc_taxi_data;

fn main() {
    let locustdb = LocustDB::memory_only();
    let load = block_on(locustdb.load_csv(
        nyc_taxi_data::ingest_file("test_data/nyc-taxi.csv.gz", "default")
            .with_chunk_size(999)));
    load.unwrap().ok();
    // let query = "select pickup_ntaname, to_year(pickup_datetime), trip_distance / 1000, count(0), sum(total_amount) from default where cab_type = \"CMS\";";
    let query = "select payment_method, count(0), sum(total_amount) from default;";
    block_on(locustdb.run_query(&query, false, vec![0])).unwrap();
}
