extern crate locustdb;
extern crate futures;

use locustdb::{LocustDB, IngestFile};
use futures::executor::block_on;

fn main() {
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.load_csv(IngestFile::new("test_data/yellow_tripdata_2009-01.csv", "test")));
    println!("Load completed");
    loop {
        let _ = block_on(locustdb.run_query("select Passenger_Count, count(1) from test;"));
    }
}
