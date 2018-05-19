extern crate locustdb;
extern crate futures;

use locustdb::LocustDB;
use futures::executor::block_on;

fn main() {
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.load_csv("test_data/yellow_tripdata_2009-01.csv", None, "test", 1 << 16, vec![]));
    println!("Load completed");
    loop {
        let _ = block_on(locustdb.run_query("select Passenger_Count, count(1) from test;"));
    }
}
