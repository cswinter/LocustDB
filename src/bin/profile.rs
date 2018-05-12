extern crate ruba;
extern crate futures;

use ruba::Ruba;
use futures::executor::block_on;

fn main() {
    let ruba = Ruba::memory_only();
    let _ = block_on(ruba.load_csv("test_data/yellow_tripdata_2009-01.csv", None, "test", 1 << 16, vec![]));
    println!("Load completed");
    loop {
        let _ = block_on(ruba.run_query("select Passenger_Count, count(1) from test;"));
    }
}
