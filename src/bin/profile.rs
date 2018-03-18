extern crate ruba;
extern crate futures;

use ruba::Ruba;
use futures::executor::block_on;

fn main() {
    let ruba = Ruba::memory_only();
    let _ = block_on(ruba.load_csv("test_data/green_tripdata_2017-06.csv", "test", 16_384));
    loop {
        let _ = block_on(ruba.run_query("select passenger_count, count(1) from test;"));
    }
}
