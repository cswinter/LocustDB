#![feature(test)]
extern crate locustdb;
extern crate test;
extern crate futures;

use std::path::Path;

use locustdb::LocustDB;
use futures::executor::block_on;


const YELLOW_PATH: &str = "test_data/yellow_tripdata_2009-01.csv";
const YELLOW_URL: &str = "https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2009-01.csv";
static mut YELLOW_RUBA: Option<LocustDB> = None;

fn get_yellow() -> &'static LocustDB {
    unsafe {
        match YELLOW_RUBA {
            Some(ref locustdb) => locustdb,
            None => {
                if !Path::new(YELLOW_PATH).exists() {
                    panic!("{} not found. Download dataset at {}", YELLOW_PATH, YELLOW_URL);
                }
                let locustdb = LocustDB::memory_only();
                let load = locustdb.load_csv(YELLOW_PATH, "test", 1 << 16, vec![]);
                let _ = block_on(load);
                YELLOW_RUBA = Some(locustdb);
                YELLOW_RUBA.as_ref().unwrap()
            }
        }
    }
}

fn bench_query_ytd_14m(b: &mut test::Bencher, query_str: &str) {
    let locustdb = get_yellow();
    b.iter(|| {
        let query = locustdb.run_query(query_str);
        block_on(query)
    });
}

#[bench]
fn yt_14m_count_by_passenger_count(b: &mut test::Bencher) {
    bench_query_ytd_14m(b, "select Passenger_Count, count(1) from test;");
}

#[bench]
fn yt_14m_sum_total_amt_group_by_passenger_count(b: &mut test::Bencher) {
    bench_query_ytd_14m(b, "select Passenger_Count, sum(Total_Amt) from test;");
}

#[bench]
fn yt_14m_select_passenger_count_sparse_filter(b: &mut test::Bencher) {
    // there are a total of 718 entries with Passenger_Count = 0
    bench_query_ytd_14m(b, "select Passenger_Count from test where Passenger_Count < 1 limit 1000;");
}

#[bench]
fn yt_14m_select_star_limit_10000(b: &mut test::Bencher) {
    bench_query_ytd_14m(b, "select * from test limit 10000;");
}

#[bench]
fn yt_14m_count_group_by_vendor_name_and_passenger_count(b: &mut test::Bencher) {
    bench_query_ytd_14m(b, "select vendor_name, Passenger_Count, count(1) from test;");
}
