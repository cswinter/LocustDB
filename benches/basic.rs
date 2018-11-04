#![feature(test)]
extern crate futures_executor;
extern crate locustdb;
extern crate test;

use futures_executor::block_on;
use locustdb::{LocustDB, Options};
use std::env;
use std::path::Path;
use std::u32;

const DOWNLOAD_URL: &str = "https://www.dropbox.com/sh/4xm5vf1stnf7a0h/AADRRVLsqqzUNWEPzcKnGN_Pa?dl=0";
static mut DB: Option<LocustDB> = None;

fn db() -> &'static LocustDB {
    unsafe {
        // Prints each argument on a separate line
        let thread_count = env::var_os("LOCUSTDB_THREADS")
            .map(|x| x.to_str().unwrap().parse::<usize>().unwrap());
        match DB {
            Some(ref locustdb) => locustdb,
            None => {
                let mut opts = Options::default();
                opts.threads = thread_count.unwrap_or(opts.threads);
                let locustdb = LocustDB::new(&opts);

                let mut loads = Vec::new();
                for x in &["aa", "ab", "ac", "ad", "ae"] {
                    let path = format!("test_data/nyc-taxi-data/trips_x{}.csv.gz", x);
                    if !Path::new(&path).exists() {
                        panic!("{} not found. Download dataset at {}", path, DOWNLOAD_URL);
                    }
                    loads.push(locustdb.load_csv(
                        locustdb::nyc_taxi_data::ingest_reduced_file(&path, "test")
                            .with_partition_size(1 << 20)));
                }
                for l in loads {
                    let _ = block_on(l);
                }

                gen_table(&locustdb, "trips_e8", 95, 1 << 20);
                gen_table(&locustdb, "trips_e7", 80, 1 << 17);
                gen_table(&locustdb, "trips_e6", 64, 1 << 14);

                DB = Some(locustdb);
                DB.as_ref().unwrap()
            }
        }
    }
}


fn gen_table(db: &LocustDB, name: &str, partitions: usize, partition_size: usize) {
    let _ = block_on(db.gen_table(
        locustdb::colgen::GenTable {
            name: name.to_string(),
            partitions,
            partition_size,
            columns: vec![
                ("total_amount".to_string(),
                 locustdb::colgen::int_uniform(-1000, 50_000)),
                ("pickup_datetime".to_string(),
                 locustdb::colgen::splayed(1_200_000_000, 3)),
                ("uniform_u32".to_string(),
                 locustdb::colgen::int_uniform(0, u32::MAX.into())),
                ("trip_id".to_string(),
                 locustdb::colgen::incrementing_int()),
                ("passenger_count".to_string(),
                 locustdb::colgen::int_weighted(
                     vec![0, 1, 2, 4, 5, 6, 7, 8, 9, 208],
                     vec![4.0, 1000.0, 200.0, 60.0, 30.0, 95.0, 34.0, 1.0, 1.0, 0.001],
                 )),
                ("vendor_id".to_string(),
                 locustdb::colgen::string_weighted(
                     ["1", "2", "3", "CMT", "DDS", "VTS"].into_iter().map(|s| s.to_string()).collect(),
                     vec![195.0, 260.0, 0.006, 493.0, 142.0, 503.0],
                 )),
                ("reducible1".to_string(),
                 locustdb::colgen::int_weighted(
                     (1..4000).step_by(67).collect(),
                     vec![1.0; 3998 / 67 + 1],
                 )),
                ("reducible2".to_string(),
                 locustdb::colgen::int_weighted(
                     (1..4000).step_by(67).collect(),
                     vec![1.0; 3998 / 67 + 1],
                 )),
                ("cab_type".to_string(),
                 locustdb::colgen::string_markov_chain(
                     vec!["green".to_string(), "yellow".to_string()],
                     vec![
                         vec![1.0, 0.0],
                         vec![0.0, 1.0],
                     ],
                 )),
            ],
        }
    ));
}

fn bench_query(b: &mut test::Bencher, query_str: &str) {
    let locustdb = db();
    b.iter(|| {
        let query = locustdb.run_query(query_str, false, vec![]);
        block_on(query)
    });
}

#[bench]
fn count_by_passenger_count_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(1) from test;");
}

#[bench]
fn count_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(1) from trips_e8;");
}

#[bench]
fn sum_total_amt_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, sum(total_amount) from trips_e8;");
}

#[bench]
fn sum_total_amt_by_passenger_count_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, sum(total_amount) from test;");
}

#[bench]
fn select_passenger_count_sparse_filter(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime) from trips_e8 where (passenger_count = 9) and (to_year(pickup_datetime) = 2014);");
}

#[bench]
fn select_passenger_count_sparse_filter_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime) from test where (passenger_count = 9) and (to_year(pickup_datetime) = 2014);");
}

#[bench]
fn count_by_vendor_id_and_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select vendor_id, passenger_count, count(1) from trips_e8;");
}

#[bench]
fn count_by_vendor_id_and_passenger_count_old(b: &mut test::Bencher) {
    bench_query(b, "select vendor_id, passenger_count, count(1) from test;");
}

#[bench]
fn q1_count_cab_type(b: &mut test::Bencher) {
    bench_query(b, "select cab_type, count(0) from trips_e8;");
}

#[bench]
fn q1_count_cab_type_old(b: &mut test::Bencher) {
    bench_query(b, "select cab_type, count(0) from test;");
}

#[bench]
fn q2_avg_total_amount_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(0), sum(total_amount) from trips_e8;");
}

#[bench]
fn q2_avg_total_amount_by_passenger_count_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(0), sum(total_amount) from test;");
}

#[bench]
fn q3_count_by_passenger_count_pickup_year(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), count(0) from trips_e8;");
}

#[bench]
fn q3_count_by_passenger_count_pickup_year_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), count(0) from test;");
}

#[bench]
fn q4_count_by_passenger_count_pickup_year_trip_distance(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), total_amount / 1000, count(0) from trips_e8;");
}

#[bench]
fn q4_count_by_passenger_count_pickup_year_trip_distance_old(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from test;");
}

#[bench]
fn q5_sparse_filter(b: &mut test::Bencher) {
    // select trip_id from test where (passenger_count = 5) AND (vendor_id = "CMT") AND (total_amount < 500) AND (cab_type = "green") limit 100;
    bench_query(b, "select trip_id from trips_e8 where (passenger_count = 0) AND (vendor_id = \"DDS\") AND (total_amount < 500) AND (cab_type = \"green\") limit 100;");
}

#[bench]
fn q5_sparse_filter_old(b: &mut test::Bencher) {
    // select trip_id from test where (passenger_count = 5) AND (vendor_id = "CMT") AND (total_amount < 500) AND (store_and_fwd_flag = "1") limit 100;
    bench_query(b, "select trip_id from test where (passenger_count = 5) AND (vendor_id = \"CMT\") AND (total_amount < 500) AND (store_and_fwd_flag = \"1\") limit 100;");
}

#[bench]
fn q6_top_n(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, uniform_u32, total_amount FROM trips_e8 ORDER BY total_amount DESC LIMIT 100;");
}

#[bench]
fn q6_top_n_old(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, trip_distance, total_amount FROM test ORDER BY total_amount DESC LIMIT 100;");
}

#[bench]
fn q7_hashmap_grouping_old(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, pickup_puma, dropoff_puma, count(0) FROM test;");
}

#[bench]
fn q7_hashmap_grouping(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, reducible1, reducible2, count(0) FROM trips_e8;");
}

#[bench]
#[ignore]
fn q8_group_by_trip_id_old(b: &mut test::Bencher) {
    bench_query(b, "SELECT trip_id / 5, sum(total_amount) FROM test;");
}

#[bench]
fn q8_group_by_trip_id(b: &mut test::Bencher) {
    bench_query(b, "SELECT trip_id, sum(total_amount) FROM trips_e6;");
}
