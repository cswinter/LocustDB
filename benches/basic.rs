#![feature(test)]
extern crate futures_executor;
extern crate locustdb;
extern crate test;

use futures_executor::block_on;
use locustdb::{LocustDB, Options};
use std::env;
use std::u32;

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

                eprintln!("Synthesizing tables");
                gen_table(&locustdb, "trips_e8", 100, 1 << 20);
                gen_table(&locustdb, "trips_e7", 80, 1 << 17);
                gen_table(&locustdb, "trips_e6", 64, 1 << 14);
                eprintln!("Done");

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
                     ["1", "2", "3", "CMT", "DDS", "VTS"].iter().map(|s| s.to_string()).collect(),
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
fn count_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(1) from trips_e8;");
}

#[bench]
fn sum_total_amt_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, sum(total_amount) from trips_e8;");
}

#[bench]
fn count_by_vendor_id_and_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select vendor_id, passenger_count, count(1) from trips_e8;");
}

#[bench]
fn count_cab_type(b: &mut test::Bencher) {
    bench_query(b, "select cab_type, count(0) from trips_e8;");
}

#[bench]
fn avg_total_amount_by_passenger_count(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, count(0), sum(total_amount) from trips_e8;");
}

// TODO(#102): 50% performance regression after query planner refactor. not elimination to_year anymore?
#[bench]
fn count_by_passenger_count_pickup_year_trip_distance(b: &mut test::Bencher) {
    bench_query(b, "select passenger_count, to_year(pickup_datetime), total_amount / 1000, count(0) from trips_e7;");
}

#[bench]
fn sparse_filter(b: &mut test::Bencher) {
    // select trip_id from test where (passenger_count = 0) AND (vendor_id = "DDS") AND (total_amount < 500) AND (cab_type = "green") limit 100;
    bench_query(b, "select trip_id from trips_e8 where (passenger_count = 0) AND (vendor_id = 'DDS') AND (total_amount < 500) AND (cab_type = 'green') limit 100;");
}

#[bench]
fn top_n(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, uniform_u32, total_amount FROM trips_e8 ORDER BY total_amount DESC LIMIT 100;");
}

#[bench]
fn hashmap_grouping(b: &mut test::Bencher) {
    bench_query(b, "SELECT passenger_count, reducible1, reducible2, count(0) FROM trips_e7;");
}

#[bench]
fn group_by_trip_id(b: &mut test::Bencher) {
    bench_query(b, "SELECT trip_id / 5, sum(total_amount) FROM trips_e6;");
}
