#![feature(test)]
extern crate ruba;
extern crate test;
extern crate futures;

use ruba::Ruba;
use futures::executor::block_on;
use futures::FutureExt;


// #[bench]
fn bench_ingest_2mb_once(b: &mut test::Bencher) {
    let ruba = Ruba::memory_only();
    b.iter(|| {
        let load_finished = ruba.load_csv(&"test_data/small.csv", &"test", 4000);
        let _ = block_on(load_finished);
    });
}

// #[bench]
fn bench_ingest_2mb_twice(b: &mut test::Bencher) {
    let ruba = Ruba::memory_only();
    b.iter(|| {
        let f1 = ruba.load_csv(&"test_data/small.csv", &"test", 4000);
        let f2 = f1.join(ruba.load_csv(&"test_data/small.csv", &"test", 4000));
        let _ = block_on(f2);
    });
}

#[bench]
fn bench_sum_4000(b: &mut test::Bencher) {
    let data = (0..4000).collect::<Vec<_>>();
    b.iter(|| {
        let mut sum = 0;
        for i in 0..4000 {
            sum = sum + data[i]
        }
        test::black_box(sum)
    });
}

fn bench_query_2mb(b: &mut test::Bencher, query_str: &str) {
    let ruba = Ruba::memory_only();
    let load = ruba.load_csv(&"test_data/small.csv", &"test", 4000);
    let _ = block_on(load);
    b.iter(|| {
        let query = ruba.run_query(query_str);
        let _ = block_on(query);
    });
}

fn bench_query_gtd_1m(b: &mut test::Bencher, query_str: &str) {
    let ruba = Ruba::memory_only();
    let load = ruba.load_csv(&"test_data/green_tripdata_2017-06.csv", &"test", 16_384);
    let _ = block_on(load);
    b.iter(|| {
        let query = ruba.run_query(query_str);
        let _ = block_on(query);
    });
}

#[bench]
fn bench_2mb_select_name(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name from test limit 1;");
}

#[bench]
fn bench_2mb_select_name_num(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name, num from test limit 1;");
}

#[bench]
fn bench_2mb_filter_select(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name from test where num < 2 limit 2;");
}

#[bench]
fn bench_2mb_string_equality(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name from test where first_name = \"Adam\" limit 2;");
}

// #[bench]
fn bench_2mb_count_all(b: &mut test::Bencher) {
    bench_query_2mb(b, "select count(first_name) from test;");
}

#[bench]
fn bench_2mb_group_count(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name, count(1) from test limit 2;");
}

#[bench]
fn bench_2mb_group_filter_count(b: &mut test::Bencher) {
    bench_query_2mb(b, "select num, count(1) from test where num < 2;");
}

#[bench]
fn bench_2mb_sort_strings(b: &mut test::Bencher) {
    bench_query_2mb(b, "select first_name from test order by first_name limit 1;");
}

#[bench]
fn bench_2mb_sort_integers(b: &mut test::Bencher) {
    bench_query_2mb(b, "select num from test order by num limit 1;");
}

#[bench]
fn gt_1m_select_passenger_count_count(b: &mut test::Bencher) {
    bench_query_gtd_1m(b, "select passenger_count, count(1) from test;");
}

#[bench]
fn gt_1m_int_sort(b: &mut test::Bencher) {
    bench_query_gtd_1m(b, "select total_amount from test order by total_amount limit 10000;");
}
