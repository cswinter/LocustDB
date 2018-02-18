#![feature(test)]
extern crate ruba;
extern crate test;

use ruba::mem_store::csv_loader::ingest_file;
use ruba::parser::parse_query;


#[bench]
fn bench_ingest_2mb_1_chunk(b: &mut test::Bencher) {
    b.iter(|| {
        ingest_file("test_data/small.csv", 4000)
    });
}

#[bench]
fn bench_ingest_2mb_10_chunks(b: &mut test::Bencher) {
    b.iter(|| {
        ingest_file("test_data/small.csv", 400)
    });
}

#[bench]
fn bench_sum_4000(b: &mut test::Bencher) {
    let data = ( 0..4000).collect::<Vec<_>>();
    b.iter(|| {
        let mut sum = 0;
        for i in 0..4000 {
            sum = sum + data[i]
        }
        test::black_box(sum)
    });
}

#[bench]
fn bench_2mb_select_name(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select first_name from test limit 1;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_select_name_num(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select first_name, num from test limit 1;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_filter_select(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select first_name from test where num < 2 limit 2;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

// #[bench]
fn bench_2mb_count_all(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select count(first_name) from test;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_group_count(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select first_name, count(1) from test limit 2;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_group_filter_count(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 4000);
    let query = parse_query("select num, count(1) from test where num < 2;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}
