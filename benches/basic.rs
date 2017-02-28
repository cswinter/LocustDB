#![feature(test)]
extern crate ruba;
extern crate test;

use ruba::ingest_file;
use ruba::parser::parse_query;
use ruba::query_engine;

#[bench]
fn bench_ingest_2mb_1_chunk(b: &mut test::Bencher) {
    b.iter(|| {
        ingest_file("test_data/small.csv", 0)
    });
}

#[bench]
fn bench_ingest_2mb_10_chunks(b: &mut test::Bencher) {
    b.iter(|| {
        ingest_file("test_data/small.csv", 400)
    });
}

#[bench]
fn bench_2mb_count_all(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 0);
    let query = parse_query("select count(first_name);".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_group_count(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 0);
    let query = parse_query("select num, count(1);".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}

#[bench]
fn bench_2mb_group_filter_count(b: &mut test::Bencher) {
    let batches = ingest_file("test_data/small.csv", 0);
    let query = parse_query("select num, count(1) where num > 3;".as_bytes()).to_result().unwrap();
    b.iter(|| {
        let mut compiled_query = query.compile(&batches);
        test::black_box(compiled_query.run());
    });
}
