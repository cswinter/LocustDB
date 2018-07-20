extern crate env_logger;
extern crate futures_executor;
extern crate locustdb;
extern crate log;

use futures_executor::block_on;
use locustdb::*;
use locustdb::nyc_taxi_data;
use locustdb::Value;
use std::cmp::min;

fn test_query(query: &str, expected_rows: &[Vec<Value>]) {
    let _ =
        env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.load_csv(
        IngestFile::new("test_data/tiny.csv", "default")
            .with_chunk_size(40)));
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_ec(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.load_csv(
        IngestFile::new("test_data/edge_cases.csv", "default")
            .with_chunk_size(3)));
    let result = block_on(locustdb.run_query(query, false, vec![])).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_nyc(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let load = block_on(locustdb.load_csv(
        nyc_taxi_data::ingest_file("test_data/nyc-taxi.csv.gz", "default")
            .with_chunk_size(999)));
    load.unwrap().ok();
    let result = block_on(locustdb.run_query(query, false, vec![])).unwrap();
    let actual_rows = result.0.unwrap().rows;
    assert_eq!(&actual_rows[..min(5, actual_rows.len())], expected_rows);
}

#[test]
fn test_select_string() {
    test_query(
        "select first_name from default order by first_name limit 2;",
        &[
            vec!["Adam".into()],
            vec!["Adam".into()]
        ],
    )
}

#[test]
fn test_select_integer() {
    test_query(
        "select num from default order by num limit 2;",
        &[
            vec![0.into()],
            vec![0.into()]
        ],
    )
}

#[test]
fn test_sort_string() {
    test_query(
        "select first_name from default order by first_name limit 2;",
        &[
            vec!["Adam".into()],
            vec!["Adam".into()],
        ],
    )
}

#[test]
fn test_sort_string_desc() {
    test_query(
        &"select first_name from default order by first_name desc limit 2;",
        &[
            vec!["Willie".into()],
            vec!["William".into()],
        ],
    )
}

#[test]
fn group_by_integer_filter_integer_lt() {
    test_query(
        "select num, count(1) from default where num < 8;",
        &[
            vec![0.into(), 8.into()],
            vec![1.into(), 49.into()],
            vec![2.into(), 24.into()],
            vec![3.into(), 11.into()],
            vec![4.into(), 5.into()],
            vec![5.into(), 2.into()],
        ],
    )
}

#[test]
fn lt_filter_on_offset_encoded_column() {
    test_query_ec(
        "select u8_offset_encoded from default where u8_offset_encoded < 257;",
        &[vec![256.into()]],
    )
}

#[test]
fn group_by_string_filter_string_eq() {
    test_query(
        "select first_name, count(1) from default where first_name = \"Adam\";",
        &[vec!["Adam".into(), 2.into()]],
    )
}

#[test]
fn test_and_or() {
    test_query(
        "select first_name, last_name from default where ((first_name = \"Adam\") OR (first_name = \"Catherine\")) AND (num = 3);",
        &[vec!["Adam".into(), "Crawford".into()]],
    )
}

#[test]
fn test_sum() {
    test_query(
        "select tld, sum(num) from default where (tld = \"name\");",
        &[vec!["name".into(), 26.into()]],
    )
}

#[test]
fn test_sum_2() {
    test_query_ec(
        "select non_dense_ints, sum(u8_offset_encoded) from default;",
        &[
            vec![0.into(), 756.into()],
            vec![1.into(), 689.into()],
            vec![2.into(), 1112.into()],
            vec![3.into(), 759.into()],
            vec![4.into(), 275.into()],
        ],
    )
}

#[test]
fn test_multiple_group_by() {
    test_query(
        "select first_name, num, count(1) from default where num = 5;",
        &[
            vec!["Christina".into(), 5.into(), 1.into()],
            vec!["Joshua".into(), 5.into(), 1.into()],
        ],
    )
}

#[test]
fn test_multiple_group_by_2() {
    test_query_ec(
        "select enum, non_dense_ints, count(1) from default;",
        &[
            vec!["aa".into(), 0.into(), 2.into()],
            vec!["aa".into(), 1.into(), 1.into()],
            vec!["aa".into(), 2.into(), 1.into()],
            vec!["aa".into(), 3.into(), 1.into()],
            vec!["bb".into(), 1.into(), 1.into()],
            vec!["bb".into(), 3.into(), 1.into()],
            vec!["bb".into(), 4.into(), 1.into()],
            vec!["cc".into(), 2.into(), 2.into()],
        ],
    )
}

#[test]
fn test_division() {
    test_query(
        "select num / 10, count(1) from default;",
        &[
            vec![0.into(), 100.into()],
        ],
    )
}

#[test]
fn test_not_equals() {
    use Value::*;
    test_query(
        "select num, count(1) from default where num <> 0;",
        &[
            vec![Int(1), Int(49)],
            vec![Int(2), Int(24)],
            vec![Int(3), Int(11)],
            vec![Int(4), Int(5)],
            vec![Int(5), Int(2)],
            vec![Int(8), Int(1)]
        ],
    )
}


// Tests are run in alphabetical order (why ;_;) and these take a few seconds to run, so prepend z to run last
#[test]
fn z_test_count_by_passenger_count_pickup_year_trip_distance() {
    use Value::*;
    test_query_nyc(
        "select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from default;",
        &[
            vec![Int(0), Int(2013), Int(0), Int(2)],
            vec![Int(0), Int(2013), Int(2), Int(1)],
            vec![Int(1), Int(2013), Int(0), Int(1965)],
            vec![Int(1), Int(2013), Int(1), Int(1167)],
            vec![Int(1), Int(2013), Int(2), Int(824)]
        ],
    )
}

#[test]
fn z_test_top_n() {
    use Value::*;
    test_query_nyc(
        "SELECT passenger_count, trip_distance, total_amount FROM default ORDER BY total_amount DESC LIMIT 100;",
        &[
            vec![Int(2), Int(0), Int(357050)],
            vec![Int(1), Int(0), Int(326000)],
            vec![Int(1), Int(0), Int(68010)],
            vec![Int(1), Int(0), Int(66858)],
            vec![Int(1), Int(0), Int(61950)],
        ],
    )
}

#[test]
fn z_test_sparse_filter() {
    test_query_nyc(
        "select trip_id from default where (passenger_count = 5) AND (vendor_id = \"CMT\") AND (total_amount < 500) AND (store_and_fwd_flag = \"1\") limit 100;",
        &[],
    )
}

#[test]
fn z_test_group_by_trip_id() {
    use Value::*;
    test_query_nyc(
        "SELECT trip_id / 5, sum(total_amount) FROM default;",
        &[
            vec![Int(0), Int(10160)],
            vec![Int(1), Int(3694)],
            vec![Int(2), Int(1758)],
            vec![Int(3), Int(2740)],
            vec![Int(4), Int(377955)]
        ],
    )
}
