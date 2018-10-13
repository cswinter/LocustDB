extern crate env_logger;
extern crate futures_executor;
extern crate locustdb;
extern crate log;
extern crate tempdir;

use futures_executor::block_on;
use locustdb::*;
use locustdb::Value;
use locustdb::nyc_taxi_data;
use std::cmp::min;

fn test_query(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/tiny.csv", "default")
            .with_partition_size(40)));
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_ec(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    #[allow(unused_mut)]
    let mut opts = Options::default();
    let locustdb = LocustDB::new(&opts);
    let _ = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/edge_cases.csv", "default")
            .with_partition_size(3)));
    let result = block_on(locustdb.run_query(query, false, vec![])).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_nyc(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    #[allow(unused_mut)]
    let mut opts = Options::default();
    // opts.threads = 1;
    let locustdb = LocustDB::new(&opts);
    let load = block_on(locustdb.load_csv(
        nyc_taxi_data::ingest_reduced_file("test_data/nyc-taxi.csv.gz", "default")
            .with_partition_size(999)));
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
fn test_select_twice() {
    test_query(
        "select first_name, first_name from default order by first_name limit 2;",
        &[
            vec!["Adam".into(), "Adam".into()],
            vec!["Adam".into(), "Adam".into()]
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
fn test_group_by_limit() {
    use Value::*;
    test_query_ec(
        "select enum, enum, count(0) from default limit 2;",
        &[
            vec![Str("aa".to_string()), Str("aa".to_string()), Int(5)],
            vec![Str("bb".to_string()), Str("bb".to_string()), Int(3)],
        ],
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
fn group_by_col_and_aliasing_const_cols() {
    use Value::*;
    test_query_ec(
        "select enum, constant0, constant0_2, count(0) from default;",
        &[
            vec![Str("aa".to_string()), Int(0), Int(0), Int(5)],
            vec![Str("bb".to_string()), Int(0), Int(0), Int(3)],
            vec![Str("cc".to_string()), Int(0), Int(0), Int(2)]
        ],
    )
}

#[test]
fn test_string_packed_column() {
    test_query_ec(
        "select string_packed from default where string_packed = \"xyz\";",
        &[vec!["xyz".into()]],
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
        "select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from default limit 10000;",
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

#[test]
fn test_gen_table() {
    use Value::*;
    let _ = env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.gen_table(
        locustdb::colgen::GenTable {
            name: "test".to_string(),
            partitions: 8,
            partition_size: 2 << 14,
            columns: vec![
                ("yum".to_string(), locustdb::colgen::string_markov_chain(
                    vec!["Walnut".to_string(), "Cashew".to_string(), "Hazelnut".to_string()],
                    vec![
                        vec![0., 0.5, 0.5],
                        vec![0.1, 0.5, 0.4],
                        vec![0.1, 0.9, 0.],
                    ],
                ))
            ],
        }
    ));
    let query = "SELECT yum, count(1) FROM test;";
    let expected_rows = vec![
        [Str("Cashew".to_string()), Int(162035)],
        [Str("Hazelnut".to_string()), Int(76401)],
        [Str("Walnut".to_string()), Int(23708)]
    ];
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

#[test]
fn test_column_with_null_partitions() {
    use Value::*;
    let _ = env_logger::try_init();
    let mut opts = locustdb::Options::default();
    opts.threads = 1;
    let locustdb = LocustDB::new(&opts);
    let _ = block_on(locustdb.gen_table(
        locustdb::colgen::GenTable {
            name: "test".to_string(),
            partitions: 5,
            partition_size: 1,
            columns: vec![
                ("partition_sparse".to_string(),
                 locustdb::colgen::partition_sparse(
                     0.5,
                     locustdb::colgen::string_markov_chain(
                         vec!["A".to_string(), "B".to_string()],
                         vec![
                             vec![0.3, 0.7],
                             vec![0.3, 0.7],
                         ],
                     ))
                )
            ],
        }
    ));
    let query = "SELECT partition_sparse FROM test;";
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap().0.unwrap();
    assert_eq!(result.rows.iter().filter(|&x| x == &[Null]).count(), 2);
    assert_eq!(result.rows.iter().filter(|&x| x == &[Str("A".to_string())]).count(), 1);
    assert_eq!(result.rows.iter().filter(|&x| x == &[Str("B".to_string())]).count(), 2);
}

#[cfg(feature = "enable_rocksdb")]
#[test]
fn test_restore_from_disk() {
    use std::{thread, time};
    use tempdir::TempDir;
    let _ = env_logger::try_init();
    let tmp_dir = TempDir::new("rocks").unwrap();
    let mut opts = Options::default();
    opts.db_path = Some(tmp_dir.path().to_str().unwrap().to_string());
    {
        let locustdb = LocustDB::new(&opts);
        let load = block_on(locustdb.load_csv(
            nyc_taxi_data::ingest_reduced_file("test_data/nyc-taxi.csv.gz", "default")
                .with_partition_size(999)));
        load.unwrap().ok();
    }
    // Dropping the LocustDB object will cause all threads to be stopped
    // This eventually drops RocksDB and relinquish the file lock, however this happens asynchronously
    // TODO(clemens): make drop better?
    thread::sleep(time::Duration::from_millis(2000));
    let locustdb = LocustDB::new(&opts);
    let query = "select passenger_count, to_year(pickup_datetime), trip_distance / 1000, count(0) from default;";
    let result = block_on(locustdb.run_query(query, false, vec![])).unwrap();
    let actual_rows = result.0.unwrap().rows;
    use Value::*;
    assert_eq!(&actual_rows[..min(5, actual_rows.len())], &[
        vec![Int(0), Int(2013), Int(0), Int(2)],
        vec![Int(0), Int(2013), Int(2), Int(1)],
        vec![Int(1), Int(2013), Int(0), Int(1965)],
        vec![Int(1), Int(2013), Int(1), Int(1167)],
        vec![Int(1), Int(2013), Int(2), Int(824)]
    ]);
}

