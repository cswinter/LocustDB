extern crate ruba;
extern crate futures;
#[macro_use]
extern crate log;
extern crate env_logger;

use ruba::*;
use futures::executor::block_on;


fn test_query(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let ruba = Ruba::memory_only();
    let _ = block_on(ruba.load_csv("test_data/tiny.csv", "default", 40, vec![]));
    let result = block_on(ruba.run_query(query)).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_ec(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let ruba = Ruba::memory_only();
    let _ = block_on(ruba.load_csv("test_data/edge_cases.csv", "default", 2, vec![]));
    let result = block_on(ruba.run_query(query)).unwrap();
    assert_eq!(result.0.unwrap().rows, expected_rows);
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

/*
#[test]
fn test_sort_string_desc() {
    test_query(
        &"select first_name from default order by first_name desc limit 2;",
        vec![vec!["Willie".into()],
             vec!["William".into()],
        ],
    )
}*/

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
            vec![5.into(), 2.into()]],
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
