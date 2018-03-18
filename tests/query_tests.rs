extern crate ruba;
extern crate futures;

use ruba::*;
use futures::executor::block_on;


fn test_query(query: &str, expected_rows: Vec<Vec<Value>>) {
    let ruba = Ruba::memory_only();
    let _= block_on(ruba.load_csv(&"test_data/tiny.csv", &"default", 4000));
    let result = block_on(ruba.run_query(query)).unwrap();
    assert_eq!(result.rows, expected_rows);
}

#[test]
fn test_select_string() {
    test_query(
        &"select first_name from default limit 2;",
        vec![vec!["Victor".into()],
             vec!["Catherine".into()]
        ],
    )
}

#[test]
fn test_select_string_integer() {
    test_query(
        &"select first_name, num from default limit 2;",
        vec![vec!["Victor".into(), 1.into()],
             vec!["Catherine".into(), 1.into()]
        ],
    )
}

#[test]
fn test_sort_string() {
    test_query(
        &"select first_name from default order by first_name limit 2;",
        vec![vec!["Adam".into()],
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
        &"select num, count(1) from default where num < 8;",
        vec![vec![0.into(), 8.into()],
             vec![1.into(), 49.into()],
             vec![2.into(), 24.into()],
             vec![3.into(), 11.into()],
             vec![4.into(), 5.into()],
             vec![5.into(), 2.into()]],
    )
}

#[test]
fn group_by_string_filter_string_eq() {
    test_query(
        &"select first_name, count(1) from default where first_name = \"Adam\";",
        vec![vec!["Adam".into(), 2.into()]],
    )
}

#[test]
fn test_and_or() {
    test_query(
        &"select first_name, last_name from default where ((first_name = \"Adam\") OR (first_name = \"Catherine\")) AND (last_name = \"Cox\");",
        vec![vec!["Adam".into(), "Cox".into()]],
    )
}
