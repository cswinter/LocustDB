extern crate env_logger;
extern crate futures_executor;
extern crate locustdb;
extern crate log;
extern crate tempdir;

use futures_executor::block_on;

use locustdb::*;
use locustdb::nyc_taxi_data;
use locustdb::Value;
use std::cmp::min;
use std::env;
use value_syntax::*;

fn test_query(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    let mut opts = Options::default();
    if env::var("DEBUG_TESTS").is_ok() {
        opts.threads = 1;
    }
    let locustdb = LocustDB::new(&opts);
    let _ = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/tiny.csv", "default")
            .with_partition_size(40)));
    let result = if env::var("DEBUG_TESTS").is_ok() {
        block_on(locustdb.run_query(query, true, vec![0, 1, 2])).unwrap()
    } else {
        block_on(locustdb.run_query(query, true, vec![])).unwrap()
    };
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_ec(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    #[allow(unused_mut)]
    let mut opts = Options::default();
    if env::var("DEBUG_TESTS").is_ok() || true {
        opts.threads = 1;
    }
    let locustdb = LocustDB::new(&opts);
    let _ = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/edge_cases.csv", "default")
            .with_partition_size(3)
            .allow_nulls_all_columns()));
    let result = if env::var("DEBUG_TESTS").is_ok() {
        block_on(locustdb.run_query(query, false, vec![0, 1, 2, 3])).unwrap()
    } else {
        block_on(locustdb.run_query(query, false, vec![])).unwrap()
    };
    assert_eq!(result.0.unwrap().rows, expected_rows);
}

fn test_query_ec_err(query: &str, _expected_err: QueryError) {
    let _ = env_logger::try_init();
    #[allow(unused_mut)]
    let mut opts = Options::default();
    if env::var("DEBUG_TESTS").is_ok() || true {
        opts.threads = 1;
    }
    let locustdb = LocustDB::new(&opts);
    let _ = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/edge_cases.csv", "default")
            .with_partition_size(3)
            .allow_nulls_all_columns()));
    let result = if env::var("DEBUG_TESTS").is_ok() {
        block_on(locustdb.run_query(query, false, vec![0, 1, 2, 3])).unwrap()
    } else {
        block_on(locustdb.run_query(query, false, vec![])).unwrap()
    };
    assert!(result.0.is_err());
}

fn test_query_nyc(query: &str, expected_rows: &[Vec<Value>]) {
    let _ = env_logger::try_init();
    #[allow(unused_mut)]
    let mut opts = Options::default();
    if env::var("DEBUG_TESTS").is_ok() {
        opts.threads = 1;
    }
    let locustdb = LocustDB::new(&opts);
    let load = block_on(locustdb.load_csv(
        LoadOptions::new("test_data/nyc-taxi.csv.gz", "default")
            .with_schema(&nyc_taxi_data::reduced_nyc_schema())
            .with_partition_size(999)));
    load.unwrap().ok();
    let result = block_on(locustdb.run_query(query, false, vec![])).unwrap();
    let actual_rows = result.0.unwrap().rows;
    assert_eq!(&actual_rows[..min(expected_rows.len(), actual_rows.len())], expected_rows);
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
fn test_select_nullable_integer() {
    test_query_ec(
        "SELECT nullable_int FROM default ORDER BY id DESC;",
        &[
            vec![Int(13)],
            vec![Null],
            vec![Int(20)],
            vec![Null],
            vec![Null],
            vec![Int(10)],
            vec![Null],
            vec![Null],
            vec![Int(-40)],
            vec![Int(-1)],
        ],
    )
}

#[test]
fn test_select_nullable_string() {
    test_query_ec(
        "SELECT country FROM default ORDER BY id DESC;",
        &[
            vec![Str("Germany")],
            vec![Null],
            vec![Null],
            vec![Str("Turkey")],
            vec![Null],
            vec![Str("France")],
            vec![Null],
            vec![Str("France")],
            vec![Str("USA")],
            vec![Str("Germany")]
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
fn test_regex() {
    test_query(
        "SELECT first_name FROM default WHERE regex(first_name, '^C.+h.a');",
        &[vec![Str("Cynthia")]],
    );
}

#[test]
fn test_not_regex() {
    test_query(
        "SELECT first_name FROM default WHERE not(regex(first_name, '^C.+h.a')) LIMIT 1;",
        &[vec![Str("Victor")]],
    );
}

#[test]
fn test_like() {
    test_query(
        "SELECT first_name FROM default WHERE first_name LIKE 'C%h_a';",
        &[vec![Str("Cynthia")]],
    );
}

#[test]
fn test_not_like() {
    test_query(
        "SELECT first_name FROM default WHERE first_name NOT LIKE 'C%h_a' LIMIT 1;",
        &[vec![Str("Kathryn")]],
    );
}

#[test]
fn test_like_mismatch() {
    test_query(
        // shouldn't match Joshua etc.
        "SELECT first_name FROM default WHERE first_name LIKE '%hu';",
        &[],
    );
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

#[test]
fn test_not_equals_2() {
    use Value::*;
    test_query(
        "select num, count(1) from default where not(num = 0);",
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

#[test]
fn test_order_by_aggregate() {
    test_query_nyc(
        "SELECT passenger_count, count(0) FROM default ORDER BY count(0) DESC LIMIT 10;",
        &[
            vec![Int(1), Int(6016)],
            vec![Int(5), Int(2197)],
            vec![Int(2), Int(1103)],
            vec![Int(3), Int(383)],
            vec![Int(6), Int(222)],
            vec![Int(4), Int(76)],
            vec![Int(0), Int(3)],
        ],
    )
}


#[test]
fn test_groupless_aggregate() {
    test_query_nyc(
        "SELECT count(0) FROM default",
        &[vec![Int(10000)]],
    );
    test_query_nyc(
        "SELECT sum(total_amount), count(0) FROM default",
        &[vec![Int(16197630), Int(10000)]],
    );
    test_query_nyc(
         "SELECT count(0) FROM default WHERE NOT passenger_count <> 1;",
        &[vec![Int(6016)]],
    );
}

#[test]
fn test_order_by_grouping() {
    test_query_nyc(
        "SELECT passenger_count, count(0) FROM default ORDER BY passenger_count DESC LIMIT 10;",
        &[
            vec![Int(6), Int(222)],
            vec![Int(5), Int(2197)],
            vec![Int(4), Int(76)],
            vec![Int(3), Int(383)],
            vec![Int(2), Int(1103)],
            vec![Int(1), Int(6016)],
            vec![Int(0), Int(3)],
        ],
    )
}

#[test]
fn test_composite_aggregate() {
    test_query_nyc(
        "select passenger_count, count(0)/10, sum(total_amount)/count(0) from default limit 10;",
        &[
            vec![Int(0), Int(0), Int(1833)],
            vec![Int(1), Int(601), Int(1580)],
            vec![Int(2), Int(110), Int(2073)],
            vec![Int(3), Int(38), Int(1677)],
            vec![Int(4), Int(7), Int(2194)],
        ],
    )
}

#[test]
fn test_average() {
    test_query_ec(
        "select avg(nullable_int * nullable_int2) from default;",
        &[
            vec![Int(624)]
        ],
    )
}

#[test]
fn test_count_by_passenger_count_pickup_year_trip_distance() {
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
fn test_min_max() {
    test_query_nyc(
        "SELECT passenger_count, max(total_amount), min(total_amount) FROM default;",
        &[
            vec![Int(0), Int(5200), Int(150)],
            vec![Int(1), Int(326000), Int(0)],
            vec![Int(2), Int(357050), Int(0)],
            vec![Int(3), Int(52750), Int(150)],
            vec![Int(4), Int(44550), Int(200)]
        ],
    )
}

#[test]
fn test_top_n() {
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
fn test_sparse_filter() {
    test_query_nyc(
        "select trip_id from default where (passenger_count = 5) AND (vendor_id = \"CMT\") AND (total_amount < 500) AND (store_and_fwd_flag = \"1\") limit 100;",
        &[],
    )
}

#[test]
fn test_addition() {
    test_query_ec(
        "SELECT u8_offset_encoded + negative FROM default ORDER BY id LIMIT 5;",
        &[vec![Int(57)], vec![Int(297)], vec![Int(159)], vec![Int(291)], vec![Int(4306)]],
    );
    // TODO(sqlparser-rs#29): s/2/-2/ once supported parser
    test_query_ec(
        "SELECT 2 + non_dense_ints FROM default ORDER BY id LIMIT 5;",
        &[vec![Int(2)], vec![Int(4)], vec![Int(5)], vec![Int(3)], vec![Int(6)]],
        // &[vec![Int(-2)], vec![Int(0)], vec![Int(1)], vec![Int(-1)], vec![Int(2)]],
    );
}

#[test]
fn test_numeric_operators() {
    test_query_ec(
        "SELECT (non_dense_ints * negative / (id + 1) - u8_offset_encoded) % (id + 1) FROM default ORDER BY id;",
        &[
            vec![Int(0)],
            vec![Int(-1)],
            vec![Int(-2)],
            vec![Int(-1)],
            vec![Int(4)],
            vec![Int(-2)],
            vec![Int(-2)],
            vec![Int(-7)],
            vec![Int(2)],
            vec![Int(-2)]
        ],
    );
}

#[test]
fn test_comparison_operators() {
    test_query_ec(
        "SELECT u8_offset_encoded, negative FROM default WHERE u8_offset_encoded < negative ORDER BY id;",
        &[
            vec![Int(275), Int(4031)],
            vec![Int(511), Int(4010)],
        ],
    );
    test_query_ec(
        "SELECT non_dense_ints FROM default WHERE non_dense_ints = id ORDER BY id;",
        &[
            vec![Int(0)],
            vec![Int(4)],
        ],
    );
    test_query_ec(
        "SELECT id FROM default WHERE id <> id / 8 + id ORDER BY id;",
        &[
            vec![Int(8)],
            vec![Int(9)],
        ],
    );
    test_query_ec(
        "SELECT id FROM default WHERE id <= 4 AND non_dense_ints >= 3 AND enum > string_packed;",
        &[
            vec![Int(4)],
        ],
    );
}

#[test]
fn test_group_by_trip_id() {
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
fn test_string_length() {
    test_query_nyc(
        "SELECT length(pickup_ntaname), pickup_ntaname, COUNT(0)
         FROM default
         ORDER BY length(pickup_ntaname) DESC
         LIMIT 3;",
        &[
            vec![Int(56), Str("Todt Hill-Emerson Hill-Heartland Village-Lighthouse Hill"), Int(1)],
            vec![Int(50), Str("Mariner\'s Harbor-Arlington-Port Ivory-Graniteville"), Int(3)],
            vec![Int(48), Str("DUMBO-Vinegar Hill-Downtown Brooklyn-Boerum Hill"), Int(245)],
        ],
    )
}

#[test]
fn test_group_by_negative_expression() {
    test_query_ec(
        "SELECT negative/100, count(1) FROM default;",
        &[
            vec![Int(-1), Int(4)],
            vec![Int(0), Int(4)],
            vec![Int(40), Int(2)],
        ],
    )
}

#[test]
fn test_order_by_expression() {
    test_query_ec(
        "SELECT negative FROM default ORDER BY negative/100, string_packed;",
        &[
            vec![Int(-120)],
            vec![Int(-130)],
            vec![Int(-100)],
            vec![Int(-199)],
            vec![Int(32)],
            vec![Int(34)],
            vec![Int(39)],
            vec![Int(-40)],
            vec![Int(4031)],
            vec![Int(4010)],
        ],
    )
}

#[test]
fn test_order_by_multiple() {
    test_query_ec(
        "SELECT enum, string_packed
         FROM default
         ORDER BY enum DESC, string_packed;",
        &[
            vec![Str("cc"), Str("asd")],
            vec![Str("cc"), Str("t")],
            vec![Str("bb"), Str("AXY")],
            vec![Str("bb"), Str("azy")],
            vec![Str("bb"), Str("😈")],
            vec![Str("aa"), Str("$sss")],
            vec![Str("aa"), Str("_f")],
            vec![Str("aa"), Str("abc")],
            vec![Str("aa"), Str("axz")],
            vec![Str("aa"), Str("xyz")],
        ],
    )
}

#[test]
fn test_null_aggregators() {
    test_query_ec(
        "SELECT id/5, SUM(nullable_int), COUNT(nullable_int2), MIN(nullable_int), MAX(nullable_int2)
         FROM default
         ORDER BY id/5;",
        &[
            vec![Int(0), Int(-31), Int(3), Int(-40), Int(9)],
            vec![Int(1), Int(33), Int(3), Int(13), Int(14)],
        ],
    );
}

#[test]
fn test_sort_by_nullable() {
    test_query_ec(
        "SELECT nullable_int, nullable_int2, country
         FROM default
         ORDER BY nullable_int, nullable_int2 DESC, country;",
        &[
            vec![Null, Int(6), Null],
            vec![Null, Int(1), Null],
            vec![Null, Int(0), Null],
            vec![Null, Null, Str("France")],
            vec![Null, Null, Str("Turkey")],
            vec![Int(-40), Int(-40), Str("USA")],
            vec![Int(-1), Null, Str("Germany")],
            vec![Int(10), Int(9), Str("France")],
            vec![Int(13), Int(14), Str("Germany")],
            vec![Int(20), Null, Null],
        ],
    );
    test_query_ec(
        "SELECT nullable_int2, country
         FROM default
         ORDER BY nullable_int2, country DESC;",
        &[
            vec![Null, Str("Turkey")],
            vec![Null, Str("Germany")],
            vec![Null, Str("France")],
            vec![Null, Null],
            vec![Int(-40), Str("USA")],
            vec![Int(0), Null],
            vec![Int(1), Null],
            vec![Int(6), Null],
            vec![Int(9), Str("France")],
            vec![Int(14), Str("Germany")],
        ],
    );
}

#[test]
fn test_group_by_nullable() {
    test_query_ec(
        "SELECT country, COUNT(0)
         FROM default;",
        &[
            vec![Null, Int(4)],
            vec![Str("France"), Int(2)],
            vec![Str("Germany"), Int(2)],
            vec![Str("Turkey"), Int(1)],
            vec![Str("USA"), Int(1)]
        ],
    );
    test_query_ec(
        "SELECT nullable_int, COUNT(0)
         FROM default;",
        &[
            vec![Null, Int(5)],
            vec![Int(-40), Int(1)],
            vec![Int(-1), Int(1)],
            vec![Int(10), Int(1)],
            vec![Int(13), Int(1)],
            vec![Int(20), Int(1)]
        ],
    );
    test_query_ec(
        "SELECT nullable_int2, country, COUNT(0)
         FROM default;",
        &[
            vec![Null, Null, Int(1)],
            vec![Null, Str("France"), Int(1)],
            vec![Null, Str("Germany"), Int(1)],
            vec![Null, Str("Turkey"), Int(1)],
            vec![Int(-40), Str("USA"), Int(1)],
            vec![Int(0), Null, Int(1)],
            vec![Int(1), Null, Int(1)],
            vec![Int(6), Null, Int(1)],
            vec![Int(9), Str("France"), Int(1)],
            vec![Int(14), Str("Germany"), Int(1)],
        ],
    );
    test_query_ec(
        "SELECT nullable_int, string_packed, COUNT(0)
         FROM default;",
        &[
            vec![Null, Str("$sss"), Int(1)],
            vec![Null, Str("AXY"), Int(1)],
            vec![Null, Str("asd"), Int(1)],
            vec![Null, Str("axz"), Int(1)],
            vec![Null, Str("t"), Int(1)],
            vec![Int(-40), Str("abc"), Int(1)],
            vec![Int(-1), Str("xyz"), Int(1)],
            vec![Int(10), Str("azy"), Int(1)],
            vec![Int(13), Str("😈"), Int(1)],
            vec![Int(20), Str("_f"), Int(1)],
        ],
    );
}

#[test]
fn test_null_operators() {
    test_query_ec(
        "SELECT id, nullable_int, nullable_int2
         FROM default
         WHERE nullable_int < nullable_int2
         ORDER BY id;",
        &[
            vec![Int(9), Int(13), Int(14)],
        ],
    );
    test_query_ec(
        "SELECT id, nullable_int, nullable_int2
         FROM default
         WHERE nullable_int = nullable_int2
         ORDER BY id;",
        &[
            vec![Int(1), Int(-40), Int(-40)],
        ],
    );
    test_query_ec(
        "SELECT id, nullable_int, nullable_int2
         FROM default
         WHERE nullable_int <> nullable_int2 AND nullable_int >= nullable_int2
         ORDER BY id;",
        &[
            vec![Int(4), Int(10), Int(9)],
        ],
    );
    test_query_ec(
        "SELECT id, nullable_int, nullable_int2
         FROM default
         WHERE nullable_int <= nullable_int2 OR nullable_int > nullable_int2
         ORDER BY id;",
        &[
            vec![Int(1), Int(-40), Int(-40)],
            vec![Int(4), Int(10), Int(9)],
            vec![Int(9), Int(13), Int(14)],
        ],
    );
    test_query_ec(
        "SELECT country
         FROM default
         WHERE country <> 'Germany'
         ORDER BY id;",
        &[
            vec![Str("USA")],
            vec![Str("France")],
            vec![Str("France")],
            vec![Str("Turkey")]
        ],
    );
    test_query_ec(
        "SELECT (nullable_int - nullable_int2 / (id + 1)) + (nullable_int - 2 * nullable_int2) % (id + 1)
         FROM default
         ORDER BY id;",
        &[
            vec![Null],
            vec![Int(-20)],
            vec![Null],
            vec![Null],
            vec![Int(6)],
            vec![Null],
            vec![Null],
            vec![Null],
            vec![Null],
            vec![Int(7)],
        ],
    );
}

#[test]
fn test_is_null() {
    test_query_ec(
        "SELECT id FROM default WHERE nullable_int IS NULL ORDER BY id;",
        &[
            vec![Int(2)],
            vec![Int(3)],
            vec![Int(5)],
            vec![Int(6)],
            vec![Int(8)],
        ],
    );
    test_query_ec(
        "SELECT id FROM default WHERE nullable_int IS NOT NULL ORDER BY id;",
        &[
            vec![Int(0)],
            vec![Int(1)],
            vec![Int(4)],
            vec![Int(7)],
            vec![Int(9)],
        ],
    );
}

#[test]
fn test_overflow() {
    test_query_ec_err(
        "SELECT largenum + non_dense_ints FROM default;",
        QueryError::Overflow,
    );
    test_query_ec_err(
        "SELECT largenum + nullable_int FROM default;",
        QueryError::Overflow,
    );
    test_query_ec(
        "SELECT largenum / nullable_int FROM default ORDER BY id;",
        &[
            vec![Int(9223372036854775807)],
            vec![Int(-230584300921369395)],
            vec![Null],
            vec![Null],
            vec![Int(-922337203685477580)],
            vec![Null],
            vec![Null],
            vec![Int(461168601842738790)],
            vec![Null],
            vec![Int(709490156681136600)],
        ],
    );
    test_query_ec_err(
        "SELECT sum(largenum) FROM default;",
        QueryError::Overflow,
    );
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

#[test]
fn test_group_by_string() {
    use value_syntax::*;
    let _ = env_logger::try_init();
    let locustdb = LocustDB::memory_only();
    let _ = block_on(locustdb.gen_table(
        locustdb::colgen::GenTable {
            name: "test".to_string(),
            partitions: 3,
            partition_size: 4096 + 100,
            columns: vec![
                ("hex".to_string(),
                 locustdb::colgen::random_hex_string(8)),
                ("scrambled".to_string(),
                 locustdb::colgen::random_string(1, 2)),
                ("ints".to_string(),
                 locustdb::colgen::int_uniform(-10, 256))
            ],
        }
    ));

    let query = "SELECT scrambled, count(1) FROM test LIMIT 5;";
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap().0.unwrap();
    let expected_rows = vec![
        [Str("0"), Int(98)],
        [Str("01"), Int(5)],
        [Str("02"), Int(2)],
        [Str("03"), Int(4)],
        [Str("04"), Int(2)],
    ];
    assert_eq!(result.rows, expected_rows);

    let query = "SELECT scrambled, scrambled, count(1) FROM test LIMIT 5;";
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap().0.unwrap();
    let expected_rows = vec![
        [Str("0"), Str("0"), Int(98)],
        [Str("01"), Str("01"), Int(5)],
        [Str("02"), Str("02"), Int(2)],
        [Str("03"), Str("03"), Int(4)],
        [Str("04"), Str("04"), Int(2)],
    ];
    assert_eq!(result.rows, expected_rows);

    let query = "SELECT hex, scrambled, count(1) FROM test LIMIT 5;";
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap().0.unwrap();
    let expected_rows = vec![
        [Str("00075c14106c259a"), Str("gA"), Int(1)],
        [Str("00096542e285cb32"), Str("g"), Int(1)],
        [Str("001228dae6b3e755"), Str("m"), Int(1)],
        [Str("0013492a884ee3ab"), Str("P"), Int(1)],
        [Str("0016b50c9677802d"), Str("Y"), Int(1)]
    ];
    assert_eq!(result.rows, expected_rows);

    let query = "SELECT ints, scrambled, count(1) FROM test LIMIT 5;";
    let result = block_on(locustdb.run_query(query, true, vec![])).unwrap().0.unwrap();
    let expected_rows = vec![
        [Int(-10), Str("3I"), Int(1)],
        [Int(-10), Str("8p"), Int(1)],
        [Int(-10), Str("9D"), Int(1)],
        [Int(-10), Str("9m"), Int(1)],
        [Int(-10), Str("C"), Int(1)]
    ];
    assert_eq!(result.rows, expected_rows);
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

