use std::i64;

use chrono::{NaiveDateTime, Datelike};

use super::map_operator::MapOp;


pub struct ToYear;

impl MapOp<i64, i64> for ToYear {
    fn apply(&self, unix_ts: i64) -> i64 { i64::from(NaiveDateTime::from_timestamp(unix_ts, 0).year()) }
    fn name() -> &'static str { "to_year" }
}


pub struct BooleanNot;

impl MapOp<u8, u8> for BooleanNot {
    fn apply(&self, boolean: u8) -> u8 { boolean ^ true as u8 }
    fn name() -> &'static str { "not" }
}


