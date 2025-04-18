use chrono::{DateTime, Datelike};

use crate::engine::of64;

use super::map_operator::MapOp;


pub struct ToYear;

impl MapOp<i64, i64> for ToYear {
    fn apply(&self, unix_ts: i64) -> i64 { i64::from(DateTime::from_timestamp(unix_ts, 0).unwrap().year()) }
    fn name() -> &'static str { "to_year" }
}

pub struct Floor;

impl MapOp<of64, i64> for Floor {
    fn apply(&self, f: of64) -> i64 { f.floor() as i64 }
    fn name() -> &'static str { "floor" }
}

pub struct BooleanNot;

impl MapOp<u8, u8> for BooleanNot {
    fn apply(&self, boolean: u8) -> u8 { boolean ^ true as u8 }
    fn name() -> &'static str { "not" }
}


pub struct RegexMatch {
    pub r: regex::Regex
}

impl<'a> MapOp<&'a str, u8> for RegexMatch {
    fn apply(&self, s: &'a str) -> u8 {
        match self.r.find(s) {
            Some(_) => 1,
            None => 0,
        }
    }
    fn name() -> &'static str { "not" }
}


pub struct Length;

impl<'a> MapOp<&'a str, i64> for Length {
    fn apply(&self, s: &'a str) -> i64 { s.len() as i64 }
    fn name() -> &'static str { "length" }
}
