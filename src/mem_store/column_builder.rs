use std::cmp;
use std::i64;
use std::sync::Arc;

use crate::mem_store::integers::*;
use crate::mem_store::column::*;
use crate::mem_store::strings::*;
use crate::stringpack::*;


pub trait ColumnBuilder<T: ?Sized>: Default {
    fn new() -> Self;
    fn push(&mut self, elem: &T);
    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column>;
}

pub struct StringColBuilder {
    values: IndexedPackedStrings,
    lhex: bool,
    uhex: bool,
    string_bytes: usize,
}

impl Default for StringColBuilder {
    fn default() -> StringColBuilder {
        StringColBuilder {
            values: IndexedPackedStrings::default(),
            lhex: true,
            uhex: true,
            string_bytes: 0,
        }
    }
}

impl<T: AsRef<str>> ColumnBuilder<T> for StringColBuilder {
    fn new() -> StringColBuilder { StringColBuilder::default() }

    fn push(&mut self, elem: &T) {
        let elem = elem.as_ref();
        self.lhex = self.lhex && is_lowercase_hex(elem);
        self.uhex = self.uhex && is_uppercase_hex(elem);
        self.string_bytes += elem.as_bytes().len();
        self.values.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        fast_build_string_column(name, self.values.iter(), self.values.len(),
                                 self.lhex, self.uhex, self.string_bytes, present)
    }
}


pub struct IntColBuilder {
    data: Vec<i64>,
    min: i64,
    max: i64,
    increasing: u64,
    allow_delta_encode: bool,
    last: i64,
}

impl Default for IntColBuilder {
    fn default() -> IntColBuilder {
        IntColBuilder {
            data: Vec::new(),
            min: i64::MAX,
            max: i64::MIN,
            increasing: 0,
            allow_delta_encode: true,
            last: i64::MIN,
        }
    }
}

impl ColumnBuilder<Option<i64>> for IntColBuilder {
    fn new() -> IntColBuilder { IntColBuilder::default() }

    #[inline]
    fn push(&mut self, elem: &Option<i64>) {
        // PERF: can set arbitrary values for null to help compression (extend from last/previous value)
        let elem = elem.unwrap_or(0);
        self.min = cmp::min(elem, self.min);
        self.max = cmp::max(elem, self.max);
        if elem > self.last {
            self.increasing += 1;
        } else if elem.checked_sub(self.last).is_none() {
            self.allow_delta_encode = false;
        };
        self.last = elem;
        self.data.push(elem);
    }

    fn finalize(self, name: &str, present: Option<Vec<u8>>) -> Arc<Column> {
        // PERF: heuristic for deciding delta encoding could probably be improved
        let delta_encode = self.allow_delta_encode &&
            (self.increasing * 10 > self.data.len() as u64 * 9 && cfg!(feature = "enable_lz4"));
        IntegerColumn::new_boxed(name,
                                 self.data,
                                 self.min,
                                 self.max,
                                 delta_encode,
                                 present)
    }
}


fn is_lowercase_hex(string: &str) -> bool {
    string.len() & 1 == 0 && string.chars().all(|c| {
        c == '0' || c == '1' || c == '2' || c == '3' ||
            c == '4' || c == '5' || c == '6' || c == '7' ||
            c == '8' || c == '9' || c == 'a' || c == 'b' ||
            c == 'c' || c == 'd' || c == 'e' || c == 'f'
    })
}

fn is_uppercase_hex(string: &str) -> bool {
    string.len() & 1 == 0 && string.chars().all(|c| {
        c == '0' || c == '1' || c == '2' || c == '3' ||
            c == '4' || c == '5' || c == '6' || c == '7' ||
            c == '8' || c == '9' || c == 'A' || c == 'B' ||
            c == 'C' || c == 'D' || c == 'E' || c == 'F'
    })
}

