use std::cmp;
use std::i64;
use std::hash::Hash;
use std::collections::hash_set::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use mem_store::integers::*;
use mem_store::column::*;
use mem_store::strings::*;
use bitvec::*;


pub trait ColumnBuilder<T: ?Sized>: Default {
    fn new(allow_null: bool) -> Self;
    fn push(&mut self, elem: &T);
    fn finalize(self, name: &str) -> Arc<Column>;
}

pub struct StringColBuilder {
    data: Vec<Option<Rc<String>>>,
    uniques: UniqueValues<Option<Rc<String>>>,
}

impl Default for StringColBuilder {
    fn default() -> StringColBuilder {
        StringColBuilder {
            data: Vec::new(),
            uniques: UniqueValues::new(1 << 22), // TODO(clemens): Limit?
        }
    }
}

impl<T: AsRef<str>> ColumnBuilder<T> for StringColBuilder {
    fn new(_allow_null: bool) -> StringColBuilder { StringColBuilder::default() }

    fn push(&mut self, elem: &T) {
        let str_opt = Some(Rc::new(elem.as_ref().to_string()));
        self.data.push(str_opt.clone());
        self.uniques.insert(str_opt);
    }

    fn finalize(self, name: &str) -> Arc<Column> {
        build_string_column(name, &self.data, self.uniques)
    }
}


pub struct IntColBuilder {
    data: Vec<i64>,
    present: Vec<u8>,
    min: i64,
    max: i64,
    increasing: u64,
    allow_delta_encode: bool,
    last: i64,
    nullable: bool,
    any_null: bool,
}

impl Default for IntColBuilder {
    fn default() -> IntColBuilder {
        IntColBuilder {
            data: Vec::new(),
            present: Vec::new(),
            min: i64::MAX,
            max: i64::MIN,
            increasing: 0,
            allow_delta_encode: true,
            last: i64::MIN,
            nullable: true,
            any_null: false,
        }
    }
}

impl ColumnBuilder<Option<i64>> for IntColBuilder {
    fn new(allow_null: bool) -> IntColBuilder {
        let mut result = IntColBuilder::default();
        result.nullable = allow_null;
        result
    }

    #[inline]
    fn push(&mut self, elem: &Option<i64>) {
        if self.nullable && elem.is_some() {
            self.present.set(self.data.len())
        } else {
            self.any_null = true;
        }

        // TODO(clemens): cannot set arbitrary values for null to help compression?
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

    fn finalize(self, name: &str) -> Arc<Column> {
        // TODO(clemens): heuristic for deciding delta encoding could probably be improved
        let delta_encode = self.allow_delta_encode &&
            (self.increasing * 10 > self.data.len() as u64 * 9 && cfg!(feature = "enable_lz4"));
        let present = if self.any_null && self.nullable { Some(self.present) } else { None };
        IntegerColumn::new_boxed(name,
                                 self.data,
                                 self.min,
                                 self.max,
                                 delta_encode,
                                 present)
    }
}


pub struct UniqueValues<T> {
    max_count: usize,
    values: HashSet<T>,
}

impl<T: cmp::Eq + Hash> UniqueValues<T> {
    fn new(max_count: usize) -> UniqueValues<T> {
        UniqueValues {
            max_count,
            values: HashSet::new(),
        }
    }

    fn insert(&mut self, value: T) {
        if self.values.len() < self.max_count {
            self.values.insert(value);
        }
    }

    pub fn get_values(self) -> Option<HashSet<T>> {
        if self.values.len() < self.max_count {
            Some(self.values)
        } else {
            None
        }
    }
}
