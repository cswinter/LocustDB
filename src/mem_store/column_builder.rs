use std::cmp;
use std::i64;
use std::hash::Hash;
use std::collections::hash_set::HashSet;
use std::rc::Rc;
use std::sync::Arc;

use mem_store::integers::*;
use mem_store::column::*;
use mem_store::strings::*;


pub trait ColumnBuilder<T: ?Sized> {
    fn push(&mut self, elem: &T);
    fn finalize(self, name: &str) -> Arc<Column>;
}


pub struct StringColBuilder {
    data: Vec<Option<Rc<String>>>,
    uniques: UniqueValues<Option<Rc<String>>>,
}

impl StringColBuilder {
    pub fn new() -> StringColBuilder {
        StringColBuilder {
            data: Vec::new(),
            uniques: UniqueValues::new(MAX_UNIQUE_STRINGS),
        }
    }
}

impl ColumnBuilder<str> for StringColBuilder {
    fn push(&mut self, elem: &str) {
        let str_opt = Some(Rc::new(elem.to_string()));
        self.data.push(str_opt.clone());
        self.uniques.insert(str_opt);
    }

    fn finalize(self, name: &str) -> Arc<Column> {
        build_string_column(name, &self.data, self.uniques)
    }
}


pub struct IntColBuilder {
    data: Vec<i64>,
    min: i64,
    max: i64,
}

impl IntColBuilder {
    pub fn new() -> IntColBuilder {
        IntColBuilder {
            data: Vec::new(),
            min: i64::MAX,
            max: i64::MIN,
        }
    }
}

impl ColumnBuilder<i64> for IntColBuilder {
    #[inline]
    fn push(&mut self, elem: &i64) {
        let elem = *elem;
        self.min = cmp::min(elem, self.min);
        self.max = cmp::max(elem, self.max);
        self.data.push(elem);
    }

    fn finalize(self, name: &str) -> Arc<Column> {
        IntegerColumn::new_boxed(name, self.data, self.min, self.max)
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
