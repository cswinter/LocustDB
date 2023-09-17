use std::marker::PhantomData;
use std::sync::Arc;

use crate::ingest::alias_method_fork::*;
use hex;
use rand;
use rand::distributions::{Alphanumeric, Standard};
use rand::Rng;
use rand::SeedableRng;

use crate::mem_store::column::*;
use crate::mem_store::column_builder::{ColumnBuilder, IntColBuilder, StringColBuilder};
use crate::scheduler::inner_locustdb::InnerLocustDB;

pub trait ColumnGenerator: Sync + Send {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column>;
}

pub fn int_markov_chain(
    elements: Vec<i64>,
    transition_probabilities: Vec<Vec<f64>>,
) -> Box<dyn ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements.into_iter().map(Some).collect(),
        p_transition: transition_probabilities,
        s: PhantomData::<IntColBuilder>,
    })
}

pub fn int_uniform(low: i64, high: i64) -> Box<dyn ColumnGenerator> {
    Box::new(UniformInteger { low, high })
}

pub fn splayed(offset: i64, coefficient: i64) -> Box<dyn ColumnGenerator> {
    Box::new(Splayed {
        offset,
        coefficient,
    })
}

pub fn int_weighted(values: Vec<i64>, weights: Vec<f64>) -> Box<dyn ColumnGenerator> {
    Box::new(Weighted {
        elem: values.into_iter().map(Some).collect(),
        weights,
        s: PhantomData::<IntColBuilder>,
    })
}

pub fn incrementing_int() -> Box<dyn ColumnGenerator> {
    Box::new(IncrementingInteger)
}

pub fn string_markov_chain(
    elements: Vec<String>,
    transition_probabilities: Vec<Vec<f64>>,
) -> Box<dyn ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
        s: PhantomData::<StringColBuilder>,
    })
}

pub fn string_weighted(values: Vec<String>, weights: Vec<f64>) -> Box<dyn ColumnGenerator> {
    Box::new(Weighted {
        elem: values,
        weights,
        s: PhantomData::<StringColBuilder>,
    })
}

pub fn random_hex_string(length: usize) -> Box<dyn ColumnGenerator> {
    Box::new(HexString { length })
}

pub fn random_string(min_length: usize, max_length: usize) -> Box<dyn ColumnGenerator> {
    Box::new(RandomString {
        min_length,
        max_length,
    })
}

pub fn partition_sparse(
    null_probability: f64,
    generator: Box<dyn ColumnGenerator>,
) -> Box<dyn ColumnGenerator> {
    Box::new(PartitionSparse {
        null_probability,
        generator,
    })
}

#[derive(Clone)]
struct MarkovChain<T, S> {
    elem: Vec<T>,
    p_transition: Vec<Vec<f64>>,
    s: PhantomData<S>,
}

unsafe impl<T: Send, S: ColumnBuilder<T>> Send for MarkovChain<T, S> {}

unsafe impl<T: Sync, S: ColumnBuilder<T>> Sync for MarkovChain<T, S> {}

impl<T: Sync + Send, S: ColumnBuilder<T>> ColumnGenerator for MarkovChain<T, S> {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        let mut builder = S::default();
        let mut state = rng.gen_range(0, self.elem.len());
        let p = self
            .p_transition
            .iter()
            .map(|p| new_alias_table(p).unwrap())
            .collect::<Vec<_>>();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            state = alias_method.random(&p[state]);
            builder.push(&self.elem[state]);
        }
        builder.finalize(name, None)
    }
}

#[derive(Clone)]
struct Weighted<T, S> {
    elem: Vec<T>,
    weights: Vec<f64>,
    s: PhantomData<S>,
}

unsafe impl<T: Send, S: ColumnBuilder<T>> Send for Weighted<T, S> {}

unsafe impl<T: Sync, S: ColumnBuilder<T>> Sync for Weighted<T, S> {}

impl<T: Sync + Send, S: ColumnBuilder<T>> ColumnGenerator for Weighted<T, S> {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let rng = seeded_rng(seed);
        let mut builder = S::default();
        let p = new_alias_table(&self.weights).unwrap();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            let i = alias_method.random(&p);
            builder.push(&self.elem[i]);
        }
        builder.finalize(name, None)
    }
}

struct UniformInteger {
    low: i64,
    high: i64,
}

impl ColumnGenerator for UniformInteger {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        let mut builder = IntColBuilder::default();
        for _ in 0..length {
            builder.push(&Some(rng.gen_range::<i64>(self.low, self.high)));
        }
        ColumnBuilder::<Option<i64>>::finalize(builder, name, None)
    }
}

struct Splayed {
    offset: i64,
    coefficient: i64,
}

impl ColumnGenerator for Splayed {
    fn generate(&self, length: usize, name: &str, partition: u64) -> Arc<Column> {
        let mut rng = seeded_rng(partition);
        let mut builder = IntColBuilder::default();
        for _ in 0..length {
            builder.push(&Some(rng.gen_range::<i64>(
                self.offset + self.coefficient * length as i64 * partition as i64,
                self.offset + self.coefficient * length as i64 * (partition as i64 + 1),
            )));
        }
        ColumnBuilder::<Option<i64>>::finalize(builder, name, None)
    }
}

struct PartitionSparse {
    null_probability: f64,
    generator: Box<dyn ColumnGenerator>,
}

impl ColumnGenerator for PartitionSparse {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        if rng.gen::<f64>() < self.null_probability {
            Arc::new(Column::null(name, length))
        } else {
            self.generator.generate(length, name, seed)
        }
    }
}

struct HexString {
    length: usize,
}

impl ColumnGenerator for HexString {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        let mut builder = StringColBuilder::default();
        for _ in 0..length {
            let bytes: Vec<u8> = rng.sample_iter(&Standard).take(self.length).collect();
            builder.push(&hex::encode(&bytes));
        }
        ColumnBuilder::<&str>::finalize(builder, name, None)
    }
}

struct RandomString {
    min_length: usize,
    max_length: usize,
}

impl ColumnGenerator for RandomString {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        let mut builder = StringColBuilder::default();
        for _ in 0..length {
            let len = rng.gen_range(self.min_length, self.max_length + 1);
            let string: String = rng
                .sample_iter::<char, _>(&Alphanumeric)
                .take(len)
                .collect();
            builder.push(&string);
        }
        ColumnBuilder::<&str>::finalize(builder, name, None)
    }
}

struct IncrementingInteger;

impl ColumnGenerator for IncrementingInteger {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut builder = IntColBuilder::default();
        for i in seed as i64 * length as i64..length as i64 * (seed as i64 + 1) {
            builder.push(&Some(i));
        }
        builder.finalize(name, None)
    }
}

pub struct GenTable {
    pub name: String,
    pub partitions: usize,
    pub partition_size: usize,
    pub columns: Vec<(String, Box<dyn ColumnGenerator>)>,
}

impl GenTable {
    pub fn gen(&self, db: &InnerLocustDB, partition_number: u64) {
        let partition = self
            .columns
            .iter()
            .map(|(name, c)| c.generate(self.partition_size, name, partition_number))
            .collect();
        db.store_partition(&self.name, partition);
    }
}

fn seeded_rng(seed: u64) -> rand::XorShiftRng {
    rand::XorShiftRng::seed_from_u64(seed)
}
