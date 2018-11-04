use std::marker::PhantomData;
use std::sync::Arc;

use crypto::digest::Digest;
use crypto::md5::Md5;
use hex;
use ingest::alias_method_fork::*;
use rand::Rng;
use rand::SeedableRng;
use rand::distributions::{Alphanumeric, Standard};
use rand;

use mem_store::column::*;
use mem_store::column_builder::{ColumnBuilder, IntColBuilder, StringColBuilder};
use mem_store::lru::LRU;
use mem_store::partition::Partition;
use mem_store::table::Table;

pub trait ColumnGenerator: Sync + Send {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column>;
}

pub fn int_markov_chain(
    elements: Vec<i64>,
    transition_probabilities: Vec<Vec<f64>>) -> Box<ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
        s: PhantomData::<IntColBuilder>,
    })
}

pub fn int_uniform(low: i64, high: i64) -> Box<ColumnGenerator> {
    Box::new(UniformInteger { low, high })
}

pub fn string_markov_chain(
    elements: Vec<String>,
    transition_probabilities: Vec<Vec<f64>>) -> Box<ColumnGenerator> {
    Box::new(MarkovChain {
        elem: elements,
        p_transition: transition_probabilities,
        s: PhantomData::<StringColBuilder>,
    })
}

pub fn random_hex_string(length: usize) -> Box<ColumnGenerator> {
    Box::new(HexString { length })
}

pub fn random_string(min_length: usize, max_length: usize) -> Box<ColumnGenerator> {
    Box::new(RandomString {
        min_length,
        max_length,
    })
}

pub fn partition_sparse(
    null_probability: f64,
    generator: Box<ColumnGenerator>) -> Box<ColumnGenerator> {
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

unsafe impl<T: Sync + Send, S: ColumnBuilder<T>> Send for MarkovChain<T, S> {}

unsafe impl<T: Sync + Send, S: ColumnBuilder<T>> Sync for MarkovChain<T, S> {}

impl<T: Sync + Send, S: ColumnBuilder<T>> ColumnGenerator for MarkovChain<T, S> {
    fn generate(&self, length: usize, name: &str, seed: u64) -> Arc<Column> {
        let mut rng = seeded_rng(seed);
        let mut builder = S::default();
        let mut state = rng.gen_range(0, self.elem.len());
        let p = self.p_transition.iter()
            .map(|p| new_alias_table(p).unwrap())
            .collect::<Vec<_>>();
        let mut alias_method = AliasMethod::new(rng);
        for _ in 0..length {
            state = alias_method.random(&p[state]);
            builder.push(&self.elem[state]);
        }
        builder.finalize(name)
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
            builder.push(&rng.gen_range::<i64>(self.low, self.high));
        }
        ColumnBuilder::<i64>::finalize(builder, name)
    }
}

struct PartitionSparse {
    null_probability: f64,
    generator: Box<ColumnGenerator>,
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
        ColumnBuilder::<&str>::finalize(builder, name)
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
            let string: String = rng.sample_iter::<char, _>(&Alphanumeric).take(len).collect();
            builder.push(&string);
        }
        ColumnBuilder::<&str>::finalize(builder, name)
    }
}

pub struct GenTable {
    pub name: String,
    pub partitions: usize,
    pub partition_size: usize,
    pub columns: Vec<(String, Box<ColumnGenerator>)>,
}

impl GenTable {
    pub fn gen(&self, lru: &LRU, seed: u64) -> Table {
        let table = Table::new(self.partition_size, &self.name, lru.clone());
        for p in 0..self.partitions {
            let mut partition = self.columns
                .iter()
                .map(|(name, c)| c.generate(self.partition_size, &name, seed + p as u64))
                .collect();
            table.load_partition(
                Partition::new(
                    p as u64,
                    partition,
                    lru.clone(),
                ).0);
        }
        table
    }
}

fn seeded_rng(seed: u64) -> rand::XorShiftRng {
    let mut seed_bytes = [0u8; 16];
    let mut hasher = Md5::new();
    hasher.input(&seed.to_ne_bytes());
    hasher.result(&mut seed_bytes);
    rand::XorShiftRng::from_seed(seed_bytes)
}
