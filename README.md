# LocustDB [![Build Status](https://travis-ci.org/cswinter/LocustDB.svg?branch=master)](https://travis-ci.org/cswinter/LocustDB) [![Join the chat at https://gitter.im/LocustDB/Lobby](https://badges.gitter.im/LocustDB/Lobby.svg)](https://gitter.im/LocustDB/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

An experimental analytics database aiming to set a new standard for query performance and storage efficiency on commodity hardware.
See [How to Analyze Billions of Records per Second on a Single Desktop PC][blogpost] for an overview of current capabilities.

## How to use

1. [Install Rust][rustup]
2. Clone the repository

```Bash
git clone https://github.com/cswinter/LocustDB.git
cd LocustDB
```

3. Run the repl!

```Bash
RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo +nightly run --release --bin repl -- test_data/nyc-taxi.csv.gz
```

Instead of `test_data/nyc-taxi.csv.gz`, you can also pass a path to any other `.csv` or gzipped `.csv.gz` file. The first line of the file will need to contain the names for each column. The datatypes for each column will be derived automatically, but things might break for columns that contain a mixture of numbers/strings/empty entries.

You can pass the magic strings `nyc100m` or `nyc` to load the first 5 files (100m records) or full 1.46 billion taxi rides dataset which you will need to [download][nyc-taxi-trips] first (for the full dataset, you will need about 120GB of disk space and 60GB of RAM).

### No such subcommand: +nightly

If you run into this error, you likely have an old version of `cargo` installed (`cargo -V` has to resturn 1.0.0 at the very least). You should uninstall cargo/rust from your system and reinstall using [rustup][rustup]. On Unix, you can get a good install with the following command (restart your shell afterwards):

```
curl https://sh.rustup.rs -sSf | RUSTUP_INIT_SKIP_PATH_CHECK=yes sh
```

## Running tests or benchmarks

`cargo +nightly test`

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo +nightly bench`

## Diskstore

LocustDB has experimental support for persisting data to disk, and running queries on data stored on disk.
This feature is disabled by default, and has to be enabled explicitly by passing `--features "rocksdb"` to cargo during compilartion.
The database backend uses RocksDB, which is a somewhat complex C++ dependency that has to be compiled from source and requires gcc and various libraries to be available.
You will have to manually install those on your system, instructions can be found [here][rocksdb-dependencies].
You may also have to install various other random tools until compilation succeeds.

## Goals
A vision for LocustDB.

### Fast
Query performance for analytics workloads is best-in-class on commodity hardware, both for data cached in memory and for data read from disk.

### Cost-efficient
LocustDB automatically achieves spectacular compression ratios, has minimal indexing overhead, and requires less machines to store the same amount of data than any other system. The trade-off between performance and storage efficiency is configurable.

### Low latency
New data is available for queries within seconds.

### Scalable
LocustDB scales seamlessly from a single machine to large clusters.

### Flexible and easy to use
LocustDB should be usable with minimal configuration or schema-setup as:
- a highly available distributed analytics system continuously ingesting data and executing queries
- a commandline tool/repl for loading and analysing data from CSV files
- an embedded database/query engine included in other Rust programs via cargo


## Non-goals
Until LocustDB is production ready these are distractions at best, if not wholly incompatible with the main goals.

### Strong consistency and durability guarantees
- small amounts of data may be lost during ingestion
- when a node is unavailable, queries may return incomplete results
- results returned by queries may not represent a consistent snapshot

### High QPS
LocustDB does not efficiently execute queries inserting or operating on small amounts of data.

### Full SQL support
- All data is append only and can only be deleted/expired in bulk.
- LocustDB does not support queries that cannot be evaluated independently by each node (large joins, complex subqueries, precise set sizes, precise top n).

### Support for cost-inefficient or specialised hardware
LocustDB does not run on GPUs.

[nyc-taxi-trips]: https://www.dropbox.com/sh/4xm5vf1stnf7a0h/AADRRVLsqqzUNWEPzcKnGN_Pa?dl=0
[blogpost]: https://clemenswinter.com/2018/07/09/how-to-analyze-billions-of-records-per-second-on-a-single-desktop-pc/
[rustup]: https://rustup.rs/
[rocksdb-dependencies]: https://github.com/facebook/rocksdb/blob/master/INSTALL.md#dependencies
