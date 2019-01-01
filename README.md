# LocustDB

[![Build Status][bi]][bl] [![Crates.io][ci]][cl] [![Gitter][gi]][gl]

[bi]: https://travis-ci.org/cswinter/LocustDB.svg?branch=master
[bl]: https://travis-ci.org/cswinter/LocustDB

[ci]: https://img.shields.io/crates/v/locustdb.svg
[cl]: https://crates.io/crates/locustdb/

[gi]: https://badges.gitter.im/LocustDB/Lobby.svg
[gl]: https://gitter.im/LocustDB/Lobby

An experimental analytics database aiming to set a new standard for query performance and storage efficiency on commodity hardware.
See [How to Analyze Billions of Records per Second on a Single Desktop PC][blogpost] and [How to Read 100s of Millions of Records per Second from a Single Disk][blogpost-2] for an overview of current capabilities.

## Usage

Download the [latest binary release][latest-release], which can be run from the command line on most x64 Linux systems, including Windows Subsystem for Linux. For example, to load the file `test_data/nyc-taxi.csv.gz` in this repository and start the repl run:

```Bash
./locustdb --load test_data/nyc-taxi.csv.gz --trips
```

When loading `.csv` or `.csv.gz` files with `--load`, the first line of each file is assumed to be a header containing the names for all columns. The type of each column will be derived automatically, but this might break for columns that contain a mixture of numbers/strings/empty entries.

To persist data to disk in LocustDB's internal storage format (which allows fast queries from disk after the initial load), specify the storage location with `--db-path`
When creating/opening a persistent database, LocustDB will open a lot of files and might crash if the limit on the number of open files is too low.
On Linux, you can check the current limit with `ulimit -n` and set a new limit with e.g. `ulimit -n 4096`.

The `--trips` flag will configure the ingestion schema for loading the 1.46 billion taxi ride dataset which can be downloaded [here][nyc-taxi-trips].

For additional usage info, invoke with `--help`:

```Bash
$ ./locustdb --help
LocustDB 0.1.0-alpha
Clemens Winter <clemenswinter1@gmail.com>
Massively parallel, high performance analytics database that will rapidly devour all of your data.

USAGE:
    locustdb [FLAGS] [OPTIONS]

FLAGS:
    -h, --help             Prints help information
        --mem-lz4          Keep data cached in memory lz4 encoded. Decreases memory usage and query speeds.
        --reduced-trips    Set ingestion schema for select set of columns from nyc taxi ride dataset
        --seq-disk-read    Improves performance on HDD, can hurt performance on SSD.
        --trips            Set ingestion schema for nyc taxi ride dataset
    -V, --version          Prints version information

OPTIONS:
        --db-path <PATH>           Path to data directory
        --load <CSV_FILE>...       Load .csv or .csv.gz files into the database
        --mem-limit-tables <GB>    Limit for in-memory size of tables in GiB [default: 8]
        --partition-size <ROWS>    Number of rows per partition when loading new data [default: 65536]
        --readahead <MB>           How much data to load at a time when reading from disk during queries in MiB
                                   [default: 256]
        --table <NAME>             Name for the table populated with --load [default: default]
        --threads <INTEGER>        Number of worker threads. [default: number of cores (12)]
```

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


## Compiling from source

1. Install Rust: [rustup.rs][rustup]
2. Clone the repository

```Bash
git clone https://github.com/cswinter/LocustDB.git
cd LocustDB
```

3. Compile with `RUSTFLAGS="-Ccodegen-units=1"` and `CARGO_INCREMENTAL=0` for optimal performance:

```Bash
RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo run --release --bin repl -- --load test_data/nyc-taxi.csv.gz --reduced-trips
```

### Running tests or benchmarks

`cargo test`

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo bench`

### Storage backend
LocustDB has support for persisting data to disk and running queries on data stored on disk.
This feature is disabled by default, and has to be enabled explicitly by passing `--features "enable_rocksdb"` to cargo during compilation.
The database backend uses RocksDB, which is a somewhat complex C++ dependency that has to be compiled from source and requires gcc and various libraries to be available.
You will have to manually install those on your system, instructions can be found [here][rocksdb-dependencies].
You may also have to install various other random tools until compilation succeeds.

### LZ4

Compile with `--features "enable_lz4"` to enable an additional lz4 compression pass which can significantly reduce data size both on disk and in-memory, at the cost of slightly slower in-memory queries.


[nyc-taxi-trips]: https://www.dropbox.com/sh/4xm5vf1stnf7a0h/AADRRVLsqqzUNWEPzcKnGN_Pa?dl=0
[blogpost]: https://clemenswinter.com/2018/07/09/how-to-analyze-billions-of-records-per-second-on-a-single-desktop-pc/
[blogpost-2]: https://clemenswinter.com/2018/08/13/how-read-100s-of-millions-of-records-per-second-from-a-single-disk/
[rustup]: https://rustup.rs/
[rocksdb-dependencies]: https://github.com/facebook/rocksdb/blob/master/INSTALL.md#dependencies
[latest-release]: https://github.com/cswinter/LocustDB/releases/download/v0.1.0-alpha/locustdb-0.1.0-alpha-x64-linux.0-alpha
