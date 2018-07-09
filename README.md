# LocustDB [![Build Status](https://travis-ci.org/cswinter/LocustDB.svg?branch=master)](https://travis-ci.org/cswinter/LocustDB) [![Join the chat at https://gitter.im/LocustDB/Lobby](https://badges.gitter.im/LocustDB/Lobby.svg)](https://gitter.im/LocustDB/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

An experimental analytics database aiming to set a new standard for query performance on commodity hardware.
See [How to Analyze Billions of Records per Second on a Single Desktop PC][blogpost] for an overview of current capabilities.

## How to Build

LocustDB requires the latest nightly.

**Tests**

`cargo test`

**Repl**

`cargo run --release --bin repl -- path/to/data.csv`

**Repl (max performance)**

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo run --release --bin repl -- path/to/data.csv`

**Benchmarks**

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo bench`


# Goals
A vision for LocustDB.

## Fast
Query performance for analytics workloads is best-in-class on commodity hardware, both for data cached in memory and for data read from disk.

## Cost-efficient
LocustDB automatically achieves spectacular compression ratios, has minimal indexing overhead, and requires less machines to store the same amount of data than any other system. The trade-off between performance and storage efficiency is configurable.

## Low latency
New data is available for queries within seconds.

## Scalable
LocustDB scales seamlessly from a single machine to large clusters.

## Flexible and easy to use
LocustDB should be usable with minimal configuration or schema-setup as:
- a highly available distributed analytics system continuously ingesting data and executing queries
- a commandline tool/repl for loading and analysing data from CSV files
- an embedded database/query engine included in other Rust programs via cargo


# Non-goals
Until LocustDB is production ready these are distracttions at best, if not wholly incompatible with the main goals.

## Strong consistency and durability guarantees
- small amounts of data may be lost during ingestion
- when a node is unavailable, queries may return incomplete results
- results returned by queries may not represent a consistent snapshot

## High QPS
LocustDB does not efficiently execute queries inserting or operating on small amounts of data.

## Full SQL support
- All data is append only and can only be deleted/expired in bulk.
- LocustDB does not support queries that cannot be evaluated independently by each node (large joins, complex subqueries, precise set sizes, precise top n).

## Support for cost-ineffective or specialised hardware
LocustDB does not run on GPUs.

[blogpost]: TODO
