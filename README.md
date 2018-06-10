# LocustDB [![Build Status](https://travis-ci.org/cswinter/LocustDB.svg?branch=master)](https://travis-ci.org/cswinter/LocustDB)

An experimental, in-memory analytics database aiming to set a new standard for query performance on commodity hardware.

## Build Tasks

**Tests**

`cargo tests`

**Repl**

`cargo run --release --bin repl -- path/to/data.csv`

**Repl (max performance)**

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo run --release --bin repl -- path/to/data.csv`

**Benchmark**

`RUSTFLAGS="-Ccodegen-units=1" CARGO_INCREMENTAL=0 cargo bench`
