# LocustDB Compression Utils

Collection of utils for compressing a series of values. 

## XOR float compression

One of compression algorithms implemented is a variant of the XOR float compression algorithm described in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf). The "gorilla_time" example program creates visualizations of the compression algorithm. You can run the visualization with:

```bash
cargo run --example gorilla_time -- --verbose
```

Run the following command to see more options:

```bash
cargo run --example gorilla_time -- --help
```