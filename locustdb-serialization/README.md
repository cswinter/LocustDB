# LocustDB Serialization

Util crate that defines Cap'n Proto schema and serialization/deserialization logic for data structures used for persistent storage and client-server communication in LocustDB.

To regenerate the Cap'n Proto definitions, follow this process:

1. [Install the Cap'n Proto CLI tool][install-capnproto]
2. `cargo install capnpc`
3. `capnp compile -orust:src --src-prefix=schemas schemas/{dbmeta,partition_segment,wal_segment,api}.capnp`
