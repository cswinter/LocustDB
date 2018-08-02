#[cfg(feature = "enable_rocksdb")]
extern crate capnpc;

fn main() {
    #[cfg(feature = "enable_rocksdb")]
        ::capnpc::CompilerCommand::new()
        .src_prefix("src")
        .file("src/storage_format.capnp")
        .run()
        .unwrap();
}
