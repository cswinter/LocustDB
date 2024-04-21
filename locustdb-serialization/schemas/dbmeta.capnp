@0xafa9b81d5e8e2ef5;

struct DBMeta {
    nextWalId @0 :UInt64;
    partitions @1 :List(PartitionMetadata);
}

struct PartitionMetadata {
    id @0 :UInt64;
    tablename @1 :Text;
    offset @2 :UInt64;
    len @3 :UInt64;
    subpartitions @4 :List(SubpartitionMetadata);
}

struct SubpartitionMetadata {
    sizeBytes @0 :UInt64;
    subpartitionKey @1 :Text;
    columns @2 :List(Text);
}