@0xafa9b81d5e8e2ef5;

struct DBMeta {
    nextWalId @0 :UInt64;
    partitions @1 :List(PartitionMetadata);
    strings @2 :List(Text);  # [v1] unused in legacy format and deprecated in new format
    compressedStrings @3 :Data; # [v2..]
    lengthsCompressedStrings@4 :List(UInt16); # [v2..]
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
    columns @2 :List(Text);  # [v0] deprecated in favor of internedColumns
    internedColumns @3 :List(UInt64); # [v1] unused in legacy format and deprecated in new format
    compressedInternedColumns @4 :Data; # [v2..]
}