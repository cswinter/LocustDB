@0x863175761840d583;

struct MetaData {
    tablename @0 :Text;
    len @1 :UInt64;
    columns @2 :List(ColumnMetaData);
}

struct ColumnMetaData {
    name @0 :Text;
    sizeBytes @1 :UInt64;
}

struct Column {
    name @0 :Text;
    len @1 :UInt64;
    range: union {
        range @2 :Range;
        empty @3 :Void;
    }
    codec @4 :List(CodecOp);
    data @5 :List(DataSection);
}

struct Range {
    start @0 :Int64;
    end @1: Int64;
}

struct CodecOp {
    union {
        add @0 :Add;
        delta @1 :EncodingType;
        toI64 @2 :EncodingType;
        pushDataSection @3 :UInt64;
        dictLookup @4 :EncodingType;
        lz4 @5 :LZ4;
        unpackStrings @6 :Void;
        unhexpackStrings @7 :UnhexpackStrings;
        nullable  @8 :Void;
    }
}

struct DataSection {
    union {
        null @0 :UInt64;
        u8 @1 :List(UInt8);
        u16 @2 :List(UInt16);
        u32 @3 :List(UInt32);
        u64 @4 :List(UInt64);
        i64 @5 :List(Int64);
        f64 @6 :List(Float64);
    }
}

struct Add {
    type @0 :EncodingType;
    amount @1 :Int64;
}

struct LZ4 {
    type @0 :EncodingType;
    lenDecoded @1 :UInt64;
}

struct UnhexpackStrings {
    uppercase @0 :Bool;
    totalBytes @1 :UInt64;
}

enum EncodingType {
    null @0;
    u8 @1;
    u16 @2;
    u32 @3;
    u64 @4;
    i64 @5;
    f64 @6;
}
