@0xc2e3685626e6e832;

struct PartitionSegment {
    columns @0 :List(Column);
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
    end @1 :Int64;
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
        pco @9 :Pco;
    }
}

struct DataSection {
    union {
        u8 @0 :List(UInt8);
        u16 @1 :List(UInt16);
        u32 @2 :List(UInt32);
        u64 @3 :List(UInt64);
        i64 @4 :List(Int64);
        null @5 :UInt64;
        f64 @6 :List(Float64);
        bitvec @7 :List(UInt8);
        lz4 :group {
            decodedBytes @8 :UInt64;
            bytesPerElement @9 :UInt64;
            data @10 :List(UInt8);
        }
        pco :group {
            decodedBytes @11 :UInt64;
            bytesPerElement @12 :UInt64;
            data @13 :List(UInt8);
            isFp32 @14 :Bool;
        }
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

struct Pco {
    type @0 :EncodingType;
    lenDecoded @1 :UInt64;
    isFp32 @2 :Bool;
}

struct UnhexpackStrings {
    uppercase @0 :Bool;
    totalBytes @1 :UInt64;
}

enum EncodingType {
    u8 @0;
    u16 @1;
    u32 @2;
    u64 @3;
    i64 @4;
    null @5;
    f64 @6;
    bitvec @7;
}