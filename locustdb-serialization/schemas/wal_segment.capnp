@0xdb2bd6b471f245ca;

struct WalSegment {
    id @0 :UInt64;
    data @1 :TableSegmentList;
}

struct TableSegmentList {
    data @0 :List(TableSegment);
}

struct TableSegment {
    name @0 :Text;
    len @1 :UInt64;
    columns @2 :List(Column);
}

struct Column {
    name @0 :Text;

    data :union {
        f64 @1 :List(Float64);
        sparseF64 :group {
            indices @2 :List(UInt64);
            values @3 :List(Float64);
        }
        i64 @4 :List(Int64);
        string @5 :List(Text);
        empty @6 :Void;
        sparseI64 :group {
            indices @7 :List(UInt64);
            values @8 :List(Int64);
        }
    }
}
