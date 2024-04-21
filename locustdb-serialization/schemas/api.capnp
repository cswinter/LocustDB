@0x88a093a148e409e4;


struct QueryResponse {
    columns @0 :List(Column);
}

struct MultiQueryResponse {
    responses @0 :List(QueryResponse);
}

struct Column {
    name @0 :Text;

    data :union {
        f64 @1 :List(Float64);
        i64 @2 :List(Int64);
        string @3 :List(Text);
        mixed @4 :List(AnyVal);
        null @5 :UInt64;
        xorF64 @6 :Data;
    }
}

struct AnyVal {
    union {
        f64 @0 :Float64;
        i64 @1 :Int64;
        string @2 :Text;
        null @3 :Void;
    }
}