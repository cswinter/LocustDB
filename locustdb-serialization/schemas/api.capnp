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
        deltaEncodedI8 :group {
            first @7 :Int64;
            data @8 :List(Int8);
        }
        deltaEncodedI16 :group {
            first @9 :Int64;
            data @10 :List(Int16);
        }
        deltaEncodedI32 :group {
            first @11 :Int64;
            data @12 :List(Int32);
        }
        doubleDeltaEncodedI8 :group {
            first @13 :Int64;
            second @14 :Int64;
            data @15 :List(Int8);
        }
        doubleDeltaEncodedI16 :group {
            first @16 :Int64;
            second @17 :Int64;
            data @18 :List(Int16);
        }
        doubleDeltaEncodedI32 :group {
            first @19 :Int64;
            second @20 :Int64;
            data @21 :List(Int32);
        }
        range :group {
            start @22 :Int64;
            len @23 :UInt64;
            step @24 :Int64;
        }
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