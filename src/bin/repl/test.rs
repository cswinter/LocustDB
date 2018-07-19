struct Subtraction;

impl<T: Integer, U: Integer> Op2<T, U, i64> for Subtraction {
    fn op(x: T, y: U) -> i64 { x.to_i64() - y.to_i64() }

    // optional properties used by query engine/optimiser
    fn is_commutative() -> bool { false }
    fn is_order_preserving() -> bool { true }
    // ...

    fn display_name() -> &'static str { "-" }
    fn display_infix() -> bool { true }
}

fn init() {
    register_function(
        Box::new(Subtraction),
        specialize_int_int_i64!(Subtraction),
    );
}
