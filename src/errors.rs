#[derive(Fail, Debug)]
pub enum QueryError {
    #[fail(display = "Failed to parser query. Chars remaining: {}", _0)]
    SytaxErrorCharsRemaining(String),
    #[fail(display = "Failed to parser query. Bytes remaining: {:?}", _0)]
    SyntaxErrorBytesRemaining(Vec<u8>),
    #[fail(display = "Failed to parser query: {}", _0)]
    ParseError(String),
    #[fail(display = "Some assumption was violated. This is a bug: {}", _0)]
    FatalError(String),
    #[fail(display = "Not implemented: {}", _0)]
    NotImplemented(String),
    #[fail(display = "Type error: {}", _0)]
    TypeError(String),
}

#[macro_export]
macro_rules! bail {
    ($kind:expr, $e:expr) => {
        return Err($kind($e.to_owned()));
    };
    ($kind:expr, $fmt:expr, $($arg:tt)+) => {
        return Err($kind(format!($fmt, $($arg)+).to_owned()));
    };
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            bail!(QueryError::FatalError, $e);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            bail!(QueryError::FatalError, $fmt, $($arg)+);
        }
    };
}
