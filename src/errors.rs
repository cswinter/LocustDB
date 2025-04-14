use std::backtrace::Backtrace;
use futures::channel::oneshot;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Failed to parse query. Chars remaining: {}", _0)]
    SytaxErrorCharsRemaining(String),
    #[error("Failed to parse query. Bytes remaining: {:?}", _0)]
    SyntaxErrorBytesRemaining(Vec<u8>),
    #[error("Failed to parse query: {}", _0)]
    ParseError(String),
    #[error("Some assumption was violated. This is a bug: {}", _0)]
    FatalError(String, Backtrace),
    #[error("Not implemented: {}", _0)]
    NotImplemented(String),
    #[error("Type error: {}", _0)]
    TypeError(String),
    #[error("Overflow or division by zero")]
    Overflow,
    #[error("Query execution was canceled")]
    Canceled {
        #[from]
        source: oneshot::Canceled,
    },
}

#[macro_export]
macro_rules! fatal {
    ($e:expr) => {
        QueryError::FatalError($e.to_owned(), std::backtrace::Backtrace::capture())
    };
    ($fmt:expr, $($arg:tt)+) => {
        QueryError::FatalError(format!($fmt, $($arg)+).to_string(), std::backtrace::Backtrace::capture())
    };
}

#[macro_export]
macro_rules! bail {
    ($kind:expr, $e:expr) => {
        return Err($kind($e.to_owned()))
    };
    ($kind:expr, $fmt:expr, $($arg:tt)+) => {
        return Err($kind(format!($fmt, $($arg)+).to_owned()))
    };
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err(QueryError::FatalError($e.to_string(), std::backtrace::Backtrace::capture()))
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)+) => {
        if !($cond) {
            return Err(QueryError::FatalError(format!($fmt, $($arg)+).to_string(), std::backtrace::Backtrace::capture()))
        }
    };
}
