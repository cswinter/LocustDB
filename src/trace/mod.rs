#![cfg_attr(not(feature = "trace"), allow(dead_code))]

mod span_collector;
mod trace_builder;

use std::cell::RefCell;

pub use self::trace_builder::TraceBuilder;
pub use self::span_collector::{SpanCollector, SpanGuard};


thread_local!(
    static THREAD_LOCAL_COLLECTOR: RefCell<SpanCollector> = RefCell::new(SpanCollector::default());
);

#[macro_export]
#[cfg(feature = "trace")]
macro_rules! trace_start {
    ( $( $x:expr),* ) => (
        #[cfg_attr(feature = "cargo-clippy", allow(useless_format))]
        let _guard = $crate::_start(format!( $( $x ),* ).to_owned());
    )
}

#[cfg(feature = "trace")]
macro_rules! trace_replace {
    ( $( $x:expr),* ) => (
        #[cfg_attr(feature = "cargo-clippy", allow(useless_format))]
        let _guard = $crate::_replace(format!( $( $x ),* ).to_owned());
    )
}

#[cfg(not(feature = "trace"))]
macro_rules! trace_start {
    // Drop refs to args (which is no-op) to prevent unused variable warnings.
    ( $( $x:expr),* ) => {
        $(
            #[cfg_attr(feature="cargo-clippy", allow(drop_ref))]
            drop(& $x);
        )*
    }
}

#[cfg(not(feature = "trace"))]
#[cfg_attr(feature = "cargo-clippy", allow(drop_ref))]
#[allow(unused_macros)]
macro_rules! trace_replace {
    // Drop refs to args (which is no-op) to prevent unused variable warnings.
    ( $( $x:expr),* ) => {
        $(
            #[cfg_attr(feature="cargo-clippy", allow(drop_ref))]
            drop(& $x);
        )*
    }
}

#[doc(hidden)]
#[inline]
pub fn _start(name: String) -> SpanGuard {
    THREAD_LOCAL_COLLECTOR.with(|sc| {
        sc.borrow_mut().start_span(name)
    })
}

#[doc(hidden)]
#[inline]
pub fn _replace(name: String) -> SpanGuard {
    THREAD_LOCAL_COLLECTOR.with(|sc| {
        sc.borrow_mut().replace_span(name)
    })
}

pub fn start_toplevel(name: &str) -> TraceBuilder {
    TraceBuilder::new(name.to_owned())
}

pub struct Trace {
    toplevel_span: Span,
}

pub struct Span {
    name: String,
    start_time: u64,
    end_time: u64,
    children: Vec<Span>,
}

impl Trace {
    pub fn print(&self) {
        Trace::_print(&self.toplevel_span, 0, self.toplevel_span.start_time);
    }

    fn _print(span: &Span, depth: usize, offset: u64) {
        println!("{}{} {} [{}, {}]",
                 " ".repeat(depth * 2),
                 span.name,
                 Trace::format_duration(span.end_time - span.start_time),
                 span.start_time - offset,
                 span.end_time - offset);
        for child in &span.children {
            Trace::_print(child, depth + 1, offset);
        }
    }

    fn format_duration(ns: u64) -> String {
        if ns < 10_000 {
            format!("{}ns", ns)
        } else if ns < 10_000_000 {
            format!("{}μs", ns / 1000)
        } else if ns < 10_000_000_000 {
            format!("{}ms", ns / 1_000_000)
        } else {
            format!("{}s", ns / 1_000_000_000)
        }
    }
}

