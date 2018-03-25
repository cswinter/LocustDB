use std::sync::atomic::{AtomicU64, Ordering};
use time::precise_time_ns;

use super::THREAD_LOCAL_COLLECTOR;


static SPAN_COUNT: AtomicU64 = AtomicU64::new(1);

pub fn new_id() -> u64 {
    SPAN_COUNT.fetch_add(1, Ordering::SeqCst)
}

pub struct SpanCollector {
    pub start_spans: Vec<StartSpan>,
    pub finish_spans: Vec<FinishSpan>,
    pub active_spans: Vec<u64>,
}

impl SpanCollector {
    pub fn start_span(&mut self, name: String) -> SpanGuard {
        let parent_id = self.active_spans.last().map_or(0, |id| *id);
        let span = StartSpan::new(name, parent_id);
        let id = span.id;
        self.start(span);
        SpanGuard { id }
    }

    pub fn replace_span(&mut self, name: String) -> SpanGuard {
        self.finish_current_span();
        self.start_span(name)
    }

    pub fn finish_current_span(&mut self) {
        if let Some(id) = self.active_spans.pop() {
            self.finish(FinishSpan {
                id,
                end_time: precise_time_ns(),
            })
        }
    }

    fn finish_span(&mut self, id: u64) {
        if self.active_spans.last() == Some(&id) {
            self.active_spans.pop();
            self.finish(FinishSpan {
                id,
                end_time: precise_time_ns(),
            });
        }
    }

    fn start(&mut self, span: StartSpan) {
        self.active_spans.push(span.id);
        self.start_spans.push(span);
    }

    fn finish(&mut self, span: FinishSpan) {
        self.finish_spans.push(span);
    }
}

impl Default for SpanCollector {
    fn default() -> SpanCollector {
        SpanCollector {
            start_spans: Vec::new(),
            finish_spans: Vec::new(),
            active_spans: Vec::new(),
        }
    }
}

pub struct StartSpan {
    pub name: String,
    pub id: u64,
    pub parent_id: u64,
    pub start_time_ns: u64,
}

impl StartSpan {
    fn new(name: String, parent_id: u64) -> StartSpan {
        let id = new_id();
        StartSpan {
            name,
            id,
            parent_id,
            start_time_ns: precise_time_ns(),
        }
    }
}

pub struct FinishSpan {
    pub id: u64,
    pub end_time: u64,
}

pub struct SpanGuard {
    id: u64,
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        THREAD_LOCAL_COLLECTOR.with(|sc| {
            sc.borrow_mut().finish_span(self.id);
        })
    }
}

