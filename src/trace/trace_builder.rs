use std::collections::HashMap;

use super::THREAD_LOCAL_COLLECTOR;
use super::span_collector::new_id;
use super::{Trace, Span, SpanCollector};
use time::precise_time_ns;


pub struct TraceBuilder {
    id: u64,
    name: String,
    start_time: u64,
    finalized_collectors: Vec<SpanCollector>,
}

impl TraceBuilder {
    pub fn new(name: String) -> TraceBuilder {
        TraceBuilder {
            id: new_id(),
            name,
            start_time: precise_time_ns(),
            finalized_collectors: Vec::new(),
        }
    }

    pub fn activate(&self) {
        THREAD_LOCAL_COLLECTOR.with(|sc| {
            let mut sc = sc.borrow_mut();
            sc.active_spans.push(self.id);
        })
    }

    pub fn collect(&mut self) {
        let local_collector = THREAD_LOCAL_COLLECTOR.with(|sc| {
            sc.replace(SpanCollector::default())
        });
        self.finalized_collectors.push(local_collector);
    }

    pub fn finalize(self) -> Trace {
        let toplevel = self.create_toplevel();
        let id = self.id;
        let mut spans = self.reassemble_spans();
        spans.insert(id, (toplevel, 0));
        let mut toplevel = TraceBuilder::reconstruct_tree(spans);
        TraceBuilder::chronological_sort(&mut toplevel);
        Trace { toplevel_span: toplevel }
    }

    fn create_toplevel(&self) -> Span {
        Span {
            name: self.name.clone(),
            start_time: self.start_time,
            end_time: precise_time_ns(),
            children: Vec::new(),
        }
    }

    fn reassemble_spans(self) -> HashMap<u64, (Span, u64)> {
        let mut spans = HashMap::default();
        for collector in self.finalized_collectors {
            for span in collector.start_spans {
                spans.insert(span.id, (Span {
                    name: span.name.clone(),
                    start_time: span.start_time_ns,
                    end_time: 0,
                    children: Vec::new(),
                }, span.parent_id));
            }
            for span_end in collector.finish_spans {
                spans.get_mut(&span_end.id).unwrap().0.end_time = span_end.end_time;
            }
        }
        spans
    }

    fn reconstruct_tree(mut spans: HashMap<u64, (Span, u64)>) -> Span {
        let mut keys_sorted = spans.keys().cloned().collect::<Vec<_>>();
        keys_sorted.sort();
        for span_id in keys_sorted.iter().rev() {
            let (span, parent_id) = spans.remove(span_id).unwrap();
            if parent_id == 0 { return span; }
            spans.get_mut(&parent_id).unwrap().0.children.push(span);
        }
        unreachable!();
    }

    fn chronological_sort(span: &mut Span) {
        span.children.sort_by_key(|s| s.start_time);
        for child in &mut span.children {
            TraceBuilder::chronological_sort(child);
        }
    }
}
