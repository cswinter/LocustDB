use std::collections::HashMap;
use std::fmt::Display;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct SimpleTracer {
    completed_spans: Vec<SimpleSpan>,
    span_stack: Vec<(&'static str, Instant, usize)>,
    annotations: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct SimpleSpan {
    pub name: String,
    pub duration: Duration,
    pub depth: usize,
    pub annotations: Vec<(String, String)>,
    pub child_count: usize,
}

pub struct SpanToken(&'static str);

impl SimpleTracer {
    pub fn new() -> Self {
        SimpleTracer {
            completed_spans: Vec::new(),
            span_stack: Vec::new(),
            annotations: Vec::new(),
        }
    }

    #[must_use]
    pub fn start_span(&mut self, full_name: &'static str) -> SpanToken {
        self.span_stack.push((full_name, Instant::now(), 0));
        SpanToken(full_name)
    }

    pub fn end_span(&mut self, span_token: SpanToken) {
        assert!(
            self.span_stack.last().unwrap().0 == span_token.0,
            "Span token mismatch"
        );
        let (_, start_time, child_count) = self.span_stack.pop().unwrap();
        if let Some((_, _, child_count)) = self.span_stack.last_mut() {
            *child_count += 1;
        }
        let duration = start_time.elapsed();
        self.completed_spans.push(SimpleSpan {
            name: span_token.0.to_string(),
            duration,
            depth: self.span_stack.len(),
            annotations: std::mem::take(&mut self.annotations),
            child_count,
        });
    }

    pub fn annotate<S: Display>(&mut self, key: &'static str, value: S) {
        self.annotations.push((key.to_string(), value.to_string()));
    }

    pub fn summary(&self) -> String {
        // Prints a summary of the spans in the format
        // Example of format:
        // wal_flush: 18.5s
        //   compaction: 10.0s
        //     string_interning: 5.0s
        //   metastore_serialization: 1.0s
        //     - total_bytes: 1000
        //     - column_names_bytes: 100
        //     string_interning: 0.5s
        //     string_sorting: 0.2s

        let mut result = String::new();

        // Process spans in reverse order
        let mut i = self.completed_spans.len();
        while i > 0 {
            i -= 1;
            let span = &self.completed_spans[i];
            let indent = "  ".repeat(span.depth);

            // Render span name, duration, and annotations
            result.push_str(&format!(
                "{}{}: {}\n",
                indent,
                span.name,
                format_duration(span.duration)
            ));
            for (key, value) in &span.annotations {
                result.push_str(&format!("{}  - {}: {}\n", indent, key, value));
            }

            // If span has more than 10 children, aggregate similar subspans
            if span.child_count > 10 {
                // Find and aggregate all children of this span
                let mut aggregated_spans: HashMap<String, (Duration, usize)> = HashMap::new();
                let mut path = Vec::new();
                let mut last_depth = span.depth;

                while i > 0 && self.completed_spans[i - 1].depth > span.depth {
                    i -= 1;
                    if last_depth == self.completed_spans[i].depth {
                        path.pop();
                    }
                    path.push(self.completed_spans[i].name.clone());
                    let name = path.join(".");
                    let entry = aggregated_spans.entry(name).or_insert((Duration::ZERO, 0));
                    entry.0 += self.completed_spans[i].duration;
                    entry.1 += 1;

                    last_depth = self.completed_spans[i].depth;
                }

                // Print aggregated children sorted by name
                let mut agg_spans: Vec<_> = aggregated_spans.into_iter().collect();
                agg_spans.sort_by(|a, b| a.0.cmp(&b.0));

                for (name, (total_duration, count)) in agg_spans {
                    let child_indent = "  ".repeat(span.depth + 1);
                    let duration_str = format_duration(total_duration);
                    if count > 1 {
                        result.push_str(&format!(
                            "{}{}: {} (× {})\n",
                            child_indent, name, duration_str, count
                        ));
                    } else {
                        result.push_str(&format!("{}{}: {}\n", child_indent, name, duration_str));
                    }
                }

                // Skip the children we've already summarized
                i -= span.child_count;
            }
        }

        result
    }
}
fn format_duration(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    let value: f64;
    let unit: &str;

    if nanos < 1_000 {
        return format!("{}ns", nanos);
    } else if nanos < 1_000_000 {
        value = nanos as f64 / 1_000.0;
        unit = "µs";
    } else if nanos < 1_000_000_000 {
        value = nanos as f64 / 1_000_000.0;
        unit = "ms";
    } else {
        value = duration.as_secs_f64();
        unit = "s";
    }

    // Calculate number of decimal places needed for 3 significant figures
    let digits_before_decimal = (value.log10().floor() + 1.0) as i32;
    let decimal_places = if digits_before_decimal >= 3 {
        0
    } else {
        3 - digits_before_decimal
    };

    match decimal_places {
        0 => format!("{:.0}{}", value, unit),
        1 => format!("{:.1}{}", value, unit),
        2 => format!("{:.2}{}", value, unit),
        _ => format!("{:.3}{}", value, unit),
    }
}
