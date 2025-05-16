use std::collections::HashMap;
use std::fmt::Display;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub struct SimpleTracer {
    open_spans: Vec<OpenSpan>,
}

#[derive(Debug)]
pub struct SimpleSpan {
    pub name: &'static str,
    pub duration: Duration,
    pub depth: usize,
    pub annotations: Vec<(String, String)>,
    pub children: Vec<SimpleSpan>,
}

#[derive(Debug)]
struct OpenSpan {
    pub name: &'static str,
    pub depth: usize,
    pub start_time: Instant,
    pub annotations: Vec<(String, String)>,
    pub children: Vec<SimpleSpan>,
}

pub struct SpanToken(&'static str);

impl Default for SimpleTracer {
    fn default() -> Self {
        SimpleTracer {
            open_spans: vec![OpenSpan {
                name: "",
                depth: 0,
                start_time: Instant::now(),
                annotations: Vec::new(),
                children: Vec::new(),
            }],
        }
    }
}

impl SimpleTracer {
    #[must_use]
    pub fn start_span(&mut self, name: &'static str) -> SpanToken {
        self.open_spans.push(OpenSpan {
            name,
            depth: self.open_spans.len() - 1,
            start_time: Instant::now(),
            annotations: Vec::new(),
            children: Vec::new(),
        });
        SpanToken(name)
    }

    pub fn end_span(&mut self, span_token: SpanToken) {
        assert!(
            self.open_spans.last().unwrap().name == span_token.0,
            "Span token mismatch"
        );
        let span = self.open_spans.pop().unwrap();
        let duration = span.start_time.elapsed();
        let span = SimpleSpan {
            name: span.name,
            duration,
            depth: span.depth,
            annotations: span.annotations,
            children: span.children,
        };
        self.open_spans.last_mut().unwrap().children.push(span);
    }

    pub fn annotate<S: Display>(&mut self, key: &'static str, value: S) {
        self.open_spans
            .last_mut()
            .unwrap()
            .annotations
            .push((key.to_string(), value.to_string()));
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
        for span in &self.open_spans.last().unwrap().children {
            result.push_str(&span.summary().to_string());
        }
        result
    }

    pub fn push_tracer(&mut self, mut tracer: SimpleTracer) {
        assert_eq!(tracer.open_spans.len(), 1);
        let mut children = tracer.open_spans.pop().unwrap().children;
        // Set depth of children to the depth of the current span + 1
        for child in children.iter_mut() {
            set_depth(child, self.open_spans.len());
        }
        self.open_spans.last_mut().unwrap().children.extend(children);
    }

    pub fn elapsed(&self) -> Duration {
        self.open_spans.last().unwrap().start_time.elapsed()
    }
}

fn set_depth(child: &mut SimpleSpan, depth: usize) {
    child.depth = depth;
    for child in child.children.iter_mut() {
        set_depth(child, depth + 1);
    }
}

impl SimpleSpan {
    fn summary(&self) -> String {
        let mut result = String::new();

        let indent = "  ".repeat(self.depth);

        result.push_str(&format!(
            "{}{}: {}\n",
            indent,
            self.name,
            format_duration(self.duration)
        ));

        for (key, value) in &self.annotations {
            result.push_str(&format!("{}  - {}: {}\n", indent, key, value));
        }

        if self.children.len() > 10 {
            let mut aggregated_spans: HashMap<&'static str, (Duration, usize)> = HashMap::new();
            for child in &self.children {
                let entry = aggregated_spans
                    .entry(child.name)
                    .or_insert((Duration::ZERO, 0));
                entry.0 += child.duration;
                entry.1 += 1;
            }

            let mut agg_spans: Vec<_> = aggregated_spans.into_iter().collect();
            agg_spans.sort_by(|a, b| a.0.cmp(b.0));

            for (name, (total_duration, count)) in agg_spans {
                let duration_str = format_duration(total_duration);
                if count > 1 {
                    result.push_str(&format!("{}  {}: {} (×{})\n", indent, name, duration_str, count));
                } else {
                    result.push_str(&format!("{}  {}: {}\n", indent, name, duration_str));
                }
            }
        } else {
            for child in &self.children {
                result.push_str(&child.summary().to_string());
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
