use std::cmp;
use std::fmt;

use engine::vector_op::*;
use engine::*;


pub struct QueryExecutor<'a> {
    ops: Vec<Box<VecOperator<'a> + 'a>>,
    stages: Vec<ExecutorStage>,
    encoded_group_by: Option<BufferRef>,
    filter: Filter,
    count: usize,
    last_buffer: BufferRef,
}

#[derive(Default)]
struct ExecutorStage {
    // Vec<(index to op, streamable output)>
    ops: Vec<(usize, bool)>,
    stream: bool,
}

impl<'a> QueryExecutor<'a> {
    pub fn named_buffer(&mut self, name: &'static str) -> BufferRef {
        let buffer = BufferRef(self.count, name);
        self.count += 1;
        self.last_buffer = buffer;
        buffer
    }

    pub fn last_buffer(&self) -> BufferRef { self.last_buffer }

    pub fn push(&mut self, op: Box<VecOperator<'a> + 'a>) {
        self.ops.push(op);
    }

    pub fn set_encoded_group_by(&mut self, gb: BufferRef) {
        self.encoded_group_by = Some(gb)
    }

    pub fn encoded_group_by(&self) -> Option<BufferRef> { self.encoded_group_by }

    pub fn filter(&self) -> Filter { self.filter }

    pub fn set_filter(&mut self, filter: Filter) {
        self.filter = filter;
    }

    pub fn prepare(&mut self) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.count)
    }

    pub fn run(&mut self, len: usize, scratchpad: &mut Scratchpad<'a>) {
        for stage in 0..self.stages.len() {
            self.run_stage(len, stage, scratchpad);
        }
    }

    fn partition(&self) -> Vec<ExecutorStage> {
        let mut consumes = vec![vec![]; self.count];
        let mut producers = vec![vec![]; self.count];
        for (i, op) in self.ops.iter().enumerate() {
            for input in op.inputs() {
                consumes[input.0].push(i);
            }
            for output in op.outputs() {
                producers[output.0].push(i);
            }
        }

        let mut visited = vec![false; self.ops.len()];
        let mut stages = vec![];
        loop {
            let mut to_visit = vec![];
            for i in 0..self.ops.len() {
                if !visited[i] {
                    to_visit.push(i as usize);
                    visited[i] = true;
                    break;
                }
            }
            if to_visit.is_empty() { break; }

            let mut ops = vec![];
            let mut stream = false;
            while let Some(current) = to_visit.pop() {
                let op = &self.ops[current];
                ops.push(current);

                if op.can_stream_input() {
                    let mut streaming_input = false;
                    let mut nonstreaming_input = false;
                    for input in op.inputs() {
                        // TODO(clemens): if a stage is streamed, any any of the inputs are (non-streamed) results need to break those up
                        for &p in &producers[input.0] {
                            let can_stream = self.ops[p].can_stream_output();
                            if !visited[p] && can_stream {
                                to_visit.push(p);
                                visited[p] = true;
                                stream = stream || self.ops[p].allocates();
                            }
                        }
                    }
                    assert!(!(streaming_input && nonstreaming_input),
                    "Streaming and nonstreaming inputs to {:?}:\n{:?}",
                    op,
                    op.inputs().iter().map(|i| &self.ops[i.0]).collect::<Vec<_>>())
                }
                if op.can_stream_output() {
                    for output in op.outputs() {
                        for &consumer in &consumes[output.0] {
                            if !visited[consumer] && self.ops[consumer].can_stream_input() {
                                to_visit.push(consumer);
                                visited[consumer] = true;
                                stream = stream || self.ops[consumer].allocates();
                            }
                        }
                    }
                }
            }
            ops.sort();
            let ops = ops.into_iter().map(|op| {
                if self.ops[op].can_stream_output() {
                    let mut streaming_consumers = false;
                    let mut block_consumers = false;
                    for output in self.ops[op].outputs() {
                        for &consumer in &consumes[output.0] {
                            streaming_consumers |= self.ops[consumer].can_stream_input();
                            block_consumers |= !self.ops[consumer].can_stream_input();
                        }
                    }
                    assert!(!streaming_consumers || !block_consumers);
                    (op, streaming_consumers)
                } else {
                    (op, false)
                }
            }).collect();
            // TODO(clemens): Make streaming possible for stages other than zero (Need to be able to consume fully computed TypedVec in streaming fashion)
            let stage0 = stages.len() == 0;
            stages.push(ExecutorStage { ops, stream: stream && stage0 })
        }
        stages
    }

    fn init_stage(&mut self, column_length: usize, stage: usize, scratchpad: &mut Scratchpad<'a>) -> (usize, usize) {
        trace!("INITIALIZING STAGE {}", stage);
        let mut max_length = 0;
        for &(op, s) in &self.stages[stage].ops {
            trace!("{:?}, streamable={}", &self.ops[op], s);
            for input in self.ops[op].inputs() {
                max_length = cmp::max(max_length, scratchpad.get_any(input).len());
                trace!("{}: {}", input.0, scratchpad.get_any(input).len());
            }
        }
        // TODO(clemens): this is kind of a hack, should have more rigorous way of figuring out when stage reads directly from columns
        if max_length == 0 {
            max_length = column_length;
        }
        let batch_size = if self.stages[stage].stream {
            1024
        } else {
            max_length
        };
        for &(op, streamable) in &self.stages[stage].ops {
            self.ops[op].init(max_length, if streamable { batch_size } else { max_length }, streamable, scratchpad);
        }
        (max_length, batch_size)
    }

    fn run_stage(&mut self, column_length: usize, stage: usize, scratchpad: &mut Scratchpad<'a>) {
        let (max_length, batch_size) = self.init_stage(column_length, stage, scratchpad);
        let iters = (max_length - 1) / batch_size + 1;
        let stream = self.stages[stage].stream;
        trace!("batch_size: {}, max_length: {}, column_length: {}, iters: {}", batch_size, max_length, column_length, iters);
        for i in 0..iters {
            for &(op, streamable) in &self.stages[stage].ops {
                //println!("{:?}", self.ops[op]);
                self.ops[op].execute(stream && streamable, scratchpad);
                if i + 1 == iters {
                    self.ops[op].finalize(scratchpad);
                }
            }
        }
    }
}

impl<'a> Default for QueryExecutor<'a> {
    fn default() -> QueryExecutor<'a> {
        QueryExecutor {
            ops: vec![],
            stages: vec![],
            encoded_group_by: None,
            filter: Filter::None,
            count: 0,
            last_buffer: BufferRef(0xdeadbeef, "ERROR"),
        }
    }
}

impl<'a> fmt::Display for QueryExecutor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for (i, stage) in self.stages.iter().enumerate() {
            if stage.stream {
                write!(f, "\n-- {} (streaming) --", i)?;
            } else {
                write!(f, "\n-- {} --", i)?;
            }
            for &(op, _) in &stage.ops {
                write!(f, "\n{}", self.ops[op].display())?;
                // if stage.stream && streamable {
                // write!(f, " (stream)")?;
                // }
            }
        }
        Ok(())
    }
}

