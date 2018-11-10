use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;

use engine::*;
use super::*;
use ingest::raw_val::RawVal;


pub struct QueryExecutor<'a> {
    ops: Vec<Box<VecOperator<'a> + 'a>>,
    ops_cache: HashMap<[u8; 16], TypedBufferRef>,
    stages: Vec<ExecutorStage>,
    encoded_group_by: Option<TypedBufferRef>,
    count: usize,
    last_buffer: TypedBufferRef,
    shared_buffers: HashMap<&'static str, TypedBufferRef>,
}

#[derive(Default, Clone)]
struct ExecutorStage {
    // Vec<(index to op, streamable output)>
    ops: Vec<(usize, bool)>,
    stream: bool,
}

impl<'a> QueryExecutor<'a> {
    pub fn named_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        let buffer = TypedBufferRef::new(BufferRef { i: self.count, name, t: PhantomData }, tag);
        self.count += 1;
        self.last_buffer = buffer;
        buffer
    }

    pub fn buffer_merge_op(&mut self, name: &'static str) -> BufferRef<MergeOp> {
        self.named_buffer(name, EncodingType::MergeOp).merge_op().unwrap()
    }

    pub fn buffer_premerge(&mut self, name: &'static str) -> BufferRef<Premerge> {
        self.named_buffer(name, EncodingType::Premerge).premerge().unwrap()
    }

    pub fn buffer_str(&mut self, name: &'static str) -> BufferRef<&'a str> {
        self.named_buffer(name, EncodingType::Str).str().unwrap()
    }

    pub fn buffer_usize(&mut self, name: &'static str) -> BufferRef<usize> {
        self.named_buffer(name, EncodingType::USize).usize().unwrap()
    }

    pub fn buffer_i64(&mut self, name: &'static str) -> BufferRef<i64> {
        self.named_buffer(name, EncodingType::I64).i64().unwrap()
    }

    pub fn buffer_u32(&mut self, name: &'static str) -> BufferRef<u32> {
        self.named_buffer(name, EncodingType::U32).u32().unwrap()
    }

    pub fn buffer_u8(&mut self, name: &'static str) -> BufferRef<u8> {
        self.named_buffer(name, EncodingType::U8).u8().unwrap()
    }

    pub fn buffer_raw_val(&mut self, name: &'static str) -> BufferRef<RawVal> {
        self.named_buffer(name, EncodingType::Val).raw_val().unwrap()
    }

    pub fn shared_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        if self.shared_buffers.get(name).is_none() {
            let buffer = self.named_buffer(name, tag);
            self.shared_buffers.insert(name, buffer);
        }
        self.shared_buffers[name]
    }

    pub fn last_buffer(&self) -> TypedBufferRef { self.last_buffer }

    pub fn push(&mut self, op: Box<VecOperator<'a> + 'a>) {
        self.ops.push(op);
    }

    pub fn set_encoded_group_by(&mut self, gb: TypedBufferRef) {
        self.encoded_group_by = Some(gb)
    }

    pub fn encoded_group_by(&self) -> Option<TypedBufferRef> { self.encoded_group_by }

    pub fn prepare(&mut self, columns: HashMap<String, Vec<&'a AnyVec<'a>>>) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.count, columns)
    }

    pub fn prepare_no_columns(&mut self) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.count, HashMap::default())
    }

    pub fn get(&self, signature: &[u8; 16]) -> Option<Box<QueryPlan>> {
        self.ops_cache.get(signature).map(|x| Box::new(QueryPlan::ReadBuffer(*x)))
    }

    pub fn cache_last(&mut self, signature: [u8; 16]) {
        self.ops_cache.insert(signature, self.last_buffer);
    }

    pub fn run(&mut self, len: usize, scratchpad: &mut Scratchpad<'a>, show: bool) {
        for stage in 0..self.stages.len() {
            self.run_stage(len, stage, scratchpad, show);
        }
    }

    // TODO(clemens): Make this nicer?
    #[allow(clippy::cyclomatic_complexity)]
    fn partition(&self) -> Vec<ExecutorStage> {
        // Construct execution graph
        let mut consumers = vec![vec![]; self.count];
        let mut producers = vec![vec![]; self.count];
        for (i, op) in self.ops.iter().enumerate() {
            for input in op.inputs() {
                consumers[input.i].push(i);
            }
            for output in op.outputs() {
                producers[output.i].push(i);
            }
        }

        // Disable streaming output for operators that have streaming + nonstreaming consumers
        let mut streaming_disabled = vec![false; self.ops.len()];
        for (i, op) in self.ops.iter().enumerate() {
            for output in op.outputs() {
                if op.can_stream_output(output.i) {
                    let mut streaming = false;
                    let mut block = false;
                    for &p in &consumers[output.i] {
                        streaming |= self.ops[p].can_stream_input(output.i);
                        block |= !self.ops[p].can_stream_input(output.i);
                    }
                    streaming_disabled[i] = streaming & block;
                }
            }
        }

        // Group operators into stages
        let mut visited = vec![false; self.ops.len()];
        let mut stages = vec![];
        loop {
            // Find an op that hasn't been assigned to a stage yet
            let mut to_visit = vec![];
            for (i, visited) in visited.iter_mut().enumerate() {
                if !*visited {
                    to_visit.push(i as usize);
                    *visited = true;
                    break;
                }
            }
            if to_visit.is_empty() { break; }

            let mut ops = vec![];
            let mut stream = false;
            while let Some(current) = to_visit.pop() {
                let op = &self.ops[current];
                ops.push(current);

                // Find producers that can be streamed
                for input in op.inputs() {
                    if op.can_stream_input(input.i) {
                        for &p in &producers[input.i] {
                            let can_stream =
                                self.ops[p].can_stream_output(input.i) && !streaming_disabled[p];
                            if !visited[p] && can_stream {
                                to_visit.push(p);
                                visited[p] = true;
                                stream = stream || self.ops[p].allocates();
                            }
                        }
                    }
                }
                // Find consumers that can be streamed to
                for output in op.outputs() {
                    if op.can_stream_output(output.i) && !streaming_disabled[current] {
                        for &consumer in &consumers[output.i] {
                            if !visited[consumer] && self.ops[consumer].can_stream_input(output.i) {
                                to_visit.push(consumer);
                                visited[consumer] = true;
                                stream = stream || self.ops[consumer].allocates();
                            }
                        }
                    }
                }
            }
            ops.sort();
            let mut has_streaming_producer = false;
            let ops = ops.into_iter().map(|op| {
                has_streaming_producer |= self.ops[op].is_streaming_producer();
                let mut streaming_consumers = false;
                let mut block_consumers = false;
                for output in self.ops[op].outputs() {
                    if self.ops[op].can_stream_output(output.i) {
                        for &consumer in &consumers[output.i] {
                            streaming_consumers |= self.ops[consumer].can_stream_input(output.i);
                            block_consumers |= !self.ops[consumer].can_stream_input(output.i);
                        }
                    }
                }
                (op, streaming_consumers && !block_consumers)
            }).collect();
            // TODO(clemens): Make streaming possible for stages reading from temp results
            stages.push(ExecutorStage { ops, stream: stream && has_streaming_producer })
        }

        // TODO(clemens): need some kind of "anti-dependency" or "consume" marker to enforce ordering of e.g. NonzeroCompact
        // ## Topological Sort ##
        // Determine what stage each operation is in
        let mut stage_for_op = vec![0; self.ops.len()];
        for (i, stage) in stages.iter().enumerate() {
            for (op, _) in &stage.ops {
                stage_for_op[*op] = i;
            }
        }

        // Determine what stages each stage depends on
        let mut dependencies = Vec::new();
        for stage in &stages {
            let mut deps = HashSet::new();
            for &(op, _) in &stage.ops {
                for input in self.ops[op].inputs() {
                    for &producer in &producers[input.i] {
                        deps.insert(stage_for_op[producer]);
                    }
                }
            }
            dependencies.push(deps);
        }

        let mut visited = vec![false; stages.len()];
        let mut total_order = Vec::new();
        fn visit(stage_index: usize,
                 dependencies: &[HashSet<usize>],
                 stage: &[ExecutorStage],
                 visited: &mut Vec<bool>,
                 total_order: &mut Vec<ExecutorStage>) {
            if visited[stage_index] { return; }
            visited[stage_index] = true;
            for &dependency in &dependencies[stage_index] {
                visit(dependency, dependencies, stage, visited, total_order);
            }
            total_order.push(stage[stage_index].clone());
        };
        stages.iter().enumerate().for_each(|(i, _)|
            visit(i, &dependencies, &stages, &mut visited, &mut total_order));
        total_order
    }

    fn init_stage(&mut self, column_length: usize, stage: usize, scratchpad: &mut Scratchpad<'a>) -> (usize, usize) {
        trace!("INITIALIZING STAGE {}", stage);
        let mut max_input_length = 0;
        let mut has_streaming_producer = false;
        for &(op, s) in &self.stages[stage].ops {
            has_streaming_producer |= self.ops[op].is_streaming_producer();
            trace!("{:?}, streamable={}", &self.ops[op], s);
            for input in self.ops[op].inputs() {
                max_input_length = cmp::max(max_input_length, scratchpad.get_any(input).len());
                trace!("{}: {}", input.i, scratchpad.get_any(input).len());
            }
        }
        // TODO(clemens): once we can stream from intermediary results this will be overestimate
        if has_streaming_producer {
            max_input_length = column_length;
        }
        let batch_size = if self.stages[stage].stream {
            1024
        } else {
            max_input_length
        };
        for &(op, streamable) in &self.stages[stage].ops {
            let buffer_length = if streamable {
                batch_size
            } else {
                self.ops[op].custom_output_len().unwrap_or(max_input_length)
            };
            self.ops[op].init(max_input_length, buffer_length, scratchpad);
        }
        (max_input_length, batch_size)
    }

    fn run_stage(&mut self, column_length: usize, stage: usize, scratchpad: &mut Scratchpad<'a>, show: bool) {
        let (max_length, batch_size) = self.init_stage(column_length, stage, scratchpad);
        let stream = self.stages[stage].stream;
        if show {
            println!("\n-- Stage {} --", stage);
            println!("batch_size: {}, max_length: {}, column_length: {}, stream: {}", batch_size, max_length, column_length, stream);
        }
        let mut has_more = true;
        let mut iters = 0;
        while has_more {
            has_more = false;
            for &(op, streamable) in &self.stages[stage].ops {
                self.ops[op].execute(stream && streamable, scratchpad);
                if show && iters == 0 {
                    println!("{}", self.ops[op].display(true));
                    for output in self.ops[op].outputs() {
                        let data = scratchpad.get_any(output);
                        println!("{}", data.display());
                    }
                }
                has_more |= self.ops[op].has_more() && stream;
            }
            iters += 1;
        }
        for &(op, _) in &self.stages[stage].ops {
            self.ops[op].finalize(scratchpad);
        }
        if show && iters > 1 {
            println!("\n[{} more iterations]", iters - 1);
        }
    }
}

impl<'a> Default for QueryExecutor<'a> {
    fn default() -> QueryExecutor<'a> {
        QueryExecutor {
            ops: vec![],
            ops_cache: HashMap::default(),
            stages: vec![],
            encoded_group_by: None,
            count: 0,
            last_buffer: TypedBufferRef::new(error_buffer_ref("ERROR"), EncodingType::Null),
            shared_buffers: HashMap::default(),
        }
    }
}

impl<'a> fmt::Display for QueryExecutor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let alternate = f.alternate();
        if alternate {
            write!(f, "### RAW OPS ###")?;
            for op in &self.ops {
                write!(f, "\n{}", op.display(alternate))?;
            }

            write!(f, "\n\n### STAGES ###")?;
        }
        for (i, stage) in self.stages.iter().enumerate() {
            if stage.stream {
                write!(f, "\n-- Stage {} (streaming) --", i)?;
            } else {
                write!(f, "\n-- Stage {} --", i)?;
            }
            for &(op, _) in &stage.ops {
                write!(f, "\n{}", self.ops[op].display(alternate))?;
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

