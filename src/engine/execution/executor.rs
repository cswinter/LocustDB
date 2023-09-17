use crate::bitvec::BitVec;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;

pub struct QueryExecutor<'a> {
    ops: Vec<Box<dyn VecOperator<'a> + 'a>>,
    stages: Vec<ExecutorStage>,
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
    pub fn set_buffer_count(&mut self, count: usize) {
        self.count = count
    }

    pub fn named_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        let buffer = TypedBufferRef::new(
            BufferRef {
                i: self.count,
                name,
                t: PhantomData,
            },
            tag,
        );
        self.count += 1;
        self.last_buffer = buffer;
        buffer
    }

    pub fn buffer_merge_op(&mut self, name: &'static str) -> BufferRef<MergeOp> {
        self.named_buffer(name, EncodingType::MergeOp)
            .merge_op()
            .unwrap()
    }

    pub fn buffer_premerge(&mut self, name: &'static str) -> BufferRef<Premerge> {
        self.named_buffer(name, EncodingType::Premerge)
            .premerge()
            .unwrap()
    }

    pub fn buffer_str(&mut self, name: &'static str) -> BufferRef<&'a str> {
        self.named_buffer(name, EncodingType::Str).str().unwrap()
    }

    pub fn buffer_str2(&mut self, name: &'static str) -> BufferRef<&'static str> {
        self.named_buffer(name, EncodingType::Str).str().unwrap()
    }

    pub fn buffer_usize(&mut self, name: &'static str) -> BufferRef<usize> {
        self.named_buffer(name, EncodingType::USize)
            .usize()
            .unwrap()
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

    pub fn buffer_scalar_i64(&mut self, name: &'static str) -> BufferRef<Scalar<i64>> {
        self.named_buffer(name, EncodingType::ScalarI64)
            .scalar_i64()
            .unwrap()
    }

    pub fn buffer_scalar_str<'b>(&mut self, name: &'static str) -> BufferRef<Scalar<&'b str>> {
        self.named_buffer(name, EncodingType::ScalarStr)
            .scalar_str()
            .unwrap()
    }

    pub fn buffer_scalar_string(&mut self, name: &'static str) -> BufferRef<Scalar<String>> {
        self.named_buffer(name, EncodingType::ScalarString)
            .scalar_string()
            .unwrap()
    }

    pub fn buffer_raw_val(&mut self, name: &'static str) -> BufferRef<RawVal> {
        self.named_buffer(name, EncodingType::Val)
            .raw_val()
            .unwrap()
    }

    pub fn shared_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        if self.shared_buffers.get(name).is_none() {
            let buffer = self.named_buffer(name, tag);
            self.shared_buffers.insert(name, buffer);
        }
        self.shared_buffers[name]
    }

    pub fn last_buffer(&self) -> TypedBufferRef {
        self.last_buffer
    }
    pub fn set_last_buffer(&mut self, buffer: TypedBufferRef) {
        self.last_buffer = buffer;
    }

    pub fn push(&mut self, op: Box<dyn VecOperator<'a> + 'a>) {
        self.ops.push(op);
    }

    pub fn prepare(&mut self, columns: HashMap<String, Vec<&'a dyn Data<'a>>>) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.count, columns)
    }

    pub fn prepare_no_columns(&mut self) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.count, HashMap::default())
    }

    pub fn run(
        &mut self,
        len: usize,
        scratchpad: &mut Scratchpad<'a>,
        show: bool,
    ) -> Result<(), QueryError> {
        for stage in 0..self.stages.len() {
            self.run_stage(len, stage, scratchpad, show)?;
        }
        Ok(())
    }

    #[allow(clippy::cognitive_complexity)]
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
        let mut dependencies_visited = vec![false; self.ops.len()];
        let mut topo_pushed = vec![false; self.ops.len()];
        let mut stage = vec![-1i32; self.ops.len()];
        let mut stages = vec![];
        loop {
            // Find an op that hasn't been assigned to a stage yet
            let mut to_visit = vec![];
            for (i, visited) in visited.iter_mut().enumerate() {
                if !*visited {
                    to_visit.push(i);
                    *visited = true;
                    break;
                }
            }
            if to_visit.is_empty() {
                break;
            }

            let mut ops = vec![];
            let mut stream = false;
            let mut transitive_input = vec![false; self.ops.len()];
            let mut transitive_output = vec![false; self.ops.len()];
            let mut consumers_to_revisit = vec![];
            let mut producers_to_revisit = vec![];
            while let Some(current) = to_visit.pop() {
                let op = &self.ops[current];
                let current_stage = stages.len() as i32;
                stage[current] = current_stage;
                ops.push(current);
                // Mark any new transitive inputs/outputs
                let mut inputs = vec![current];
                while let Some(op) = inputs.pop() {
                    if transitive_input[op] {
                        continue;
                    }
                    transitive_input[op] = true;
                    for input in self.ops[op].inputs() {
                        for &p in &producers[input.i] {
                            inputs.push(p);
                        }
                    }
                }
                let mut outputs = vec![current];
                while let Some(op) = outputs.pop() {
                    // Could do cycle detect here
                    if transitive_output[op] {
                        continue;
                    }
                    transitive_output[op] = true;
                    for output in self.ops[op].outputs() {
                        for &c in &consumers[output.i] {
                            outputs.push(c);
                        }
                    }
                }

                // Find producers that can be streamed
                for input in op.inputs() {
                    if op.can_stream_input(input.i) {
                        'l1: for &p in &producers[input.i] {
                            if visited[p] {
                                continue;
                            }
                            let can_stream =
                                self.ops[p].can_stream_output(input.i) && !streaming_disabled[p];
                            if !can_stream {
                                continue;
                            }
                            // Including op in this stage would introduce a cycle if any of the
                            // outputs is consumed by a transitive input to stage
                            for output in self.ops[p].outputs() {
                                for &c in &consumers[output.i] {
                                    if transitive_input[c] && stage[c] != current_stage {
                                        producers_to_revisit.push(p);
                                        continue 'l1;
                                    }
                                }
                            }

                            visited[p] = true;
                            to_visit.push(p);
                            stream = stream || self.ops[p].allocates();
                        }
                    }
                }
                // Find consumers that can be streamed to
                for output in op.outputs() {
                    if op.can_stream_output(output.i) && !streaming_disabled[current] {
                        'l2: for &consumer in &consumers[output.i] {
                            if visited[consumer] {
                                continue;
                            }
                            if !self.ops[consumer].can_stream_input(output.i) {
                                continue;
                            }
                            // Including op in this stage would introduce a cycle if any of the
                            // inputs is produces by a transitive output to stage
                            for input in self.ops[consumer].inputs() {
                                for &p in &producers[input.i] {
                                    if transitive_output[p] && stage[p] != current_stage {
                                        consumers_to_revisit.push(consumer);
                                        continue 'l2;
                                    }
                                }
                            }

                            visited[consumer] = true;
                            to_visit.push(consumer);
                            stream = stream || self.ops[consumer].allocates();
                        }
                    }
                }

                // Maybe some of the operations that were excluded because they would introduce
                // cycle are now possible to include because additional operations are part of
                // the stage.
                if to_visit.is_empty() {
                    let ptr = std::mem::take(&mut producers_to_revisit);
                    'l3: for p in ptr {
                        for output in self.ops[p].outputs() {
                            for &c in &consumers[output.i] {
                                if transitive_input[c] && stage[c] != current_stage {
                                    producers_to_revisit.push(p);
                                    continue 'l3;
                                }
                            }
                        }
                        visited[p] = true;
                        to_visit.push(p);
                        stream = stream || self.ops[p].allocates();
                    }
                    let ctr = std::mem::take(&mut consumers_to_revisit);
                    'l4: for consumer in ctr {
                        for input in self.ops[consumer].inputs() {
                            for &p in &producers[input.i] {
                                if transitive_output[p] && stage[p] != current_stage {
                                    consumers_to_revisit.push(consumer);
                                    continue 'l4;
                                }
                            }
                        }
                        visited[consumer] = true;
                        to_visit.push(consumer);
                        stream = stream || self.ops[consumer].allocates();
                    }
                }
            }

            // Topological sort of ops
            let mut total_order = vec![];
            while let Some(op) = ops.pop() {
                if !dependencies_visited[op] {
                    dependencies_visited[op] = true;
                    ops.push(op);
                    for input in self.ops[op].inputs() {
                        for &parent in &producers[input.i] {
                            if stage[parent] == stages.len() as i32 {
                                if parent == op {
                                    panic!("TRIVIAL CYCLE: {}", self.ops[op].display(true));
                                }
                                ops.push(parent);
                            }
                        }
                        if self.ops[op].mutates(input.i) {
                            for &consumer in &consumers[input.i] {
                                if consumer != op && stage[consumer] == stages.len() as i32 {
                                    ops.push(consumer);
                                }
                            }
                        }
                    }
                } else if !topo_pushed[op] {
                    topo_pushed[op] = true;
                    total_order.push(op);
                }
            }

            // Determine if stage/ops should be streaming
            let mut has_streaming_producer = false;
            let ops = total_order
                .into_iter()
                .map(|op| {
                    has_streaming_producer |= self.ops[op].is_streaming_producer();
                    let mut streaming_consumers = false;
                    let mut block_consumers = false;
                    for output in self.ops[op].outputs() {
                        if self.ops[op].can_stream_output(output.i) {
                            for &consumer in &consumers[output.i] {
                                streaming_consumers |=
                                    self.ops[consumer].can_stream_input(output.i);
                                block_consumers |= !self.ops[consumer].can_stream_input(output.i);
                            }
                        }
                    }
                    (op, streaming_consumers && !block_consumers)
                })
                .collect::<Vec<_>>();

            // TODO(#98): Make streaming possible for stages reading from temp results
            stages.push(ExecutorStage {
                ops,
                stream: stream && has_streaming_producer,
            })
        }

        // ## Topological Sort of Stages ##
        // Determine what stage each operation is in
        let mut stage_for_op = vec![0; self.ops.len()];
        for (i, stage) in stages.iter().enumerate() {
            for (op, _) in &stage.ops {
                stage_for_op[*op] = i;
            }
        }

        // Determine what stages each stage depends on
        let mut dependencies = Vec::new();
        for (istage, stage) in stages.iter().enumerate() {
            let mut deps = HashSet::new();
            for &(op, _) in &stage.ops {
                for input in self.ops[op].inputs() {
                    for &producer in &producers[input.i] {
                        if stage_for_op[producer] != istage {
                            deps.insert(stage_for_op[producer]);
                        }
                    }
                    if self.ops[op].mutates(input.i) {
                        for &consumer in &consumers[input.i] {
                            if stage_for_op[consumer] != istage {
                                deps.insert(stage_for_op[consumer]);
                            }
                        }
                    }
                }
            }
            dependencies.push(deps);
        }

        let mut visited = vec![false; stages.len()];
        let mut committed = vec![false; stages.len()];
        let mut total_order = Vec::new();
        fn visit(
            stage_index: usize,
            dependencies: &[HashSet<usize>],
            stages: &[ExecutorStage],
            visited: &mut Vec<bool>,
            committed: &mut Vec<bool>,
            total_order: &mut Vec<ExecutorStage>,
        ) {
            if committed[stage_index] {
                return;
            }
            trace!(">>> Visit {}", stage_index);
            if visited[stage_index] {
                error!("CYCLE DETECTED");
                for (i, _) in stages.iter().enumerate() {
                    error!("Stage {} Dependencies {:?}", i, &dependencies[i]);
                }
                return;
            }
            visited[stage_index] = true;
            for &dependency in &dependencies[stage_index] {
                visit(
                    dependency,
                    dependencies,
                    stages,
                    visited,
                    committed,
                    total_order,
                );
            }
            trace!(">>> Commit {}", stage_index);
            committed[stage_index] = true;
            total_order.push(stages[stage_index].clone());
        }
        stages.iter().enumerate().for_each(|(i, stage)| {
            trace!(">>> Stage {}", i);
            trace!("Dependencies: {:?}", &dependencies[i]);
            for (op, _) in &stage.ops {
                trace!("{}", &self.ops[*op].display(true));
            }
            visit(
                i,
                &dependencies,
                &stages,
                &mut visited,
                &mut committed,
                &mut total_order,
            )
        });
        total_order
    }

    fn init_stage(
        &mut self,
        column_length: usize,
        stage: usize,
        scratchpad: &mut Scratchpad<'a>,
    ) -> (usize, usize) {
        trace!("INITIALIZING STAGE {}", stage);
        let mut max_input_length = 0;
        let mut has_streaming_producer = false;
        for &(op, s) in &self.stages[stage].ops {
            has_streaming_producer |= self.ops[op].is_streaming_producer();
            trace!("{}, streamable={}", &self.ops[op].display(true), s);
            for input in self.ops[op].inputs() {
                max_input_length = cmp::max(max_input_length, scratchpad.get_any(input).len());
                trace!("{}: {}", input.i, scratchpad.get_any(input).len());
            }
        }
        // TODO(#98): once we can stream from intermediary results this will be overestimate
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

    fn run_stage(
        &mut self,
        column_length: usize,
        stage: usize,
        scratchpad: &mut Scratchpad<'a>,
        show: bool,
    ) -> Result<(), QueryError> {
        let (max_length, batch_size) = self.init_stage(column_length, stage, scratchpad);
        let stream = self.stages[stage].stream;
        if show {
            println!("\n-- Stage {} --", stage);
            println!(
                "batch_size: {}, max_length: {}, column_length: {}, stream: {}",
                batch_size, max_length, column_length, stream
            );
        }
        let mut has_more = true;
        let mut iters = 0;
        while has_more {
            has_more = false;
            for &(op, streamable) in &self.stages[stage].ops {
                self.ops[op].execute(stream && streamable, scratchpad)?;
                if show && iters == 0 {
                    println!("{}", self.ops[op].display(true));
                    for output in self.ops[op].outputs() {
                        let data = scratchpad.get_any(output);
                        println!("{}", data.display());
                        if let Some(present) = scratchpad.try_get_null_map(output) {
                            print!("null map: ");
                            for i in 0..cmp::min(present.len() * 8, 100) {
                                if (&*present).is_set(i) {
                                    print!("1")
                                } else {
                                    print!("0")
                                }
                            }
                            println!()
                        }
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
        Ok(())
    }
}

impl<'a> Default for QueryExecutor<'a> {
    fn default() -> QueryExecutor<'a> {
        QueryExecutor {
            ops: vec![],
            stages: vec![],
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
