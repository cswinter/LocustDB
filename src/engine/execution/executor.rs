use crate::bitvec::BitVec;
use crate::engine::*;
use crate::ingest::raw_val::RawVal;
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;

use self::planner::BufferProvider;

pub struct QueryExecutor<'a> {
    ops: Vec<Box<dyn VecOperator<'a> + 'a>>,
    stages: Vec<ExecutorStage>,
    batch_size: usize,
    buffer_provider: BufferProvider,
}

#[derive(Default, Clone)]
struct ExecutorStage {
    // Vec<(index to op, streamable output)>
    ops: Vec<(usize, bool)>,
    stream: bool,
}

impl<'a> QueryExecutor<'a> {
    pub fn new(batch_size: usize, buffer_provider: BufferProvider) -> QueryExecutor<'a> {
        QueryExecutor {
            ops: vec![],
            stages: vec![],
            batch_size,
            buffer_provider,
        }
    }

    pub fn named_buffer(&mut self, name: &'static str, tag: EncodingType) -> TypedBufferRef {
        self.buffer_provider.named_buffer(name, tag)
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
        self.buffer_provider.shared_buffer(name, tag)
    }

    pub fn last_buffer(&self) -> TypedBufferRef {
        self.buffer_provider.last_buffer()
    }

    pub fn push(&mut self, op: Box<dyn VecOperator<'a> + 'a>) {
        self.ops.push(op);
    }

    pub fn prepare(&mut self, columns: HashMap<String, Vec<&'a dyn Data<'a>>>) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.buffer_provider.buffer_count(), columns)
    }

    pub fn prepare_no_columns(&mut self) -> Scratchpad<'a> {
        self.stages = self.partition();
        Scratchpad::new(self.buffer_provider.buffer_count(), HashMap::default())
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
    fn partition(&mut self) -> Vec<ExecutorStage> {
        /* Partitions the operators into stages which are executed as a group and stream intermediate results.
         * At a high level, we group operators into stages by finding the largest connected subgraphs of
         * operators that are connected by streaming inputs/outputs.
         * After determining stages and whether a stage should be streamed, we maybe have to insert additional
         * operators in-between stages to convert between stream and block inputs/outputs.
         *
         * An important edge case is when operator A has both a streaming input B and a non-streaming input C
         * which come from the same stage.
         * In this case, we cannot include A in the same stage as B and C because A cannot start until B has
         * produced all its output.
         * A <= B <- C
         * A <- C
         *
         * More complex example:
         * D <- C <= B <= A
         * D <- E <- A
         *
         *
         * Also note that an operator might have itself as an input if it mutates the buffer in place
         *
         * Notation:
         * A <- B (a is a consumer of b and can stream input from b)
         * A <= B (a is a consumer of b and cannot stream input from b)
         */

        // Construct execution graph
        let mut consumers = vec![vec![]; self.buffer_provider.buffer_count()];
        let mut producers = vec![vec![]; self.buffer_provider.buffer_count()];
        for (i, op) in self.ops.iter().enumerate() {
            for input in op.inputs() {
                consumers[input.i].push(i);
            }
            for output in op.outputs() {
                producers[output.i].push(i);
            }
        }

        // `block_output` keeps track of streaming operators that have only nonstreaming consumers and can produce block output when streaming inputs
        // we disable streaming output for these so we don't have to insert additional block -> stream conversion operators
        let mut block_output = vec![false; self.ops.len()];
        for (i, op) in self.ops.iter().enumerate() {
            if !op.can_block_output() {
                continue;
            }
            log::debug!("DETERMINING STREAMING FOR {}", op.display(true));
            for output in op.outputs() {
                log::debug!("  DETERMINING STREAMING FOR OUTPUT {}", output);
                if op.can_stream_output(output.i) {
                    log::debug!("  CHECKING STREAMABLE");
                    // Are there any ops consuming this output that are streaming?
                    let mut streaming = false;
                    // Are there any ops consuming this output that are not streaming?
                    let mut block = false;
                    log::debug!("  CONSUMERS: {:?}", &consumers[output.i]);
                    for &consumer in &consumers[output.i] {
                        streaming |= self.ops[consumer].can_stream_input(output.i);
                        block |= !self.ops[consumer].can_stream_input(output.i);
                        log::debug!(
                            "  CONSUMER: {} CAN STREAM INPUT: {}, STREAMING: {}, BLOCK: {}",
                            self.ops[consumer].display(true),
                            self.ops[consumer].can_stream_input(output.i),
                            streaming,
                            block
                        );
                    }
                    block_output[i] = !streaming & block;
                    log::debug!(
                        "  STREAMING: {}, BLOCK: {}, DISABLED: {}",
                        streaming,
                        block,
                        block_output[i]
                    );
                }
            }
        }

        // Group operators into stages
        // This is done by taking an operator, and transitively adding all operators that can be
        // streamed to/from this operator while checking for any constraints that would prevent streaming
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

            let mut ops_in_stage = vec![];
            // keeps track of whether any op in the stage allocates (in which case we enable potentially enable streaming)
            let mut stage_allocates = false;
            // current transitive closure of all ops of the stage and inputs to these ops (entire set of upstream ops)
            let mut transitive_input = vec![false; self.ops.len()];
            // current transitive closure of all ops of the stage and outputs from these ops (entire set of downstream ops)
            let mut transitive_output = vec![false; self.ops.len()];
            let mut consumers_to_revisit = vec![];
            let mut producers_to_revisit = vec![];
            while let Some(current) = to_visit.pop() {
                let op = &self.ops[current];
                stage_allocates |= op.allocates();
                let current_stage = stages.len() as i32;
                log::debug!("VISITING {} IN STAGE {}", op.display(true), current_stage);
                stage[current] = current_stage;
                ops_in_stage.push(current);

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

                // Find ops which produce a streamable input to this op and add them to this stage
                for input in op.inputs() {
                    if op.can_stream_input(input.i) {
                        'l1: for &p in &producers[input.i] {
                            if visited[p] || !self.ops[p].can_stream_output(input.i) {
                                continue;
                            }
                            // Including op in this stage could introduce a cycle between stages if any of the
                            // outputs is consumed by a transitive input to stage (avoids edge case 1)
                            //
                            // Example:
                            // op=A is only op in stage, p=C:
                            // A <= B <- C
                            // A <- C
                            // B is not (currently) in stage but is a transitive input that also has C as an input
                            // We can't include B in the stage because it has to fully run before A can run, but
                            // B also has to be in the same or later stage as C.
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
                            log::debug!(
                                "  ADDING STREAMING PRODUCER {} TO STAGE {}",
                                self.ops[p].display(true),
                                current_stage
                            );
                        }
                    }
                }
                // Find consumers that can be streamed to
                for output in op.outputs() {
                    if op.can_stream_output(output.i) && !block_output[current] {
                        'l2: for &consumer in &consumers[output.i] {
                            if visited[consumer] || !self.ops[consumer].can_stream_input(output.i) {
                                continue;
                            }
                            // Including op in this stage could introduce a cycle if any of the
                            // inputs is produced by a transitive output to stage
                            //
                            // Example:
                            // op=C is only op in stage, c=A:
                            // A <= B <- C
                            // A <- C
                            // B is not (currently) in stage but is a transitive output that also has A as an output
                            // We can't include A in the stage because it requires full completion of B which is a part
                            // or downstream of this stage.
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
                            log::debug!(
                                "  ADDING STREAMING CONSUMER {} TO STAGE {}",
                                self.ops[consumer].display(true),
                                current_stage
                            );
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
                        log::debug!(
                            "  ADDING PREVIOUSLY CYCLE EXCLUDED STREAMING PRODUCER {} TO STAGE {}",
                            self.ops[p].display(true),
                            current_stage
                        );
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
                        log::debug!(
                            "  ADDING PREVIOUSLY CYCLE EXCLUDED STREAMING CONSUMER {} TO STAGE {}",
                            self.ops[consumer].display(true),
                            current_stage
                        );
                    }
                }
            }

            // Topological sort of ops
            let mut total_order = vec![];
            while let Some(op) = ops_in_stage.pop() {
                if !dependencies_visited[op] {
                    dependencies_visited[op] = true;
                    ops_in_stage.push(op);
                    for input in self.ops[op].inputs() {
                        for &parent in &producers[input.i] {
                            if stage[parent] == stages.len() as i32 {
                                if parent == op {
                                    panic!("TRIVIAL CYCLE: {}", self.ops[op].display(true));
                                }
                                ops_in_stage.push(parent);
                            }
                        }
                        if self.ops[op].mutates(input.i) {
                            for &consumer in &consumers[input.i] {
                                if consumer != op && stage[consumer] == stages.len() as i32 {
                                    ops_in_stage.push(consumer);
                                }
                            }
                        }
                    }
                } else if !topo_pushed[op] {
                    topo_pushed[op] = true;
                    total_order.push(op);
                }
            }

            let mut has_streaming_operator = false;
            let ops = total_order
                .into_iter()
                .map(|op| {
                    let any_streaming_input = self.ops[op]
                        .inputs()
                        .iter()
                        .any(|input| self.ops[op].can_stream_input(input.i));
                    let streaming_producer = self.ops[op].is_streaming_producer();
                    has_streaming_operator |= any_streaming_input || streaming_producer;
                    (op, !block_output[op])
                })
                .collect::<Vec<_>>();

            stages.push(ExecutorStage {
                ops,
                stream: stage_allocates && has_streaming_operator,
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

        let mut stage_for_op = vec![0; self.ops.len()];
        for (i, stage) in total_order.iter().enumerate() {
            for (op, _) in &stage.ops {
                stage_for_op[*op] = i;
            }
        }

        // Insert operation to buffer output for any streaming op in a streaming stage that has non-streaming consumers
        let mut substitutions = vec![];
        let mut count = self.buffer_provider.buffer_count();
        let mut block_output_buffers = HashMap::new();
        let mut new_ops = vec![]; // new op and corresponding stage
        for (i, consumer) in self.ops.iter().enumerate() {
            for input in consumer.inputs() {
                if !consumer.can_stream_input(input.i) || !total_order[stage_for_op[i]].stream {
                    for &producer in &producers[input.i] {
                        if self.ops[producer].can_stream_output(input.i)
                            && !block_output[producer]
                            && total_order[stage_for_op[producer]].stream
                        {
                            substitutions.push((i, input.i));
                            if let Entry::Vacant(e) = block_output_buffers.entry(input.i) {
                                new_ops.push((
                                    self.buffer_provider.all_buffers[input.i].tag,
                                    input,
                                    stage_for_op[producer],
                                ));
                                e.insert(count);
                                count += 1
                            }
                            break;
                        }
                    }
                }
            }
        }
        for (tag, input, stage) in new_ops {
            let buffer = self.buffer_provider.named_buffer("block_buffer", tag);
            let op = vector_operator::operator::buffer(
                self.buffer_provider.all_buffers[input.i],
                buffer,
            )
            .unwrap();
            total_order[stage].ops.push((self.ops.len(), false));
            self.ops.push(op);
            stage_for_op.push(stage);
        }
        for (consumer, old_input) in substitutions {
            let new_input = *block_output_buffers.get(&old_input).unwrap();
            self.ops[consumer].update_input(old_input, new_input);
        }

        // Insert operation to stream input for any streaming op that has non-streaming producers
        // substitutions: (operation id, buffer id of input to replace)
        let mut substitutions = vec![];
        for (i, consumer) in self.ops.iter().enumerate() {
            if !total_order[stage_for_op[i]].stream {
                continue;
            }
            let mut already_substituted = vec![];
            for input in consumer.inputs() {
                if consumer.can_stream_input(input.i) && !already_substituted.contains(&input.i) {
                    for &producer in &producers[input.i] {
                        if !self.ops[producer].can_stream_output(input.i)
                            || block_output[producer]
                            || !total_order[stage_for_op[producer]].stream
                        {
                            substitutions.push((i, input.i));
                            already_substituted.push(input.i);
                            break;
                        }
                    }
                }
            }
        }
        for (i, input_i) in substitutions {
            let stage = stage_for_op[i];
            let input = self.buffer_provider.all_buffers[input_i];
            let buffer = self
                .buffer_provider
                .named_buffer("buffer_stream", input.tag);
            if input.tag.is_scalar() {
                continue;
            }
            let op = if input.tag == EncodingType::Null {
                vector_operator::operator::stream_null_vec(input.any(), buffer.any())
            } else if input.tag.is_nullable() {
                vector_operator::operator::stream_nullable(
                    input,
                    self.buffer_provider
                        .named_buffer("buffer_stream_data", input.tag),
                    self.buffer_provider.buffer_u8("buffer_stream_present"),
                    buffer,
                )
                .unwrap()
            } else {
                vector_operator::operator::stream(input, buffer).unwrap()
            };
            self.ops[i].update_input(input_i, buffer.buffer.i);
            total_order[stage].ops.insert(0, (self.ops.len(), true));
            self.ops.push(op);
            stage_for_op.push(stage);
        }

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
        // TODO: this may be overestimate, we may have inserted a StreamBuffer op to stream from a materialized filtered column that is smaller that the full column
        if has_streaming_producer {
            max_input_length = column_length;
        }
        let batch_size = if self.stages[stage].stream {
            self.batch_size
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
            println!("\n-- Stage {stage} --    batch_size: {batch_size}, max_length: {max_length}, column_length: {column_length}, stream: {stream}");
        }
        let mut has_more = true;
        let mut iters = 0;
        while has_more {
            has_more = false;
            for &(op, streamable) in &self.stages[stage].ops {
                if show && iters < 2 {
                    let types = self.ops[op]
                        .outputs()
                        .iter()
                        .map(|b| format!("{:?}", self.buffer_provider.all_buffers[b.i].tag))
                        .collect::<Vec<_>>()
                        .join(",");
                    println!(
                        "{}     streamable={streamable} types={types}",
                        self.ops[op].display(true)
                    );
                }
                self.ops[op].execute(stream && streamable, scratchpad)?;
                if show && iters < 2 {
                    for output in self.ops[op].outputs() {
                        let data = scratchpad.get_any(output);
                        println!("  {}", data.display());
                        if let Some(present) = scratchpad.try_get_null_map(output) {
                            print!("  null map: ");
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
