use crate::engine::*;


#[derive(Debug)]
pub struct MapOperator<Input, Output, Map> {
    pub input: BufferRef<Input>,
    pub output: BufferRef<Output>,
    pub map: Map,
}

impl<'a, Input, Output, Map> VecOperator<'a> for MapOperator<Input, Output, Map>
    where Input: VecData<Input> + 'a,
          Output: VecData<Output> + 'a,
          Map: MapOp<Input, Output> {
    fn execute(&mut self, stream: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError>{
        let input = scratchpad.get(self.input);
        let mut output = scratchpad.get_mut(self.output);
        if stream { output.clear() }
        for i in input.iter() {
            output.push(self.map.apply(*i));
        }
        Ok(())
    }

    fn init(&mut self, _: usize, batch_size: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.output, Vec::with_capacity(batch_size));
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.output.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { true }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("{}({})", Map::name(), self.input)
    }
}

pub trait MapOp<Input, Output> {
    fn apply(&self, input: Input) -> Output;
    fn name() -> &'static str;
}