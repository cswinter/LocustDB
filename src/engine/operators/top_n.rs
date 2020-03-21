use crate::engine::*;
use std::cell::Ref;
use std::cmp;
use std::fmt;
use std::marker::PhantomData;

#[derive(Debug)]
pub struct TopN<T, C> {
    pub input: BufferRef<T>,
    pub indices: BufferRef<usize>,
    pub keys: BufferRef<T>,
    pub n: usize,
    pub last_index: usize,
    pub c: PhantomData<C>,
}

impl<'a, T: VecData<T> + 'a, C: Comparator<T> + fmt::Debug> VecOperator<'a> for TopN<T, C> {
    fn init(&mut self, _: usize, _: usize, scratchpad: &mut Scratchpad<'a>) {
        scratchpad.set(self.indices, Vec::with_capacity(self.n));
        scratchpad.set(self.keys, Vec::with_capacity(self.n));
    }

    fn execute(&mut self, _: bool, scratchpad: &mut Scratchpad<'a>) -> Result<(), QueryError> {
        let mut input = scratchpad.get(self.input);
        let mut indices = scratchpad.get_mut(self.indices);
        let mut keys = scratchpad.get_mut(self.keys);

        assert_eq!(indices.len(), keys.len());
        if indices.len() < indices.capacity() {
            let count = cmp::min(indices.capacity() - indices.len(), input.len());
            for (i, input) in input.iter().take(count).enumerate() {
                indices.push(self.last_index + i);
                keys.push(*input);
            }
            if indices.capacity() == indices.len() {
                // Could replace O(n log n) sort with O(n) heapify
                if C::is_less_than() {
                    indices.sort_unstable_by(|i, j| keys[*i].cmp(&keys[*j]).reverse());
                    keys.sort_unstable_by(|i, j| i.cmp(j).reverse());
                } else {
                    indices.sort_unstable_by(|i, j| keys[*i].cmp(&keys[*j]));
                    keys.sort_unstable();
                }
            }
            input = Ref::map(input, |x| &x[count..]);
            self.last_index += count;
        }

        assert!(indices.len() == indices.capacity() || input.len() == 0);
        for (i, &key) in input.iter().enumerate() {
            if C::cmp(key, keys[0]) {
                heap_replace::<_, C>(&mut keys, &mut indices, key, self.last_index + i, 0);
            }
        }
        self.last_index += input.len();
        Ok(())
    }

    fn finalize(&mut self, scratchpad: &mut Scratchpad<'a>) {
        let output = {
            let indices = scratchpad.get_mut(self.indices);
            let keys = scratchpad.get_mut(self.keys);
            let mut sort_indices = (0..keys.len()).collect::<Vec<usize>>();
            if C::is_less_than() {
                sort_indices.sort_unstable_by_key(|i| keys[*i]);
            } else {
                sort_indices.sort_unstable_by(|i, j| keys[*i].cmp(&keys[*j]).reverse());
            }
            let mut output = Vec::with_capacity(indices.len());
            for i in sort_indices {
                output.push(indices[i]);
            }
            output
        };
        scratchpad.set(self.indices, output);
    }

    fn inputs(&self) -> Vec<BufferRef<Any>> { vec![self.input.any()] }
    fn outputs(&self) -> Vec<BufferRef<Any>> { vec![self.indices.any()] }
    fn can_stream_input(&self, _: usize) -> bool { true }
    fn can_stream_output(&self, _: usize) -> bool { false }
    fn allocates(&self) -> bool { true }

    fn display_op(&self, _: bool) -> String {
        format!("top_n({})", self.input)
    }
}

#[inline]
fn heap_replace<T: PartialOrd + Copy, C: Comparator<T>>(keys: &mut [T], values: &mut [usize], key: T, value: usize, mut node: usize) {
    while 2 * node + 1 < keys.len() {
        let left_child = 2 * node + 1;
        let right_child = 2 * node + 2;
        if C::cmp(key, keys[left_child]) && (right_child >= keys.len() || C::cmp(keys[right_child], keys[left_child])) {
            keys[node] = keys[left_child];
            values[node] = values[left_child];
            node = left_child;
        } else if right_child < keys.len() && C::cmp(key, keys[right_child]) {
            keys[node] = keys[right_child];
            values[node] = values[right_child];
            node = right_child;
        } else {
            break;
        }
    }
    keys[node] = key;
    values[node] = value;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_replace() {
        let mut keys = vec![10_u32, 20, 10, 20, 30, 15, 10, 30];
        let mut indices = vec![0, 1, 5, 2, 3, 4, 6, 7];
        heap_replace::<_, CmpGreaterThan>(&mut keys, &mut indices, 3, 10, 0);
        assert_eq!(&keys, &[3, 20, 10, 20, 30, 15, 10, 30]);
        heap_replace::<_, CmpGreaterThan>(&mut keys, &mut indices, 25, 10, 0);
        assert_eq!(&keys, &[10, 20, 10, 20, 30, 15, 25, 30]);
    }
}
