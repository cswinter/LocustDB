pub trait BitVecMut {
    fn set(&mut self, index: usize);
}

pub trait BitVec {
    fn is_set(&self, index: usize) -> bool;
}

impl BitVecMut for Vec<u8> {
    fn set(&mut self, index: usize) {
        let slot = index >> 3;
        while slot >= self.len() {
            self.push(0);
        }
        self[slot] |= 1 << (index as u8 & 7)
    }
}

impl BitVec for Vec<u8> {
    fn is_set(&self, index: usize) -> bool {
        let slot = index >> 3;
        slot < self.len() && self[slot] & (1 << (index as u8 & 7)) > 0
    }
}

impl<'a> BitVec for &'a [u8] {
    fn is_set(&self, index: usize) -> bool {
        let slot = index >> 3;
        slot < self.len() && self[slot] & (1 << (index as u8 & 7)) > 0
    }
}
