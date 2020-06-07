use byteorder::{ByteOrder, LittleEndian};

pub struct LengthPrefixedVec {
    data: Vec<u8>,
}

impl LengthPrefixedVec {
    pub fn with_capacity(length: usize) -> LengthPrefixedVec {
        let mut data = Vec::with_capacity(length + 4);
        data.extend(vec![0; 4]);
        LengthPrefixedVec { data }
    }

    pub fn zeros(length: usize) -> LengthPrefixedVec {
        let mut data = vec![0; length + 4];
        LittleEndian::write_u32(&mut data, length as u32);
        LengthPrefixedVec { data }
    }

    pub fn length_prefixed_bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data[4..]
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data[4..]
    }

    pub fn extend<'a, T: IntoIterator<Item = &'a u8>>(&mut self, iter: T) {
        self.data.extend(iter);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }

    pub fn truncate(&mut self, new_length: usize) {
        self.data.truncate(new_length + 4);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }

    pub fn push(&mut self, value: u8) {
        self.data.push(value);
        let length = self.data.len() as u32;
        LittleEndian::write_u32(&mut self.data, length - 4);
    }
}
