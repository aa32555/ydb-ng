use super::*;

/// Represents a compressed record stored on disk
#[derive(Debug, Clone)]
pub struct RawRec<'a> {
    pub(crate) header: rec_hdr,
    pub(crate) data: &'a [u8],
    pub(crate) offset: usize,
}

/// Represents an exapnded record with a key and a "data" section
#[derive(Debug, Clone)]
pub struct Rec<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) data: &'a [u8],
}

named_args!(pub(crate) record_header<'a>(offset: usize)<RawRec>,
       do_parse!(
           rsiz: le_u16 >>
           cmpc: le_u8  >>
           cmpc2: le_u8 >>
           data: take!(rsiz - mem::size_of::<rec_hdr>() as u16) >>
           (RawRec{ header: rec_hdr {rsiz, cmpc, cmpc2}, data, offset})
        )
);

impl<'a> RawRec<'a> {
    pub fn ptr(&self) -> BlkNum {
        let ret;
        let data = self.data;
        if data.len() == 4 {
            // This is a * record; transmute the data to be a little endian int
            let data = &self.data[self.data.len() - 4..self.data.len()];
            let block = unsafe {
                mem::transmute::<[u8; 4], u32>([data[0],
                                                       data[1],
                                                       data[2],
                                                       data[3],
            ])};
            ret = BlkNum::Block(block as usize);
        } else {
            // Fetch the tail end of the pointer, and transmute it
            let data = &self.data();
            let block = unsafe {
                mem::transmute::<[u8; 4], u32>([data[0],
                                                       data[1],
                                                       data[2],
                                                       data[3],
            ])};
            ret = BlkNum::Block(block as usize);
        }
        ret
    }

    pub fn data(&self) -> &[u8] {
        let mut offset = 0;
        while offset + 1 < self.data.len() {
            if self.data[offset] == 0 && self.data[offset + 1] == 0 {
                offset += 2;
                break;
            }
            offset += 1;
        }
        &self.data[offset..]
    }
}
