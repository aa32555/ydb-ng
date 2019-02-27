use super::*;

use self::rec::*;

#[derive(Debug, Clone, PartialEq)]
pub enum BlkNum {
    Unknown,
    NewBlock,
    Block(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BlkType {
    DirectoryTree,
    IndexBlock,
    DataBlock,
    LocalBitmap,
    MasterBitmap,
}

/// Represents a database block, trimmed to exactly fit the data in use
#[derive(Debug, Clone)]
pub struct Blk<'a>{
    header: blk_hdr,
    data: &'a [u8],
    blk_num: BlkNum,
    typ: BlkType,
}

named_args!(pub(crate) read_block(blk_num: BlkNum, typ: BlkType)<Blk>,
       do_parse!(
           bver: le_u16     >>
           filler: le_u8    >>
           levl: le_u8      >>
           bsiz: le_u32     >>
           tn: le_u64       >>
           data: take!(bsiz - mem::size_of::<blk_hdr>() as u32) >>
           (Blk{ header: blk_hdr{bver, filler, levl, bsiz, tn}, data,
               blk_num, typ})
        )
);

pub fn get_block<'a>(data: &[u8], blk_num: usize, typ: BlkType) -> Result<Blk, ValueError>  {
    let (_, b) = read_block(data, BlkNum::Block(blk_num), typ)?;
    Ok(b)
}

impl<'a> Blk<'a> {
    pub fn header(&self) -> &blk_hdr {
        &self.header
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn integ(&self, start: &[u8], queue: &IntegQueueType) -> Result<(), ValueError> {
        // We don't need to scan records for these types, but should verify the blocks they point
        // too
        if self.typ == BlkType::MasterBitmap || self.typ == BlkType::LocalBitmap {
            return Ok(());
        }
        let mut state = State{compression: 0, goal: start};
        let rc = RecordCursor::new(&self);
        //let mut next_goal = Vec::from(start);
        let mut goal = Vec::new();
        for (i, record) in rc.enumerate() {
            // This will check for block too short
            let record = record?;
            // Check for a datablock which has an empty compression count
            if self.typ == BlkType::DataBlock && i > 0 && record.header.cmpc == 0 {
                return Err(ValueError::from(RecordError::ZeroCompressionCount));
            }
            // Note down where we started so the next integ can compare
            let start = goal.clone();
            // Verify that this record sorts after the previous record
            let sorts = RecordCursor::compare(&record, &mut state);
            RecordCursor::expand_key(&record, &mut goal)?;
            // Note down where we end
            let end = goal.clone();
            if RecordCursor::compare_strings(&start, &end) == SortOrder::SortsAfter
                    && record.header.rsiz != 8 {
                return Err(ValueError::from(RecordError::IncorrectSort));
            }

            // If needed, add the pointer of this block to be scanned
            // TODO: we should detect loops
            //println!("Here! {:#?}", self);
            if self.typ == BlkType::IndexBlock || self.typ == BlkType::DirectoryTree {
                let mut queue = queue.write().unwrap();
                // TODO: we need to fetch the local bitmap for the next block to look at these
                // things
                let typ = match self.header.levl {
                    0 => BlkType::IndexBlock,
                    1 => {
                        if self.typ == BlkType::DirectoryTree {
                            BlkType::IndexBlock
                        } else {
                            BlkType::DataBlock
                        }
                    },
                    _ => BlkType::IndexBlock,
                };
                queue.push_back(IntegBlock {
                    blk_num: record.ptr(),
                    typ,
                    start: start,
                    end: end,
                });
            }
        }
        Ok(())
    }
}

pub struct RecordCursor<'a> {
    remaining_data: &'a [u8],
    current_offset: usize,
    blk: &'a Blk<'a>,
}

impl<'a> RecordCursor<'a> {
    pub fn new(block: &'a Blk) -> RecordCursor<'a> {
        RecordCursor {
            remaining_data: block.data,
            current_offset: mem::size_of::<blk_hdr>(),
            blk: block,
        }
    }

    /// Comparies a to b and returns SortsBefore if a occurs before b, SortsAfter if a occurs after
    /// b, or SortsEqual if they are the same string
    pub fn compare_strings(a: &'a [u8], b: &'a [u8]) -> SortOrder {
        let mut i = 0;
        while i < a.len() && i < b.len() {
            if a[i] < b[i] {
                return SortOrder::SortsBefore;
            } else if a[i] > b[i] {
                return SortOrder::SortsAfter;
            }
            i += 1;
        }
        if i < a.len() {
            return SortOrder::SortsAfter;
        }
        if i < b.len() {
            return SortOrder::SortsBefore;
        }
        return SortOrder::SortsEqual;
    }

    /// Copies new values from the raw record into the key. Must be called for every record to be
    /// accurate
    pub fn expand_key(record: &'a RawRec, key: &mut Vec<u8>) -> Result<(), ValueError> {
        let data = record.data;
        let mut i = 0;
        // This will overdo the size a bit, but that's OK
        //state.compression = record.header.cmpc as usize;
        let mut cmpc = record.header.cmpc as usize;
        key.resize(cmpc + 2 + record.header.rsiz as usize, 0);
        while i + 1 < data.len() && !(data[i] == 0 && data[i+1] == 0) {
            key[cmpc] = data[i];
            cmpc +=1;
            i += 1;
        }
        if i + 1 == data.len() {
            return Err(ValueError::RecordError(RecordError::NoTerminatingCharacter));
        }
        unsafe {
            key.set_len(cmpc + 2);
        }
        key[cmpc] = 0;
        key[cmpc+1] = 0;
        Ok(())
    }

    pub fn compare(record: &'a RawRec, state: &mut State) -> SortOrder {
        let goal = state.goal;
        // If the rsiz is 8, this is a * key and comes after everything
        if record.header.rsiz == 8 {
            return SortOrder::SortsAfter;
        }

        let compression_count = record.header.cmpc as usize;
        if compression_count < state.compression {
            //state.compression = compression_count;
            return SortOrder::SortsAfter
        }
        if compression_count == state.compression {
            let mut index = 0;
            let data = &record.data;
            let data_len = data.len();
            let goal_len = goal.len();
            while index < data_len && state.compression < goal_len
                && data[index] == goal[state.compression] {
                    state.compression += 1;
                    index += 1;
            }
            // Case 0: this is the record, keys equal
            if index == data_len && state.compression == goal_len {
                return SortOrder::SortsEqual;
            }
            // Case 1: the record key is shorter than goal
            if index == data_len {
                return SortOrder::SortsBefore;
            }
            // Case 2: the goal key is short than the record
            if state.compression == goal_len {
                return SortOrder::SortsAfter;
            }
            // Case 3: Same length, different value
            if data[index] > goal[state.compression] {
                return SortOrder::SortsAfter;
            }
        }
        SortOrder::SortsBefore
    }
}

impl<'a> Iterator for RecordCursor<'a> {
    type Item = Result<RawRec<'a>, ValueError>;

    fn next(&mut self) -> Option<Result<RawRec<'a>, ValueError>> {
        if self.remaining_data.len() == 0 {
            return None;
        }
        // This feels ugly; is there a cleanier way to do this?
        let next = record_header(self.remaining_data, self.current_offset);
        if next.is_err() {
            return Some(Err(ValueError::from(next.unwrap_err())));
        }
        let (rest, rec) = next.unwrap();
        self.remaining_data = rest;
        self.current_offset += rec.header.rsiz as usize;
        Some(Ok(rec))
    }
}

