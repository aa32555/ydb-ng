#[macro_use]
extern crate serde;
extern crate bincode;
#[macro_use]
extern crate nom;

extern crate ydb_ng_bridge;
use serde::{Serialize, Deserialize};

use std::io::Read;
use std::mem;
use std::slice;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::iter::FromIterator;
use std::io::{Error, ErrorKind};
use std::fs::OpenOptions;
use bincode::serialize;
use nom::{le_u8, le_u16};

use ydb_ng_bridge::ydb::{sgmnt_data_struct, blk_hdr, rec_hdr};

static PHYSICAL_DATABASE_BLOCK_SIZE: i32 = 512;

#[derive(Debug, Clone)]
pub struct Rec<'a> {
    header: rec_hdr,
    data: &'a [u8]
}

named!(record_header<&[u8], Rec>,
       do_parse!(
           rsiz: le_u16 >>
           cmpc: le_u8  >>
           cmpc2: le_u8 >>
           data: take!(rsiz - 4) >>
           (Rec{ header: rec_hdr {rsiz, cmpc, cmpc2}, data})
        )
);

pub struct Database {
    pub fhead: sgmnt_data_struct,
    pub master_bitmap: [u8; 253952],
    pub handle: File,
}

#[derive(Debug, Clone)]
pub struct Block {
    header: blk_hdr,
    blk_num: usize,
    data: Box<[u8]>,
}

#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
pub struct Record {
    pub length: u16,
    pub compression_count: u8,
    pub filler: u8,
    pub data: Box<[u8]>
}

#[derive(Debug, Clone)]
struct State {
    compression: usize,
    matched_so_far: Vec<u8>
}

#[derive(Debug)]
pub enum ValueError {
    IoError(std::io::Error),
    RecordError(RecordError),
    GlobalNotFound,
    SubscriptNotFound,
    MalformedRecord,
}

#[derive(Debug)]
pub enum RecordError {
    IoError(std::io::Error),
    TooBig,
    LengthZero,
}

pub struct RecIterator<'a> {
    data: &'a [u8],
    block: &'a Block,
}

pub struct RecordIterator {
    data: Box<[u8]>,
    offset: usize,
}


impl From<std::io::Error> for ValueError {
    fn from(error: std::io::Error) -> Self {
        ValueError::IoError(error)
    }
}

impl From<RecordError> for ValueError {
    fn from(error: RecordError) -> Self {
        match error {
            RecordError::IoError(x) => ValueError::IoError(x),
            x => ValueError::RecordError(x),
        }
    }
}

impl Record {
    // Inteprets this record as a key-value; there should be some
    //  kind of indication as to whether this is correct or not somewhere,
    //  but I'm not sure what it is
    fn ptr(&self) -> usize {
        let mut key_len = 0;
        if self.data.len() != 4 {
            loop {
                if self.data[key_len] == 0 && self.data[key_len + 1] == 0 {
                    break;
                }
                key_len += 1;
            }
            key_len += 2;
        }
        let mut index: [u8; 4] = unsafe { mem::zeroed() };
        index[..4].clone_from_slice(&self.data[key_len..key_len+4]);
        let index: usize = unsafe { mem::transmute::<[u8; 4], u32>(index) } as usize;
        return index;
    }

    fn data(&self) -> Vec<u8> {
        let data_len = self.data.len();
        let mut key_len = 0;
        loop {
            if self.data[key_len] == 0 && self.data[key_len + 1] == 0 {
                break;
            }
            key_len += 1;
        }
        key_len += 2;
        let mut ret = vec![0; data_len - key_len];
        for (i, v) in self.data[key_len..].iter().enumerate() {
            ret[i] = *v;
        }
        return ret;
    }

    /// Creates a new record given a compression state, key, and value
    fn make(state: &State, key: &Vec<u8>, value: Vec<u8>) -> Record {
        // There is an assumption here that this key will always sort after the last record in
        // state
        let mut compression_count = 0;
        while compression_count < key.len() && compression_count < state.compression
            && key[compression_count] == state.matched_so_far[compression_count] {
            compression_count += 1;
        }
        let mut data = Vec::from(&key[compression_count..]);
        data.append(&mut vec![0, 0]);
        let mut value = value.clone();
        data.append(&mut value.clone());
        let len = data.len() + mem::size_of::<rec_hdr>();
        Record{
            length: len as u16,
            compression_count: compression_count as u8,
            filler: 0,
            data: data.into_boxed_slice(),
        }
    }

    fn into_boxed_slice(self) -> std::io::Result<Vec<u8>> {
        println!("Making record value");
        let mut ret = Vec::with_capacity(self.length as usize);
        ret.write(&self.length.to_le_bytes())?;
        ret.write(&self.compression_count.to_le_bytes())?;
        ret.write(&self.filler.to_le_bytes())?;
        ret.write(&self.data)?;
        return Ok(ret);
    }
}

impl<'a> Iterator for RecIterator<'a> {
    type Item = Result<Rec<'a>, RecordError>;

    fn next(&mut self) -> Option<Result<Rec<'a>, RecordError>> {
        let (rest, rec) = record_header(self.data).unwrap();
        self.data = rest;
        Some(Ok(rec))
    }
}

impl Iterator for RecordIterator {
    type Item = Result<Record, RecordError>;

    fn next(&mut self) -> Option<Result<Record, RecordError>> {
        // Get a pointer to where we left off in the block
        let mut data = &self.data[self.offset..];
        let data = data.by_ref();

        let mut rec_size: [u8; 2] = unsafe {mem::zeroed() };
        if data.read_exact(&mut rec_size).is_err() {
            // If there is no more to read from the block, we are done here
            return None;
        }
        let rec_size = unsafe { mem::transmute::<[u8; 2], u16>(rec_size) };
        if rec_size == 0 {
            // If the rec_size is 0, there are no more records in the block
            return None;
        }
        self.offset += rec_size as usize;
        let mut rec_compression_count: [u8; 1] = unsafe { mem::zeroed() };
        if data.read_exact(&mut rec_compression_count).is_err() {
            return Some(Err(RecordError::TooBig));
        };
        let mut junk: [u8; 1] = unsafe { mem::zeroed() };
        if data.read_exact(&mut junk).is_err() {
            return Some(Err(RecordError::TooBig));
        };
        let content_length = rec_size - 4;
        let mut content = vec![0; content_length  as usize];
        if data.read_exact(&mut content.as_mut_slice()).is_err() {
            return Some(Err(RecordError::TooBig));
        };
        let ret = Record{
            length: rec_size,
            compression_count: rec_compression_count[0],
            filler: junk[0],
            data: content.into_boxed_slice(),
        };
        Some(Ok(ret))
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[repr(C)]
struct BlkHdrDef {
    bver: u16,
    filler: u8,
    levl: u8,
    bsiz: u32,
    tn: u64,
}

impl Database {
    // We should check some sort of cache here for the block
    pub fn get_block(&self, blk_num: usize) -> std::io::Result<Block> {
        //let mut ret = vec!([0; fhead.blk_size]);
        let mut handle = self.handle.try_clone()?;
        let blk_size = self.fhead.blk_size as usize;
        let mut raw_block = vec![0; blk_size];
        handle.seek(SeekFrom::Start(
            (((self.fhead.start_vbn - 1) * PHYSICAL_DATABASE_BLOCK_SIZE) as usize
             + blk_size * blk_num) as u64)
        )?;
        handle.read_exact(&mut raw_block)?;
        let mut block = raw_block.as_slice();
        let mut block_header: blk_hdr = unsafe { mem::zeroed() };
        let buffer_size = mem::size_of::<blk_hdr>();
        unsafe {
            let block_header_slice = slice::from_raw_parts_mut(
                &mut block_header as *mut _ as *mut u8,
                buffer_size
            );
            block.read_exact(block_header_slice).unwrap();
        }
        let data_portion = Vec::from_iter(raw_block[buffer_size..].iter().cloned());
        let block = Block{
            header: block_header,
            blk_num: blk_num,
            data: data_portion.into_boxed_slice(),
        };
        Ok(block)
    }

    pub fn write_block(&mut self, old_blk_hdr: &blk_hdr, blk_num: usize, new_value: Vec<u8>) -> std::io::Result<()> {
        let mut handle = self.handle.try_clone()?;
        let blk_size = self.fhead.blk_size as usize;
        let blk_header = BlkHdrDef {
            bver: old_blk_hdr.bver,
            filler: 0,
            levl: old_blk_hdr.levl,
            bsiz: (new_value.len() + mem::size_of::<blk_hdr>()) as u32,
            tn: old_blk_hdr.tn + 1,
        };
        handle.seek(SeekFrom::Start(
            (((self.fhead.start_vbn - 1) * PHYSICAL_DATABASE_BLOCK_SIZE) as usize
             + blk_size * blk_num) as u64)
        )?;
        handle.write(&serialize(&blk_header).unwrap())?;
        handle.write(&new_value)?;
        println!("New value written for block {}! Length is {}", blk_num, blk_header.bsiz);
        Ok(())
    }

    fn make_global_block(&mut self, key: &Vec<u8>) -> Result<Block, ValueError> {
        let block = self.get_block(10)?;
        panic!("Not implemented!");
        //Ok(block)
    }

    pub fn set_value(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), ValueError> {
        let mut new_value = Vec::with_capacity(value.len() + mem::size_of::<rec_hdr>());
        // Get the block the value should exist in; if we can't find it, create these blocks
        let value_block = match self.find_block(&key) {
            Ok(x) => x,
            Err(ValueError::GlobalNotFound) => self.make_global_block(&key)?,
            x => x?,
        };
        let old_blk_hdr = value_block.header.clone();
        let old_blk_num = value_block.blk_num;
        // Go through each record in this block until we get the value or one that is after
        let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
        let mut found = false;
        let mut previous_state = state.clone();
        let mut iter = value_block.into_iter();
        for record in &mut iter {
            let record = record?;
            previous_state = state.clone();
            let cmp = compare(&mut state, &record, key);
            if cmp == SortOrder::SortsAfter {
                break;
            } else if cmp == SortOrder::SortsEqual {
                found = true;
                break;
            }
            println!("Writing old value");
            new_value.write(&record.into_boxed_slice()?)?;
        }
        // Place the new record
        if !found {
            state = previous_state;
        }
        // TODO: we need to check for space and allocate a new record, if needed
        let record = Record::make(&state, &key, value);
        println!("New record: {:#?}", record);
        //new_value.write(&serialize(&record).unwrap())?;
        new_value.write(&record.into_boxed_slice()?);
        // Skip the old value, if it was found
        if found {
            iter.next();
        }
        for record in iter {
            // Place each of the remaining records
            let record = record?;
            new_value.write(&record.into_boxed_slice()?)?;
        }
        // Finally, write the value
        println!("Value: {:#?}", String::from_utf8_lossy(&new_value));
        self.write_block(&old_blk_hdr, old_blk_num, new_value)?;
        Ok(())
    }

    pub fn find_block(&self, item: &Vec<u8>) -> Result<Block, ValueError> {
        let root_block = self.get_block(1)?;
        // Scan through the root block until we find one with key
        // greater than the value we are looking for
        let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
        let mut next_block = 0;
        let mut global_end = 0;
        let mut found = false;
        while global_end < item.len() && item[global_end] != 0 {
            global_end += 1;
        }
        println!("Searching block {}", next_block);
        let global = &item[0..global_end];
        for record in root_block.into_iter() {
            let record = record?;
            found = match compare(&mut state, &record, global) {
                SortOrder::SortsAfter => true,
                _ => false,
            };
            if found == true {
                next_block = record.ptr();
                break;
            }
        }
        if !found {
            return Err(ValueError::MalformedRecord);
        }

        println!("Searching block {}", next_block);
        let gvt_block = self.get_block(next_block)?;
        let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
        let mut next_block = 0;
        found = false;
        for record in gvt_block.into_iter() {
            let record = record?;
            found = match compare(&mut state, &record, global) {
                SortOrder::SortsAfter => true,
                SortOrder::SortsEqual => true,
                _ => false,
            };
            if found == true {
                next_block = record.ptr();
                break;
            }
        }
        if !found {
            return Err(ValueError::GlobalNotFound);
        }

        loop {
            // Now scan for the specific global we are after
            println!("Searching block {}", next_block);
            let gvt_block = self.get_block(next_block)?;
            println!("Next block level {}", gvt_block.header.levl);
            if gvt_block.header.levl == 0 {
                return Ok(gvt_block);
            }

            let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
            found = false;
            for record in gvt_block.into_iter() {
                let record = record?;
                found = match compare(&mut state, &record, item) {
                    SortOrder::SortsAfter => true,
                    SortOrder::SortsEqual => true,
                    _ => false,
                };
                if found == true {
                    next_block = record.ptr();
                    break;
                }
            }
            if !found {
                break;
            }
        }
        Err(ValueError::SubscriptNotFound)
    }

    /// Searches block for item, and return the value or not found
    pub fn find_value(&self, item: &Vec<u8>, block: Block) -> Result<Vec<u8>, ValueError> {
        let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
        for record in block.into_iter() {
            let record = record?;
            let found = compare(&mut state, &record, &item);
            if found == SortOrder::SortsEqual {
                return Ok(record.data());
            } else if found == SortOrder::SortsAfter {
                return Err(ValueError::SubscriptNotFound);
            }
        }
        Err(ValueError::SubscriptNotFound)
    }

    pub fn open(path: &str) -> std::io::Result<Database> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut fhead: sgmnt_data_struct = unsafe { mem::zeroed() };
        let buffer_size = mem::size_of::<sgmnt_data_struct>();
        unsafe {
            let fhead_slice = slice::from_raw_parts_mut(
                &mut fhead as *mut _ as *mut u8,
                buffer_size
                );
            file.read_exact(fhead_slice)?;
        }
        let mut master_bitmap: [u8; 253952] = unsafe { mem::zeroed() };
        file.read_exact(&mut master_bitmap)?;
        Ok(Database{
            fhead: fhead,
            master_bitmap: master_bitmap,
            handle: file,
        })
    }
}

impl Block {
    //pub fn find_key(&self, goal: &[u8]) -> Record {
    //    Record {
    //    }
    //}
}

impl std::iter::IntoIterator for Block {
    type Item = Result<Record, RecordError>;
    type IntoIter = RecordIterator;

    fn into_iter(self) -> RecordIterator {
        RecordIterator{
            data: self.data,
            offset: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    SortsBefore,
    SortsEqual,
    SortsAfter,
}

/// Returns SortsBefore, SortsEqual, SortsAfter if the record sorts before the goal, the same as
/// the goal, or after the goal
fn compare(state: &mut State, record: &Record, goal: &[u8]) -> SortOrder {
    if record.length == 8 {
        return SortOrder::SortsAfter;
    }

    let compression_count = record.compression_count as usize;
    if compression_count < state.compression {
        return SortOrder::SortsAfter
    }
    if compression_count == state.compression {
        let mut index = 0;
        let data = &record.data;
        let data_len = data.len();
        let goal_len = goal.len();
        while index < data_len && state.compression < goal_len
            && data[index] == goal[state.compression] {
            state.matched_so_far[state.compression] = data[index];
            state.compression += 1;
            index += 1;
        }
        // Case 1: the record key is shorter than goal
        if index == data_len {
            return SortOrder::SortsBefore;
        }
        // Case 2: the goal key is short than the record
        if state.compression == goal_len {
            // If what's left of the record is two 0 bytes, this is they key
            if data[index] == 0 && data[index+1] == 0 {
                return SortOrder::SortsEqual;
            }
            return SortOrder::SortsAfter;
        }
        // Case 3: Same length, different value
        if data[index] > goal[state.compression] {
            return SortOrder::SortsAfter;
        }
    }
    return SortOrder::SortsBefore;
}
