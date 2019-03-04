#[macro_use]
extern crate nom;

extern crate ydb_ng_bridge;
//use serde::{Serialize, Deserialize};

use std::collections::VecDeque;
use std::io::Read;
use std::mem;
use std::slice;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
//use std::iter::FromIterator;
use std::fs::OpenOptions;
use std::sync::{RwLock};
//use bincode::serialize;
use nom::{le_u8, le_u16, le_u32, le_u64};

use ydb_ng_bridge::ydb::{sgmnt_data_struct, blk_hdr, rec_hdr};

pub mod rec;
pub mod block;

pub use block::{Blk, get_block, BlkNum, RecordCursor, BlkType};
pub use rec::Rec;

static PHYSICAL_DATABASE_BLOCK_SIZE: i32 = 512;

pub type IntegQueueType = RwLock<VecDeque<IntegBlock>>;

#[derive(Debug, Clone, PartialEq)]
pub struct IntegBlock {
    pub blk_num: BlkNum,
    pub typ: BlkType,
    pub start: Vec<u8>,
    pub end: Vec<u8>,
}

pub struct Database {
    pub fhead: sgmnt_data_struct,
    pub master_bitmap: [u8; 253952],
    pub handle: File,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    SortsBefore,
    SortsEqual,
    SortsAfter,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LocalBitmapStatus {
    Busy,
    NeverUsed,
    Invalid,
    Free,
}

#[derive(Debug, Clone)]
pub struct State<'a> {
    pub(crate) compression: usize,
    pub(crate) goal: &'a [u8],
}

#[derive(Debug)]
pub enum ValueError {
    IoError(std::io::Error),
    RecordError(RecordError),
    GlobalNotFound,
    SubscriptNotFound,
    MalformedRecord,
    BlockIncorrectlyMarkedFree,
    BlockIncorrectlyMarkedBusy,
}

#[derive(Debug)]
pub enum RecordError {
    IoError(std::io::Error),
    TooBig,
    TooSmall,
    LengthZero,
    ZeroCompressionCount,
    IncorrectSort,
    NoTerminatingCharacter,
}

impl From<std::io::Error> for ValueError {
    fn from(error: std::io::Error) -> Self {
        ValueError::IoError(error)
    }
}

impl<T> From<nom::Err<T>> for ValueError {
    fn from(_: nom::Err<T>) -> Self {
        ValueError::RecordError(RecordError::TooSmall)
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

impl Database {
    pub fn local_block_status(&self, blk_num: usize) -> Result<LocalBitmapStatus, ValueError> {
        // Get the local bitmap closest to that block; they occur every 512 blocks, so at 0, 511,
        // 1023, etc. divide blk_nu by 512, then multiply by 512
        let bm_blk_num = blk_num / 512;
        let bm_blk_num = bm_blk_num * 512;
        let blk = self.get_block(blk_num)?;
        Ok(match blk[blk_num - bm_blk_num] {
            0 => LocalBitmapStatus::Busy,
            1 => LocalBitmapStatus::NeverUsed,
            3 => LocalBitmapStatus::Free,
            _ => LocalBitmapStatus::Invalid,
        })
    }

    // We should check some sort of cache here for the block
    pub fn get_block(&self, blk_num: usize) -> std::io::Result<Vec<u8>> {
        //let mut ret = vec!([0; fhead.blk_size]);
        let mut handle = self.handle.try_clone()?;
        let blk_size = self.fhead.blk_size as usize;
        let mut raw_block = vec![0; blk_size];
        handle.seek(SeekFrom::Start(
            (((self.fhead.start_vbn - 1) * PHYSICAL_DATABASE_BLOCK_SIZE) as usize
             + blk_size * blk_num) as u64)
        )?;
        handle.read_exact(&mut raw_block)?;
        /*let mut block = raw_block.as_slice();
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
        };*/
        Ok(raw_block)
    }

    /*pub fn write_block(&mut self, old_blk_hdr: &blk_hdr, blk_num: usize, new_value: Vec<u8>) -> std::io::Result<()> {
        /*let mut handle = self.handle.try_clone()?;
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
        */
        Ok(())
    }*/

    /*fn make_global_block(&mut self, _key: &Vec<u8>) -> Result<Block, ValueError> {
        //let block = self.get_block(10)?;
        panic!("Not implemented!");
        //Ok(block)
    }*/

    /*pub fn set_value(&mut self, key: &Vec<u8>, value: Vec<u8>) -> Result<(), ValueError> {
        /*let mut new_value = Vec::with_capacity(value.len() + mem::size_of::<rec_hdr>());
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
        new_value.write(&record.into_boxed_slice()?)?;
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
        self.write_block(&old_blk_hdr, old_blk_num, new_value)?;*/
        Ok(())
    }*/

    /// Given a key, finds the block number with the data for that block
    pub fn find_value_block(&self, item: &[u8]) -> Result<BlkNum, ValueError> {
        let root_block = self.get_block(1)?;
        let root_block = get_block(&root_block, 1, BlkType::DirectoryTree)?;
        // Scan through the root block until we find one with key
        // greater than the value we are looking for
        let mut next_block = 0;
        let mut global_end = 0;
        let mut found = false;
        while global_end < item.len() && item[global_end] != 0 {
            global_end += 1;
        }
        println!("Searching block {}", next_block);
        let global = &item[0..global_end];
        let mut state = State{compression: 0, goal: global};
        for record in RecordCursor::new(&root_block) {
            let record = record?;
            found = match RecordCursor::compare(&record, &mut state) {
                SortOrder::SortsAfter => true,
                _ => false,
            };
            if found == true {
                next_block = match record.ptr().unwrap() {
                    BlkNum::Block(x) => x,
                    _ => 0,
                };
                break;
            }
        }
        if !found || next_block == 0 {
            return Err(ValueError::MalformedRecord);
        }

        println!("Searching block {}", next_block);
        let gvt_block = self.get_block(next_block)?;
        let gvt_block = get_block(&gvt_block, next_block, BlkType::IndexBlock)?;
        let mut state = State{compression: 0, goal: item};
        let mut next_block = 0;
        found = false;
        for record in RecordCursor::new(&gvt_block) {
            let record = record?;
            found = match RecordCursor::compare(&record, &mut state) {
                SortOrder::SortsAfter => true,
                SortOrder::SortsEqual => true,
                _ => false,
            };
            if found == true {
                next_block = match record.ptr().unwrap() {
                    BlkNum::Block(x) => x,
                    _ => 0,
                };
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
            let gvt_block = get_block(&gvt_block, next_block, BlkType::IndexBlock)?;
            println!("Next block level {}", gvt_block.header().levl);
            if gvt_block.header().levl == 0 {
                return Ok(BlkNum::Block(next_block));
            }

            //let mut state = State{compression: 0, matched_so_far: vec![0; 1024]};
            found = false;
            for record in RecordCursor::new(&gvt_block) {
                let record = record?;
                found = match RecordCursor::compare(&record, &mut state) {
                    SortOrder::SortsAfter => true,
                    SortOrder::SortsEqual => true,
                    _ => false,
                };
                if found == true {
                    next_block = match record.ptr().unwrap() {
                        BlkNum::Block(x) => x,
                        _ => 0,
                    };
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
    pub fn find_value<'a>(&self, item: &[u8], block: &'a Blk) -> Result<Vec<u8>, ValueError> {
        let mut state = State{compression: 0, goal: item};
        for record in RecordCursor::new(&block) {
            let record = record?;
            let found = match RecordCursor::compare(&record, &mut state) {
                SortOrder::SortsAfter => true,
                SortOrder::SortsEqual => true,
                _ => false,
            };
            if found == true {
                return Ok(record.data().to_vec());
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

