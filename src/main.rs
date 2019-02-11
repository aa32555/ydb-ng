//extern crate ydb_ng;
extern crate clap;
extern crate ydb_ng_bridge;

use ydb_ng_bridge::ydb::{sgmnt_data_struct, blk_hdr};
use clap::{Arg, App};

use std::io::Read;
use std::mem;
use std::slice;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;
use std::iter::FromIterator;

// File format is:
//  sgmnt_data_struct
//  master_bitmap
//  - length = sgmnt_data_struct->master_map_len
// This is the same as ydb::DISK_BLOCK_SIZE, but given a more descriptive name
// Note that it is hard-coded to 512 in YDB, and is unlikely to change
static PHYSICAL_DATABASE_BLOCK_SIZE: i32 = 512;

pub struct Database {
    pub fhead: sgmnt_data_struct,
    pub master_bitmap: [u8; 253952],
    pub handle: File,
}

pub struct Block {
    pub header: blk_hdr,
    pub data: Box<[u8]>,
}

pub struct Record {
    pub length: u16,
    pub compression_count: u8,
    pub filler: u8,
    pub data: Box<[u8]>
}

struct State {
    compression: usize,
    matched_so_far: [u8; 1024]
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
}

impl Iterator for RecordIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        // Get a pointer to where we left off in the block
        let mut data = &self.data[self.offset..];
        let data = data.by_ref();

        let mut rec_size: [u8; 2] = unsafe {mem::zeroed() };
        if data.read_exact(&mut rec_size).is_err() {
            return None
        }
        let rec_size = unsafe { mem::transmute::<[u8; 2], u16>(rec_size) };
        if rec_size == 0 {
            return None
        }
        self.offset += rec_size as usize;
        let mut rec_compression_count: [u8; 1] = unsafe { mem::zeroed() };
        if data.read_exact(&mut rec_compression_count).is_err() {
            return None
        };
        let mut junk: [u8; 1] = unsafe { mem::zeroed() };
        if data.read_exact(&mut junk).is_err() {
            return None
        };
        let content_length = rec_size - 4;
        let mut content = vec![0; content_length  as usize];
        if data.read_exact(&mut content.as_mut_slice()).is_err() {
            return None
        };
        let ret = Record{
            length: rec_size,
            compression_count: rec_compression_count[0],
            filler: junk[0],
            data: content.into_boxed_slice(),
        };
        Some(ret)
    }
}

impl Database {
    // We should check some sort of cache here for the block
    pub fn get_block(&mut self, blk_num: usize) -> std::io::Result<Block> {
        //let mut ret = vec!([0; fhead.blk_size]);
        let blk_size = self.fhead.blk_size as usize;
        let mut raw_block = vec![0; blk_size];
        self.handle.seek(SeekFrom::Start(
            (((self.fhead.start_vbn - 1) * PHYSICAL_DATABASE_BLOCK_SIZE) as usize
             + blk_size * blk_num) as u64)
        )?;
        self.handle.read_exact(&mut raw_block)?;
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
        Ok(Block{
            header: block_header,
            data: data_portion.into_boxed_slice(),
        })
    }
}

impl Block {
    //pub fn find_key(&self, goal: &[u8]) -> Record {
    //    Record {
    //    }
    //}
}

pub struct RecordIterator {
    data: Box<[u8]>,
    offset: usize,
}

impl std::iter::IntoIterator for Block {
    type Item = Record;
    type IntoIter = RecordIterator;

    fn into_iter(self) -> RecordIterator {
        RecordIterator{
            data: self.data,
            offset: 0,
        }
    }
}

fn compare(state: &mut State, record: &Record, goal: &[u8]) -> bool {
    let mut index = 0;
    let data = &record.data;
    // Roll back the compression count to 0
    state.compression = record.compression_count as usize;
    // Roll forward, copying values to the state
    while state.compression < goal.len() && data[index] == goal[state.compression] {
        state.matched_so_far[state.compression] = goal[state.compression];
        state.compression = state.compression + 1;
        if data[index] == 0 && data[index+1] == 0 {
            break;
        }
        index = index + 1;
        if goal.len() == state.compression && data[index] == 0 && data[index+1] == 0 {
            state.compression += 2;
            return true;
        }
    }
    return false;
}

fn main() -> std::io::Result<()> {
    let matches = App::new("ydb-ng")
        .version("0.1")
        .author("Charles Hathaway <chathaway@logrit.com>")
        .about("Reads YottaDB databases and allows clustered operation")
        .arg(Arg::with_name("INPUT")
             .help("The file to read from")
             .required(true)
             .index(1))
        .arg(Arg::with_name("global")
             .help("Global to search the database for")
             .short("g")
             .long("global")
             .takes_value(true))
        .arg(Arg::with_name("subscripts")
             .help("Subscript of the key we are searching for")
             .short("s")
             .long("subscripts")
             .takes_value(true))
        .get_matches();
    // Read the header into memory
    let global = matches.value_of("global").unwrap_or("hello").as_bytes();
    let subs: Vec<Vec<u8>> = matches.value_of("subscripts").unwrap_or("")
        .split(",").map(|s| { Vec::from(s) }).collect();
    let partial_match: Vec<u8> = vec![0, 0xFF];
    let mut combined_search = Vec::from(global);
    if matches.value_of("subscripts").is_some() {
        for sub in subs {
            combined_search.extend(&partial_match);
            combined_search.extend(&sub);
        }
    }
    let combined_search = combined_search.into_boxed_slice();
    //println!("Combined search: {:#?}", combined_search);
    let mut file = File::open(matches.value_of("INPUT").unwrap())?;
    let mut fhead: sgmnt_data_struct = unsafe { mem::zeroed() };
    let buffer_size = mem::size_of::<sgmnt_data_struct>();
    unsafe {
        let fhead_slice = slice::from_raw_parts_mut(
            &mut fhead as *mut _ as *mut u8,
            buffer_size
        );
        file.read_exact(fhead_slice).unwrap();
    }
    let mut master_bitmap: [u8; 253952] = unsafe { mem::zeroed() };
    file.read_exact(&mut master_bitmap).unwrap();
    let mut database = Database{
        fhead: fhead,
        master_bitmap: master_bitmap,
        handle: file,
    };
    // First block is the index block
    let block = database.get_block(1)?;
    let directory_tree: Vec<Record> = block.into_iter().collect();
    let directory_tree = &directory_tree[0];
    let mut next_block = directory_tree.ptr();
    let block = database.get_block(next_block)?;
    let mut state = State{compression: 0, matched_so_far: [0; 1024]};
    for globals in block.into_iter() {
        if compare(&mut state, &globals, global) {
            next_block = globals.ptr();
            break;
        }
    }
    // Now that we know what block a GVT, load up that block
    let block = database.get_block(next_block)?;
    // Search in each of the child blocks
    for data_block_record in block.into_iter() {
        let block = database.get_block(data_block_record.ptr())?;
        let mut state = State{compression: 0, matched_so_far: [0; 1024]};
        for sub in block.into_iter() {
            if compare(&mut state, &sub, &combined_search) {
                //println!("Value: {}", String::from_utf8(sub.data()).unwrap());
            }
        }
    }
    Ok(())
}
