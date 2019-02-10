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

pub struct BlockIterator {
    data: Read,
}

impl Iterator for BlockIterator {
    type Item = Record;

    fn next(&mut self) -> Option<Record> {
        let mut rec_size: [u8; 2] = unsafe {mem::zeroed() };
        if self.data.read_exact(&mut rec_size).is_err() {
            return None
        }
        let rec_size = unsafe { mem::transmute::<[u8; 2], u16>(rec_size) };
        let mut rec_compression_count: [u8; 1] = unsafe { mem::zeroed() };
        if self.data.read_exact(&mut rec_compression_count).is_err() {
            return None
        };
        let mut junk: [u8; 1] = unsafe { mem::zeroed() };
        if self.data.read_exact(&mut junk).is_err() {
            return None
        };
        let content_length = rec_size - 4;
        let mut content = vec![0; content_length  as usize];
        if self.data.read_exact(&mut content.as_mut_slice()).is_err() {
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

fn read_block(blk_num: usize, input_file: &mut File, fhead: &sgmnt_data_struct) -> std::io::Result<Vec<u8>> {
    //let mut ret = vec!([0; fhead.blk_size]);
    let blk_size = fhead.blk_size as usize;
    let mut ret = vec![0; blk_size];
    input_file.seek(SeekFrom::Start(
        (((fhead.start_vbn - 1) * PHYSICAL_DATABASE_BLOCK_SIZE) as usize
         + blk_size * blk_num) as u64)
    )?;
    input_file.read_exact(&mut ret)?;
    return Ok(ret);
}

fn read_record<T: Read>(data: &mut T) -> std::io::Result<Record> {
    let mut rec_size: [u8; 2] = unsafe {mem::zeroed() };
    data.read_exact(&mut rec_size)?;
    let rec_size = unsafe { mem::transmute::<[u8; 2], u16>(rec_size) };
    let mut rec_compression_count: [u8; 1] = unsafe { mem::zeroed() };
    data.read_exact(&mut rec_compression_count)?;
    let mut junk: [u8; 1] = unsafe { mem::zeroed() };
    data.read_exact(&mut junk)?;
    let content_length = rec_size - 4;
    let mut content = vec![0; content_length  as usize];
    data.read_exact(&mut content.as_mut_slice())?;
    let ret = Record{
        length: rec_size,
        compression_count: rec_compression_count[0],
        filler: junk[0],
        data: content.into_boxed_slice(),
    };
    Ok(ret)
}

fn compare(state: &mut State, data: &[u8], goal: &[u8]) -> bool {
    println!("Data: {:?}", state.compression);
    let mut index = 0;
    // Roll back the compression count to 0
    while state.compression != 0 && data[0] != state.matched_so_far[state.compression] {
        state.compression = state.compression - 1;
    }
    // Roll forward, copying values to the state
    while data[index] == goal[state.compression] {
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
        .get_matches();
    // Read the header into memory
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
    //println!("label: {:#?}", fhead.label);
    println!("sgmnt_data_struct size: {}", buffer_size);
    println!("blk_size: {}", fhead.blk_size);
    println!("start_vbn: {}", fhead.start_vbn);
    // the first local bitmap is block 0; directory tree is block 1
    let directory_tree_block = read_block(1, &mut file, &fhead)?;
    let mut directory_tree_block = directory_tree_block.as_slice();
    // Read the block header in
    let mut block_header: blk_hdr = unsafe { mem::zeroed() };
    let buffer_size = mem::size_of::<blk_hdr>();
    unsafe {
        let block_header_slice = slice::from_raw_parts_mut(
            &mut block_header as *mut _ as *mut u8,
            buffer_size
        );
        directory_tree_block.read_exact(block_header_slice).unwrap();
    }
    println!("Transaction number: {}", block_header.tn);
    // Scan through the directory tree until we find a value greater than the global; jump to that block
    let record = read_record(directory_tree_block.by_ref())?;
    /*let mut raw_record_size: [u8; 2] = unsafe { mem::zeroed() };
    directory_tree_block.read_exact(&mut raw_record_size).unwrap();
    let record_size = unsafe { mem::transmute::<[u8; 2], u16>(raw_record_size) };
    println!("Size of first record: {:#?}", record_size);
    // After reading the directory tree local map, skip ahead to +1000 to get the first "block" with data
    let mut record_compression_count: [u8; 1] = unsafe { mem::zeroed() };
    let mut junk: [u8; 1] = unsafe { mem::zeroed() };
    directory_tree_block.read_exact(&mut record_compression_count).unwrap();
    directory_tree_block.read_exact(&mut junk).unwrap();
    let mut data = vec![0; (record_size - 4) as usize];
    directory_tree_block.read_exact(&mut data.as_mut_slice()).unwrap();
    let record = Record{
        length: record_size,
        compression_count: record_compression_count[0],
        filler: junk[0],
        data: data.as_slice(),
    };*/
    println!("Record size: {}\nCompression count: {}\nValue: {:#?}", record.length, record.compression_count, record.data);
    // The first record contains a pointer to another block; I don't know under what
    //  Circumstances we go past the first pointer (maybe when there are more
    //  global subscripts than are can fit in that first block?
    // For now, ready the block pointed too by that
    let mut cur_block_num = [0; 4];
    cur_block_num[..4].clone_from_slice(&record.data);
    let cur_block_num: usize = unsafe { mem::transmute::<[u8; 4], u32>(cur_block_num) } as usize;
    let cur_block = read_block(cur_block_num, &mut file, &fhead)?;
    let mut cur_block = cur_block.as_slice();
    let mut cur_block_header: blk_hdr = unsafe { mem::zeroed() };
    let buffer_size = mem::size_of::<blk_hdr>();
    unsafe {
        let cur_block_header_slice = slice::from_raw_parts_mut(
            &mut cur_block_header as *mut _ as *mut u8,
            buffer_size
        );
        cur_block.read_exact(cur_block_header_slice).unwrap();
    }
    println!("Data block TN is {}", cur_block_header.tn);
    // We know the exact location of the list of globals; fetch the block, then scan through it
    //  for a key matching the one we specify
    let mut state = State{
        compression: 0,
        matched_so_far: [0; 1024]
    };
    let goal = "hello";
    let goal = goal.as_bytes();
    loop {
        let record = read_record(cur_block.by_ref())?;
        if compare(&mut state, &record.data, goal) {
            //println!("Record size: {}\nCompression count: {}\nValue: {:#?}", record.length, record.compression_count, record.data);
            // We found the record; get the block with the data for this global
            // State should have a compression of length + 2 (2 null bytes represent end)
            let mut cur_block_num = [0; 4];
            cur_block_num[..4].clone_from_slice(&record.data[state.compression..]);
            let mut cur_block_num: usize = unsafe { mem::transmute::<[u8; 4], u32>(cur_block_num) } as usize;
            println!("Next block: {}", cur_block_num);
            println!("State compression: {}", state.compression);
            break;
        }
    }
    /*
    let record = read_record(cur_block.by_ref())?;
    println!("Record size: {}\nCompression count: {}\nValue: {:#?}", record.length, record.compression_count, record.data);*/
    Ok(())
}
