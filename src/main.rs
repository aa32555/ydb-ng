//extern crate ydb_ng;
extern crate clap;

use ydb_ng::ydb::{sgmnt_data_struct, blk_hdr};
use clap::{Arg, App, SubCommand};

use std::io::Read;
use std::mem;
use std::slice;
use std::fs::File;
use std::io::prelude::*;
use std::io::SeekFrom;

struct Database {
    fhead: sgmnt_data_struct,
    master_bitmap: [u8; 253952],
}

// File format is:
//  sgmnt_data_struct
//  master_bitmap
//  - length = sgmnt_data_struct->master_map_len
// This is the same as ydb::DISK_BLOCK_SIZE, but given a more descriptive name
// Note that it is hard-coded to 512 in YDB, and is unlikely to change
static PHYSICAL_DATABASE_BLOCK_SIZE: i32 = 512;

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
    for c in &fhead.label {
        print!("{}", c);
    }
    println!("sgmnt_data_struct size: {}", buffer_size);
    println!("blk_size: {}", fhead.blk_size);
    println!("start_vbn: {}", fhead.start_vbn);
    let directory_tree_block = read_block(0, &mut file, &fhead)?;
    // Read the block header in
    let mut block_header: blk_hdr = unsafe { mem::zeroed() };
    let buffer_size = mem::size_of::<blk_hdr>();
    unsafe {
        let block_header_slice = slice::from_raw_parts_mut(
            &mut block_header as *mut _ as *mut u8,
            buffer_size
        );
        directory_tree_block.as_slice().read_exact(block_header_slice).unwrap();
    }
    println!("Transaction number: {}", block_header.tn);
    // After reading the directory tree local map, skip ahead to +1000 to get the first "block" with data
    //let master_bitmap = read_block(0, &mut file, &fhead).unwrap();
    /*for byte in master_bitmap {
        print!(".");
    }*/
    Ok(())
}
