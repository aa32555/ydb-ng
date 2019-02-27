extern crate ydb_ng;
extern crate clap;
extern crate ydb_ng_bridge;
extern crate fnv;

use std::collections::VecDeque;

use clap::{Arg, App, ArgMatches};

use ydb_ng::*;

// File format is:
//  sgmnt_data_struct
//  master_bitmap
//  - length = sgmnt_data_struct->master_map_len
// This is the same as ydb::DISK_BLOCK_SIZE, but given a more descriptive name
// Note that it is hard-coded to 512 in YDB, and is unlikely to change

fn find_value(matches: &ArgMatches, database: &Database) -> std::io::Result<()> {
    let global = matches.value_of("global").unwrap_or("hello").as_bytes();
    let subs: Vec<Vec<u8>> = matches.value_of("subscripts").unwrap_or("")
        .split(",").map(|s| { Vec::from(s) }).collect();
    let partial_match: Vec<u8> = vec![0, 0];
    let mut combined_search = Vec::from(global);
    if matches.value_of("subscripts").is_some() {
        for sub in subs {
            combined_search.extend(&partial_match);
            combined_search.extend(&sub);
        }
    }
    combined_search.extend(vec![0, 0]);
    println!("Combined search: {:#?}",
             String::from_utf8_lossy(&combined_search));
    let mut set = false;
    if matches.value_of("value").is_some() {
        set = true;
    }
    if set {
        // let new_value = Vec::from(matches.value_of("value").unwrap());
        // database.set_value(&combined_search, new_value).unwrap();
    } else {
        //let combined_search = combined_search.into_boxed_slice();
        // Find the block containing this subscript
        let block = match database.find_value_block(&combined_search).unwrap() {
            BlkNum::Block(x) => x,
            _ => 0,
        };
        let b = database.get_block(block)?;
        let b = get_block(&b, block, BlkType::DataBlock).unwrap();
        // Search that block
        let value = database.find_value(&combined_search, &b).unwrap();
        // Print the value
        println!("Value: {:#?}", String::from_utf8_lossy(&value));
    }
    Ok(())
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
        .arg(Arg::with_name("value")
             .help("If present, indicates that the indicated key should be set to value")
             .short("v")
             .long("value")
             .takes_value(true))
        .arg(Arg::with_name("integ")
             .help("Runs an integrity check on all blocks")
             .short("i")
             .long("integ"))                   
        .get_matches();
    // Load the database
    let database = Database::open(matches.value_of("INPUT").unwrap())?;
    if matches.is_present("integ") {
        let queue = IntegQueueType::new(VecDeque::new());
        {
            let mut q = queue.write().unwrap();
            q.push_back(IntegBlock {
                blk_num: BlkNum::Block(1),
                typ: BlkType::DirectoryTree,
                start: vec![],
                end: vec![],
            });
        }

        loop {
            let next = {
                let mut q = queue.write().unwrap();
                q.pop_front()
            };
            if next.is_none() {
                break;
            }
            let next = next.unwrap();
            let blk_num = match next.blk_num {
                BlkNum::Block(x) => x,
                _ => panic!("Expected a known block; didn't get it"),
            };
            println!("Integ on block {}", blk_num);
            let blk = database.get_block(blk_num)?;
            let blk = get_block(&blk, blk_num, next.typ).unwrap();
            blk.integ(&next.start, &queue).unwrap();
        }
    } else {
        find_value(&matches, &database)?;
    }
    Ok(())
}
