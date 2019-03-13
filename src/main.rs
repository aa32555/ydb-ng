extern crate ydb_ng;
extern crate clap;
extern crate ydb_ng_bridge;
extern crate fnv;
extern crate threadpool;
extern crate spin;

use std::collections::{VecDeque, HashSet};

use clap::{Arg, App, ArgMatches};
use std::mem;
use std::sync::{Arc};
use spin::Mutex;
use std::time::Duration;
use std::thread::sleep;
use fnv::FnvHashSet;
use threadpool::ThreadPool;
use ydb_ng_bridge::ydb::{blk_hdr};

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

fn do_integ(database: &Mutex<Database>,
            next: &IntegBlock,
            to_visit: &Mutex<FnvHashSet<usize>>) -> Result<Vec<IntegBlock>, std::io::Error> {
    let blk_num = match next.blk_num {
        BlkNum::Block(x) => x,
        _ => panic!("Expected a known block; didn't get it"),
    };
    to_visit.lock().remove(&blk_num);
    //println!("Integ on block {}", blk_num);
    let blk = database.lock().get_block(blk_num)?;
    let blk = get_block(&blk, blk_num, next.typ.clone()).unwrap();
    let next_blocks = blk.integ(&next.start).unwrap();
    Ok(next_blocks)
}

fn add_block_to_pool(database: &Arc<Mutex<Database>>,
                     to_visit: &Arc<Mutex<FnvHashSet<usize>>>,
                     pool: &Arc<Mutex<ThreadPool>>,
                     blk: IntegBlock) {
    let next_blocks = do_integ(&database, &blk, &to_visit);
    let next_blocks = next_blocks.unwrap();
    let mut blocks_to_queue = Vec::with_capacity(next_blocks.len());
    {
        let mut b_to_visit = to_visit.lock();
        for blk in next_blocks {
            let blk_num = match blk.blk_num {
                BlkNum::Block(x) => x,
                _ => panic!("Scanning unknown block!"),
            };
            {
                if b_to_visit.contains(&blk_num) {
                    b_to_visit.remove(&blk_num);
                    blocks_to_queue.push(blk);
                    //println!("Adding block to queue: {:?}", blk);
                }
            }
        }
    }
    {
        let l_pool = pool.lock();
        for blk in blocks_to_queue {
            let database = database.clone();
            let to_visit = to_visit.clone();
            let n_pool = pool.clone();
            l_pool.execute(move || {
                add_block_to_pool(&database, &to_visit, &n_pool, blk);
            });
        }
    }
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
        .arg(Arg::with_name("integ-threads")
             .help("Number of concurrent threads to use for integrity check")
             .short("t")
             .long("integ-threads")
             .takes_value(true))
        .get_matches();
    // Load the database
    let database = Arc::new(Mutex::new(Database::open(matches.value_of("INPUT").unwrap())?));
    if matches.is_present("integ") {
        let pool = Arc::new(Mutex::new(ThreadPool::new(4)));
        let to_visit = Arc::new(Mutex::new(FnvHashSet::default()));
        // Scan through the local maps; when we get an empty one, there are no more. Verify that
        // they are marked correctly in the master bitmap
        for i in 0.. {
            let blk = database.lock().get_block(i * 512);
            // We attempted to read past the end of the database, meaning no more local
            // bitmaps
            if blk.is_err() {
                break;
            }
            let blk = blk.unwrap();
            let blk = &blk[mem::size_of::<blk_hdr>()..];
            let mut byte_num = 0;
            let mut block_num = 0;
            for byte in blk {
                let byte = byte.clone();
                // 2 bits for each block, so we have 4 blocks per byte to check
                for bittle in 0..4 {
                    // The 0th block is the local bitmap; skip it
                    if byte & (0b11 << (2*bittle)) == 0b00 && block_num != 0 {
                        let b = i * 512 + (4 * byte_num + (bittle as usize));
                        //println!("Adding block {} to be scanned", b);
                        to_visit.lock().insert(b);
                    }
                    block_num += 1;
                    if block_num == 512 {
                        break;
                    }
                }
                byte_num += 1;
                if block_num == 512 {
                    break;
                }
            }
        }
        // Add the first block
        {
            let blk = IntegBlock {
                blk_num: BlkNum::Block(1),
                typ: BlkType::DirectoryTree,
                start: vec![],
                end: vec![],
            };
            add_block_to_pool(&database, &to_visit, &pool, blk);
        }

        loop {
            {
                let p = pool.lock();
                if p.active_count() == 0  && p.queued_count() == 0 {
                    break;
                }
            }
            sleep(Duration::from_millis(100));
        }

        let t = to_visit.lock();
        for blk in t.iter() {
            println!("Block {} incorrectly marked busy", blk);
        }
    } else {
        find_value(&matches, &database.lock())?;
    }
    Ok(())
}
