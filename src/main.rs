mod parser;
mod build_request;

use std::fs::{self};
use std::io::prelude::*;
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces};

fn main() {
    let mut torrent_file = fs::read("/home/daksh/Downloads/Resident Evil 4 (2023) [FitGirl Repack].torrent").unwrap();
    // let mut contents = String::new();
    // let _ =torrent_file.read_to_string(&mut contents);

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();

    println!("{:?}", file_content.info.piece_len);

    // extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    split_pieces(&file_content.info.pieces);

    // match info_hash {
    //     Ok(hash) => println!("SHA-1 info hash - {}", hash),
    //     Err(e) => eprintln!("Error : {}", e)
    // }
}