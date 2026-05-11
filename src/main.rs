mod parser;
mod build_header;

use std::fs::{self, File};
use std::io::prelude::*;
use parser::Torrent;
use build_header::{calculate_info_hash, extract_value};

fn main() {
    let mut torrent_file = fs::read("/home/daksh/Downloads/Resident Evil 4 (2023) [FitGirl Repack].torrent").unwrap();
    // let mut contents = String::new();
    // let _ =torrent_file.read_to_string(&mut contents);

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();

    // println!("{:?}", file_content);

    extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file);

    match info_hash {
        Ok(hash) => println!("SHA-1 info hash - {}", hash),
        Err(e) => eprintln!("Error : {}", e)
    }
}
