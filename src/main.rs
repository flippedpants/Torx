mod parser;

use std::fs::{self, File};
use std::io::prelude::*;
use parser::Torrent;

fn main() {
    let mut torrent_file = fs::read("/home/daksh/Downloads/Resident Evil 4 (2023) [FitGirl Repack].torrent").unwrap();
    // let mut contents = String::new();
    // let _ =torrent_file.read_to_string(&mut contents);

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();

    // println!("{:?}", file_content);

    let announce_url = file_content.announce;
    let announce_list = file_content.announce_list.unwrap_or_default();
    let torrent_name = file_content.info.name;
    let pieces = file_content.info.pieces;
    let piece_len = file_content.info.piece_len;
    
    let mut single_file_length: u64;
    let mut torrent_files: Vec<parser::TorrentFile>;
    
    match file_content.info.mode {
        parser::FileMode::SingleFileMode { length } => {
            single_file_length = length;
        },
        parser::FileMode::MultiFileMode { files } => {
            torrent_files = files;
        }
    }
}
