mod parser;
mod build_request;
mod response;

use std::{fs::{self}, io::Read};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, build_http_url};
use reqwest::blocking::Client;

use crate::response::parse_response;

fn main() {
    let torrent_file = fs::read("/home/daksh/Downloads/Resident Evil 4 (2023) [FitGirl Repack].torrent").unwrap();

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();

    println!("{:?}", file_content.info.mode);

    // extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    split_pieces(&file_content.info.pieces);
    println!("{:?}", calculate_torrent_size(&file_content));
    println!("{}", generate_id());

    let url = build_http_url(&file_content, &torrent_file);

    let http_client = Client::new();
    let response = http_client.get(url).send();

    // println!("{:?}", response);

    let response_body = response.unwrap().bytes().unwrap();
    parse_response(&response_body);

}