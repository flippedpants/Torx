mod parser;
mod build_request;
mod response;
mod connect_peers;

use std::{fs::{self}, io::Read};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, build_http_url};


use crate::{connect_peers::{connect_to_peer}, response::parse_response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let torrent_file = fs::read("/home/daksh/Downloads/Resident Evil 4 (2023) [FitGirl Repack].torrent").unwrap();

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();

    println!("{:?}", file_content.info.mode);

    // extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    let peer_id = generate_id();

    split_pieces(&file_content.info.pieces);
    println!("{:?}", calculate_torrent_size(&file_content));
    println!("{}", peer_id);

    let url = build_http_url(&file_content, &torrent_file, &peer_id);

    let http_client = reqwest::Client::new();
    let response = http_client.get(url).send().await?;

    // println!("{:?}", response);

    let tracker_response_body = response.bytes().await?;
    parse_response(&tracker_response_body);

    let peer_id_bytes: [u8; 20] = generate_id().as_bytes().try_into().expect("Length Mismatch");
    let info_hash_bytes = info_hash.1;

    match connect_to_peer(&tracker_response_body, peer_id_bytes, info_hash_bytes).await{
        Ok(s) => {
            println!("Connected and handshaked successfully!");
        }
        Err(e) => {
            eprintln!("Error: {}", e)
        }
    }

    Ok(())

}