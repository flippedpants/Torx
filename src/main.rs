mod parser;
mod build_request;
mod response;
mod peer;
mod download;
mod piece;

use std::fs::{self};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, build_http_url};

use crate::{peer::bit_torrent_handshake, download::download_piece, response::parse_response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let torrent_file = fs::read("/home/daksh/Downloads/ubuntu-26.04-desktop-amd64.iso.torrent").unwrap();

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();
    
    println!("{:?}", file_content.info.mode);

    // extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    let peer_id = generate_id();

    let pieces_split = split_pieces(&file_content.info.pieces);
    println!("{:?}", calculate_torrent_size(&file_content));
    println!("{}", peer_id);

    let url = build_http_url(&file_content, &torrent_file, &peer_id);

    let http_client = reqwest::Client::new();
    let response = http_client.get(url).send().await?;

    // println!("{:?}", response);

    let tracker_response_body = response.bytes().await?;
    // parse_response(&tracker_response_body);

    // println!("seeders: {:?}, leechers: {:?}", response.complete, response.incomplete);

    let peer_id_bytes: [u8; 20] = peer_id.as_bytes().try_into().expect("Length Mismatch");
    let info_hash_bytes = info_hash.1;

    let peers = parse_response(&tracker_response_body);

    let mut piece_index = 0;

    let mut current_peer_index = 0;
    
    while current_peer_index < peers.len(){
        match bit_torrent_handshake(&peers[current_peer_index], peer_id_bytes, info_hash_bytes).await{
            Ok(mut s) => {
                match download_piece(&mut s, file_content.info.piece_len, piece_index).await {
                    Ok(piece_buf) => {
                        if piece_index as usize == pieces_split.len() - 1{
                            println!("All pieces downloaded!");
                            break;
                        }
                        else if piece_index as usize != pieces_split.len() - 1 && current_peer_index == peers.len() - 1{
                            current_peer_index = 0;
                        }
                        

                        println!("piece downloaded successfully");
                        piece_index += 1;
                        
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                    }
                }
            }
            Err(e) => {
                eprintln!("Error: {}", e);
            }
        }
        current_peer_index += 1;
    }

    Ok(())

}