mod parser;
mod build_request;
mod response;
mod peer;
mod download;
mod piece;

use std::{fs::{self}, sync::Arc};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, build_http_url};
use tokio::{net::TcpStream, sync::Mutex};

use crate::{download::{DownloadState, bit_torrent_handshake, download_piece, run_download}, peer::PeerRegistry, piece::piece_to_hash, response::parse_response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let torrent_file = fs::read("/home/daksh/Downloads/big-buck-bunny.torrent").unwrap();

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

    let mut handshaked_peers: Vec<(String, TcpStream)> = vec![];
    for peer in peers{
        match bit_torrent_handshake(&peer, peer_id_bytes, info_hash_bytes).await {
            Ok(stream) => {
                handshaked_peers.push((format!("{}:{}", peer.ip, peer.port), stream));
            }
            Err(e) =>{
                eprintln!("{}", e);
                continue;
            }
        }
    }

    let mut piece_hashes: Vec<[u8; 20]> = vec![];
    for piece in pieces_split{
        piece_hashes.push(piece_to_hash(&piece));
    }

    let (total_length, num_pieces) = calculate_torrent_size(&file_content);
    let standard_piece_length = file_content.info.piece_len;

    let registry = Arc::new(Mutex::new(PeerRegistry::new(num_pieces)));
    let dl_state = Arc::new(Mutex::new(DownloadState::new(num_pieces)));
    let arc_piece_hashes = Arc::new(piece_hashes);
    let single_file_name = file_content.info.name;
    let output_dir = format!("/home/daksh/Downloads/{}", single_file_name);

    println!("got required data");

    run_download(handshaked_peers, registry, dl_state, standard_piece_length, total_length, num_pieces, arc_piece_hashes, &output_dir).await?;

    Ok(())

}