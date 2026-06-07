mod parser;
mod build_request;
mod response;
mod peer;
mod download;
mod piece;
mod ui;
mod logger;
mod storage;

use std::{fs::{self}, sync::Arc};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, build_http_url};
use tokio::{net::TcpStream, sync::Mutex};

use crate::{download::{DownloadState, bit_torrent_handshake, run_download}, peer::PeerRegistry, response::parse_response};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let torrent_file = fs::read("/home/daksh/Downloads/ubuntu-26.04-desktop-amd64.iso.torrent").unwrap();

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();
    
    // println!("{:?}", file_content.info.mode);

    // extract_value(file_content);
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    let peer_id = generate_id();

    let pieces_split = split_pieces(&file_content.info.pieces);
    // println!("{:?}", calculate_torrent_size(&file_content));
    // println!("{}", peer_id);

    let url = build_http_url(&file_content, &torrent_file, &peer_id);

    println!("Fetching peers from tracker...");
    let http_client = reqwest::Client::new();
    let response = http_client.get(url).send().await?;

    let tracker_response_body = response.bytes().await?;
    // println!("seeders: {:?}, leechers: {:?}", response.complete, response.incomplete);

    let peer_id_bytes: [u8; 20] = peer_id.as_bytes().try_into().expect("Length Mismatch");
    let info_hash_bytes = info_hash.1;

    let peers = parse_response(&tracker_response_body);
    println!("Found {} peers! Handshaking and connecting... (this may take up to 20 seconds depending on peer response times)", peers.len());

    let mut handles = vec![];
    for peer in peers {
        let handle = tokio::spawn(async move {
            match bit_torrent_handshake(&peer, peer_id_bytes, info_hash_bytes).await {
                Ok(stream) => Some((format!("{}:{}", peer.ip, peer.port), stream)),
                Err(_e) => {
                    // eprintln!("{}", _e);
                    None
                }
            }
        });
        handles.push(handle);
    }

    let mut handshaked_peers: Vec<(String, TcpStream)> = vec![];
    for handle in handles {
        if let Ok(Some(peer_data)) = handle.await {
            
            handshaked_peers.push(peer_data);
        }
    }

    let mut piece_hashes: Vec<[u8; 20]> = vec![];
    for piece in pieces_split{
        piece_hashes.push(piece);
    }

    let (total_length, num_pieces) = calculate_torrent_size(&file_content);
    let standard_piece_length = file_content.info.piece_len;

    let registry = Arc::new(Mutex::new(PeerRegistry::new(num_pieces)));
    let dl_state = Arc::new(Mutex::new(DownloadState::new(num_pieces)));
    let arc_piece_hashes = Arc::new(piece_hashes);
    let single_file_name = file_content.info.name.clone();
    let output_dir = format!("/home/daksh/Downloads/{}", single_file_name);

    // println!("got required data");

    let active_peers = handshaked_peers.len();

    let mut file_names = vec![];
    match &file_content.info.mode {
        parser::FileMode::SingleFileMode { .. } => file_names.push(file_content.info.name.clone()),
        parser::FileMode::MultiFileMode { files } => {
            for f in files {
                file_names.push(f.path.join("/"));
            }
        }
    }

    let ui_state = Arc::new(std::sync::Mutex::new(ui::UiState {
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        needed_count: num_pieces as usize,
        total_pieces: num_pieces,
        complete: false,
        active_peers,
        active_tab: ui::AppTab::Overview,
        pieces: vec![ui::PieceStatus::Missing; num_pieces as usize],
        logs: vec!["[SYSTEM] Torx client initialized".to_string(), format!("[SYSTEM] Found {} handshaked peers", active_peers)],
        peers: vec![],
        file_names,
    }));

    let dl_state_clone = Arc::clone(&dl_state);
    let ui_state_clone = Arc::clone(&ui_state);
    let token = tokio_util::sync::CancellationToken::new();
    let token_ui = token.clone();

    let download_handle = tokio::spawn(async move {
        if let Err(e) = run_download(handshaked_peers, registry, dl_state_clone, ui_state_clone, standard_piece_length, total_length, num_pieces, arc_piece_hashes, &output_dir, token).await {
            crate::logger::log(&format!("run_download failed: {:?}", e));
        }
    });

    let ui_handle = tokio::spawn(async move {
        let _ = ui::run_ui(ui_state, single_file_name, total_length, token_ui).await;
    });

    let _ = tokio::join!(download_handle, ui_handle);

    Ok(())
}