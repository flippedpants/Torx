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
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, collect_all_peers};
use tokio::{net::TcpStream, sync::Mutex};

use crate::{download::{DownloadState, bit_torrent_handshake, run_download}, peer::PeerRegistry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let torrent_file = fs::read("/home/daksh/Downloads/ubuntu-26.04-desktop-amd64.iso.torrent").unwrap();

    let file_content: Torrent = serde_bencode::from_bytes(&torrent_file).unwrap();
    
    let info_hash = calculate_info_hash(&torrent_file).unwrap();

    let peer_id = generate_id();

    let pieces_split = split_pieces(&file_content.info.pieces);

    println!("Fetching peers from trackers...");
    let peers = collect_all_peers(&file_content, &torrent_file, &peer_id).await?;

    let peer_id_bytes: [u8; 20] = peer_id.as_bytes().try_into().expect("Length Mismatch");
    let info_hash_bytes = info_hash.1;

    println!("Found {} peers! Starting connection worker pool...", peers.len());

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
        active_peers: 0,
        active_tab: ui::AppTab::Overview,
        pieces: vec![ui::PieceStatus::Missing; num_pieces as usize],
        logs: vec!["[SYSTEM] Torx client initialized".to_string()],
        peers: vec![],
        file_names,
    }));

    let output_dir_path = std::path::Path::new("/home/daksh/Downloads");
    let storage = Arc::new(crate::storage::FileEntry::new(&file_content, output_dir_path));
    
    {
        storage.preallocate().await.unwrap();
    }

    let (ui_tx, ui_rx) = tokio::sync::mpsc::channel(100);

    let dl_state_clone = Arc::clone(&dl_state);
    let token = tokio_util::sync::CancellationToken::new();
    let token_ui = token.clone();

    let storage_dl = Arc::clone(&storage);
    let ui_tx_dl = ui_tx.clone();
    let download_handle = tokio::spawn(async move {
        if let Err(e) = run_download(
            peers, 
            peer_id_bytes,
            info_hash_bytes,
            registry, 
            dl_state_clone, 
            standard_piece_length, 
            total_length, 
            num_pieces, 
            arc_piece_hashes, 
            storage_dl, 
            ui_tx_dl,
            token
        ).await {
            crate::logger::log(&format!("run_download failed: {:?}", e));
        }
    });

    let ui_handle = tokio::spawn(async move {
        let _ = ui::run_ui(ui_state, single_file_name, total_length, ui_rx, token_ui).await;
    });

    let _ = tokio::join!(download_handle, ui_handle);

    Ok(())
}