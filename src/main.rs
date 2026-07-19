mod parser;
mod build_request;
mod response;
mod peer;
mod download;
mod piece;
mod ui;
mod logger;
mod storage;
mod upload;
mod cli;

use std::{fs::{self}, io, sync::Arc};
use parser::Torrent;
use build_request::{calculate_info_hash, split_pieces, calculate_torrent_size, generate_id, collect_all_peers};
use tokio::{sync::Mutex};

use crate::{download::{DownloadState,run_download}, peer::PeerRegistry};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let _cli = cli::parse_args();

    let (ui_tx, ui_rx) = tokio::sync::mpsc::channel(10000);
    let (setup_tx, mut setup_rx) = tokio::sync::mpsc::channel(10);
    let token = tokio_util::sync::CancellationToken::new();

    let ui_state = Arc::new(std::sync::Mutex::new(ui::UiState {
        downloaded_bytes: 0,
        uploaded_bytes: 0,
        needed_count: 0,
        total_pieces: 0,
        complete: false,
        active_peers: 0,
        active_tab: ui::AppTab::Setup,
        pieces: vec![],
        logs: vec!["[SYSTEM] Torx client initialized".to_string()],
        peers: vec![],
        file_names: vec![],
        trackers: vec![],
        torrent_name: String::new(),
        total_length: 0,
        setup_error: None,
        log_scroll_offset: 0,
        log_auto_scroll: true,
    }));

    let ui_state_clone = Arc::clone(&ui_state);
    let token_ui = token.clone();
    let ui_handle = tokio::spawn(async move {
        let _ = ui::run_ui(ui_state_clone, ui_rx, setup_tx, token_ui).await;
    });

    let (_torrent_path, download_path, torrent_file, file_content) = loop {
        let (t_path, d_path) = match setup_rx.recv().await {
            Some(paths) => paths,
            None => return Ok(()),
        };

        let t_file = match fs::read(t_path.trim()) {
            Ok(f) => f,
            Err(e) => {
                let _ = ui_tx.send(ui::UiUpdate::SetupError(format!("Failed to read torrent file: {}", e))).await;
                continue;
            }
        };

        let content: Torrent = match serde_bencode::from_bytes(&t_file) {
            Ok(c) => c,
            Err(e) => {
                let _ = ui_tx.send(ui::UiUpdate::SetupError(format!("Failed to parse torrent file: {}", e))).await;
                continue;
            }
        };
        
        break (t_path, d_path, t_file, content);
    };

    let info_hash = calculate_info_hash(&torrent_file).unwrap();
    let peer_id = generate_id();
    let pieces_split = split_pieces(&file_content.info.pieces);

    let (total_length, num_pieces) = calculate_torrent_size(&file_content);
    let standard_piece_length = file_content.info.piece_len;

    let mut file_names = vec![];
    match &file_content.info.mode {
        parser::FileMode::SingleFileMode { .. } => file_names.push(file_content.info.name.clone()),
        parser::FileMode::MultiFileMode { files } => {
            for f in files {
                file_names.push(f.path.join("/"));
            }
        }
    }

    let _ = ui_tx.send(ui::UiUpdate::Init {
        torrent_name: file_content.info.name.clone(),
        total_length,
        num_pieces,
        file_names,
    }).await;

    let mut trackers = vec![];
    trackers.push(file_content.announce.clone());
    if let Some(announce_list) = &file_content.announce_list {
        for t in announce_list {
            if let Some(url) = t.first() {
                trackers.push(url.clone());
            }
        }
    }
    let _ = ui_tx.send(ui::UiUpdate::TrackersQueried(trackers)).await;

    let peers = collect_all_peers(&file_content, &torrent_file, &peer_id, token.clone()).await?;
    let _ = ui_tx.send(ui::UiUpdate::StartTimer).await;

    let peers_queue = Arc::new(Mutex::new(peers));
    let peers_queue_refill = Arc::clone(&peers_queue);

    let peer_id_bytes: [u8; 20] = peer_id.as_bytes().try_into().expect("Length Mismatch");
    let info_hash_bytes = info_hash.1;

    let mut piece_hashes: Vec<[u8; 20]> = vec![];
    for piece in pieces_split{
        piece_hashes.push(piece);
    }

    let registry = Arc::new(Mutex::new(PeerRegistry::new(num_pieces)));
    let dl_state = Arc::new(Mutex::new(DownloadState::new(num_pieces)));
    let arc_piece_hashes = Arc::new(piece_hashes);

    let output_dir_path_buf = std::path::PathBuf::from(download_path.trim());
    let storage = Arc::new(crate::storage::FileEntry::new(&file_content, &output_dir_path_buf));
    
    {
        storage.preallocate().await.unwrap();
    }

    let (have_tx, _) = tokio::sync::broadcast::channel::<u32>(512);
    let upload_mgr = Arc::new(Mutex::new(upload::UploadManager::new()));

    let dl_state_clone = Arc::clone(&dl_state);
    let token_upload = token.clone();
    let token_choking = token.clone();
    let token_tracker = token.clone();

    let storage_dl = Arc::clone(&storage);
    let ui_tx_dl = ui_tx.clone();
    let upload_mgr_dl = Arc::clone(&upload_mgr);
    let have_tx_dl = have_tx.clone();
    let download_handle = tokio::spawn(async move {
        if let Err(e) = run_download(
            peers_queue, 
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
            token,
            upload_mgr_dl,
            have_tx_dl,
        ).await {
            crate::logger::log(&format!("run_download failed: {:?}", e));
        }
    });

    let ui_tx_tracker = ui_tx.clone();
    let file_content_tracker = file_content.clone();
    let torrent_file_tracker = torrent_file.clone();
    let peer_id_tracker = peer_id.clone();
    let tracker_handle = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(120));
        interval.tick().await; // consume first tick
        loop {
            tokio::select! {
                _ = token_tracker.cancelled() => break,
                _ = interval.tick() => {
                    let _ = ui_tx_tracker.send(ui::UiUpdate::Log("[SYSTEM] Fetching more peers from trackers...".to_string())).await;
                    if let Ok(new_peers) = collect_all_peers(&file_content_tracker, &torrent_file_tracker, &peer_id_tracker, token_tracker.clone()).await {
                        let mut q = peers_queue_refill.lock().await;
                        // Avoid adding existing peers (basic dedup by ip/port)
                        for p in new_peers {
                            if !q.iter().any(|existing| existing.ip == p.ip && existing.port == p.port) {
                                q.push(p);
                            }
                        }
                        let _ = ui_tx_tracker.send(ui::UiUpdate::Log(format!("[SYSTEM] Tracker refill: Peer queue size is now {}", q.len()))).await;
                    }
                }
            }
        }
    });

    let dl_state_upload = Arc::clone(&dl_state);
    let storage_upload = Arc::clone(&storage);
    let upload_mgr_upload = Arc::clone(&upload_mgr);
    let have_tx_upload = have_tx.clone();
    let ui_tx_upload = ui_tx.clone();

    let upload_handle = tokio::spawn(async move {
        if let Err(e) = upload::run_upload_listener(
            6881,
            info_hash_bytes,
            peer_id_bytes,
            dl_state_upload,
            upload_mgr_upload,
            storage_upload,
            standard_piece_length,
            have_tx_upload,
            ui_tx_upload,
            token_upload,
        ).await {
            crate::logger::log(&format!("Upload listener error: {:?}", e));
        }
    });

    let upload_mgr_choking = Arc::clone(&upload_mgr);
    let choking_handle = tokio::spawn(async move {
        upload::run_choking_algorithm(upload_mgr_choking, token_choking).await;
    });

    let _ = tokio::join!(download_handle, ui_handle, upload_handle, choking_handle, tracker_handle);

    Ok(())
}