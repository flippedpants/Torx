use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, Semaphore, broadcast};
use tokio_util::sync::CancellationToken;

use crate::download::{DownloadState, BLOCK_SIZE, build_bitfield};
use crate::peer::PeerMessage;
use crate::storage::FileEntry;
use crate::ui::UiUpdate;

const MAX_INCOMING: usize = 50;
const MAX_UNCHOKED: usize = 4;
const UNCHOKE_INTERVAL_SECS: u64 = 10;

// ──────────────────────────────────────────────────────────────────────────────
// Upload Manager — Tit-for-Tat Choking Engine
// ──────────────────────────────────────────────────────────────────────────────

struct UploadManagerPeer {
    choked: bool,
    interested: bool,
    bytes_received_from: u64,
    bytes_sent_to: u64,
    choke_tx: tokio::sync::watch::Sender<bool>,
}

pub struct UploadManager {
    peers: HashMap<String, UploadManagerPeer>,
    max_unchoked: usize,
    optimistic_peer: Option<String>,
}

impl UploadManager {
    pub fn new() -> Self {
        UploadManager {
            peers: HashMap::new(),
            max_unchoked: MAX_UNCHOKED,
            optimistic_peer: None,
        }
    }

    /// Register a peer and return a watch receiver for choke state changes.
    /// Peer starts in choked state.
    pub fn register_peer(&mut self, addr: String) -> tokio::sync::watch::Receiver<bool> {
        let (tx, rx) = tokio::sync::watch::channel(true); // true = choked
        self.peers.insert(addr, UploadManagerPeer {
            choked: true,
            interested: false,
            bytes_received_from: 0,
            bytes_sent_to: 0,
            choke_tx: tx,
        });
        rx
    }

    pub fn remove_peer(&mut self, addr: &str) {
        self.peers.remove(addr);
        if self.optimistic_peer.as_deref() == Some(addr) {
            self.optimistic_peer = None;
        }
    }

    pub fn set_interested(&mut self, addr: &str, interested: bool) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.interested = interested;
        }
    }

    pub fn record_download_from(&mut self, addr: &str, bytes: u64) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.bytes_received_from += bytes;
        }
    }

    pub fn record_upload_to(&mut self, addr: &str, bytes: u64) {
        if let Some(peer) = self.peers.get_mut(addr) {
            peer.bytes_sent_to += bytes;
        }
    }

    pub fn is_peer_choked(&self, addr: &str) -> bool {
        self.peers.get(addr).map(|p| p.choked).unwrap_or(true)
    }

    /// Regular unchoke round (every 10s).
    /// Unchoke the top MAX_UNCHOKED interested peers ranked by bytes they uploaded
    /// to us (tit-for-tat). The optimistic unchoke slot is preserved separately.
    pub fn run_unchoke_round(&mut self) {
        let mut interested_addrs: Vec<String> = self.peers.iter()
            .filter(|(_, p)| p.interested)
            .map(|(addr, _)| addr.clone())
            .collect();

        // Sort by tit-for-tat: peers who gave us the most data get rewarded
        interested_addrs.sort_by(|a, b| {
            let a_bytes = self.peers.get(a).map(|p| p.bytes_received_from).unwrap_or(0);
            let b_bytes = self.peers.get(b).map(|p| p.bytes_received_from).unwrap_or(0);
            b_bytes.cmp(&a_bytes)
        });

        let mut unchoked_set: HashSet<String> = HashSet::new();

        // Protect optimistic unchoke slot (doesn't count toward limit)
        if let Some(ref opt_peer) = self.optimistic_peer {
            if self.peers.get(opt_peer).map(|p| p.interested).unwrap_or(false) {
                unchoked_set.insert(opt_peer.clone());
            }
        }

        // Unchoke top MAX_UNCHOKED interested peers by contribution
        let mut regular_count = 0;
        for addr in &interested_addrs {
            if regular_count >= self.max_unchoked { break; }
            if unchoked_set.contains(addr) { continue; }
            unchoked_set.insert(addr.clone());
            regular_count += 1;
        }

        // Apply choke/unchoke state changes and send via watch channels
        for (addr, peer) in self.peers.iter_mut() {
            let should_unchoke = unchoked_set.contains(addr);
            if should_unchoke && peer.choked {
                peer.choked = false;
                let _ = peer.choke_tx.send(false);
            } else if !should_unchoke && !peer.choked {
                peer.choked = true;
                let _ = peer.choke_tx.send(true);
            }
        }
    }

    /// Optimistic unchoke (every 30s).
    /// Randomly unchoke one choked+interested peer to give new peers a chance.
    pub fn run_optimistic_unchoke(&mut self) {
        let candidates: Vec<String> = self.peers.iter()
            .filter(|(_, p)| p.choked && p.interested)
            .map(|(addr, _)| addr.clone())
            .collect();

        self.optimistic_peer = None;

        if !candidates.is_empty() {
            let idx = rand::random_range(0..candidates.len());
            let chosen = candidates[idx].clone();
            if let Some(peer) = self.peers.get_mut(&chosen) {
                peer.choked = false;
                let _ = peer.choke_tx.send(false);
            }
            self.optimistic_peer = Some(chosen);
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Helper: read a BitTorrent message from any AsyncRead source
// ──────────────────────────────────────────────────────────────────────────────

async fn read_msg<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<PeerMessage, Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut buf_length = [0u8; 4];
    reader.read_exact(&mut buf_length).await?;
    let message_length = u32::from_be_bytes(buf_length);

    if message_length == 0 {
        return Ok(PeerMessage::KeepAlive);
    }
    // Guard against memory exhaustion (max 1 MB message)
    if message_length > 1 << 20 {
        return Err("Message too large".into());
    }

    let mut message = vec![0u8; message_length as usize];
    reader.read_exact(&mut message).await?;

    let id = message[0];
    PeerMessage::parse_peer_message(&id, &message[1..])
}

// ──────────────────────────────────────────────────────────────────────────────
// Shared serve loop — used by both incoming and outbound peers
// ──────────────────────────────────────────────────────────────────────────────

/// Serve pieces to a connected peer. Handles Interested/NotInterested,
/// Request (block upload), choke/unchoke signaling, and Have broadcasts.
///
/// Called by both `handle_incoming_peer` (for inbound connections) and
/// `peer_task` (for outbound connections transitioning to seed mode).
pub async fn serve_peer(
    stream: &mut tokio::net::TcpStream,
    peer_addr: &str,
    dl_state: &Arc<Mutex<DownloadState>>,
    upload_mgr: &Arc<Mutex<UploadManager>>,
    storage: &Arc<FileEntry>,
    standard_piece_length: u64,
    have_tx: &broadcast::Sender<u32>,
    ui_tx: &tokio::sync::mpsc::Sender<UiUpdate>,
    token: &CancellationToken,
) {
    // Register with the choking engine
    let mut choke_rx = {
        let mut mgr = upload_mgr.lock().await;
        mgr.register_peer(peer_addr.to_string())
    };

    // Announce all pieces we currently have (batch write for efficiency)
    {
        let state = dl_state.lock().await;
        let mut batch = Vec::new();
        for (i, &needed) in state.needed.iter().enumerate() {
            if !needed {
                batch.extend_from_slice(&5u32.to_be_bytes()); // length = 5
                batch.push(4); // Have message ID
                batch.extend_from_slice(&(i as u32).to_be_bytes());
            }
        }
        if !batch.is_empty() {
            if stream.write_all(&batch).await.is_err() {
                let mut mgr = upload_mgr.lock().await;
                mgr.remove_peer(peer_addr);
                return;
            }
        }
    }

    let mut have_rx = have_tx.subscribe();

    // Split stream so we can read and write concurrently in select!
    let (mut reader, mut writer) = stream.split();
    let mut keepalive_interval = tokio::time::interval(std::time::Duration::from_secs(120));
    keepalive_interval.tick().await; // consume initial immediate tick

    loop {
        tokio::select! {
            _ = token.cancelled() => break,

            _ = keepalive_interval.tick() => {
                if writer.write_all(&[0, 0, 0, 0]).await.is_err() { break; }
            }

            result = choke_rx.changed() => {
                if result.is_err() { break; }
                let choked = *choke_rx.borrow();
                let msg_id: u8 = if choked { 0 } else { 1 }; // 0=Choke, 1=Unchoke
                if writer.write_all(&[0, 0, 0, 1, msg_id]).await.is_err() { break; }
                crate::logger::log(&format!(
                    "[SERVE:{}] Sent {}", peer_addr, if choked { "choke" } else { "unchoke" }
                ));
            }

            result = have_rx.recv() => {
                match result {
                    Ok(piece_index) => {
                        let mut msg = Vec::with_capacity(9);
                        msg.extend_from_slice(&5u32.to_be_bytes());
                        msg.push(4);
                        msg.extend_from_slice(&piece_index.to_be_bytes());
                        if writer.write_all(&msg).await.is_err() { break; }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        crate::logger::log(&format!(
                            "[SERVE:{}] Lagged {} have messages", peer_addr, n
                        ));
                    }
                    Err(_) => break,
                }
            }

            msg = read_msg(&mut reader) => {
                match msg {
                    Ok(PeerMessage::Interested) => {
                        let mut mgr = upload_mgr.lock().await;
                        mgr.set_interested(peer_addr, true);
                        crate::logger::log(&format!("[SERVE:{}] Peer interested", peer_addr));
                    }
                    Ok(PeerMessage::NotInterested) => {
                        let mut mgr = upload_mgr.lock().await;
                        mgr.set_interested(peer_addr, false);
                    }
                    Ok(PeerMessage::Request { index, begin, length }) => {
                        // Drop requests from choked peers (per BT spec)
                        let is_choked = {
                            let mgr = upload_mgr.lock().await;
                            mgr.is_peer_choked(peer_addr)
                        };
                        if is_choked { continue; }

                        // Validate we actually have the piece
                        let have_piece = {
                            let state = dl_state.lock().await;
                            (index as usize) < state.needed.len()
                                && !state.needed[index as usize]
                        };
                        if !have_piece { continue; }

                        // Reject oversized block requests (anti-abuse)
                        if length > BLOCK_SIZE as u32 * 2 { continue; }

                        // Read block from disk and send Piece response
                        match storage.read_block(
                            index, standard_piece_length, begin, length as u64
                        ).await {
                            Ok(data) => {
                                let msg_len = (9 + data.len()) as u32;
                                let mut piece_msg = Vec::with_capacity(4 + 9 + data.len());
                                piece_msg.extend_from_slice(&msg_len.to_be_bytes());
                                piece_msg.push(7); // Piece message ID
                                piece_msg.extend_from_slice(&index.to_be_bytes());
                                piece_msg.extend_from_slice(&(begin as u32).to_be_bytes());
                                piece_msg.extend_from_slice(&data);

                                if writer.write_all(&piece_msg).await.is_err() { break; }

                                let bytes_sent = data.len() as u64;
                                {
                                    let mut mgr = upload_mgr.lock().await;
                                    mgr.record_upload_to(peer_addr, bytes_sent);
                                }
                                {
                                    let mut state = dl_state.lock().await;
                                    state.uploaded_bytes += bytes_sent;
                                    let _ = ui_tx.send(
                                        UiUpdate::UploadedBytes(state.uploaded_bytes)
                                    ).await;
                                }
                                let _ = ui_tx.send(UiUpdate::PeerStats {
                                    ip: peer_addr.to_string(),
                                    downloaded_delta: 0,
                                    uploaded_delta: bytes_sent,
                                    progress: 0.0,
                                }).await;
                            }
                            Err(e) => {
                                crate::logger::log(&format!(
                                    "[SERVE:{}] Read block error: {}", peer_addr, e
                                ));
                            }
                        }
                    }
                    Ok(PeerMessage::Cancel) => {
                        // Requests are served immediately so there's nothing to cancel
                    }
                    Ok(PeerMessage::KeepAlive) => {}
                    Ok(_) => {}
                    Err(e) => {
                        crate::logger::log(&format!(
                            "[SERVE:{}] Read error: {}", peer_addr, e
                        ));
                        break;
                    }
                }
            }
        }
    }

    // Cleanup: deregister from the choking engine
    {
        let mut mgr = upload_mgr.lock().await;
        mgr.remove_peer(peer_addr);
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// TCP Listener — accept incoming peer connections
// ──────────────────────────────────────────────────────────────────────────────

pub async fn run_upload_listener(
    bind_port: u16,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    dl_state: Arc<Mutex<DownloadState>>,
    upload_mgr: Arc<Mutex<UploadManager>>,
    storage: Arc<FileEntry>,
    standard_piece_length: u64,
    have_tx: broadcast::Sender<u32>,
    ui_tx: tokio::sync::mpsc::Sender<UiUpdate>,
    token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = match TcpListener::bind(("0.0.0.0", bind_port)).await {
        Ok(l) => l,
        Err(e) => {
            crate::logger::log(&format!(
                "[UPLOAD] Failed to bind port {}: {}. Upload disabled.", bind_port, e
            ));
            let _ = ui_tx.send(UiUpdate::Log(format!(
                "[WARN] Upload listener failed to bind port {}: {}", bind_port, e
            ))).await;
            // Don't crash — just wait for shutdown and run without upload
            token.cancelled().await;
            return Ok(());
        }
    };

    let semaphore = Arc::new(Semaphore::new(MAX_INCOMING));

    crate::logger::log(&format!("[UPLOAD] Listening on port {}", bind_port));
    let _ = ui_tx.send(UiUpdate::Log(format!(
        "[SYSTEM] Upload listener active on port {}", bind_port
    ))).await;

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                crate::logger::log("[UPLOAD] Listener shutting down");
                break;
            }
            accept_result = listener.accept() => {
                match accept_result {
                    Ok((stream, addr)) => {
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                crate::logger::log(
                                    "[UPLOAD] Max incoming connections reached, rejecting"
                                );
                                continue;
                            }
                        };

                        let peer_addr = addr.to_string();
                        let dl_state = Arc::clone(&dl_state);
                        let upload_mgr = Arc::clone(&upload_mgr);
                        let storage = Arc::clone(&storage);
                        let have_tx = have_tx.clone();
                        let ui_tx = ui_tx.clone();
                        let token = token.clone();

                        tokio::spawn(async move {
                            handle_incoming_peer(
                                stream, peer_addr, info_hash, peer_id,
                                dl_state, upload_mgr, storage,
                                standard_piece_length,
                                have_tx, ui_tx, token,
                            ).await;
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        crate::logger::log(&format!("[UPLOAD] Accept error: {}", e));
                    }
                }
            }
        }
    }

    Ok(())
}

/// Handle a single incoming peer: validate handshake, send bitfield, then serve.
async fn handle_incoming_peer(
    mut stream: tokio::net::TcpStream,
    peer_addr: String,
    info_hash: [u8; 20],
    peer_id: [u8; 20],
    dl_state: Arc<Mutex<DownloadState>>,
    upload_mgr: Arc<Mutex<UploadManager>>,
    storage: Arc<FileEntry>,
    standard_piece_length: u64,
    have_tx: broadcast::Sender<u32>,
    ui_tx: tokio::sync::mpsc::Sender<UiUpdate>,
    token: CancellationToken,
) {
    crate::logger::log(&format!("[UPLOAD:{}] Incoming connection", peer_addr));

    // 1. Read their handshake (with timeout)
    let mut handshake_buf = [0u8; 68];
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        stream.read_exact(&mut handshake_buf),
    ).await {
        Ok(Ok(_)) => {}
        _ => {
            crate::logger::log(&format!(
                "[UPLOAD:{}] Handshake read failed/timeout", peer_addr
            ));
            return;
        }
    }

    // 2. Validate protocol string and info hash
    if handshake_buf[0] != 19 || &handshake_buf[1..20] != b"BitTorrent protocol" {
        crate::logger::log(&format!("[UPLOAD:{}] Invalid protocol string", peer_addr));
        return;
    }
    if &handshake_buf[28..48] != &info_hash {
        crate::logger::log(&format!("[UPLOAD:{}] Info hash mismatch", peer_addr));
        return;
    }

    // 3. Send our handshake response
    let mut our_handshake = [0u8; 68];
    our_handshake[0] = 19;
    our_handshake[1..20].copy_from_slice(b"BitTorrent protocol");
    our_handshake[28..48].copy_from_slice(&info_hash);
    our_handshake[48..68].copy_from_slice(&peer_id);
    if stream.write_all(&our_handshake).await.is_err() {
        crate::logger::log(&format!("[UPLOAD:{}] Failed to send handshake", peer_addr));
        return;
    }

    // 4. Send our bitfield
    {
        let state = dl_state.lock().await;
        let bitfield = build_bitfield(&state.needed);
        let bf_len = (1 + bitfield.len()) as u32;
        let mut bf_msg = Vec::with_capacity(4 + 1 + bitfield.len());
        bf_msg.extend_from_slice(&bf_len.to_be_bytes());
        bf_msg.push(5); // Bitfield message ID
        bf_msg.extend_from_slice(&bitfield);
        if stream.write_all(&bf_msg).await.is_err() {
            crate::logger::log(&format!("[UPLOAD:{}] Failed to send bitfield", peer_addr));
            return;
        }
    }

    let _ = ui_tx.send(UiUpdate::ActivePeers(1)).await;
    let _ = ui_tx.send(UiUpdate::Log(format!(
        "[UPLOAD] Incoming peer {} connected", peer_addr
    ))).await;

    // 5. Enter the shared serve loop
    serve_peer(
        &mut stream, &peer_addr,
        &dl_state, &upload_mgr, &storage,
        standard_piece_length,
        &have_tx, &ui_tx, &token,
    ).await;

    let _ = ui_tx.send(UiUpdate::ActivePeers(-1)).await;
    let _ = ui_tx.send(UiUpdate::Log(format!(
        "[UPLOAD] Incoming peer {} disconnected", peer_addr
    ))).await;
}

// ──────────────────────────────────────────────────────────────────────────────
// Choking Algorithm Timer
// ──────────────────────────────────────────────────────────────────────────────

/// Runs the tit-for-tat choking algorithm on a timer:
/// - Every 10s: regular unchoke round (reward top contributors)
/// - Every 30s: optimistic unchoke (give a random peer a chance)
pub async fn run_choking_algorithm(
    upload_mgr: Arc<Mutex<UploadManager>>,
    token: CancellationToken,
) {
    let mut unchoke_interval = tokio::time::interval(
        std::time::Duration::from_secs(UNCHOKE_INTERVAL_SECS)
    );
    let mut optimistic_counter: u64 = 0;

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = unchoke_interval.tick() => {
                let mut mgr = upload_mgr.lock().await;
                mgr.run_unchoke_round();

                // Optimistic unchoke every 3rd round (30s)
                optimistic_counter += 1;
                if optimistic_counter % 3 == 0 {
                    mgr.run_optimistic_unchoke();
                }
            }
        }
    }
}
