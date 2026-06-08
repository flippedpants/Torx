use std::{collections::{HashMap, HashSet}, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};
use tokio_util::sync::CancellationToken;

use crate::{download::{DownloadState, download_piece}, piece::verify_piece};

#[derive(Debug)]
pub enum PeerMessage{
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have (u32),
    Bitfield (Vec<u8>),
    Request { index: u32, begin: u64, length: u32},
    Piece {index: u32, begin: u64, data: Vec<u8>},
    Cancel
}

impl PeerMessage{
    pub fn parse_peer_message(id: &u8, payload: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>>{
        match id {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have(u32::from_be_bytes(payload[..4].try_into()?))),
            5 => Ok(Self::Bitfield(payload.to_vec())),
            6 => Ok(Self::Request {
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u32::from_be_bytes(payload[4..8].try_into()?) as u64,
                length: u32::from_be_bytes(payload[8..12].try_into()?)
            }),
            7 => Ok(Self::Piece{
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u32::from_be_bytes(payload[4..8].try_into()?) as u64,
                data: payload[8..].to_vec(),
            }),
            8 => Ok(Self::Cancel),
            _ => Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid message id!")))

        }
    }
}

pub struct PeerRegistry{
    peers: HashMap<String, HashSet<u32>>,
    num_pieces: u32,
    piece_availability: Vec<u32>,
}

impl PeerRegistry{
    pub fn new(num_pieces: u32) -> Self{
        PeerRegistry { peers: HashMap::new(), num_pieces, piece_availability: vec![0; num_pieces as usize] }
    }

    pub fn set_bitfield(&mut self, peer_addr: &String, bitfield: &[u8]){
        let pieces: HashSet<u32> = bitfield
            .iter()
            .enumerate()
            .flat_map(|(byte_i, byte)| {
                (0..8).rev().filter_map(move |bit_i| {
                    if byte & (1 << bit_i) != 0 {
                        Some((byte_i * 8 + (7 - bit_i)) as u32)
                    } else {
                        None
                    }
                })
            })
            .filter(|&i| i < self.num_pieces)
            .collect();

        for &piece in &pieces{
            self.piece_availability[piece as usize] += 1;
        }

        self.peers.insert(peer_addr.to_string(), pieces);
    }

    pub fn set_have(&mut self, peer_addr: &String, piece_index: u32){
        let entry = self.peers
            .entry(peer_addr.to_string())
            .or_insert_with(HashSet::new);

        if entry.insert(piece_index) {
            self.piece_availability[piece_index as usize] += 1;
        }
    }

    pub fn peer_has(&self, peer_adr: &String, piece_index: u32) -> bool {
        self.peers.get(peer_adr)
            .map(|pieces| pieces.contains(&piece_index))
            .unwrap_or(false)
    }

    pub fn peers_with_pieces(&self, piece_index: u32) -> Vec<String>{
        self.peers.iter()
            .filter(|(_, pieces)| pieces.contains(&piece_index))
            .map(|(addr, _)| addr.clone())
            .collect()
    }

    pub fn rarest_piece_for_peer(&self, peer_addr: &String, needed: &[bool], in_progress: &HashSet<u32>) -> Option<u32> {
        let peer_pieces = self.peers.get(peer_addr)?;

        peer_pieces.iter()
            .filter(|&&i| needed.get(i as usize).copied().unwrap_or(false) && !in_progress.contains(&i))
            .min_by_key(|&&i| self.piece_availability[i as usize])
            .copied()
    }
}

pub async fn read_message(stream: &mut TcpStream) -> Result<PeerMessage, Box<dyn std::error::Error + Send + Sync + 'static>>{
    let mut buf_length = [0u8; 4];
    stream.read_exact(&mut buf_length).await?;
    let message_length = u32::from_be_bytes(buf_length);

    if message_length == 0 {
        return Ok(PeerMessage::KeepAlive);
    }

    let mut message = vec![0u8; message_length as usize];
    stream.read_exact(&mut message).await?;

    let id = message[0];
    PeerMessage::parse_peer_message(&id, &message[1..])
}

pub async fn peer_task(
    peer_addr: String,
    mut stream: TcpStream,
    registry: Arc<Mutex<PeerRegistry>>,
    dl_state: Arc<Mutex<DownloadState>>,
    token: CancellationToken,
    standard_piece_length: u64,
    total_length: u64,
    num_pieces: u32,
    piece_hashes: Arc<Vec<[u8; 20]>>,
    storage: Arc<crate::storage::FileEntry>,
    ui_tx: tokio::sync::mpsc::Sender<crate::ui::UiUpdate>
){
    crate::logger::log(&format!("[{}] - task started", peer_addr));

    loop{
        tokio::select! {
            _ = token.cancelled() => {
                let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
                return;
            },
            msg = read_message(&mut stream) => {
                match msg {
                    Ok(PeerMessage::Bitfield(b)) => {
                        let mut reg = registry.lock().await;
                        reg.set_bitfield(&peer_addr, &b);
                        break;
                    }
                    Ok(PeerMessage::Have(i)) => {
                        let mut reg = registry.lock().await;
                        reg.set_have(&peer_addr, i);
                    }
                    Ok(PeerMessage::Unchoke) =>{
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        crate::logger::log(&format!("[{}] error during init - {}", peer_addr, e));
                        let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
                        return;
                    }
                }
            }
        }
    }

    if stream.write_all(&[0,0,0,1,2]).await.is_err() {
        crate::logger::log(&format!("Could not send interested to [{}]", peer_addr));
        let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
        return;
    }

    // Upsert peer info initially
    let _ = ui_tx.send(crate::ui::UiUpdate::PeerStats {
        ip: peer_addr.clone(),
        downloaded_delta: 0,
        uploaded_delta: 0,
        progress: 0.0,
    }).await;

    loop {
        if token.is_cancelled() {
            let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
            return;
        }

        let (needed, in_progress) = {
            let state = dl_state.lock().await;
            (state.needed.clone(), state.in_progress.clone())
        };

        let piece_index = {
            let reg = registry.lock().await;
            reg.rarest_piece_for_peer(&peer_addr, &needed, &in_progress)
        };

        let Some(piece_index) = piece_index else{
            crate::logger::log(&format!("[{}] no more pieces to download", peer_addr));
            let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[SYSTEM] Peer {} disconnected (no more pieces)", peer_addr))).await;
            let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
            return;
        };

        {
            let mut state = dl_state.lock().await;
            state.in_progress.insert(piece_index);
            
            let _ = ui_tx.send(crate::ui::UiUpdate::PieceStatus(piece_index, crate::ui::PieceStatus::Downloading)).await;
            let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[DEBUG] Requesting piece {} from {}", piece_index, peer_addr))).await;
        }

        let piece_length = if piece_index == num_pieces - 1 {
            total_length - (standard_piece_length * (num_pieces as u64 - 1))
        } else {
            standard_piece_length
        };

        match download_piece(&mut stream, &peer_addr, piece_length, piece_index, &token).await {
            Ok(piece_buf) => {
                if !verify_piece(&piece_buf.data, &piece_hashes[piece_index as usize]){
                    crate::logger::log(&format!("[{}] Piece mismatch, discarding piece {}", peer_addr, piece_index));
                    let mut state = dl_state.lock().await;
                    state.mark_failed(piece_index);
                    let _ = ui_tx.send(crate::ui::UiUpdate::PieceStatus(piece_index, crate::ui::PieceStatus::Missing)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[ERROR] Piece {} verification failed from {}", piece_index, peer_addr))).await;
                    continue;
                }

                crate::logger::log(&format!("[{}] piece verified - {}", peer_addr, piece_index));

                if storage.write_piece(piece_index, standard_piece_length, &piece_buf.data).await.is_err() {
                    crate::logger::log(&format!("[{}] failed to write piece {}", peer_addr, piece_index));
                    let mut state = dl_state.lock().await;
                    state.mark_failed(piece_index);
                    let _ = ui_tx.send(crate::ui::UiUpdate::PieceStatus(piece_index, crate::ui::PieceStatus::Missing)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[ERROR] Failed to write piece {} to disk", piece_index))).await;
                    continue;
                }

                {
                    let mut state = dl_state.lock().await;
                    state.mark_done(piece_index);
                    state.downloaded_bytes += piece_length as u64;

                    let _ = ui_tx.send(crate::ui::UiUpdate::DownloadedBytes(state.downloaded_bytes)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::PieceStatus(piece_index, crate::ui::PieceStatus::Complete)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[INFO] Verified and saved piece {} from {}", piece_index, peer_addr))).await;
                    
                    let progress = (state.num_pieces - state.needed.iter().filter(|&&n| n).count() as u32) as f64 / state.num_pieces as f64;
                    let _ = ui_tx.send(crate::ui::UiUpdate::PeerStats {
                        ip: peer_addr.clone(),
                        downloaded_delta: piece_length as u64,
                        uploaded_delta: 0,
                        progress,
                    }).await;

                    if state.is_complete() {
                        token.cancel();
                        let _ = ui_tx.send(crate::ui::UiUpdate::Log("[SYSTEM] Download complete!".to_string())).await;
                        let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
                        return;
                    }
                }
            }

            Err(e) => {
                crate::logger::log(&format!("[{}] piece {} failed: {}", peer_addr, piece_index, e));
                
                let is_terminal = if let Some(io_err) = e.downcast_ref::<std::io::Error>() {
                    matches!(io_err.kind(), 
                        std::io::ErrorKind::BrokenPipe | 
                        std::io::ErrorKind::ConnectionReset | 
                        std::io::ErrorKind::ConnectionAborted |
                        std::io::ErrorKind::UnexpectedEof |
                        std::io::ErrorKind::NotConnected)
                } else {
                    false
                };

                {
                    let mut state = dl_state.lock().await;
                    state.mark_failed(piece_index);
                    let _ = ui_tx.send(crate::ui::UiUpdate::PieceStatus(piece_index, crate::ui::PieceStatus::Missing)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[WARN] Piece {} failed from {}: {}", piece_index, peer_addr, e))).await;
                }

                if is_terminal {
                    let _ = ui_tx.send(crate::ui::UiUpdate::ActivePeers(-1)).await;
                    let _ = ui_tx.send(crate::ui::UiUpdate::Log(format!("[SYSTEM] Peer {} disconnected (terminal error)", peer_addr))).await;
                    return;
                }
                continue;
            }
        }
    }
}