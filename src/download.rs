use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::{io::{AsyncWriteExt, AsyncReadExt}, net::TcpStream, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{peer::{PeerMessage, PeerRegistry, read_message}, piece::PieceBuf, response::PeerAddress};

pub const BLOCK_SIZE: u64 = 16384;

#[derive(Debug)]
pub struct Handshake{
    info_hash: [u8; 20],
    peer_id: [u8; 20]
}

impl Handshake{
    fn new(info_hash: [u8; 20], peer_id: [u8; 20]) -> Self{
        Self {
            info_hash,
            peer_id
        }
    }

    fn serialize(&self) -> [u8;68]{
        let mut buf: [u8; 68] = [0; 68];
        buf[0] = 19;
        buf[1..=19].copy_from_slice(b"BitTorrent protocol");
        buf[28..=47].copy_from_slice(&self.info_hash);
        buf[48..=67].copy_from_slice(&self.peer_id);

        buf
    }
}

pub struct DownloadState {
    pub needed: Vec<bool>,
    pub in_progress: HashSet<u32>,
    pub num_pieces: u32,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub active_peers: u32,
}   

impl DownloadState {
    pub fn new(num_pieces: u32) -> Self{
        DownloadState{
            needed: vec![true; num_pieces as usize],
            in_progress: HashSet::new(),
            num_pieces,
            downloaded_bytes: 0,
            uploaded_bytes: 0,
            active_peers: 0,
        }
    }

    pub fn is_complete(&self) -> bool {
        self.needed.iter().all(|x| !x)
    }

    pub fn mark_done(&mut self, piece_index: u32){
        self.needed[piece_index as usize] = false;
        self.in_progress.remove(&piece_index);
    }

    pub fn mark_failed(&mut self, piece_index: u32){
        self.needed[piece_index as usize] = true;
        self.in_progress.insert(piece_index);
    }
}   

/// Build a BitTorrent-spec bitfield from the needed array.
/// A set bit means we HAVE that piece (inverse of needed).
pub fn build_bitfield(needed: &[bool]) -> Vec<u8> {
    let num_bytes = (needed.len() + 7) / 8;
    let mut bitfield = vec![0u8; num_bytes];
    for (i, &is_needed) in needed.iter().enumerate() {
        if !is_needed {
            bitfield[i / 8] |= 1 << (7 - (i % 8));
        }
    }
    bitfield
}

pub async fn bit_torrent_handshake(peer: &PeerAddress, peer_id: [u8; 20],info_hash_bytes: [u8; 20]) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync + 'static>> {
    
    let handshake = Handshake::new(info_hash_bytes, peer_id);
    let req_bytes = handshake.serialize();

    let addr = format!("{}:{}", peer.ip, peer.port);

    let handshake_future = async {
        let mut stream = TcpStream::connect(&addr).await?;
        stream.write_all(&req_bytes).await?;
        
        let mut handshake_res_buf = [0u8; 68];
        stream.read_exact(&mut handshake_res_buf).await?;
        
        if handshake_res_buf[0] != 19 || &handshake_res_buf[1..=19] != b"BitTorrent protocol" {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid protocol response from {}", addr)));
        }

        if &handshake_res_buf[28..48] != &info_hash_bytes {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Info hash mismatch with peer {}", addr)));
        }
        
        Ok::<TcpStream, std::io::Error>(stream)
    };

    match timeout(Duration::from_secs(3), handshake_future).await {
        Ok(Ok(stream)) => Ok(stream),
        Ok(Err(e)) => Err(e.into()),
        Err(_) => Err("Handshake timeout".into())
    }
}

pub async fn download_piece(stream: &mut TcpStream,peer_addr: &String, piece_length: u64, piece_index: u32, token: &CancellationToken, registry: &Arc<Mutex<PeerRegistry>>)-> Result<PieceBuf, Box<dyn std::error::Error + Send + Sync + 'static>> {

    let num_blocks = (piece_length + BLOCK_SIZE -1 )/ BLOCK_SIZE;
    let buf = Arc::new(Mutex::new(PieceBuf::new(piece_length)));
    let pipeline_depth = 250;

    // Initial fill of the pipeline
    request_blocks(stream, piece_length, piece_index, num_blocks, Arc::clone(&buf), pipeline_depth).await?;

    let mut timeout_count = 0;

    loop {
        if token.is_cancelled() {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "cancelled")));
        }

        tokio::select! {
            _ = token.cancelled() => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Cancelled"))),
            msg = tokio::time::timeout(Duration::from_secs(5), read_message(stream)) => {
                match msg {
                    Ok(Ok(PeerMessage::Piece {index, begin, data})) => {
                        timeout_count = 0;
                        if index != piece_index {continue;}
                        
                        let mut b = buf.lock().await;
                        b.add_block(begin, &data);

                        if b.is_complete() {
                            drop(b);
                            return Ok(Arc::try_unwrap(buf).unwrap().into_inner());
                        }
                        drop(b);

                        // Refill the pipeline immediately as soon as we get a block
                        request_blocks(stream, piece_length, piece_index, num_blocks, Arc::clone(&buf), pipeline_depth).await?;
                    },

                    Ok(Ok(PeerMessage::Choke)) => {
                        timeout_count = 0;
                        crate::logger::log(&format!("[{}] choked mid-piece {}", peer_addr, piece_index));
                        wait_for_unchoke(stream, peer_addr, registry).await?;
                        
                        // After unchoke, reset requested status for non-received blocks and refill
                        {
                            let mut b = buf.lock().await;
                            for i in 0..num_blocks as usize {
                                if !b.recv_blocks[i] {
                                    b.requested_blocks[i] = false;
                                }
                            }
                        }
                        request_blocks(stream, piece_length, piece_index, num_blocks, Arc::clone(&buf), pipeline_depth).await?;
                    },
                    Ok(Ok(PeerMessage::Have(i))) => { 
                        timeout_count = 0; 
                        let mut reg = registry.lock().await;
                        reg.set_have(peer_addr, i);
                    },
                    Ok(Ok(_)) => { timeout_count = 0; continue; },
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        timeout_count += 1;
                        if timeout_count >= 8 {
                            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "Piece download timed out")));
                        }
                        // Timeout: assume lost requests, reset unreceived requested blocks, and re-request
                        {
                            let mut b = buf.lock().await;
                            for i in 0..num_blocks as usize {
                                if !b.recv_blocks[i] {
                                    b.requested_blocks[i] = false;
                                }
                            }
                        }
                        request_blocks(stream, piece_length, piece_index, num_blocks, Arc::clone(&buf), pipeline_depth).await?;
                    },
                }
            }
        }
    }    
}

pub async fn wait_for_unchoke(stream: &mut TcpStream, peer_addr: &String, registry: &Arc<Mutex<PeerRegistry>>) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>{
    let unchoke  = timeout(Duration::from_secs(120), async {
    loop{
        match read_message(stream).await? {
            PeerMessage::Choke => { /* println!("got choke — continuing to wait"); */ continue;},
            PeerMessage::Unchoke => return Ok(()),
            PeerMessage::KeepAlive => {
                // println!("Keep alive");
                tokio::task::yield_now().await;
                continue;
            },
            PeerMessage::Have(i) => { 
                let mut reg = registry.lock().await;
                reg.set_have(peer_addr, i);
            },
            PeerMessage::Bitfield(b) => { 
                let mut reg = registry.lock().await;
                reg.set_bitfield(peer_addr, &b);
            },
            _ =>{
                tokio::task::yield_now().await;
                continue;
            },
        };
    }

    #[allow(unreachable_code)]
    Ok::<(), Box<dyn std::error::Error + Send + Sync + 'static>>(() )

    }).await;

    match unchoke{
        Ok(Ok(())) => Ok(()),
        _ => Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "unchoke timeout")))
    }
}

pub async fn request_blocks(
    stream: &mut TcpStream, 
    piece_length: u64, 
    piece_index: u32,
    num_blocks: u64, 
    buf: Arc<Mutex<PieceBuf>>,
    max_in_flight: usize
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {

    let mut b = buf.lock().await;
    
    let in_flight = b.requested_blocks.iter().zip(b.recv_blocks.iter())
        .filter(|&(&req, &rec)| req && !rec).count();
    
    let mut to_request = max_in_flight.saturating_sub(in_flight);

    for i in 0..num_blocks {
        if to_request == 0 { break; }
        
        if b.requested_blocks[i as usize] || b.recv_blocks[i as usize] {
            continue;
        }

        let begin = i * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(piece_length - begin);

        let mut req = Vec::with_capacity(17);
        req.extend_from_slice(&13u32.to_be_bytes());
        req.push(6);
        req.extend_from_slice(&piece_index.to_be_bytes());
        req.extend_from_slice(&(begin as u32).to_be_bytes());
        req.extend_from_slice(&(length as u32).to_be_bytes());
        
        stream.write_all(&req).await?;
        
        b.requested_blocks[i as usize] = true;
        to_request -= 1;
    }
    
    Ok(())
}

pub async fn run_download(
    peers_queue: Arc<Mutex<Vec<PeerAddress>>>,
    peer_id: [u8; 20],
    info_hash: [u8; 20],
    registry: Arc<Mutex<PeerRegistry>>,
    dl_state: Arc<Mutex<DownloadState>>,
    standard_piece_length: u64,
    total_length: u64,
    num_pieces: u32,
    piece_hashes: Arc<Vec<[u8; 20]>>,
    storage: Arc<crate::storage::FileEntry>,
    ui_tx: tokio::sync::mpsc::Sender<crate::ui::UiUpdate>,
    token: CancellationToken,
    upload_mgr: Arc<Mutex<crate::upload::UploadManager>>,
    have_tx: tokio::sync::broadcast::Sender<u32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {

    let mut handles: Vec<tokio::task::JoinHandle<()>> = vec![];

    let num_workers = 250; // Max concurrent connections

    for _ in 0..num_workers {
        let queue_clone = Arc::clone(&peers_queue);
        let registry_clone = Arc::clone(&registry);
        let dl_state_clone = Arc::clone(&dl_state);
        let hashes_clone = Arc::clone(&piece_hashes);
        let storage_clone = Arc::clone(&storage);
        let ui_tx_clone = ui_tx.clone();
        let token_clone = token.clone();
        let upload_mgr_clone = Arc::clone(&upload_mgr);
        let have_tx_clone = have_tx.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                if token_clone.is_cancelled() { break; }
                
                let peer_opt = {
                    let mut q = queue_clone.lock().await;
                    q.pop()
                };
                
                let Some(peer) = peer_opt else {
                    // Queue empty, wait for more peers from tracker
                    if dl_state_clone.lock().await.is_complete() {
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                };

                let peer_addr = format!("{}:{}", peer.ip, peer.port);
                
                match bit_torrent_handshake(&peer, peer_id, info_hash).await {
                    Ok(stream) => {
                        let _ = ui_tx_clone.send(crate::ui::UiUpdate::ActivePeers(1)).await;
                        crate::peer::peer_task(
                            peer_addr,
                            stream, 
                            Arc::clone(&registry_clone),
                            Arc::clone(&dl_state_clone),
                            token_clone.clone(),
                            standard_piece_length,
                            total_length,
                            num_pieces,
                            Arc::clone(&hashes_clone),
                            Arc::clone(&storage_clone),
                            ui_tx_clone.clone(),
                            Arc::clone(&upload_mgr_clone),
                            have_tx_clone.clone(),
                        ).await;
                    }
                    Err(_) => {
                        // Handshake failed, loop around to pop another peer
                        continue;
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}