use std::{collections::HashSet, sync::Arc};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::{io::{AsyncWriteExt, AsyncReadExt}, net::TcpStream, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{peer::{PeerMessage, PeerRegistry, peer_task, read_message}, piece::PieceBuf, response::PeerAddress};

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
    pub active_peers: u32,
}   

impl DownloadState {
    pub fn new(num_pieces: u32) -> Self{
        DownloadState{
            needed: vec![true; num_pieces as usize],
            in_progress: HashSet::new(),
            num_pieces,
            downloaded_bytes: 0,
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

pub async fn bit_torrent_handshake(peer: &PeerAddress, peer_id: [u8; 20],info_hash_bytes: [u8; 20]) -> Result<TcpStream, Box<dyn std::error::Error + Send + Sync + 'static>> {
    
    let handshake = Handshake::new(info_hash_bytes, peer_id);
    // println!("{:?}", handshake);
    let req_bytes = handshake.serialize();

    // println!("{:?}", peer);
    let addr = format!("{}:{}", peer.ip, peer.port);

    let connection_attempt = TcpStream::connect(&addr);

    match timeout(Duration::from_secs(3), connection_attempt).await {
        Ok(Ok(mut stream)) => {

            if let Err(e) = stream.write_all(&req_bytes).await {
                // eprintln!("Failed to write handshake to {}: {}", addr, e);
                return Err(e.into());
            }

            let mut handshake_res_buf = [0u8; 68];
            if let Err(e) = stream.read_exact(&mut handshake_res_buf).await {
                // eprintln!("Failed to read handshake from {}: {}", addr, e);
                return Err(e.into());
            }


            if handshake_res_buf[0] != 19 || &handshake_res_buf[1..=19] != b"BitTorrent protocol" {
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Invalid protocol response from {}", addr))));
            }

            if &handshake_res_buf[28..48] != &info_hash_bytes {
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, format!("Info hash mismatch with peer {}", addr))));
            }

            // println!("Successfully handshaked with {}", addr);
            return Ok(stream);   
        }
        Ok(Err(e)) => {
            // eprintln!("Connection failed to {}: {}", addr, e);
            return Err(e.into());
        }
        Err(e) => {
            // eprintln!("Connection to {} timed out", addr);
            return Err(e.into());
        }
    }
}

pub async fn download_piece(stream: &mut TcpStream,peer_addr: &String, piece_length: u64, piece_index: u32, token: &CancellationToken)-> Result<PieceBuf, Box<dyn std::error::Error + Send + Sync + 'static>> {

    let num_blocks = (piece_length + BLOCK_SIZE -1 )/ BLOCK_SIZE;
    let mut buf = PieceBuf::new(piece_length );

    request_blocks(stream, piece_length, piece_index, num_blocks, &buf.recv_blocks).await?;

    loop {
        if token.is_cancelled() {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "cancelled")));
        }

        tokio::select! {
            _ = token.cancelled() => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "Cancelled"))),
            msg = read_message(stream) => {
                match msg? {
                    PeerMessage::Piece {index, begin, data} => {
                        if index != piece_index {continue;}
                        buf.add_block(begin, &data);

                        if buf.is_complete() {
                            return Ok(buf);
                        }
                    },

                    PeerMessage::Choke => {
                        crate::logger::log(&format!("[{}] choked mid-piece {}", peer_addr, piece_index));
                        wait_for_unchoke(stream).await?;
                        request_blocks(stream, piece_length, piece_index, num_blocks, &buf.recv_blocks).await?;
                    },
                    PeerMessage::Have(i) => {
                        crate::logger::log(&format!("[{}] have piece {}", peer_addr, i));
                    },
                    _ => continue,

                }
            }
        }
    }    
}

pub async fn wait_for_unchoke(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>{
    let unchoke  = timeout(Duration::from_secs(30), async {
    loop{
        match read_message(stream).await? {
            PeerMessage::Choke => { /* println!("got choke — continuing to wait"); */ continue;},
            PeerMessage::Unchoke => return Ok(()),
            PeerMessage::KeepAlive => {
                // println!("Keep alive");
                tokio::task::yield_now().await;
                continue;
            },
            PeerMessage::Have(i) => { /* println!("Have piece - {}", i) */ },
            PeerMessage::Bitfield(b) => { /* println!("bitfield: {} bytes", b.len()) */ },
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

pub async fn request_blocks(stream: &mut TcpStream, piece_length: u64, piece_index: u32,num_blocks: u64, received_blocks: &[bool]) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>{

    for i in 0..num_blocks {
        if received_blocks[i as usize] {
            continue;
        }

        let begin = i * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(piece_length - begin); // last block may be smaller

        let mut req = Vec::with_capacity(17);
        req.extend_from_slice(&13u32.to_be_bytes());
        req.push(6);
        req.extend_from_slice(&piece_index.to_be_bytes());
        req.extend_from_slice(&(begin as u32).to_be_bytes());        // begin and length are u64 but to_be_bytes() on a u64 gives 8 bytes instead of 4. The peer receives a malformed request and either resets or ignores you.
        req.extend_from_slice(&(length as u32).to_be_bytes());
        stream.write_all(&req).await?;
        // println!("Request - {:?}",req );
    }
    
    // println!("Sent request");
    Ok(())
}

pub async fn run_download(
    peers: Vec<(String, TcpStream)>,
    registry: Arc<Mutex<PeerRegistry>>,
    dl_state: Arc<Mutex<DownloadState>>,
    ui_state: Arc<std::sync::Mutex<crate::ui::UiState>>,
    standard_piece_length: u64,
    total_length: u64,
    num_pieces: u32,
    piece_hashes: Arc<Vec<[u8; 20]>>,
    output_dir: &String,
    token: CancellationToken
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {

    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(output_dir)
        .await?;

    file.set_len(total_length).await?;
        let file = Arc::new(Mutex::new(file));
        let mut handles: Vec<tokio::task::JoinHandle<()>> = vec![];

    for (peer_addr, stream) in peers {
            let registry_clone = Arc::clone(&registry);
            let dl_state_clone = Arc::clone(&dl_state);
            let ui_state_clone = Arc::clone(&ui_state);
            let hashes_clone = Arc::clone(&piece_hashes);
            let file_clone = Arc::clone(&file);
            let token_clone = token.clone(); 

            let handle = tokio::spawn(
                peer_task(
                    peer_addr,
                    stream, // Move the stream directly into the thread
                    registry_clone,
                    dl_state_clone,
                    ui_state_clone,
                    token_clone,
                    standard_piece_length,
                    total_length,
                    num_pieces,
                    hashes_clone,
                    file_clone,
                )
            );
            handles.push(handle);
        }

        for handle in handles{
            let _ = handle.await;
        }

    Ok(())
}