use std::{collections::HashSet, ops::ControlFlow::Continue, sync::Arc};
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::{peer::{PeerMessage, bit_torrent_handshake, read_message}, piece::PieceBuf, response::PeerAddress};

pub const BLOCK_SIZE: u64 = 16384;

pub struct DownloadState {
    pub needed: Vec<bool>,
    pub in_progress: HashSet<u32>,
    pub num_pieces: u32
}   

impl DownloadState {
    pub fn new(num_pieces: u32) -> Self{
        DownloadState{
            needed: vec![true; num_pieces as usize],
            in_progress: HashSet::new(),
            num_pieces
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

pub async fn download_piece(stream: &mut TcpStream,peer_addr: &String, piece_length: u64, piece_index: u32, token: &CancellationToken)-> Result<PieceBuf, Box<dyn std::error::Error>>{

    let num_blocks = (piece_length + BLOCK_SIZE -1 )/ BLOCK_SIZE;
    let mut buf = PieceBuf::new(piece_length );

    request_blocks(stream, piece_length, piece_index, num_blocks, &buf.recv_blocks).await?;

    loop {
        if token.is_cancelled() {
            return Err("cancelled".into());
        }

        tokio::select! {
            _ = token.cancelled() => return Err("Cancelled".into()),
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
                        println!("[{}] choked mid-piece {}", peer_addr, piece_index);
                        wait_for_unchoke(stream).await?;
                        request_blocks(stream, piece_length, piece_index, num_blocks, &buf.recv_blocks).await?;
                    },
                    PeerMessage::Have(i) => {
                        println!("[{}] have piece {}", peer_addr, i);
                    },
                    _ => continue,

                }
            }
        }
    }    
}

pub async fn wait_for_unchoke(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>>{
    let unchoke  = timeout(Duration::from_secs(30), async {
    loop{
        match read_message(stream).await? {
            PeerMessage::Choke => {println!("got choke — continuing to wait"); continue;},
            PeerMessage::Unchoke => return Ok(()),
            PeerMessage::KeepAlive => {
                println!("Keep alive");
                tokio::task::yield_now().await;
                continue;
            },
            PeerMessage::Have(i) => println!("Have piece - {}", i),
            PeerMessage::Bitfield(b) => println!("bitfield: {} bytes", b.len()),
            _ =>{
                tokio::task::yield_now().await;
                continue;
            },
        };
    }

    #[allow(unreachable_code)]
    Ok::<(), Box<dyn std::error::Error>>(())

    }).await;

    match unchoke{
        Ok(Ok(())) => Ok(()),
        _ => Err("unchoke timeout".into())
    }
}

pub async fn request_blocks(stream: &mut TcpStream, piece_length: u64, piece_index: u32,num_blocks: u64, received_blocks: &[bool]) -> Result<(), Box<dyn std::error::Error>>{

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
        req.extend_from_slice(&begin.to_be_bytes());        // begin and length are u64 but to_be_bytes() on a u64 gives 8 bytes instead of 4. The peer receives a malformed request and either resets or ignores you.
        req.extend_from_slice(&length.to_be_bytes());
        stream.write_all(&req).await?;
        println!("Request - {:?}",req );
    }
    
    println!("Sent request");
    Ok(())
}