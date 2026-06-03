use std::{collections::{HashMap, HashSet}, sync::Arc};

use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::Mutex};
use tokio_util::sync::CancellationToken;

use crate::{download::{DownloadState, wait_for_unchoke}, peer};

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
    pub fn parse_peer_message(id: &u8, payload: &[u8]) -> Result<Self, Box<dyn std::error::Error>>{
        match id {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have(u32::from_be_bytes(payload[..4].try_into()?))),
            5 => Ok(Self::Bitfield(payload.to_vec())),
            6 => Ok(Self::Request {
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u64::from_be_bytes(payload[4..8].try_into()?),
                length: u32::from_be_bytes(payload[8..12].try_into()?)
            }),
            7 => Ok(Self::Piece{
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u64::from_be_bytes(payload[4..8].try_into()?),
                data: payload[8..].to_vec(),
            }),
            8 => Ok(Self::Cancel),
            _ => Err("Invalid message id!".into())

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

pub async fn read_message(stream: &mut TcpStream) -> Result<PeerMessage, Box<dyn std::error::Error>>{
    let mut buf_length = [0u8; 4];
    stream.read_exact(&mut buf_length).await?;
    let message_length = u32::from_be_bytes(buf_length);

    if message_length == 0 {
        // println!("keep alive");
        return Ok(PeerMessage::KeepAlive);
    }

    println!("Readed length bytes");

    let mut message = vec![0u8; message_length as usize];
    stream.read_exact(&mut message).await?;
    println!("Readed message");

    let id = message[0];
    println!("{:?}", PeerMessage::parse_peer_message(&id, &message[1..]));
    PeerMessage::parse_peer_message(&id, &message[1..])

    // Ok()
}

pub async fn peer_task(
    peer_addr: &String,
    stream: &mut TcpStream,
    registry: Arc<Mutex<PeerRegistry>>,
    dl_state: Arc<Mutex<DownloadState>>,
    token: &CancellationToken
){
    println!("[{}] - task started", peer_addr);

    loop{
        tokio::select! {
            _ = token.cancelled() => {
                println!("Task cancelled mid init");
                return;
            },
            msg = read_message(stream) => {
                match msg {
                    Ok(PeerMessage::Bitfield(b)) => {
                        let mut reg = registry.lock().await;
                        reg.set_bitfield(peer_addr, &b);
                        break;
                    }
                    Ok(PeerMessage::Have(i)) => {
                        let mut reg = registry.lock().await;
                        reg.set_have(peer_addr, i);
                    }
                    Ok(PeerMessage::Unchoke) =>{
                        println!("Got Unchoke without Bitfield - {}", peer_addr);
                        break;
                    }
                    Ok(_) => continue,
                    Err(e) => {
                        eprintln!("[{}] error during init - {}", peer_addr, e);
                        break;
                    }
                }
            }
        }
    }

    if stream.write_all(&[0,0,0,1,2]).await.is_err() {
        eprintln!("Could not sent interested to [{}] ", peer_addr);
        return;
    }

    tokio::select! {
        _ = token.cancelled() => {
            println!("Cancelled while waiting for unchoke - [{}]", peer_addr);
            return;
        }
        result = wait_for_unchoke(stream) => {
            if result.is_err() {
                eprintln!("[{}] - unchoke timeout", peer_addr);
                return;
            }
        }
    }

    println!("Starting Download for - [{}]", peer_addr);

    loop {
        if token.is_cancelled() {
            return;
        }

        let piece_index = {
            let reg = registry.lock().await;
            let state = dl_state.lock().await;
            reg.rarest_piece_for_peer(peer_addr, &state.needed, &state.in_progress)
        };

        let Some(piece_index) = piece_index else{
            println!("[{}] no more pieces to download", peer_addr);
            return;
        };

        {
            let mut state = dl_state.lock().await;
            state.in_progress.insert(piece_index);
        }
    }
}