use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::{timeout, Duration}};

use crate::response::{PeerAddress};

#[derive(Debug)]
pub enum PeerMessage{
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have (u32),
    Bitfield (Vec<u8>),
    Request { index: u32, begin: u32, length: u32},
    Piece {index: u32, begin: u32, data: Vec<u8>},
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
                begin: u32::from_be_bytes(payload[4..8].try_into()?),
                length: u32::from_be_bytes(payload[8..12].try_into()?)
            }),
            7 => Ok(Self::Piece{
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u32::from_be_bytes(payload[4..8].try_into()?),
                data: payload[8..].to_vec(),
            }),
            8 => Ok(Self::Cancel),
            _ => Err("Invalid message id!".into())

        }
    }
}


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

pub async fn bit_torrent_handshake(peer: &PeerAddress, peer_id: [u8; 20],info_hash_bytes: [u8; 20]) -> Result<TcpStream, Box<dyn std::error::Error>> {
    
    let handshake = Handshake::new(info_hash_bytes, peer_id);
    // println!("{:?}", handshake);
    let req_bytes = handshake.serialize();

    // println!("{:?}", peer);
    let addr = format!("{}:{}", peer.ip, peer.port);

    let connection_attempt = TcpStream::connect(&addr);

    match timeout(Duration::from_secs(3), connection_attempt).await {
        Ok(Ok(mut stream)) => {

            if let Err(e) = stream.write_all(&req_bytes).await {
                eprintln!("Failed to write handshake to {}: {}", addr, e);
                return Err(e.into());
            }

            let mut handshake_res_buf = [0u8; 68];
            if let Err(e) = stream.read_exact(&mut handshake_res_buf).await {
                eprintln!("Failed to read handshake from {}: {}", addr, e);
                return Err(e.into());
            }


            if handshake_res_buf[0] != 19 || &handshake_res_buf[1..=19] != b"BitTorrent protocol" {
                let e = format!("Invalid protocol response from {}", addr);
                return Err(e.into());
            }

            if &handshake_res_buf[28..48] != &info_hash_bytes {
                let e = format!("Info hash mismatch with peer {}", addr);
                return Err(e.into());
            }

            println!("Successfully handshaked with {}", addr);
            return Ok(stream);   
        }
        Ok(Err(e)) => {
            eprintln!("Connection failed to {}: {}", addr, e);
            return Err(e.into());
        }
        Err(e) => {
            eprintln!("Connection to {} timed out", addr);
            return Err(e.into());
        }
    }
}