use crate::{build_request::generate_id, response::parse_response};

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

    fn serialize(&self){
        let mut buf: [u8; 68] = [0; 68];
        buf[0] = 19;
        buf[1..=19].copy_from_slice(b"BitTorrent Protocol");
        buf[28..=47].copy_from_slice(&self.info_hash);
        buf[48..=67].copy_from_slice(&self.peer_id);
    }
}

pub fn connect(response_bytes: &bytes::Bytes, peer_id: [u8; 20], info_hash_bytes: [u8; 20]){
    let peers = parse_response(&response_bytes);
    let handshake = Handshake::new(info_hash_bytes, peer_id);

    for i in 0..peers.len(){
        
    }
}