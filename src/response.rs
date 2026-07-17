use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Response{
    #[serde(rename="failure reason")]
    failure_reason: Option<String>,
    interval: Option<u32>,

    #[serde(rename="min interval")]
    min_interval: Option<u32>,

    #[serde(rename="tracker id")]
    tracker_id: Option<String>,
    complete: Option<i32>,
    incomplete: Option<i32>,

    #[serde(with = "serde_bytes", default)]
    peers: Option<Vec<u8>>
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct PeerAddress{
    pub ip: String,
    pub port: u16
}

pub fn parse_response(response_byte: &bytes::Bytes) -> Result<Vec<PeerAddress>, Box<dyn std::error::Error + Send + Sync>> {
    let res: Response = serde_bencode::from_bytes(response_byte)?;
    
    if let Some(reason) = res.failure_reason {
        return Err(format!("Tracker failure reason: {}", reason).into());
    }

    let mut peer_list: Vec<PeerAddress> = vec![];
    
    if let Some(peers) = res.peers {
        let mut i = 0;
        while i + 6 <= peers.len() {
            let peer_slice = &peers[i..=i+5];
            let ip = format!("{}.{}.{}.{}", peer_slice[0], peer_slice[1], peer_slice[2], peer_slice[3]);
            let port = (peer_slice[4] as u16) * 256 + (peer_slice[5] as u16); 
            
            peer_list.push(PeerAddress { ip, port });
            i += 6;
        }
    }

    Ok(peer_list)
}   
