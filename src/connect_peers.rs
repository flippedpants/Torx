use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::{timeout, Duration}};

use crate::{response::parse_response};

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

pub async fn connect_to_peer(tracker_response_bytes: &bytes::Bytes, peer_id: [u8; 20],info_hash_bytes: [u8; 20]) -> Result<TcpStream, Box<dyn std::error::Error>> {
    let peers = parse_response(tracker_response_bytes);
    let handshake = Handshake::new(info_hash_bytes, peer_id);
    let req_bytes = handshake.serialize();

    for peer in peers {
        let addr = format!("{}:{}", peer.ip, peer.port);

        let connection_attempt = TcpStream::connect(&addr);

        match timeout(Duration::from_secs(3), connection_attempt).await {
            Ok(Ok(mut stream)) => {

                if let Err(e) = stream.write_all(&req_bytes).await {
                    eprintln!("Failed to write handshake to {}: {}", addr, e);
                    continue;
                }

                let mut handshake_res_buf = [0u8; 68];
                if let Err(e) = stream.read_exact(&mut handshake_res_buf).await {
                    eprintln!("Failed to read handshake from {}: {}", addr, e);
                    continue;
                }


                if handshake_res_buf[0] != 19 || &handshake_res_buf[1..=19] != b"BitTorrent protocol" {
                    eprintln!("Invalid protocol response from {}", addr);
                    continue;
                }

                if &handshake_res_buf[28..48] != &info_hash_bytes {
                    eprintln!("Info hash mismatch with peer {}", addr);
                    continue;
                }

                println!("Successfully handshaked with {}", addr);

                let mut prefix_length_buf = [0u8; 4];
                if let Err(e) = stream.read_exact(&mut prefix_length_buf).await{
                    eprintln!("Error - {}", e);
                    continue;
                }
                println!("{:?}", &prefix_length_buf);

                let payload_length = u32::from_be_bytes(prefix_length_buf) as usize;
                let mut payload = vec![0u8; payload_length];
                if payload_length > 0 {
                    match timeout(Duration::from_secs(5), stream.read_exact(&mut payload)).await {
                        Ok(Ok(_)) => println!("Successfully read all {} bytes", payload_length),
                        Ok(Err(e)) => {
                            eprintln!("Network error while reading payload: {}", e);
                            continue;
                        }
                        Err(_) => {
                            eprintln!("Timed out waiting for {} bytes from peer.", payload_length);
                            continue; 
                        }
                    }
                } 

                if payload[0] == 5{
                    let mut interested_req = [0u8, 0u8, 0u8, 1u8, 2u8];
                    if let Err(e) = stream.write_all(&mut interested_req).await{
                        eprintln!("Error while sending interested_req - {}", e);
                        continue;
                    }

                    let mut reply = [0u8; 5];
                    let x =stream.read_exact(&mut reply).await.unwrap();

                    println!("{:?}", &reply);
                }

                
                return Ok(stream);         
            }
            Ok(Err(e)) => {
                eprintln!("Connection failed to {}: {}", addr, e);
            }
             Err(_) => {
                eprintln!("Connection to {} timed out", addr);
            }
        }
        

    }

    Err("No valid peer found among the list".into())
}