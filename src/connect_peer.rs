use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::{timeout, Duration}};

use crate::{download::download_piece, parse_message::read_message, parser, response::parse_response};

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

pub async fn tcp_bitTorrent_handshake(file_content: &parser::Torrent,tracker_response_bytes: &bytes::Bytes, peer_id: [u8; 20],info_hash_bytes: [u8; 20]) -> Result<TcpStream, Box<dyn std::error::Error>> {
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
                // match download_piece(&mut stream, file_content.info.piece_len, 0).await? {
                //     Ok(piece_buf) => {println!("{:?}, piece_buf");}
                //     // Err(e) => println!("{}", e),
                // };
                stream.write_all(&[0,0,0,1,2]).await?;
                println!("Sent interested");
                let downloaded_piece = download_piece(&mut stream, file_content.info.piece_len, 0).await;

                match &downloaded_piece{
                    Ok(piece) => {println!("{:?}", piece)},
                    Err(e) if e.to_string() == "unchoke timeout — try next peer" => {
                        continue;
                    }
                    Err(e) => {println!("peer {} failed: {}", addr, e); continue;}
                }
                
                // println!("{:?}", &downloaded_piece);

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