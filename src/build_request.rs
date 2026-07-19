use crate::parser::{self};
use memchr::memmem;
use sha1::{Digest, Sha1};
use rand::{self, distr::{ Alphanumeric, SampleString}};

const TORRENT_ID: &str = "TX"; 

pub fn calculate_info_hash(torrent_file: &Vec<u8>) -> Result<(String,[u8; 20]), String>{
    let pattern = b"4:info"; 

    let info_start_index = memmem::find(&torrent_file, pattern).ok_or("4:info pattern not found")?;

    let mut depth = 0;
    let mut info_end_index: usize = 0;
    let after_info_slice = &torrent_file[info_start_index+6..];
    let after_info_len = torrent_file.len() - (info_start_index + 6);
    let mut i = 0;

    while i < after_info_len{
        if after_info_slice[i].is_ascii_digit(){
            let mut colon_index = i;

            while after_info_slice[colon_index] != b':'{
                colon_index += 1;
                if colon_index >= after_info_len {
                     return Err("Malformed torrent: no colon found after string length".into());
                }
            }

            let str_len_bytes = &after_info_slice[i..colon_index];

            let str_len: usize = std::str::from_utf8(str_len_bytes)
            .map_err(|e| format!("Invalid UTF-8 in length prefix: {e}"))?
            .parse()
            .map_err(|e| format!("Failed to parse string length: {e}"))?;

            i = colon_index + str_len + 1;
            continue; 
        }
        if after_info_slice[i] == b'i'{
            loop {
                i += 1;
                if after_info_slice[i] == b'e'{
                    i += 1;
                    break;
                }
            }
            continue;
        }

        if after_info_slice[i] == b'd' || after_info_slice[i] == b'l'{
            depth += 1;
        }
        else if after_info_slice[i] == b'e'{
            depth -= 1;
        }

        match depth{
            0 => {
                info_end_index = i ;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }

    let mut sha1_hasher = Sha1::new();
    sha1_hasher.update(&torrent_file[info_start_index+6..=info_end_index + info_start_index + 6]);

    let info_hash_bytes: [u8; 20] = sha1_hasher.finalize().into();

    let info_hash_hex: String = info_hash_bytes
    .iter()
    .map(|b| format!("{:02x}", b))
    .collect();

    // println!("SHA-1 hash : {}", info_hash_hex);
    let info_hash = (info_hash_hex, info_hash_bytes);

    Ok(info_hash)
}

pub fn split_pieces(concat_pieces: &Vec<u8>) -> Vec<[u8; 20]> {
    let mut pieces: Vec<[u8; 20]> = vec![]; 

    let mut i = 0;
    while i < concat_pieces.len(){
        if i+20 > concat_pieces.len(){
            panic!("Piece might be corrupted!");
        }

        let piece_slice = &concat_pieces[i..i+20]; 
        let piece= piece_slice.try_into().expect("Piece with incorrect length");
        pieces.push(piece);

        i += 20; 
    }

    pieces
}

pub fn calculate_torrent_size(file_content: &parser::Torrent) -> (u64, u32){
    let mut total_length = 0;

    match &file_content.info.mode{
        parser::FileMode::SingleFileMode { length } => {
            total_length = *length;
        }
        parser::FileMode::MultiFileMode { files } => {
            for i in 0..files.len(){
                total_length += files[i].length;
            }
        }
    };

    let piece_length = &file_content.info.piece_len;

    let piece_count = total_length.div_ceil(*piece_length) as u32;

    (total_length, piece_count)
}

pub fn generate_id() -> String{
    let rand_num = rand::random_range(0..=9999);
    let rand_alphanum = Alphanumeric.sample_string(&mut rand::rng(), 12);

    let peer_id = format!("-{}{:04}-{}", TORRENT_ID, rand_num, rand_alphanum);

    peer_id
}

use tokio::net::UdpSocket;
use std::time::Duration;
use tokio::time::timeout;
use std::collections::HashSet;
use crate::response::PeerAddress;

pub async fn request_udp_tracker(url_str: &str, info_hash: &[u8; 20], peer_id_str: &str, total_length: u64) -> Result<Vec<PeerAddress>, Box<dyn std::error::Error + Send + Sync>> {
    let parsed_url = reqwest::Url::parse(url_str)?;
    let host = parsed_url.host_str().ok_or("No host")?;
    let port = parsed_url.port().unwrap_or(80);
    
    let addr = format!("{}:{}", host, port);
    let mut addrs = tokio::net::lookup_host(&addr).await?;
    let target_addr = addrs.next().ok_or("DNS resolution failed")?;

    let bind_addr = if target_addr.is_ipv4() {
        "0.0.0.0:0"
    } else {
        "[::]:0"
    };

    let socket = UdpSocket::bind(bind_addr).await?;
    socket.connect(target_addr).await?;

    let transaction_id: u32 = rand::random();
    
    let mut connect_req = Vec::with_capacity(16);
    connect_req.extend_from_slice(&0x41727101980u64.to_be_bytes());
    connect_req.extend_from_slice(&0u32.to_be_bytes()); 
    connect_req.extend_from_slice(&transaction_id.to_be_bytes());
    
    socket.send(&connect_req).await?;
    
    let mut connect_res = [0u8; 16];
    timeout(Duration::from_secs(5), socket.recv(&mut connect_res)).await??;
    
    let action = u32::from_be_bytes(connect_res[0..4].try_into()?);
    if action != 0 {
        return Err("Invalid connect action".into());
    }
    let res_trans_id = u32::from_be_bytes(connect_res[4..8].try_into()?);
    if res_trans_id != transaction_id {
        return Err("Transaction ID mismatch".into());
    }
    let connection_id = u64::from_be_bytes(connect_res[8..16].try_into()?);

    let announce_trans_id: u32 = rand::random();
    let mut announce_req = Vec::with_capacity(98);
    announce_req.extend_from_slice(&connection_id.to_be_bytes());
    announce_req.extend_from_slice(&1u32.to_be_bytes()); 
    announce_req.extend_from_slice(&announce_trans_id.to_be_bytes());
    announce_req.extend_from_slice(info_hash);
    
    let mut peer_id_bytes = [0u8; 20];
    let pid_bytes = peer_id_str.as_bytes();
    let len = pid_bytes.len().min(20);
    peer_id_bytes[..len].copy_from_slice(&pid_bytes[..len]);
    announce_req.extend_from_slice(&peer_id_bytes);
    
    announce_req.extend_from_slice(&0u64.to_be_bytes()); 
    announce_req.extend_from_slice(&total_length.to_be_bytes()); 
    announce_req.extend_from_slice(&0u64.to_be_bytes()); 
    announce_req.extend_from_slice(&0u32.to_be_bytes()); 
    announce_req.extend_from_slice(&0u32.to_be_bytes()); 
    let key: u32 = rand::random();
    announce_req.extend_from_slice(&key.to_be_bytes()); 
    announce_req.extend_from_slice(&(-1i32).to_be_bytes()); 
    announce_req.extend_from_slice(&6881u16.to_be_bytes()); 
    
    socket.send(&announce_req).await?;
    
    let mut announce_res = [0u8; 8192];
    let len = timeout(Duration::from_secs(10), socket.recv(&mut announce_res)).await??;
    
    if len < 20 {
        return Err("Announce response too short".into());
    }
    
    let res_action = u32::from_be_bytes(announce_res[0..4].try_into()?);
    if res_action == 3 {
        let msg = String::from_utf8_lossy(&announce_res[8..len]);
        return Err(format!("Tracker error: {}", msg).into());
    }
    if res_action != 1 {
        return Err("Invalid announce action".into());
    }
    let res_trans_id2 = u32::from_be_bytes(announce_res[4..8].try_into()?);
    if res_trans_id2 != announce_trans_id {
        return Err("Transaction ID mismatch on announce".into());
    }
    
    let mut peers = Vec::new();
    let mut i = 20;
    while i + 6 <= len {
        let ip = format!("{}.{}.{}.{}", announce_res[i], announce_res[i+1], announce_res[i+2], announce_res[i+3]);
        let port = u16::from_be_bytes([announce_res[i+4], announce_res[i+5]]);
        peers.push(PeerAddress { ip, port });
        i += 6;
    }
    
    Ok(peers)
}

fn hash_encoding(text: String) -> String{
    let mut encoded_text = "".to_string();

    let mut i = 0;
    while i < text.len(){
        let two_chars = &text[i..=i+1];
        let two_chars_uppercase = two_chars.to_uppercase();  
        let two_chars_encoded = format!("%{}", two_chars_uppercase);

        encoded_text.push_str(&two_chars_encoded);
        i += 2;
    }

    encoded_text
}

pub async fn collect_all_peers(
    file_content: &parser::Torrent, 
    torrent_file: &Vec<u8>, 
    peer_id: &str,
    token: tokio_util::sync::CancellationToken
) -> Result<Vec<PeerAddress>, Box<dyn std::error::Error + Send + Sync>> {
    
    let (total_length, _) = calculate_torrent_size(file_content);
    let info_hash = calculate_info_hash(torrent_file)?;
    
    let mut urls = Vec::new();
    urls.push(file_content.announce.clone());
    
    if let Some(list) = &file_content.announce_list {
        for tier in list {
            for url in tier {
                urls.push(url.clone());
            }
        }
    }
    
    //fallback tracker
    urls.push("udp://zer0day.ch:1337/announce".to_string());
    
    let mut unique_urls = HashSet::new();
    let mut final_urls = Vec::new();
    for url in urls {
        if unique_urls.insert(url.clone()) {
            final_urls.push(url);
            if final_urls.len() >= 10 {
                break;
            }
        }
    }
    
    let mut all_peers = HashSet::new();
    let http_client = reqwest::Client::new();
    let mut handles = Vec::new();
    
    for url in final_urls {
        let http_client = http_client.clone();
        let url = url.clone();
        let info_hash = info_hash.clone();
        let peer_id = peer_id.to_string();
        
        handles.push(tokio::spawn(async move {
            crate::logger::log(&format!("[SYSTEM] Announcing to tracker: {}", url));
            let mut peers_found = Vec::new();
            
            if url.starts_with("http://") || url.starts_with("https://") {
                let encoded_info_hash = hash_encoding(info_hash.0.clone());
                let request_url = format!("{}?info_hash={}&peer_id={}&port=6881&uploaded=0&downloaded=0&left={}&compact=1&numwant=200",
                                    url, encoded_info_hash, peer_id, total_length);
                
                if let Ok(response) = http_client.get(&request_url).timeout(Duration::from_secs(5)).send().await {
                    if let Ok(bytes) = response.bytes().await {
                        match crate::response::parse_response(&bytes) {
                            Ok(peers) => {
                                for peer in peers {
                                    peers_found.push(peer);
                                }
                            }
                            Err(e) => {
                                crate::logger::log(&format!("[SYSTEM] HTTP Tracker error parsing response: {}", e));
                            }
                        }
                    } else {
                        crate::logger::log("[SYSTEM] Failed to read HTTP tracker response bytes");
                    }
                } else {
                    crate::logger::log("[SYSTEM] HTTP Tracker request failed or timed out");
                }
            } else if url.starts_with("udp://") {
                match request_udp_tracker(&url, &info_hash.1, &peer_id, total_length).await {
                    Ok(peers) => {
                        for peer in peers {
                            peers_found.push(peer);
                        }
                    }
                    Err(e) => {
                        crate::logger::log(&format!("[SYSTEM] UDP Tracker error: {}", e));
                    }
                }
            }
            peers_found
        }));
    }
    
    for mut handle in handles {
        tokio::select! {
            _ = token.cancelled() => {
                return Err("Cancelled by user".into());
            }
            res = &mut handle => {
                if let Ok(peers) = res {
                    for p in peers {
                        all_peers.insert(p);
                    }
                }
            }
        }
    }
    
    Ok(all_peers.into_iter().collect())
}