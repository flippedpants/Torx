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

pub fn calculate_torrent_size(file_content: &parser::Torrent) -> u64{
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

    let piece_count = total_length.div_ceil(*piece_length);

    total_length
}

pub fn generate_id() -> String{
    let rand_num = rand::random_range(0..=9999);
    let rand_alphanum = Alphanumeric.sample_string(&mut rand::rng(), 12);

    let peer_id = format!("-{}{:04}-{}", TORRENT_ID, rand_num, rand_alphanum);

    peer_id
}

pub fn build_http_url(file_content: &parser::Torrent, torrent_file: &Vec<u8>, peer_id: &str) -> String{
    let info_hash_hex = calculate_info_hash(torrent_file).unwrap();

    const PORT: i32 = 6881;

    // let announce_url = find_https_tracker(&file_content.announce_list).unwrap();
    let announce_url = "https://tracker.zhuqiy.com:443/announce".to_string(); 
    // let announce_url = "https://tracker.yemekyedim.com:443/announce".to_string(); 
    
    let encoded_info_hash: String = hash_encoding(info_hash_hex.0);
    let peer_id = peer_id;
    let uploaded = 0;
    let downloaded = 0;
    let downloading_left = calculate_torrent_size(file_content);
    let compact = 1;
    
    let url = format!("{}?info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact={}",
                                announce_url,encoded_info_hash,peer_id,PORT,uploaded,downloaded,downloading_left,compact);

    url
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

pub fn find_https_tracker(announce_list: &Option<Vec<Vec<String>>>) -> Option<String> {
    if let Some(trackers) = announce_list {

        for tier in trackers {
            for tracker in tier {

                if tracker.starts_with("https://") {
                    return Some(tracker.clone());
                }

            }
        }
    }
    None
}