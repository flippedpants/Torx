use crate::parser::{self, Torrent};
use memchr::memmem;
use sha1::{Digest, Sha1};

pub fn extract_value(file_content: parser::Torrent){
    let announce_url = file_content.announce;
    let announce_list = file_content.announce_list.unwrap_or_default();
    let torrent_name = file_content.info.name;
    let pieces = file_content.info.pieces;
    let piece_len = file_content.info.piece_len;
    
    let mut single_file_length: u64;
    let mut torrent_files: Vec<parser::TorrentFile>;
    
    match file_content.info.mode {
        parser::FileMode::SingleFileMode { length } => {
            single_file_length = length;
        },
        parser::FileMode::MultiFileMode { files } => {
            torrent_files = files;
        }
    }
}

pub fn calculate_info_hash(torrent_file: &Vec<u8>){
    let pattern = b"4:info"; 

    // Find the first occurrence
    // let index = file_content.windows(pattern.len())
    // .position(|window| window == pattern);

    let index = memmem::find(&torrent_file, pattern).unwrap();
    //Do Error handling

    let mut depth = 0;
    let mut info_end_index: usize = 0;
    let after_info_slice = &torrent_file[index+6..];
    let after_info_len = torrent_file.len() - (index + 6);
    let mut i = 0;

    while i <= after_info_len{
        if after_info_slice[i].is_ascii_digit(){
            let mut colon_index = i;

            while after_info_slice[colon_index] != b':'{
                colon_index += 1;
            }

            let str_len_bytes = &after_info_slice[i..colon_index];

            let str_len: usize = std::str::from_utf8(str_len_bytes).unwrap().parse().unwrap();

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
                info_end_index = i;
                break;
            }
            _ => {
                i += 1;
            }
        }
    }

    let mut sha1_hasher = Sha1::new();
    sha1_hasher.update(&torrent_file[index+6..=info_end_index]);

    let info_hash = sha1_hasher.finalize();

    let info_hash_hex: String = info_hash
    .iter()
    .map(|b| format!("{:02x}", b))
    .collect();

    println!("SHA-1 hash : {}", info_hash_hex);
}
