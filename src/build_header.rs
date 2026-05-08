use crate::parser::{self, Torrent};
use memchr::memmem;
use sha1::{Digest, Sha1, digest::FixedOutput};

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
  
    // println!("{:?}", &torrent_file[index+4..]);

    let mut depth = 0;
    let mut info_end_index: usize;

    for i in (index+6..torrent_file.len()){
        if torrent_file[i] == b'd' || torrent_file[i] == b'l'{
            depth += 1;
        }
        else if torrent_file[i] == b'e'{
            depth -= 1;
        }

        match depth{
            0 => {
                info_end_index = i;
                break;
            }
            _ => {
                continue;
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
