use crate::parser;
use memchr::memmem;

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

fn calculate_info_hash(file_content: Vec<u8>){
    let pattern = b"info"; 

    // Find the first occurrence
    // let index = file_content.windows(pattern.len())
    // .position(|window| window == pattern);

    let index = memmem::find(&file_content, pattern);
    //Error handling
    
}