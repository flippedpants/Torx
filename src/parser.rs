use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Torrent{
    announce: String,

    #[serde(rename = "announce-list")]
    announce_list: Option<Vec<Vec<String>>>,

    #[serde(rename = "created by")]
    created_by: Option<String>,
    encoding: Option<String>,

    #[serde(rename = "creation date")]
    creation_date: Option<u64>,
    comment: Option<String>,

    info: Info,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    name: String,

    #[serde(rename = "piece length")]
    piece_len: u64,

    #[serde(with = "serde_bytes")]
    pieces: Vec<u8>,
    private: Option<u8>,

    #[serde(flatten)]
    mode: FileMode
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FileMode {
    SingleFileMode {
        length: u64
    },

    MultiFileMode {
        files: Vec<TorrentFile>
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TorrentFile{
    length: u64,
    path: Vec<String>
}