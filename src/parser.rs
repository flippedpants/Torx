use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Torrent{
    pub announce: String,

    #[serde(rename = "announce-list")]
    pub announce_list: Option<Vec<Vec<String>>>,

    #[serde(rename = "created by")]
    pub created_by: Option<String>,
    pub encoding: Option<String>,

    #[serde(rename = "creation date")]
    pub creation_date: Option<u64>,
    pub comment: Option<String>,

    pub info: Info,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub name: String,

    #[serde(rename = "piece length")]
    pub piece_len: u64,

    #[serde(with = "serde_bytes")]
    pub pieces: Vec<u8>,
    pub private: Option<u8>,

    #[serde(flatten)]
    pub mode: FileMode
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
    pub length: u64,
    pub path: Vec<String>
}

