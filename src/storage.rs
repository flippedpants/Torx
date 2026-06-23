use std::{path::{Path, PathBuf}};
use crate::parser;

#[derive(Clone)]
pub struct StorageFile {
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64,
}

pub struct FileEntry {
    pub files: Vec<StorageFile>,
}

impl FileEntry {
    pub fn new(torrent: &parser::Torrent, base_dir: &Path) -> Self {
        let mut files = vec![];
        let mut current_offset: u64 = 0;

        match &torrent.info.mode {
            parser::FileMode::SingleFileMode { length } => {
                files.push(StorageFile {
                    path: base_dir.join(&torrent.info.name),
                    offset: 0,
                    length: *length,
                });
            }
            parser::FileMode::MultiFileMode { files: torrent_files } => {
                let base = base_dir.join(&torrent.info.name);
                for file in torrent_files {
                    let mut path = base.clone();
                    for p in &file.path {
                        path.push(p);
                    }
                    files.push(StorageFile {
                        path,
                        offset: current_offset,
                        length: file.length,
                    });
                    current_offset += file.length;
                }
            }
        }
        FileEntry { files }
    }

    pub async fn preallocate(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        for file in &self.files {
            if let Some(parent) = file.path.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let f = tokio::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&file.path)
                .await?;
            f.set_len(file.length).await?;
        }
        Ok(())
    }

    pub async fn write_piece(
        &self,
        piece_index: u32,
        standard_piece_length: u64,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let piece_start = piece_index as u64 * standard_piece_length;
        let piece_end = piece_start + data.len() as u64;

        for file in &self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            if piece_end <= file_start || piece_start >= file_end {
                continue;
            }

            let overlap_start = piece_start.max(file_start);
            let overlap_end = piece_end.min(file_end);

            let data_start = (overlap_start - piece_start) as usize;
            let data_end = (overlap_end - piece_start) as usize;
            let file_offset = overlap_start - file_start;

            let chunk = &data[data_start..data_end];

            let mut f = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&file.path)
                .await?;

            use tokio::io::{AsyncSeekExt, AsyncWriteExt};
            f.seek(std::io::SeekFrom::Start(file_offset)).await?;
            f.write_all(chunk).await?;
        }
        Ok(())
    }

    /// Read a block of data from a piece, handling multi-file boundaries.
    /// This is the inverse of write_piece, used for serving upload requests.
    pub async fn read_block(
        &self,
        piece_index: u32,
        standard_piece_length: u64,
        begin: u64,
        length: u64,
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let piece_start = piece_index as u64 * standard_piece_length;
        let block_start = piece_start + begin;
        let block_end = block_start + length;

        let mut data = vec![0u8; length as usize];

        for file in &self.files {
            let file_start = file.offset;
            let file_end = file.offset + file.length;

            if block_end <= file_start || block_start >= file_end {
                continue;
            }

            let overlap_start = block_start.max(file_start);
            let overlap_end = block_end.min(file_end);

            let data_start = (overlap_start - block_start) as usize;
            let data_end = (overlap_end - block_start) as usize;
            let file_offset = overlap_start - file_start;

            let mut f = tokio::fs::OpenOptions::new()
                .read(true)
                .open(&file.path)
                .await?;

            use tokio::io::{AsyncSeekExt, AsyncReadExt};
            f.seek(std::io::SeekFrom::Start(file_offset)).await?;
            f.read_exact(&mut data[data_start..data_end]).await?;
        }

        Ok(data)
    }
}
