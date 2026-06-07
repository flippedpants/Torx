use std::{fs::File, path::{Path, PathBuf}};

use crate::parser;

pub struct FileEntry{
    pub path: PathBuf,
    pub offset: u64,
    pub length: u64
}

// pub fn build_file_map(files: &[parser::File], base_dir: &Path) -> Vec<FileEntry>{
    // let mut file_map = vec![];
    // let offset: u64 = 0;
// 
    // for file in files{
        // file_map.push(FileEntry{
            // path: base_dir.join(Path::new(&file.path)),
            // offset,
            // length: file.length
        // });
    // }
// 
    // file_map
// }
// 
// pub fn preallocate_files(file_map: &[FileEntry]) -> Result<(), Box<dyn std::error::Error>> {
    // for file in file_map{
// 
    // }
// 
    // Ok(())
// }