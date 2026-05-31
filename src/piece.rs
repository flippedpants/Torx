// pub struct PieceState{
//     pub piece_buf: Vec<u8>,
//     pub recv: u32,
//     pub recv_blocks: Vec<bool>,
//     pub num_blocks: u32,
//     pub piece_index: u32,
//     pub piece_len: u32,
// }

// impl PieceState{
//     pub fn is_complete(&self) -> bool{
//         self.recv >= self.num_blocks
//     }

//     pub fn missing_blocks(&self) -> Vec<u32> {
//         self.recv_blocks
//             .iter()
//             .cloned()
//             .enumerate()
//             .filter(|(_, received)| !received)
//             .map(|(i, _)| i as u32)
//             .collect()
//     }
// }

use crate::download::BLOCK_SIZE;

//Temporary piece buf (lives during download)
pub struct PieceBuf{
    pub data: Vec<u8>,
    pub recv_blocks:  Vec<bool>,
    pub recv: u32,
    pub num_blocks: u32
}

impl PieceBuf{
    pub fn new(piece_len: u32) -> Self{
        let num_blocks = (piece_len + BLOCK_SIZE - 1) / BLOCK_SIZE;
        PieceBuf { data: vec![0u8; piece_len as usize], recv_blocks: vec![false; num_blocks as usize], recv: 0, num_blocks }
    }

    pub fn is_complete(&self) -> bool{
        self.recv >= self.num_blocks
    }

    pub fn missing_blocks(&self) -> Vec<u32> {
        self.recv_blocks
            .iter()
            .enumerate()
            .filter(|(_, r)| !*r)
            .map(|(i, _)| i as u32)
            .collect()
    }

    pub fn add_block(&mut self, begin: u32, data: &[u8]){
        let block_index = begin / BLOCK_SIZE;
        if !self.recv_blocks[block_index as usize] {
            self.data[begin as usize..begin as usize + data.len()].copy_from_slice(data);
            self.recv_blocks[block_index as usize] = true;
            self.recv += 1;
        }
    }
}