pub struct PieceState{
    pub piece_buf: Vec<u8>,
    pub recv: u32,
    pub recv_blocks: Vec<bool>,
    pub num_blocks: u32,
    pub piece_index: u32,
    pub piece_len: u32,
}

impl PieceState{
    pub fn is_complete(&self) -> bool{
        self.recv >= self.num_blocks
    }

    pub fn missing_blocks(&self) -> Vec<u32> {
        self.recv_blocks
            .iter()
            .cloned()
            .enumerate()
            .filter(|(_, received)| !received)
            .map(|(i, _)| i as u32)
            .collect()
    }
}

