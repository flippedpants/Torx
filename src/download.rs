use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::parse_message::{PeerMessage, read_message};

const BLOCK_SIZE: u64 = 16384;

pub async fn download_piece(stream: &mut TcpStream, piece_length: u64, piece_index: u32)-> Result<Vec<u8>, Box<dyn std::error::Error>>{
    stream.write_all(&[0,0,0,1,2]).await?;
    println!("Sent interested");

    loop{
        match read_message(stream).await? {
            PeerMessage::Choke => return Err("Peer choked".into()),
            PeerMessage::Unchoke => break,
            _ =>{continue;},
        };
    }

    println!("Unchoked");

    let num_blocks = (piece_length + BLOCK_SIZE -1 )/ BLOCK_SIZE;
    let mut piece_buf = vec![0u8; piece_length as usize];

    for i in 0..num_blocks {
        let begin = i * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(piece_length - begin); // last block may be smaller

        let mut req = Vec::with_capacity(17);
        req.extend_from_slice(&13u32.to_be_bytes());
        req.push(6);
        req.extend_from_slice(&piece_index.to_be_bytes());
        req.extend_from_slice(&begin.to_be_bytes());
        req.extend_from_slice(&length.to_be_bytes());
        stream.write_all(&req).await?;
        println!("Rquest - {:?}",req );
    }
    

    println!("Sent request");

    let mut recv = 0;
    while recv < num_blocks{
        match read_message(stream).await? {
            PeerMessage::Piece { index, begin, data }=> {
                if index != piece_index { continue; }
                piece_buf[begin as usize..begin as usize + data.len()].copy_from_slice(&data);
                recv += 1;
            },
            PeerMessage::Choke => return Err("Peer choked us in the middle of download".into()),
            _ => continue,
        };
    }

    Ok(piece_buf)
}