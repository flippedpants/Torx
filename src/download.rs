use std::time::Duration;

use tokio::{io::AsyncWriteExt, net::TcpStream, time::timeout};

use crate::parse_message::{PeerMessage, read_message};

const BLOCK_SIZE: u32 = 16384;

pub async fn download_piece(stream: &mut TcpStream, piece_length: u64, piece_index: u32)-> Result<Vec<u8>, Box<dyn std::error::Error>>{
    stream.write_all(&[0,0,0,1,2]).await?;
    println!("Sent interested");
    wait_for_unchoke(stream).await?;

    let num_blocks = ((piece_length as u32)+ BLOCK_SIZE -1 )/ BLOCK_SIZE;
    let mut received_blocks= vec![false; num_blocks as usize];
    request_blocks(stream, piece_length as u32, piece_index, num_blocks, &received_blocks).await?;

    let mut piece_buf = vec![0u8; piece_length as usize];
            
    let mut recv = received_blocks.iter().filter(|&&b| b).count() as u32;
    while recv < num_blocks{
        match read_message(stream).await? {
            PeerMessage::Piece { index, begin, data }=> {
                let block_index = begin / BLOCK_SIZE;

                if index != piece_index { continue; }
                piece_buf[begin as usize..begin as usize + data.len()].copy_from_slice(&data);
                received_blocks[block_index as usize] = true;
                recv += 1;
            },
            PeerMessage::Choke => {
                wait_for_unchoke(stream).await?;
                request_blocks(stream, piece_length as u32, piece_index, num_blocks, &received_blocks).await?;
            },
            _ => continue,
        };
    }

    Ok(piece_buf)
        
    
}

pub async fn wait_for_unchoke(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error>>{
    let unchoke  = timeout(Duration::from_secs(30), async {
    loop{
        match read_message(stream).await? {
            PeerMessage::Choke => {println!("got choke — continuing to wait"); continue;},
            PeerMessage::Unchoke => return Ok(()),
            PeerMessage::KeepAlive => {
                println!("Keep alive");
                tokio::task::yield_now().await;
                continue;
            },
            PeerMessage::Have(i) => println!("Have piece - {}", i),
            PeerMessage::Bitfield(b) => println!("bitfield: {} bytes", b.len()),
            _ =>{
                tokio::task::yield_now().await;
                continue;
            },
        };
    }

    #[allow(unreachable_code)]
    Ok::<(), Box<dyn std::error::Error>>(())

    }).await;

    match unchoke{
        Ok(Ok(())) => Ok(()),
        _ => Err("unchoke timeout".into())
    }
}

pub async fn request_blocks(stream: &mut TcpStream, piece_length: u32, piece_index: u32,num_blocks: u32, received_blocks: &[bool]) -> Result<(), Box<dyn std::error::Error>>{

    for i in 0..num_blocks {
        if received_blocks[i as usize] {
            continue;
        }

        let begin = i * BLOCK_SIZE;
        let length = BLOCK_SIZE.min(piece_length - begin); // last block may be smaller

        let mut req = Vec::with_capacity(17);
        req.extend_from_slice(&13u32.to_be_bytes());
        req.push(6);
        req.extend_from_slice(&piece_index.to_be_bytes());
        req.extend_from_slice(&begin.to_be_bytes());        // begin and length are u64 but to_be_bytes() on a u64 gives 8 bytes instead of 4. The peer receives a malformed request and either resets or ignores you.
        req.extend_from_slice(&length.to_be_bytes());
        stream.write_all(&req).await?;
        println!("Request - {:?}",req );
    }
    
    println!("Sent request");
    Ok(())
}