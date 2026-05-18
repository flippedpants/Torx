use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};

#[derive(Debug)]
pub enum PeerMessage{
    KeepAlive,
    Choke,
    Unchoke,
    Interested,
    NotInterested,
    Have (u32),
    Bitfield (Vec<u8>),
    Request { index: u32, begin: u32, length: u32},
    Piece {index: u32, begin: u32, data: Vec<u8>},
    Cancel
}

impl PeerMessage{
    pub fn parse_peer_message(id: &u8, payload: &[u8]) -> Result<Self, Box<dyn std::error::Error>>{
        match id {
            0 => Ok(Self::Choke),
            1 => Ok(Self::Unchoke),
            2 => Ok(Self::Interested),
            3 => Ok(Self::NotInterested),
            4 => Ok(Self::Have(u32::from_be_bytes(payload[..4].try_into()?))),
            5 => Ok(Self::Bitfield(payload.to_vec())),
            6 => Ok(Self::Request {
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u32::from_be_bytes(payload[4..8].try_into()?),
                length: u32::from_be_bytes(payload[8..12].try_into()?)
            }),
            7 => Ok(Self::Piece{
                index: u32::from_be_bytes(payload[..4].try_into()?),
                begin: u32::from_be_bytes(payload[4..8].try_into()?),
                data: payload[8..].to_vec(),
            }),
            8 => Ok(Self::Cancel),
            _ => Err("Invalid message id!".into())

        }
    }
}


pub async fn read_message(stream: &mut TcpStream) -> Result<PeerMessage, Box<dyn std::error::Error>>{
    let mut buf_length = [0u8; 4];
    let message_length = u32::from_be_bytes(buf_length);

    if message_length == 0 {
        // println!("keep alive");
        return Ok(PeerMessage::KeepAlive);
    }

    stream.read_exact(&mut buf_length).await?;
    let message_length = u32::from_be_bytes(buf_length);
    println!("Readed length bytes");

    let mut message = vec![0u8; message_length as usize];
    stream.read_exact(&mut message).await?;
    println!("Readed message");

    let id = message[0];
    println!("{:?}", PeerMessage::parse_peer_message(&id, &message[1..]));
    PeerMessage::parse_peer_message(&id, &message[1..])

    // Ok()
}