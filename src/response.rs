use std::{fmt::format, sync::PoisonError};

use serde::{Deserialize, Serialize};
use bytes::Bytes;

#[derive(Debug, Serialize, Deserialize)]
pub struct Response{
    #[serde(rename="failure reason")]
    failure_reason: Option<String>,
    interval: u32,

    #[serde(rename="min interval")]
    min_interval: Option<u32>,

    #[serde(rename="tracker id")]
    tracker_id: Option<String>,
    complete: i32,
    incomplete: i32,

    #[serde(with = "serde_bytes")]
    peers: Vec<u8>
}

#[derive(Debug)]
struct PeerAddress{
    ip: String,
    port: u16
}

pub fn parse_response(response_byte: &bytes::Bytes){
    let res: Response = serde_bencode::from_bytes(response_byte).unwrap();

    // println!("{:?}", response);

    let mut peer_list: Vec<PeerAddress> = vec![];
    let mut i = 0;

    while i < res.peers.len(){

        let peer_slice = &res.peers[i..=i+5];
        let peer: [u8; 6] = peer_slice.try_into().expect("peer with invalid address");

        let ip_byte = &peer[..=3];
        let port_values = &peer[4..=5];

        // let ip = ip_byte.iter()
        //     .map(|byte| byte.to_string())
        //     .collect::<Vec<String>>()
        //     .join(".");

        let ip = format!("{}.{}.{}.{}", ip_byte[0], ip_byte[1], ip_byte[2], ip_byte[3]);
        let port = (port_values[0] as u16) * 256 + (port_values[1] as u16); 
        
        peer_list.push(PeerAddress { ip, port });

        i += 6;
    }

    println!("{:#?}", peer_list);
    println!("{}", peer_list.len());

}   