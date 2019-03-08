use std::fs::File;
use std::io::{BufReader, Cursor};
use std::io::Read;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use failure::Fallible;

use message_tree::read_data;

use crate::message_tree::MessageTree;

mod message_tree;

fn main() -> Fallible<()> {
    let mut data_file = BufReader::new(File::open("cat-dat/cat-10.9.185.69.dat")?);
    let magic_number = data_file.read_i32::<BigEndian>()?;
    assert_eq!(magic_number, -1);
    let snappy_block = read_data(&mut data_file)?;
    let mut snappy_block_reader = snappy_block.as_slice();
    let mut snappy_magic_header = vec![0; 16];
    snappy_block_reader.read_exact(snappy_magic_header.as_mut_slice())?;
    let snappy_body = read_data(&mut snappy_block_reader)?;
    let mut decodeder = snap::Decoder::new();
    let message_chunks = decodeder.decompress_vec(&snappy_body)?;
    let mut counter = 0;

    let mut message_reader = Cursor::new(message_chunks);

    loop {
        let message_buf = read_data(&mut message_reader)?;
        let mut buf = Cursor::new(message_buf);
        let tree = MessageTree::decode(&mut buf)?;
//        dbg!(tree);
        if counter % 1000 == 0 {
            println!("messages: {}", counter);
        }
        counter += 1;
    }
    println!("messages: {}", counter);
    Ok(())
}
