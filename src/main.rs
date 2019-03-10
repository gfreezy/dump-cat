use std::fs::File;
use std::io::{BufReader, Cursor, Error, Write};
use std::io::Read;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::BytesMut;
use failure::Fallible;

use message_tree::try_read_data;

use crate::message_tree::MessageTree;

mod message_tree;

fn main() -> Fallible<()> {
    let mut data_file = BufReader::new(File::open("cat-dat/cat-10.9.185.69.dat")?);
    let magic_number = data_file.read_i32::<BigEndian>()?;
    assert_eq!(magic_number, -1);

    let mut counter = 0;

    loop {
        // snappy blocks
        let snappy_block = try_read_data(&mut data_file)?;
        let snappy_block = match snappy_block {
            None => break,
            Some(b) => b,
        };
        let mut snappy_reader = SnappyReader::new(snappy_block);
        let _header = snappy_reader.read_header()?;

        loop {
            let message_buf = try_read_data(&mut snappy_reader)?;
            let message_buf = match message_buf {
                None => break,
                Some(buf) => buf,
            };
            let mut buf = Cursor::new(message_buf);
            let _tree = MessageTree::decode(&mut buf)?;

            counter += 1;

            if counter % 10000 == 0 {
                println!("counter: {}", counter);
            }
        }
    }
    println!("messages: {}", counter);

    Ok(())
}

struct SnappyReader {
    reader: Cursor<Vec<u8>>,
    buf: BytesMut,
}

impl SnappyReader {
    pub fn new(buf: Vec<u8>) -> Self {
        SnappyReader {
            reader: Cursor::new(buf),
            buf: BytesMut::new(),
        }
    }

    pub fn read_header(&mut self) -> Fallible<Vec<u8>> {
        let mut snappy_magic_header = vec![0; 16];
        self.reader.read_exact(&mut snappy_magic_header)?;
        Ok(snappy_magic_header)
    }

    fn read_more_chunk(&mut self) -> Result<usize, Error> {
        let snappy_body = try_read_data(&mut self.reader)?;
        let snappy_body = match snappy_body {
            None => return Ok(0),
            Some(body) => body,
        };
        let mut decodeder = snap::Decoder::new();
        let message_chunks = decodeder.decompress_vec(&snappy_body)?;
        self.buf.extend_from_slice(&message_chunks);
        Ok(message_chunks.len())
    }
}

impl Read for SnappyReader {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        let size = buf.len();
        loop {
            if self.buf.len() < size {
                self.read_more_chunk()?;
            }

            if self.buf.len() >= size {
                break;
            }

            if self.buf.len() == 0 {
                return Ok(0);
            }
        }

        let b = self.buf.split_to(size);
        buf.write_all(&b)?;
        Ok(b.len())
    }
}
