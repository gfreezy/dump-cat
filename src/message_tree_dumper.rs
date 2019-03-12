use std::fs::File;
use std::io::{BufReader, Cursor, Error, Read, Seek, SeekFrom, Write};
use std::path::Path;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::BytesMut;
use failure::Fallible;

use crate::message_tree::{MessageTree, try_read_data};

pub struct MessageTreeDumper {
    file_reader: BufReader<File>,
}

impl MessageTreeDumper {
    pub fn open(path: impl AsRef<Path>) -> Fallible<Self> {
        let mut file_reader = BufReader::with_capacity(1024 * 1024, File::open(path)?);
        let magic_number = file_reader.read_i32::<BigEndian>()?;
        assert_eq!(magic_number, -1);

        Ok(MessageTreeDumper { file_reader })
    }
}

impl IntoIterator for MessageTreeDumper {
    type Item = MessageTree;
    type IntoIter = MessageTreeIterator;

    fn into_iter(self) -> Self::IntoIter {
        MessageTreeIterator::new(self.file_reader)
    }
}

pub struct MessageTreeIterator {
    file_reader: BufReader<File>,
    snappy_reader: Option<SnappyReader>,
}

impl MessageTreeIterator {
    pub fn new(file_reader: BufReader<File>) -> Self {
        let mut iterator = MessageTreeIterator {
            file_reader,
            snappy_reader: None,
        };
        iterator.read_next_block().expect("read next block");
        iterator
    }

    fn read_next_block(&mut self) -> Fallible<()> {
        let snappy_block = match try_read_data(&mut self.file_reader)? {
            None => {
                self.snappy_reader = None;
                return Ok(());
            },
            Some(b) => b,
        };
        let mut snappy_reader = SnappyReader::new(snappy_block);
        let _header = snappy_reader.read_header()?;
        self.snappy_reader = Some(snappy_reader);
        Ok(())
    }
}

impl Iterator for MessageTreeIterator {
    type Item = MessageTree;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let snappy_reader = match &mut self.snappy_reader {
                None => break,
                Some(reader) => reader,
            };
            let message_buf = try_read_data(snappy_reader).expect("try read data");
            let message_buf = match message_buf {
                Some(buf) => buf,
                None => {
                    self.read_next_block().expect("read next block");
                    continue;
                }
            };
            let tree =
                MessageTree::decode(&mut message_buf.as_slice()).expect("decode message tree");
            return Some(tree);
        }
        None
    }
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
