use std::fs::File;
use std::io::{BufReader, Cursor, Error, Read, Write};
use std::path::{Path, PathBuf};
use std::{iter, thread};

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use bytes::BytesMut;
use failure::Fallible;
use log::{debug, info};

use crate::message_tree::{try_read_data, MessageTree};
use crossbeam::channel::{RecvTimeoutError, SendTimeoutError};
use std::time::Duration;

use derive_builder::Builder;

fn read_block(block: Vec<u8>) -> Vec<MessageTree> {
    let snappy_reader = SnappyReader::new(block);
    let tree_reader = MessageTreeReader::new(snappy_reader);
    tree_reader.into_iter().collect()
}

#[derive(Default, Builder, Debug)]
#[builder(setter(into))]
pub struct MessageTreeDumper {
    path: PathBuf,
    #[builder(default = "1")]
    threads: usize,
    #[builder(default = "10")]
    block_reader_channel_buffer_size: usize,
    #[builder(default = "10")]
    tree_decoder_channel_buffer_size: usize,
}

impl MessageTreeDumper {
    pub fn into_iter(self) -> impl Iterator<Item = MessageTree> {
        self.read_trees().into_iter()
    }

    pub fn read_trees(self) -> crossbeam::Receiver<MessageTree> {
        let block_reader = MessageBlockReader::open(&self.path).expect("open message block reader");
        let (block_sender, block_receiver) =
            crossbeam::bounded(self.block_reader_channel_buffer_size);
        let (tree_sender, tree_receiver) =
            crossbeam::bounded(self.tree_decoder_channel_buffer_size);

        thread::Builder::new()
            .name("BlockReaderThread".to_string())
            .spawn(move || {
                for block in block_reader.into_iter() {
                    let mut to_send = block;
                    loop {
                        let ret = block_sender.send_timeout(to_send, Duration::from_secs(5));
                        to_send = match ret {
                            // Send success, continue to send the next one.
                            Ok(()) => break,
                            // Send timeout. We retry it.
                            Err(SendTimeoutError::Timeout(t)) => {
                                info!("Reading blocks too fast.");
                                t
                            }
                            // Receiver disconnected. Exit current thread.
                            Err(SendTimeoutError::Disconnected(_)) => return,
                        };
                    }
                }
            })
            .expect("spawn error");

        for i in 0..self.threads {
            let block_receiver = block_receiver.clone();
            let tree_sender = tree_sender.clone();

            thread::Builder::new()
                .name(format!("TreeDecoder{}", i))
                .spawn(move || {
                    loop {
                        let block = match block_receiver.recv_timeout(Duration::from_millis(5)) {
                            Ok(block) => block,
                            Err(RecvTimeoutError::Timeout) => {
                                info!("Waiting for new block");
                                continue;
                            }
                            Err(RecvTimeoutError::Disconnected) => {
                                break;
                            }
                        };
                        for tree in read_block(block) {
                            let mut to_send = tree;
                            loop {
                                let ret =
                                    tree_sender.send_timeout(to_send, Duration::from_millis(5));
                                to_send = match ret {
                                    // Send success, continue to send the next one.
                                    Ok(()) => break,
                                    // Send timeout. We retry it.
                                    Err(SendTimeoutError::Timeout(t)) => {
                                        info!("Decoding too fast.");
                                        t
                                    }
                                    // Receiver disconnected. Exit current thread.
                                    Err(SendTimeoutError::Disconnected(_)) => return,
                                };
                            }
                        }
                    }
                })
                .expect("spawn error");
        }

        tree_receiver
    }
}

struct SnappyReader {
    reader: Cursor<Vec<u8>>,
    buf: BytesMut,
}

impl SnappyReader {
    pub fn new(buf: Vec<u8>) -> Self {
        debug!("new SnappyReader");
        SnappyReader {
            reader: Cursor::new(buf),
            buf: BytesMut::new(),
        }
    }

    pub fn read_header(&mut self) -> Fallible<Vec<u8>> {
        let mut snappy_magic_header = vec![0; 16];
        self.reader.read_exact(&mut snappy_magic_header)?;
        debug!("read snappy header");
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

            if self.buf.is_empty() {
                return Ok(0);
            }
        }

        let b = self.buf.split_to(size);
        buf.write_all(&b)?;
        Ok(b.len())
    }
}

pub struct MessageBlockReader {
    file_reader: BufReader<File>,
}

impl MessageBlockReader {
    pub fn open(path: impl AsRef<Path>) -> Fallible<Self> {
        let mut file_reader = BufReader::with_capacity(1024 * 1024, File::open(path)?);
        let magic_number = file_reader.read_i32::<BigEndian>()?;
        assert_eq!(magic_number, -1);
        debug!("magic number: {}", magic_number);

        Ok(MessageBlockReader { file_reader })
    }

    pub fn into_iter(self) -> impl Iterator<Item = Vec<u8>> {
        let mut f = self.file_reader;
        iter::from_fn(move || try_read_data(&mut f).expect("try read data"))
    }
}

struct MessageTreeReader {
    snappy_reader: SnappyReader,
}

impl MessageTreeReader {
    fn new(snapper_reader: SnappyReader) -> Self {
        let mut reader = MessageTreeReader {
            snappy_reader: snapper_reader,
        };
        let _header = reader
            .snappy_reader
            .read_header()
            .expect("read snappy header");
        reader
    }

    fn into_iter(self) -> impl Iterator<Item = MessageTree> {
        let mut snappy_reader = self.snappy_reader;
        iter::from_fn(move || {
            let message_buf = try_read_data(&mut snappy_reader).expect("try read data");
            let message_buf = message_buf?;
            debug!("read data from snappy reader: size: {}", message_buf.len());
            let tree =
                MessageTree::decode(&mut message_buf.as_slice()).expect("decode message tree");
            debug!("decode message tree");
            Some(tree)
        })
    }
}
