use std::io::{Error, Read};
use std::rc::Rc;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use failure::Fallible;

pub type MessageId = Text;
pub type Text = Vec<u8>;

#[derive(Debug, Default, Clone)]
pub struct InnerEvent {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
}

impl InnerEvent {
    fn new(
        ty: impl Into<Text>,
        name: impl Into<Text>,
        ts: u64,
        status: impl Into<Text>,
        data: impl Into<Text>,
    ) -> Self {
        InnerEvent {
            ty: ty.into(),
            name: name.into(),
            timestamp_in_ms: ts,
            status: status.into(),
            data: data.into(),
            ..Default::default()
        }
    }
}

pub type Event = Rc<InnerEvent>;

#[derive(Debug, Default, Clone)]
pub struct InnerTransaction {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
    pub duration_in_ms: u64,
    pub children: Vec<Message>,
    pub duration_start: u64,
}

impl InnerTransaction {
    fn new(ty: impl Into<Text>, name: impl Into<Text>) -> Self {
        InnerTransaction {
            ty: ty.into(),
            name: name.into(),
            timestamp_in_ms: time::precise_time_ns() / 1_000_000,
            ..Default::default()
        }
    }

    pub fn add_child(&mut self, message: Message) {
        self.children.push(message);
    }
}

pub type Transaction = Rc<InnerTransaction>;

#[derive(Debug, Default, Clone)]
pub struct InnerHeartbeat {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
}

impl InnerHeartbeat {
    fn new(
        ty: impl Into<Text>,
        name: impl Into<Text>,
        ts: u64,
        status: impl Into<Text>,
        data: impl Into<Text>,
    ) -> Self {
        InnerHeartbeat {
            ty: ty.into(),
            name: name.into(),
            timestamp_in_ms: ts,
            status: status.into(),
            data: data.into(),
            ..Default::default()
        }
    }
}

pub type Heartbeat = Rc<InnerHeartbeat>;

#[derive(Debug, Default, Clone)]
pub struct InnerMetric {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
}

impl InnerMetric {
    fn new(
        ty: impl Into<Text>,
        name: impl Into<Text>,
        ts: u64,
        status: impl Into<Text>,
        data: impl Into<Text>,
    ) -> Self {
        InnerMetric {
            ty: ty.into(),
            name: name.into(),
            timestamp_in_ms: ts,
            status: status.into(),
            data: data.into(),
            ..Default::default()
        }
    }
}

pub type Metric = Rc<InnerMetric>;

#[derive(Debug, Default, Clone)]
pub struct InnerTrace {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
}

impl InnerTrace {
    fn new(
        ty: impl Into<Text>,
        name: impl Into<Text>,
        ts: u64,
        status: impl Into<Text>,
        data: impl Into<Text>,
    ) -> Self {
        InnerTrace {
            ty: ty.into(),
            name: name.into(),
            timestamp_in_ms: ts,
            status: status.into(),
            data: data.into(),
            ..Default::default()
        }
    }
}

pub type Trace = Rc<InnerTrace>;

#[derive(Debug, Clone)]
pub enum Message {
    Event(Event),
    Transaction(Transaction),
    Heartbeat(Heartbeat),
    Metric(Metric),
    Trace(Trace),
}

impl Default for Message {
    fn default() -> Self {
        Message::Transaction(Rc::new(InnerTransaction::default()))
    }
}

#[derive(Debug, Default, Clone)]
pub struct MessageTree {
    pub domain: Text,
    pub hostname: Text,
    pub ip_address: Text,
    pub message: Message,
    pub message_id: Text,
    pub parent_message_id: Text,
    pub root_message_id: Text,
    pub session_token: Text,
    pub thread_group_name: Text,
    pub thread_id: Text,
    pub thread_name: Text,
    pub format_message_id: MessageId,
    pub discard: bool,
    pub process_loss: bool,
    pub hit_sample: bool,
    pub events: Vec<Event>,
    pub transactions: Vec<Transaction>,
    pub heartbeats: Vec<Heartbeat>,
    pub metrics: Vec<Metric>,
    pub traces: Vec<Trace>,
}

impl MessageTree {
    pub fn add_event(&mut self, event: Event) {
        self.events.push(event)
    }
    pub fn add_transaction(&mut self, transaction: Transaction) {
        self.transactions.push(transaction)
    }
    pub fn add_heartbeat(&mut self, heartbeat: Heartbeat) {
        self.heartbeats.push(heartbeat)
    }
    pub fn add_metric(&mut self, metric: Metric) {
        self.metrics.push(metric)
    }

    pub fn add_trace(&mut self, trace: Trace) {
        self.traces.push(trace)
    }

    pub fn decode<T: Read>(buf: &mut T) -> Fallible<MessageTree> {
        let mut tree = MessageTree::default();
        decode_header(&mut tree, buf)?;
        decode_message(&mut tree, &mut None, buf)?;

        tree.message = if !tree.transactions.is_empty() {
            Message::Transaction(tree.transactions.last().unwrap().clone())
        } else if !tree.events.is_empty() {
            Message::Event(tree.events.last().unwrap().clone())
        } else if !tree.metrics.is_empty() {
            Message::Metric(tree.metrics.last().unwrap().clone())
        } else if !tree.heartbeats.is_empty() {
            Message::Heartbeat(tree.heartbeats.last().unwrap().clone())
        } else if !tree.traces.is_empty() {
            Message::Trace(tree.traces.last().unwrap().clone())
        } else {
            unreachable!()
        };

        Ok(tree)
    }
}

const ID: &[u8] = b"NT1";

fn decode_header<T: Read>(tree: &mut MessageTree, buf: &mut T) -> Fallible<()> {
    let version = read_version(buf)?;
    if version != ID {
        unimplemented!("Unrecognized version");
    }
    tree.domain = read_string(buf)?;
    tree.hostname = read_string(buf)?;
    tree.ip_address = read_string(buf)?;
    tree.thread_group_name = read_string(buf)?;
    tree.thread_id = read_string(buf)?;
    tree.thread_name = read_string(buf)?;
    tree.message_id = read_string(buf)?;
    tree.parent_message_id = read_string(buf)?;
    tree.root_message_id = read_string(buf)?;
    tree.session_token = read_string(buf)?;
    Ok(())
}

fn decode_message<T: Read>(
    tree: &mut MessageTree,
    transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let mut chs = [0];

    loop {
        let size = buf.read(&mut chs[..])?;
        if size == 0 {
            break;
        }
        let ch = chs[0];

        match ch {
            b't' => decode_transaction(tree, transaction, buf)?,
            b'T' => return Ok(()),
            b'E' => decode_event(tree, transaction, buf)?,
            b'M' => decode_metric(tree, transaction, buf)?,
            b'H' => decode_heartbeat(tree, transaction, buf)?,
            b'L' => decode_trace(tree, transaction, buf)?,
            _ => unimplemented!("unsupported type"),
        }
    }

    Ok(())
}

fn decode_transaction<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let mut name = read_string(buf)?;

    if ty == b"System" || name.starts_with(b"UploadMetric") {
        name = b"UploadMetric".to_vec();
    }

    let mut transaction = InnerTransaction::new(ty, name);
    transaction.timestamp_in_ms = ts;

    let mut t = Some(transaction);
    decode_message(tree, &mut t, buf)?;

    let mut transaction = match t {
        Some(t) => t,
        None => unreachable!(),
    };
    let status = read_string(buf)?;
    let data = read_string(buf)?;
    let duration_in_ms = read_varint(buf)?;
    transaction.status = status;
    transaction.data = data;
    transaction.duration_in_ms = duration_in_ms;

    let rc_t = Rc::new(transaction);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Transaction(rc_t.clone()))
    }
    tree.add_transaction(rc_t);

    Ok(())
}

fn decode_event<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let name = read_string(buf)?;
    let status = read_string(buf)?;
    let data = read_string(buf)?;

    let event = InnerEvent::new(ty, name, ts, status, data);

    let rc_e = Rc::new(event);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Event(rc_e.clone()));
    }
    tree.add_event(rc_e);

    Ok(())
}

fn decode_metric<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let name = read_string(buf)?;
    let status = read_string(buf)?;
    let data = read_string(buf)?;

    let metric = InnerMetric::new(ty, name, ts, status, data);
    let rc_m = Rc::new(metric);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Metric(rc_m.clone()));
    }
    tree.add_metric(rc_m);

    Ok(())
}

fn decode_heartbeat<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let name = read_string(buf)?;
    let status = read_string(buf)?;
    let data = read_string(buf)?;

    let heartbeat = InnerHeartbeat::new(ty, name, ts, status, data);
    let rc_h = Rc::new(heartbeat);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Heartbeat(rc_h.clone()));
    }
    tree.add_heartbeat(rc_h);

    Ok(())
}

fn decode_trace<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let name = read_string(buf)?;
    let status = read_string(buf)?;
    let data = read_string(buf)?;

    let trace = InnerTrace::new(ty, name, ts, status, data);
    let rc_t = Rc::new(trace);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Trace(rc_t.clone()));
    }
    tree.add_trace(rc_t);

    Ok(())
}

fn read_version<T: Read>(buf: &mut T) -> Fallible<Text> {
    let mut data = vec![0; 3];
    buf.read_exact(&mut data)?;
    Ok(data)
}

fn read_string<T: Read>(buf: &mut T) -> Fallible<Text> {
    let len = read_varint(buf)?;
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut b = vec![0; len as usize];
    buf.read_exact(&mut b)?;

    Ok(b)
}

/// https://developers.google.com/protocol-buffers/docs/encoding#varints
pub fn read_varint<T: Read>(data: &mut T) -> Fallible<u64> {
    let mut n: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let b = data.read_u8()?;
        if b < 0b1000_0000 {
            return match (b as u64).checked_shl(shift) {
                None => Ok(0),
                Some(b) => Ok(n | b),
            };
        }
        match ((b as u64) & 0b0111_1111).checked_shl(shift) {
            None => return Ok(0),
            Some(b) => n |= b,
        }
        shift += 7;
    }
}

pub fn try_read_data<T: Read>(reader: &mut T) -> Result<Option<Vec<u8>>, Error> {
    let mut buf = [0; 4];
    let size = reader.read(&mut buf)?;
    if size == 0 {
        return Ok(None);
    } else if size != 4 {
        panic!("read length error")
    }
    let length = BigEndian::read_i32(&buf);
    let mut buf = vec![0; length as usize];
    reader.read_exact(&mut buf)?;
    Ok(Some(buf))
}
