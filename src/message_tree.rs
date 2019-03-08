use std::io::Read;
use std::rc::Rc;

use byteorder::{BigEndian, ReadBytesExt};
use failure::Fallible;

pub type MessageId = String;

#[derive(Debug, Default, Clone)]
pub struct InnerEvent {
    pub status: String,
    pub ty: String,
    pub name: String,
    pub timestamp_in_ms: u64,
    pub data: String,
}

impl InnerEvent {
    fn new(
        ty: impl Into<String>,
        name: impl Into<String>,
        ts: u64,
        status: impl Into<String>,
        data: impl Into<String>,
    ) -> Self {
        let mut event = Self::default();
        event.ty = ty.into();
        event.name = name.into();
        event.timestamp_in_ms = ts;
        event.status = status.into();
        event.data = data.into();
        event
    }
}

pub type Event = Rc<InnerEvent>;

#[derive(Debug, Default, Clone)]
pub struct InnerTransaction {
    pub status: String,
    pub ty: String,
    pub name: String,
    pub timestamp_in_ms: u64,
    pub data: String,
    pub duration_in_ms: u64,
    pub children: Vec<Message>,
    pub duration_start: u64,
}

impl InnerTransaction {
    fn new(ty: impl Into<String>, name: impl Into<String>) -> Self {
        let mut transaction = Self::default();
        transaction.ty = ty.into();
        transaction.name = name.into();
        transaction.timestamp_in_ms = time::precise_time_ns() / 1_000_000;
        transaction
    }

    pub fn add_child(&mut self, message: Message) {
        self.children.push(message);
    }
}

pub type Transaction = Rc<InnerTransaction>;

#[derive(Debug, Default, Clone)]
pub struct InnerHeartbeat {
    pub status: String,
    pub ty: String,
    pub name: String,
    pub timestamp_in_ms: u64,
    pub data: String,
}

impl InnerHeartbeat {
    fn new(
        ty: impl Into<String>,
        name: impl Into<String>,
        ts: u64,
        status: impl Into<String>,
        data: impl Into<String>,
    ) -> Self {
        let mut heartbeat = Self::default();
        heartbeat.ty = ty.into();
        heartbeat.name = name.into();
        heartbeat.timestamp_in_ms = ts;
        heartbeat.status = status.into();
        heartbeat.data = data.into();
        heartbeat
    }
}

pub type Heartbeat = Rc<InnerHeartbeat>;

#[derive(Debug, Default, Clone)]
pub struct InnerMetric {
    pub status: String,
    pub ty: String,
    pub name: String,
    pub timestamp_in_ms: u64,
    pub data: String,
}

impl InnerMetric {
    fn new(
        ty: impl Into<String>,
        name: impl Into<String>,
        ts: u64,
        status: impl Into<String>,
        data: impl Into<String>,
    ) -> Self {
        let mut metric = Self::default();
        metric.ty = ty.into();
        metric.name = name.into();
        metric.timestamp_in_ms = ts;
        metric.status = status.into();
        metric.data = data.into();
        metric
    }
}

pub type Metric = Rc<InnerMetric>;

#[derive(Debug, Default, Clone)]
pub struct InnerTrace {
    pub status: String,
    pub ty: String,
    pub name: String,
    pub timestamp_in_ms: u64,
    pub data: String,
}

impl InnerTrace {
    fn new(
        ty: impl Into<String>,
        name: impl Into<String>,
        ts: u64,
        status: impl Into<String>,
        data: impl Into<String>,
    ) -> Self {
        let mut trace = Self::default();
        trace.ty = ty.into();
        trace.name = name.into();
        trace.timestamp_in_ms = ts;
        trace.status = status.into();
        trace.data = data.into();
        trace
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
    pub domain: String,
    pub hostname: String,
    pub ip_address: String,
    pub message: Message,
    pub message_id: String,
    pub parent_message_id: String,
    pub root_message_id: String,
    pub session_token: String,
    pub thread_group_name: String,
    pub thread_id: String,
    pub thread_name: String,
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
        decode_message(&mut tree, None, buf)?;

        tree.message = if tree.transactions.len() > 0 {
            Message::Transaction(tree.transactions.last().unwrap().clone())
        } else if tree.events.len() > 0 {
            Message::Event(tree.events.last().unwrap().clone())
        } else if tree.metrics.len() > 0 {
            Message::Metric(tree.metrics.last().unwrap().clone())
        } else if tree.heartbeats.len() > 0 {
            Message::Heartbeat(tree.heartbeats.last().unwrap().clone())
        } else if tree.traces.len() > 0 {
            Message::Trace(tree.traces.last().unwrap().clone())
        } else {
            unreachable!()
        };

        Ok(tree)
    }
}

const ID: &str = "NT1";

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
    transaction: Option<&mut InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let mut chs = [0];

    match transaction {
        None => {
            loop {
                let size = buf.read(&mut chs)?;
                if size == 0 {
                    break;
                }
                let ch = chs[0];

                match ch {
                    b't' => decode_transaction(tree, None, buf)?,
                    b'T' => return Ok(()),
                    b'E' => decode_event(tree, None, buf)?,
                    b'M' => decode_metric(tree, None, buf)?,
                    b'H' => decode_heartbeat(tree, None, buf)?,
                    b'L' => decode_trace(tree, None, buf)?,
                    _ => unimplemented!("unsupported type"),
                }
            }
        }
        Some(transaction) => {
            loop {
                let size = buf.read(&mut chs)?;
                if size == 0 {
                    break;
                }
                let ch = chs[0];

                match ch {
                    b't' => decode_transaction(tree, Some(transaction), buf)?,
                    b'T' => return Ok(()),
                    b'E' => decode_event(tree, Some(transaction), buf)?,
                    b'M' => decode_metric(tree, Some(transaction), buf)?,
                    b'H' => decode_heartbeat(tree, Some(transaction), buf)?,
                    b'L' => decode_trace(tree, Some(transaction), buf)?,
                    _ => unimplemented!("unsupported type"),
                }
            }
        }
    }

    Ok(())
}

fn decode_transaction<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: Option<&mut InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let mut name = read_string(buf)?;

    if ty == "System" || name.starts_with("UploadMetric") {
        name = "UploadMetric".to_string();
    }

    let mut transaction = InnerTransaction::new(ty, name);
    transaction.timestamp_in_ms = ts;

    decode_message(tree, Some(&mut transaction), buf)?;

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
    parent_transaction: Option<&mut InnerTransaction>,
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
    parent_transaction: Option<&mut InnerTransaction>,
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
    parent_transaction: Option<&mut InnerTransaction>,
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
    parent_transaction: Option<&mut InnerTransaction>,
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

fn read_version<T: Read>(buf: &mut T) -> Fallible<String> {
    let mut data = vec![0; 3];
    buf.read_exact(&mut data)?;
    Ok(String::from_utf8(data)?)
}

fn read_string<T: Read>(buf: &mut T) -> Fallible<String> {
    let len = read_varint(buf)?;
    if len == 0 {
        return Ok("".to_string());
    }
    let mut b = vec![0; len as usize];
    buf.read_exact(&mut b)?;

    Ok(String::from_utf8(b)?)
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

pub fn read_data<T: Read>(reader: &mut T) -> Fallible<Vec<u8>> {
    let length = reader.read_i32::<BigEndian>()?;
    let mut buf = vec![0; length as usize];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}
