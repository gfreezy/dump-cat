use std::convert::TryFrom;
use std::fmt::{Display, Formatter};
use std::io::{Error, Read};
use std::rc::Rc;

use byteorder::{BigEndian, ByteOrder, ReadBytesExt};
use failure::Fallible;
use log::{debug, warn};
use serde::{Deserialize, Serialize};

pub type MessageId = Text;
pub type Text = String;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
        }
    }
}

pub type Event = Rc<InnerEvent>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct InnerTransaction {
    pub status: Text,
    pub ty: Text,
    pub name: Text,
    pub timestamp_in_ms: u64,
    pub data: Text,
    pub duration_in_ms: u64,
    pub children: Vec<Message>,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
        }
    }
}

pub type Heartbeat = Rc<InnerHeartbeat>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
        }
    }
}

pub type Metric = Rc<InnerMetric>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
        }
    }
}

pub type Trace = Rc<InnerTrace>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Event(Event),
    Transaction(Transaction),
    Heartbeat(Heartbeat),
    Metric(Metric),
    Trace(Trace),
}

impl Message {
    pub fn status(&self) -> &Text {
        match self {
            Message::Event(e) => &e.status,
            Message::Transaction(e) => &e.status,
            Message::Trace(e) => &e.status,
            Message::Heartbeat(e) => &e.status,
            Message::Metric(e) => &e.status,
        }
    }

    pub fn ty(&self) -> &Text {
        match self {
            Message::Event(e) => &e.ty,
            Message::Transaction(e) => &e.ty,
            Message::Trace(e) => &e.ty,
            Message::Heartbeat(e) => &e.ty,
            Message::Metric(e) => &e.ty,
        }
    }

    pub fn name(&self) -> &Text {
        match self {
            Message::Event(e) => &e.name,
            Message::Transaction(e) => &e.name,
            Message::Trace(e) => &e.name,
            Message::Heartbeat(e) => &e.name,
            Message::Metric(e) => &e.name,
        }
    }

    pub fn ts(&self) -> i32 {
        (match self {
            Message::Event(e) => e.timestamp_in_ms / 1000,
            Message::Transaction(e) => e.timestamp_in_ms / 1000,
            Message::Trace(e) => e.timestamp_in_ms / 1000,
            Message::Heartbeat(e) => e.timestamp_in_ms / 1000,
            Message::Metric(e) => e.timestamp_in_ms / 1000,
        }) as i32
    }

    pub fn duration_in_ms(&self) -> Option<u64> {
        match self {
            Message::Transaction(e) => Some(e.duration_in_ms),
            _ => None,
        }
    }
}

impl Display for Message {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        use self::Message::*;

        match self {
            Event(e) => f
                .debug_struct("Event")
                .field("timestamp_in_ms", &e.timestamp_in_ms)
                .field("ty", &e.ty)
                .field("name", &e.name)
                .field("status", &e.status)
                .field("data", &e.data)
                .finish(),
            Transaction(e) => f
                .debug_struct("Transaction")
                .field("timestamp_in_ms", &e.timestamp_in_ms)
                .field("ty", &e.ty)
                .field("name", &e.name)
                .field("status", &e.status)
                .field("duration_in_ms", &e.duration_in_ms)
                .field("data", &e.data)
                .field(
                    "children",
                    &if e.children.is_empty() { "[]" } else { "[...]" },
                )
                .finish(),
            Heartbeat(e) => f
                .debug_struct("Heartbeat")
                .field("timestamp_in_ms", &e.timestamp_in_ms)
                .field("ty", &e.ty)
                .field("name", &e.name)
                .field("status", &e.status)
                .field("data", &e.data)
                .finish(),
            Metric(e) => f
                .debug_struct("Metric")
                .field("timestamp_in_ms", &e.timestamp_in_ms)
                .field("ty", &e.ty)
                .field("name", &e.name)
                .field("status", &e.status)
                .field("data", &e.data)
                .finish(),
            Trace(e) => f
                .debug_struct("Trace")
                .field("timestamp_in_ms", &e.timestamp_in_ms)
                .field("ty", &e.ty)
                .field("name", &e.name)
                .field("status", &e.status)
                .field("data", &e.data)
                .finish(),
        }?;
        Ok(())
    }
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

    debug!("decode header");

    Ok(())
}

fn decode_message<T: Read>(
    tree: &mut MessageTree,
    transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    let mut chs = [0];

    debug!("start decode message: {:p}", tree);

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

    debug!("finish decode message: {:p}", tree);

    Ok(())
}

fn decode_transaction<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    debug!("start decode transaction: {:p}", tree);

    let ts = read_varint(buf)?;
    let ty = read_string(buf)?;
    let mut name = read_string(buf)?;

    if ty == "System" || name.starts_with("UploadMetric") {
        name = "UploadMetric".to_string();
    }

    let mut transaction = InnerTransaction::new(ty.clone(), name.clone());
    transaction.timestamp_in_ms = ts;

    let mut t = Some(transaction);
    decode_message(tree, &mut t, buf)?;

    let mut transaction = match t {
        Some(t) => t,
        None => unreachable!(),
    };
    let status = read_string(buf)?;
    let data = read_bytes(buf)?;
    let duration_in_ms = read_varint(buf)? / 1000;
    transaction.status = status;
    let data_str = String::from_utf8(data);
    match data_str {
        Ok(s) => transaction.data = s,
        Err(err) => {
            transaction.data = String::from_utf8_lossy(err.as_bytes()).to_string();
            warn!(
                "Transaction \"{}.{}\" decoding utf8 error: bytes is \"{:?}\", lossy utf8 is \"{}\"",
                &ty,
                &name,
                err.as_bytes(),
                &transaction.data
            );
        }
    }
    transaction.duration_in_ms = duration_in_ms;

    let rc_t = Rc::new(transaction);
    if let Some(t) = parent_transaction {
        t.add_child(Message::Transaction(rc_t.clone()))
    }
    tree.add_transaction(rc_t);

    debug!("finish decode transaction: {:p}", tree);
    Ok(())
}

fn decode_event<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    debug!("start decode event: {:p}", tree);

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

    debug!("finish decode event: {:p}", tree);

    Ok(())
}

fn decode_metric<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    debug!("start decode metric: {:p}", tree);

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

    debug!("finish decode metric: {:p}", tree);
    Ok(())
}

fn decode_heartbeat<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    debug!("start decode heartbeat: {:p}", tree);

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
    debug!("finish decode heartbeat: {:p}", tree);

    Ok(())
}

fn decode_trace<T: Read>(
    tree: &mut MessageTree,
    parent_transaction: &mut Option<InnerTransaction>,
    buf: &mut T,
) -> Fallible<()> {
    debug!("start decode trace: {:p}", tree);

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
    debug!("finish decode trace: {:p}", tree);

    Ok(())
}

fn read_version<T: Read>(buf: &mut T) -> Fallible<Text> {
    let mut data = vec![0; 3];
    buf.read_exact(&mut data)?;
    Ok(String::from_utf8(data)?)
}

fn read_string<T: Read>(buf: &mut T) -> Fallible<Text> {
    let len = read_varint(buf)?;
    if len == 0 {
        return Ok("".to_string());
    }
    let mut b = vec![0; len as usize];
    buf.read_exact(&mut b)?;

    Ok(String::from_utf8(b)?)
}

fn read_bytes<T: Read>(buf: &mut T) -> Fallible<Vec<u8>> {
    let len = read_varint(buf)?;
    if len == 0 {
        return Ok(vec![]);
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
            return match u64::try_from(b)?.checked_shl(shift) {
                None => Ok(0),
                Some(b) => Ok(n | b),
            };
        }
        match (u64::try_from(b)? & 0b0111_1111).checked_shl(shift) {
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
