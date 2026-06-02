use std::time::{self, Duration, SystemTime};

use crate::hash::{ID_MASK, key_hash};
use crate::tokad::proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    InvalidIp(String),
    InvalidPort(String),
    InvalidIdLength(usize),
    MissingField(String),
    HashMismatch { expected: u128, actual: u128 },
    TimeTravel { now: SystemTime, received: SystemTime },
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataError::InvalidIp(e) => write!(f, "Invalid IP address: {}", e),
            DataError::InvalidPort(e) => write!(f, "Invalid port: {}", e),
            DataError::InvalidIdLength(len) => {
                write!(f, "Invalid ID length: expected 16 bytes, got {} bytes", len)
            }
            DataError::HashMismatch { expected, actual } => write!(
                f,
                "HashMismatch: expected {}, got {} bytes",
                expected, actual
            ),
            DataError::MissingField(msg) => write!(f, "Missing field: {}", msg),
            DataError::TimeTravel { now, received } => write!(f, "Future timestamp: now {:?}, received {:?}", now, received),
        }
    }
}

impl std::error::Error for DataError {}

#[derive(Debug, Clone, Default)]
pub struct Stub {
    pub id: u128,
    pub port: u32,
}

#[derive(Debug, Clone, Default)]
pub struct Node {
    pub id: u128,
    pub ip: String,
    pub port: u32,
}

#[derive(Debug, Clone)]
pub struct Key {
    pub key: u128,
}

#[derive(Debug, Clone)]
pub struct Store {
    pub key: u128,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StoreRequest {
    pub store: Store,
    pub publish: bool,
    pub publish_time: SystemTime,
}

#[derive(Debug, Clone)]
pub struct Nodes {
    pub nodes: Vec<Node>,
}

#[derive(Debug, Clone)]
pub enum StoreOrNodes {
    Store(Store),
    Nodes(Nodes),
}

fn parse_id(bytes: &[u8]) -> Result<u128, DataError> {
    if bytes.len() != 16 {
        return Err(DataError::InvalidIdLength(bytes.len()));
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Ok(u128::from_le_bytes(buf) & ID_MASK)
}

impl Stub {
    pub fn rep(&self) -> proto::Stub {
        proto::Stub {
            id: self.id.to_le_bytes().to_vec(),
            port: self.port,
        }
    }
}

impl proto::Stub {
    pub fn unrep(&self) -> Result<Stub, DataError> {
        let id = parse_id(&self.id)?;
        u16::try_from(self.port).map_err(|e| DataError::InvalidPort(e.to_string()))?;
        Ok(Stub {
            id,
            port: self.port,
        })
    }
}

impl Node {
    pub fn rep(self) -> proto::Node {
        proto::Node {
            id: self.id.to_le_bytes().to_vec(),
            ip: self.ip,
            port: self.port,
        }
    }
}

impl proto::Node {
    pub fn unrep(self) -> Result<Node, DataError> {
        let id = parse_id(&self.id)?;
        self.ip
            .parse::<std::net::IpAddr>()
            .map_err(|e| DataError::InvalidIp(e.to_string()))?;
        u16::try_from(self.port).map_err(|e| DataError::InvalidPort(e.to_string()))?;
        Ok(Node {
            id,
            ip: self.ip,
            port: self.port,
        })
    }
}

impl Nodes {
    pub fn or_store(self) -> StoreOrNodes {
        StoreOrNodes::Nodes(self)
    }
    pub fn rep(self, stub: Stub) -> proto::Nodes {
        proto::Nodes {
            source: Some(stub.rep()),
            nodes: self.nodes.into_iter().map(|n| n.rep()).collect(),
        }
    }
}

impl proto::Nodes {
    pub fn unrep(self) -> Result<Nodes, DataError> {
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for n in self.nodes {
            nodes.push(n.unrep()?);
        }
        Ok(Nodes { nodes })
    }
}

impl Store {
    pub fn or_nodes(self) -> StoreOrNodes {
        StoreOrNodes::Store(self)
    }
    pub fn rep(self, stub: Stub) -> proto::Store {
        proto::Store {
            source: Some(stub.rep()),
            key: self.key.to_le_bytes().to_vec(),
            value: self.value,
        }
    }
}

impl proto::Store {
    pub fn unrep(self) -> Result<Store, DataError> {
        let key = parse_id(&self.key)?;
        let expected = key_hash(&self.value);
        if key != expected {
            return Err(DataError::HashMismatch {
                expected,
                actual: key,
            });
        }
        Ok(Store {
            key,
            value: self.value,
        })
    }
}

impl StoreRequest {
    pub fn rep(self, stub: Stub) -> proto::StoreRequest {
        proto::StoreRequest {
            source: Some(stub.rep()),
            key: self.store.key.to_le_bytes().to_vec(),
            value: self.store.value,
            publish_time: self.publish_time.duration_since(time::UNIX_EPOCH)
                                           .unwrap_or(Duration::ZERO)
                                           .as_millis() as u64,
            publish: self.publish,
        }
    }
}

impl proto::StoreRequest {
    pub fn unrep(self) -> Result<StoreRequest, DataError> {
        let key = parse_id(&self.key)?;
        let expected = key_hash(&self.value);
        if key != expected {
            return Err(DataError::HashMismatch {
                expected,
                actual: key,
            });
        }
        let publish_time = time::UNIX_EPOCH + Duration::from_millis(self.publish_time);
        let now = SystemTime::now();
        if publish_time > now {
            return Err(DataError::TimeTravel{received: publish_time, now})
        }
        Ok(StoreRequest {
            store: Store {
                key,
                value: self.value,
            },
            publish: self.publish,
            publish_time,
        })
    }
}

impl Key {
    pub fn rep(self, stub: Stub) -> proto::Key {
        proto::Key {
            source: Some(stub.rep()),
            key: self.key.to_le_bytes().to_vec(),
        }
    }
}

impl proto::Key {
    pub fn unrep(self) -> Result<Key, DataError> {
        Ok(Key {
            key: parse_id(&self.key)?,
        })
    }
}

impl StoreOrNodes {
    pub fn rep(self, stub: Stub) -> proto::StoreOrNodes {
        match self {
            StoreOrNodes::Store(store) => proto::StoreOrNodes {
                oneof: Some(proto::store_or_nodes::Oneof::Store(store.rep(stub))),
            },
            StoreOrNodes::Nodes(nodes) => proto::StoreOrNodes {
                oneof: Some(proto::store_or_nodes::Oneof::Nodes(nodes.rep(stub))),
            },
        }
    }
}

impl proto::StoreOrNodes {
    pub fn unrep(self) -> Result<StoreOrNodes, DataError> {
        let oneof = match self.oneof {
            Some(o) => o,
            None => return Err(DataError::MissingField("missing store or node".to_owned())),
        };
        Ok(match oneof {
            proto::store_or_nodes::Oneof::Store(store) => {
                let store = store.unrep()?;
                store.or_nodes()
            }
            proto::store_or_nodes::Oneof::Nodes(nodes) => {
                let nodes = nodes.unrep()?;
                nodes.or_store()
            }
        })
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.value) {
            write!(f, "{}: {}", self.key, s)?;
        } else {
            write!(f, "{}: {:?}", self.key, self.value)?;
        }
        Ok(())
    }
}

impl TryFrom<&proto::Ping> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::Ping) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::Pong> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::Pong) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::Store> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::Store) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::Nodes> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::Nodes) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::StoreRequest> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::StoreRequest) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::Key> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::Key) -> Result<Self, Self::Error> {
        match &req.source {
            Some(s) => s.unrep(),
            None => Err(DataError::MissingField("missing source".to_owned())),
        }
    }
}
impl TryFrom<&proto::StoreOrNodes> for Stub {
    type Error = DataError;

    fn try_from(req: &proto::StoreOrNodes) -> Result<Self, Self::Error> {
        match &req.oneof {
            Some(proto::store_or_nodes::Oneof::Nodes(nodes)) => nodes.try_into(),
            Some(proto::store_or_nodes::Oneof::Store(store)) => store.try_into(),
            _ => Err(DataError::MissingField("missing store or node".to_owned())),
        }
    }
}
