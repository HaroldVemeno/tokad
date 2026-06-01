use crate::hash::ID_MASK;
use crate::tokad::proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataError {
    InvalidIp(String),
    InvalidPort(String),
    InvalidIdLength(usize),
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataError::InvalidIp(e) => write!(f, "Invalid IP address: {}", e),
            DataError::InvalidPort(e) => write!(f, "Invalid port: {}", e),
            DataError::InvalidIdLength(len) => write!(f, "Invalid ID length: expected 16 bytes, got {} bytes", len),
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
    pub fn rep(self) -> proto::Stub {
        proto::Stub {
            id: self.id.to_le_bytes().to_vec(),
            port: self.port,
        }
    }
}

impl proto::Stub {
    pub fn unrep(self) -> Result<Stub, DataError> {
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
        self.ip.parse::<std::net::IpAddr>().map_err(|e| DataError::InvalidIp(e.to_string()))?;
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
    pub fn rep(self, stub: Option<Stub>) -> proto::Nodes {
        proto::Nodes {
            source: stub.map(|s| s.rep()),
            nodes: self.nodes.into_iter().map(|n| n.rep()).collect(),
        }
    }
}

impl proto::Nodes {
    pub fn unrep(self) -> Result<(Option<Stub>, Nodes), DataError> {
        let stub = match self.source {
            Some(s) => Some(s.unrep()?),
            None => None,
        };
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for n in self.nodes {
            nodes.push(n.unrep()?);
        }
        Ok((stub, Nodes { nodes }))
    }
}

impl Store {
    pub fn or_nodes(self) -> StoreOrNodes {
        StoreOrNodes::Store(self)
    }
    pub fn rep(self, stub: Option<Stub>) -> proto::Store {
        proto::Store {
            source: stub.map(|s| s.rep()),
            key: self.key.to_le_bytes().to_vec(),
            value: self.value,
        }
    }
}

impl proto::Store {
    pub fn req(self, publish: bool) -> proto::StoreRequest {
        proto::StoreRequest {
            source: self.source,
            key: self.key,
            value: self.value,
            publish,
        }
    }
    pub fn unrep(self) -> Result<(Option<Stub>, Store), DataError> {
        let stub = match self.source {
            Some(s) => Some(s.unrep()?),
            None => None,
        };
        let key = parse_id(&self.key)?;
        Ok((
            stub,
            Store {
                key,
                value: self.value,
            },
        ))
    }
}

impl proto::StoreRequest {
    pub fn unrep(self) -> Result<(Option<Stub>, Store, bool), DataError> {
        let stub = match self.source {
            Some(s) => Some(s.unrep()?),
            None => None,
        };
        let key = parse_id(&self.key)?;
        Ok((
            stub,
            Store {
                key,
                value: self.value,
            },
            self.publish,
        ))
    }
}

impl Key {
    pub fn rep(self, stub: Option<Stub>) -> proto::Key {
        proto::Key {
            source: stub.map(|s| s.rep()),
            key: self.key.to_le_bytes().to_vec(),
        }
    }
}

impl proto::Key {
    pub fn unrep(self) -> Result<(Option<Stub>, Key), DataError> {
        let stub = match self.source {
            Some(s) => Some(s.unrep()?),
            None => None,
        };
        let key = parse_id(&self.key)?;
        Ok((
            stub,
            Key {
                key,
            },
        ))
    }
}

impl StoreOrNodes {
    pub fn rep(self, stub: Option<Stub>) -> proto::StoreOrNodes {
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
    pub fn unrep(self) -> Result<Option<(Option<Stub>, StoreOrNodes)>, DataError> {
        let oneof = match self.oneof {
            Some(o) => o,
            None => return Ok(None),
        };
        Ok(Some(match oneof {
            proto::store_or_nodes::Oneof::Store(store) => {
                let (stub, store) = store.unrep()?;
                (stub, store.or_nodes())
            }
            proto::store_or_nodes::Oneof::Nodes(nodes) => {
                let (stub, nodes) = nodes.unrep()?;
                (stub, nodes.or_store())
            }
        }))
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
