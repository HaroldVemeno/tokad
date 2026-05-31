use crate::hash::ID_MASK;
use crate::tokad::proto;

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

fn parse_id(bytes: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    u128::from_le_bytes(buf) & ID_MASK
}

impl Stub {
    pub fn rep(self) -> proto::Stub {
        proto::Stub {
            id: self.id.to_le_bytes().to_vec(),
            port: self.port
        }
    }
}

impl proto::Stub {
    pub fn unrep(self) -> Stub {
        Stub {
            id: parse_id(&self.id),
            port: self.port
        }
    }
}

impl Node {
    pub fn rep(self) -> proto::Node {
        proto::Node {
            id: self.id.to_le_bytes().to_vec(),
            ip: self.ip,
            port: self.port
        }
    }
}

impl proto::Node {
    pub fn unrep(self) -> Node {
        Node {
            id: parse_id(&self.id),
            ip: self.ip,
            port: self.port
        }
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
    pub fn unrep(self) -> (Option<Stub>, Nodes) {
        (self.source.map(|s| s.unrep()), Nodes {
            nodes: self.nodes.into_iter().map(|n| n.unrep()).collect(),
        })
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
    pub fn unrep(self) -> (Option<Stub>, Store) {
        (
            self.source.map(|s| s.unrep()),
            Store {
                key: parse_id(&self.key),
                value: self.value,
            },
        )
    }
}

impl proto::StoreRequest {
    pub fn unrep(self) -> (Option<Stub>, Store, bool) {
        (
            self.source.map(|s| s.unrep()),
            Store {
                key: parse_id(&self.key),
                value: self.value,
            },
            self.publish,
        )
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
    pub fn unrep(self) -> (Option<Stub>, Key) {
        (
            self.source.map(|s| s.unrep()),
            Key {
                key: parse_id(&self.key),
            }
        )
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
    pub fn unrep(self) -> Option<(Option<Stub>, StoreOrNodes)> {
        Some(match self.oneof? {
            proto::store_or_nodes::Oneof::Store(store) => {
                let (stub, store) = store.unrep();
                (stub, store.or_nodes())
            }
            proto::store_or_nodes::Oneof::Nodes(nodes) => {
                let (stub, nodes) = nodes.unrep();
                (stub, nodes.or_store())
            }
        })
    }
}
