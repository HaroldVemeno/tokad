use std::sync::Arc;
use std::{array, time};
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime};

use tokio::sync::{Mutex, mpsc::Sender};
use tokio::task::{JoinHandle, JoinSet};
use tonic::transport::Endpoint;
use tonic::{
    Request, Response, Status,
    transport::{Channel, Server, Uri},
};

pub mod proto {
    tonic::include_proto!("tokad");
}

use proto::tokad_client::TokadClient;
use proto::tokad_server::{Tokad, TokadServer};

// Number of nodes in one bucket
const K: usize = 4;

// Number of concurrent requests during a recursive lookup
const ALPHA: usize = 3;

const JIFFY: Duration = Duration::from_secs(1);
pub const EXPIRE: Duration = Duration::from_secs(240);
const REFRESH: Duration = Duration::from_secs(40);
const REPLICATE: Duration = Duration::from_secs(20);
pub const REPUBLISH: Duration = Duration::from_secs(110);

#[derive(Debug, Clone)]
pub struct Data {
    pub publish: bool,
    pub replication_time: SystemTime,
    pub publish_time: SystemTime,
    pub data: Arc<[u8]>,
}

impl Data {
    pub fn new(data: Vec<u8>, publish_time: SystemTime) -> Self {
        let now = SystemTime::now();
        Data {
            publish: false,
            replication_time: now,
            publish_time,
            data: data.into(),
        }
    }
    pub fn publish(data: Vec<u8>) -> Self {
        let now = SystemTime::now();
        Data {
            publish: true,
            replication_time: now,
            publish_time: now,
            data: data.into(),
        }
    }
    pub fn refresh(&mut self, publish_time: SystemTime) {
        self.replication_time = SystemTime::now();
        self.publish_time = max(publish_time, self.publish_time);
    }
}

pub use crate::data::{Key, Node, Nodes, Store, StoreOrNodes, StoreRequest, Stub};
use crate::error::TokadError;
use crate::hash::{ID_BITS, ID_MASK, key_hash};

#[derive(Debug)]
pub struct State {
    pub id: u128,
    pub port: u16,
    pub buckets: Mutex<[Vec<Node>; ID_BITS]>,
    refresh: Mutex<[SystemTime; ID_BITS]>,
    pub store: Mutex<HashMap<u128, Data>>,
    pub start_time: SystemTime,
    log: Option<Sender<String>>,
}

impl Default for State {
    fn default() -> State {
        let id = rand::random_range(1..=u128::MAX) & ID_MASK;
        let port = 50051;
        let now = SystemTime::now();
        let buckets: [Vec<Node>; ID_BITS] = array::from_fn(|_| Vec::with_capacity(K));
        let refresh: [SystemTime; ID_BITS] = array::from_fn(|_| now + REFRESH.mul_f64(rand::random()));
        let store = HashMap::<u128, Data>::default();
        let log = None;

        State {
            id,
            port,
            buckets: Mutex::new(buckets),
            refresh: Mutex::new(refresh),
            store: Mutex::new(store),
            start_time: now,
            log,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateRef {
    pub state: &'static State,
}

impl State {
    pub fn get_ref(&'static self) -> StateRef {
        StateRef { state: self }
    }

    pub fn stub(&self) -> Stub {
        Stub {
            id: self.id,
            port: self.port as u32,
        }
    }
}

impl Deref for StateRef {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        self.state
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "({} {} {})", self.id, self.ip, self.port)
    }
}

impl Node {
    pub fn sock(&self) -> Option<SocketAddr> {
        let ip = self.ip.parse().ok()?;
        let port = self.port.try_into().ok()?;
        Some(SocketAddr::new(ip, port))
    }

    pub fn from_sock(id: u128, sock: SocketAddr) -> Self {
        Node {
            id,
            ip: sock.ip().to_string(),
            port: sock.port() as u32,
        }
    }
    pub fn from_stub(stub: Stub, ip: impl Into<String>) -> Self {
        Node {
            id: stub.id,
            ip: ip.into(),
            port: stub.port,
        }
    }
    async fn connect(&self) -> Result<TokadClient<Channel>, Status> {
        let sock = self.sock().ok_or_else(|| {
            Status::invalid_argument("Invalid or unparseable IP/port in node metadata")
        })?;
        TokadClient::connect(
            Endpoint::from(
                Uri::builder()
                    .scheme("http")
                    .authority(sock.to_string())
                    .path_and_query("/")
                    .build()
                    .unwrap(),
            )
            .connect_timeout(Duration::from_secs(1))
            .timeout(Duration::from_secs(1)),
        )
        .await
        .map_err(|err| Status::from_error(Box::new(err)))
    }

    pub async fn raw_ping(&self, state: &State) -> Result<Stub, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(proto::Ping {
            source: Some(state.stub().rep()),
        });

        let res = con.ping(req).await.and_then(|p| {
            let inner = p.into_inner();
            Ok(Stub::try_from(&inner)?)
        });
        res
    }

    pub async fn ping(&self, state: StateRef) -> Result<Stub, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(proto::Ping {
            source: Some(state.stub().rep()),
        });

        let res = con.ping(req).await.and_then(|p| {
            let inner = p.into_inner();
            Ok(Stub::try_from(&inner)?)
        });
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn store(
        &self,
        state: StateRef,
        key: u128,
        value: &[u8],
        publish_time: SystemTime,
        publish: bool,
    ) -> Result<Stub, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(
            StoreRequest {
                store: Store {
                    key,
                    value: value.to_vec(),
                },
                publish_time,
                publish
            }
            .rep(state.stub()),
        );

        let res = con.store(req).await.and_then(|p| {
            let inner = p.into_inner();
            Ok(Stub::try_from(&inner)?)
        });
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn find_node(&self, state: StateRef, key: u128) -> Result<(Stub, Nodes), Status> {
        let mut con = self.connect().await?;
        let req = Request::new(Key { key }.rep(state.stub()));

        let res = con.find_node(req).await.and_then(|resp| {
            let inner = resp.into_inner();
            Ok((Stub::try_from(&inner)?, inner.unrep()?))
        });
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn find_value(&self, state: StateRef, key: u128) -> Result<(Stub, StoreOrNodes), Status> {
        let mut con = self.connect().await?;
        let req = Request::new(Key { key }.rep(state.stub()));

        let res = con.find_value(req).await.and_then(|resp| {
            let inner = resp.into_inner();
            Ok((Stub::try_from(&inner)?, inner.unrep()?))
        });
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }
}

impl State {
    pub async fn log(&self, into_msg: impl Into<String>) {
        let msg = into_msg.into();
        if let Some(log) = &self.log {
            if log.send(msg.clone()).await.is_err() {
                eprintln!("{}", msg);
            }
        } else {
            eprintln!("{}", msg);
        }
    }
    pub async fn log_buckets(&self) {
        {
            let buckets = self.buckets.lock().await;
            for (i, bt) in buckets.iter().enumerate() {
                if !bt.is_empty() {
                    self.log(format!(
                        "{}: {}",
                        i,
                        bt.iter()
                            .map(|n| n.to_string())
                            .collect::<Vec<_>>()
                            .join(" ")
                    ))
                    .await;
                }
            }
        }
    }

    pub async fn log_store(&self) {
        {
            let store = self.store.lock().await;
            for (k, v) in store.iter() {
                if let Ok(s) = str::from_utf8(&v.data) {
                    self.log(format!("{}: {}", k, s)).await;
                } else {
                    self.log(format!("{}: {:?}", k, v)).await;
                }
            }
        }
    }

    async fn nearest(&self, key: u128) -> Vec<Node> {
        let mut nodes: Vec<Node>;
        {
            let buckets = self.buckets.lock().await;
            nodes = buckets.iter().flatten().cloned().collect();
        }

        if nodes.len() <= K {
            return nodes;
        }
        let (slice, _, _) = nodes.select_nth_unstable_by_key(K, |n| n.id ^ key);
        slice.to_owned()
    }

    async fn retire(&self, node: Node) {
        if node.id == self.id || node.id == 0 {
            return;
        }
        let dist = node.id ^ self.id;
        let bid = (dist.leading_zeros() - ID_MASK.leading_zeros()) as usize;
        if bid >= ID_BITS {
            return;
        }
        {
            let mut buckets = self.buckets.lock().await;
            if let Some(i) = buckets[bid].iter().position(|n| n.id == node.id) {
                buckets[bid].remove(i);
            }
        }
    }

    async fn refresh(&self, node: Node) -> Result<(), Status> {
        if node.id == self.id || node.id == 0 {
            return Ok(());
        }
        //self.log(format!("{:?}", node));
        let dist = node.id ^ self.id;
        let bid = (dist.leading_zeros() - ID_MASK.leading_zeros()) as usize;
        let mut test: Option<Node> = None;
        {
            let mut buckets = self.buckets.lock().await;
            if !buckets[bid].iter().any(|n| n.id == node.id) && buckets[bid].len() == K {
                test = Some(buckets[bid][0].clone());
                buckets[bid].rotate_left(1);
            }
        }
        self.log(format!("{:?}", test)).await;

        let mut alive = false;
        if let Some(first) = &test {
            if first.raw_ping(self).await.is_ok() {
                alive = true;
            }
        }

        {
            let mut buckets = self.buckets.lock().await;
            if let Some(i) = buckets[bid].iter().position(|n| n.id == node.id) {
                buckets[bid].remove(i);
            }
            if let Some(first) = test && let Some(i) = buckets[bid].iter().position(|n| n.id == first.id) {
                buckets[bid].remove(i);
                if alive {
                    buckets[bid].push(first);
                }
            }
            if buckets[bid].len() < K {
                buckets[bid].push(node);
            }
        }
        //self.log_buckets().await;

        Ok(())
    }
}

pub type ServerResult = Result<(), tonic::transport::Error>;

impl StateRef {
    pub async fn lookup_node(&self, key: u128) -> Result<Vec<Node>, TokadError> {
        let mut queue: Vec<Node> = self.nearest(key).await;
        queue.sort_unstable_by_key(|n| n.id ^ key);
        let mut seen: Vec<Node> = queue.clone();
        seen.push(Node::from_stub(self.stub(), "::1"));
        let mut finished: Vec<Node> = vec![];
        finished.push(Node::from_stub(self.stub(), "::1"));

        let mut futs = JoinSet::new();
        while futs.len() < ALPHA && !queue.is_empty() {
            let node = queue.remove(0);
            let self_copy = *self;
            futs.spawn(async move { (node.clone(), node.find_node(self_copy, key).await) });
        }

        while !futs.is_empty() {
            let (node, reply) = futs.join_next().await.unwrap().unwrap();
            match reply {
                Ok((_, nodes)) => {
                    for new_node in nodes.nodes {
                        if !seen.iter().any(|n| n.id == new_node.id) {
                            seen.push(new_node.clone());
                            queue.insert(
                                queue.partition_point(|n| n.id ^ key < new_node.id ^ key),
                                new_node,
                            );
                        }
                    }
                    finished.insert(
                        finished.partition_point(|n| n.id ^ key < node.id ^ key),
                        node,
                    );
                }
                Err(_status) => {
                    tokio::spawn(self.state.retire(node));
                }
            }
            while futs.len() < ALPHA && !queue.is_empty() {
                let node = queue.remove(0);
                if finished.len() >= K && finished[K - 1].id ^ key < node.id ^ key {
                    break;
                }
                let self_copy = *self;
                futs.spawn(async move { (node.clone(), node.find_node(self_copy, key).await) });
            }
        }

        finished.truncate(K);
        Ok(finished)
    }

    pub async fn lookup_value(&self, key: u128) -> Result<StoreOrNodes, TokadError> {
        if let Some(value) = self.store.lock().await.get(&key) {
            return Ok(Store {
                key,
                value: value.data.to_vec(),
            }
            .or_nodes());
        }

        let mut queue: Vec<Node> = self.nearest(key).await;
        queue.sort_unstable_by_key(|n| n.id ^ key);
        let mut seen: Vec<Node> = queue.clone();
        seen.push(Node::from_stub(self.stub(), "::1"));
        let mut finished: Vec<Node> = vec![];
        finished.push(Node::from_stub(self.stub(), "::1"));

        let mut futs = JoinSet::new();
        while futs.len() < ALPHA && !queue.is_empty() {
            let node = queue.remove(0);
            let self_copy = *self;
            futs.spawn(async move { (node.clone(), node.find_value(self_copy, key).await) });
        }

        while !futs.is_empty() {
            let (node, reply) = futs.join_next().await.unwrap().unwrap();
            match reply {
                Ok((_, StoreOrNodes::Nodes(nodes))) => {
                    for new_node in nodes.nodes {
                        if !seen.iter().any(|n| n.id == new_node.id) {
                            seen.push(new_node.clone());
                            queue.insert(
                                queue.partition_point(|n| n.id ^ key < new_node.id ^ key),
                                new_node,
                            );
                        }
                    }
                    finished.insert(
                        finished.partition_point(|n| n.id ^ key < node.id ^ key),
                        node,
                    );
                }
                Ok((_, StoreOrNodes::Store(store))) => {
                    futs.shutdown().await;
                    return Ok(store.or_nodes());
                }
                Err(_status) => {
                    tokio::spawn(self.state.retire(node));
                }
            }
            while futs.len() < ALPHA && !queue.is_empty() {
                let node = queue.remove(0);
                if finished.len() >= K && finished[K - 1].id ^ key < node.id ^ key {
                    break;
                }
                let self_copy = *self;
                futs.spawn(async move { (node.clone(), node.find_value(self_copy, key).await) });
            }
        }

        finished.truncate(K);
        Ok(Nodes { nodes: finished }.or_store())
    }

    pub async fn lookup_and_store(
        &self,
        key: u128,
        value: &[u8],
        publish_time: SystemTime,
        publish: bool,
    ) -> Result<(), TokadError> {
        let close = self.lookup_node(key).await?;
        for node in close {
            if node.id == self.id {
                continue;
            }
            node.store(*self, key, value, publish_time, publish).await?;
        }

        Ok(())
    }

    pub async fn raw_publish(&self, key: u128, value: &[u8]) -> Result<(), TokadError> {
        self.store
            .lock()
            .await
            .insert(key, Data::publish(value.to_vec()));
        self.lookup_and_store(key, value, SystemTime::now(), true).await
    }

    pub async fn publish(&self, value: &[u8]) -> Result<u128, TokadError> {
        let key = key_hash(value);
        self.raw_publish(key, value).await?;
        Ok(key)
    }

    async fn refresh_bucket(&self, bid: usize) -> Result<(), TokadError> {
        if self.buckets.lock().await[bid].is_empty() {
            return Ok(());
        }
        let keep_mask = if bid == 0 {
            0
        } else {
            !0u128 << (ID_BITS - bid)
        };
        let flip_mask = 1 << (ID_BITS - 1 - bid);
        let first = (self.id & keep_mask) | (!self.id & flip_mask);
        let last = if bid == ID_BITS - 1 {
            first
        } else {
            first | (ID_MASK >> (bid + 1))
        };
        let key = rand::random_range(first..=last);
        self.lookup_node(key).await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Tokad for StateRef {
    async fn ping(&self, request: Request<proto::Ping>) -> Result<Response<proto::Pong>, Status> {
        let stub = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = stub;
        if let Some(loc) = request.remote_addr() {
            self.log(format!("Ping {} {} {}", id, loc.ip(), port)).await;
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            self.log(format!("Ping {} ? {}", id, port)).await;
        }

        Ok(Response::new(proto::Pong {
            source: Some(self.stub().rep()),
        })) // Send back our formatted greeting
    }
    async fn store(
        &self,
        request: Request<proto::StoreRequest>,
    ) -> Result<Response<proto::Pong>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            self.log(format!("Store: {} {} {}", id, loc.ip(), port))
                .await;
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            self.log(format!("Store: {} ? {}", id, port)).await;
        }

        let StoreRequest {
            store: Store { key, value },
            publish_time,
            ..
        } = request.into_inner().unrep()?;

        if let Ok(string_value) = String::from_utf8(value.clone()) {
            self.log(format!("{} -> {}", key, string_value)).await;
        } else {
            self.log(format!("{} -> {:?}", key, value)).await;
        }

        {
            let mut store = self.store.lock().await;
            if let Some(val) = store.get_mut(&key) {
                // TODO: old value check
                val.refresh(publish_time);
            } else {
                store.insert(key, Data::new(value, publish_time));
            }
        }

        Ok(Response::new(proto::Pong {
            source: Some(self.stub().rep()),
        }))
    }

    async fn find_node(
        &self,
        request: Request<proto::Key>,
    ) -> Result<Response<proto::Nodes>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            self.log(format!("Find node: {} {} {}", id, loc.ip(), port))
                .await;
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            self.log(format!("Find node: {} ? {}", id, port)).await;
        }

        let Key { key } = request.into_inner().unrep()?;

        self.log(format!("{}", key)).await;

        Ok(Response::new(
            Nodes {
                nodes: self.nearest(key).await,
            }
            .rep(self.stub()),
        ))
    }

    async fn find_value(
        &self,
        request: Request<proto::Key>,
    ) -> Result<Response<proto::StoreOrNodes>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            self.log(format!("Find value: {} {} {}", id, loc.ip(), port))
                .await;
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            self.log(format!("Find value: {} ? {}", id, port)).await;
        }

        let Key { key } = request.into_inner().unrep()?;

        self.log(format!("Find value {}", key)).await;

        {
            let store = self.store.lock().await;
            if store.contains_key(&key) {
                self.log("Value found!".to_string()).await;
                return Ok(Response::new(
                    Store {
                        key,
                        value: store[&key].data.to_vec(),
                    }
                    .or_nodes()
                    .rep(self.stub()),
                ));
            }
        }
        Ok(Response::new(
            Nodes {
                nodes: self.nearest(key).await,
            }
            .or_store()
            .rep(self.stub()),
        ))
    }
}

pub fn start_server(
    port: u16,
    log: Option<Sender<String>>,
    seed: Option<SocketAddr>,
) -> Result<(StateRef, JoinHandle<ServerResult>, JoinHandle<()>), TokadError> {
    let bind_ip = "::".parse()?;
    let bind_addr = SocketAddr::new(bind_ip, port);
    let state = Box::leak(Box::new(State::default()));
    state.log = log;
    state.port = port;

    let state_ref = state.get_ref();

    // let server4 = Server::builder()
    //     .add_service(TokadServer::new(state.clone()))
    //     .serve(addr4);

    let server_handle = tokio::spawn(
        Server::builder()
            .add_service(TokadServer::new(state_ref))
            .serve(bind_addr),
    );

    if let Some(seed) = seed {
        tokio::spawn(async move {
            match Node::from_sock(0, seed).ping(state_ref).await {
                Ok(source) => {
                    let seed_node = Node::from_stub(source, seed.ip().to_string());
                    if let Err(err) = state_ref.refresh(seed_node).await {
                        state_ref.log(format!("Seed refresh failed: {}", err)).await;
                        return;
                    };
                    if let Err(err) = state_ref.lookup_node(state_ref.id).await {
                        state_ref.log(format!("Seed lookup failed: {}", err)).await;
                        return;
                    };
                    // let mut last = 0;
                    // {
                    //     let buckets = state_ref.buckets.lock().await;
                    //     for i in 0..ID_BITS {
                    //         if !buckets[i].is_empty() {
                    //             last = i;
                    //         }
                    //     }
                    // }
                    // for i in 0..last {
                    //     if let Err(e) = state_ref.refresh_bucket(i).await {
                    //         state_ref.log(format!("init refresh error: {}", e)).await;
                    //     };
                    // }
                    // state_ref.log_buckets().await;
                }
                Err(e) => {
                    state_ref.log(format!("Seed ping error: {}", e)).await;
                }
            }
        });
    }

    let time_loop_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(JIFFY);
        state_ref.log(format!("Id: {}", state_ref.id)).await;
        loop {
            ticker.tick().await;

            // refresh
            {
                let mut to_refresh = vec![];
                let now = SystemTime::now();
                {
                    let mut refresh = state_ref.refresh.lock().await;
                    for i in 0..ID_BITS {
                        if refresh[i] + REFRESH < now {
                            refresh[i] = now;
                            to_refresh.push(i);
                        }
                    }
                }
                // if !to_refresh.is_empty() {
                //     state_ref
                //         .log(format!(
                //             "REFRESH: {}",
                //             to_refresh
                //                 .iter()
                //                 .map(|i| i.to_string())
                //                 .collect::<Vec<_>>()
                //                 .join(" ")
                //         ))
                //         .await;
                // }
                for i in to_refresh {
                    tokio::spawn(async move {
                        let _ = state_ref.refresh_bucket(i).await;
                    });
                }
            }

            // expire, replicate and republish
            {
                let now = SystemTime::now();
                let mut to_replicate: Vec<(u128, Data)> = vec![];
                let mut to_republish: Vec<(u128, Data)> = vec![];
                {
                    let mut to_expire: Vec<u128> = vec![];
                    let mut store = state_ref.store.lock().await;
                    for (&key, entry) in store.iter_mut() {
                        if entry.publish && entry.publish_time + REPUBLISH <= now {
                            entry.publish_time = now;
                            entry.replication_time = now;
                            to_republish.push((key, entry.clone()));
                        } else if !entry.publish && entry.publish_time + EXPIRE < now {
                            to_expire.push(key);
                        } else if entry.replication_time + REPLICATE < now {
                            entry.replication_time = now;
                            to_replicate.push((key, entry.clone()));
                        }
                    }
                    for key in to_expire {
                        store.remove(&key);
                    }
                }
                for (key, data) in to_republish {
                    tokio::spawn(async move {
                        state_ref.lookup_and_store(key, &data.data, data.publish_time, true).await
                    });
                }
                for (key, data) in to_replicate {
                    tokio::spawn(async move {
                        state_ref.lookup_and_store(key, &data.data, data.publish_time, false).await
                    });
                }
            }
        }
    });

    // let (r4, r6) = tokio::join!(server4, server6);
    // let _ = r4?;
    // let _ = r6?;

    Ok((state_ref, server_handle, time_loop_handle))
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn it_works() {
//
//     }
// }
