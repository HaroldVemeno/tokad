use std::array;
use std::cmp::{max, min};
use std::collections::HashMap;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use std::sync::Mutex;
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
use tracing::{debug, error, info, instrument};

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
}

impl Default for State {
    fn default() -> State {
        let id = rand::random_range(1..=u128::MAX) & ID_MASK;
        let port = 50051;
        let now = SystemTime::now();
        let buckets: [Vec<Node>; ID_BITS] = array::from_fn(|_| Vec::with_capacity(K));
        let refresh: [SystemTime; ID_BITS] =
            array::from_fn(|_| now + REFRESH.mul_f64(rand::random()));
        let store = HashMap::<u128, Data>::default();

        State {
            id,
            port,
            buckets: Mutex::new(buckets),
            refresh: Mutex::new(refresh),
            store: Mutex::new(store),
            start_time: now,
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

        let res = con.ping(req).await?;
        Ok(Stub::try_from(&res.into_inner())?)
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
        let self_clone = self.clone();
        if res.is_ok() {
            tokio::spawn(async move {
                if let Err(e) = state.state.refresh(self_clone).await {
                    error!("refresh error: {:?}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = state.state.retire(self_clone).await {
                    error!("retire error: {:?}", e);
                }
            });
        }
        res
    }

    async fn store(
        &self,
        state: StateRef,
        store_req: StoreRequest
    ) -> Result<Stub, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(store_req.rep(state.stub()),
        );

        let res = con.store(req).await.and_then(|p| {
            let inner = p.into_inner();
            Ok(Stub::try_from(&inner)?)
        });
        let self_clone = self.clone();
        if res.is_ok() {
            tokio::spawn(async move {
                if let Err(e) = state.state.refresh(self_clone).await {
                    error!("refresh error: {:?}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = state.state.retire(self_clone).await {
                    error!("retire error: {:?}", e);
                }
            });
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
        let self_clone = self.clone();
        if res.is_ok() {
            tokio::spawn(async move {
                if let Err(e) = state.state.refresh(self_clone).await {
                    error!("refresh error: {:?}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = state.state.retire(self_clone).await {
                    error!("retire error: {:?}", e);
                }
            });
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
        let self_clone = self.clone();
        if res.is_ok() {
            tokio::spawn(async move {
                if let Err(e) = state.state.refresh(self_clone).await {
                    error!("refresh error: {:?}", e);
                }
            });
        } else {
            tokio::spawn(async move {
                if let Err(e) = state.state.retire(self_clone).await {
                    error!("retire error: {:?}", e);
                }
            });
        }

        res
    }
}

impl State {
    async fn nearest(&self, key: u128) -> Result<Vec<Node>, TokadError> {
        let mut nodes: Vec<Node>;
        {
            let buckets = self.buckets.lock()?;
            let total_nodes: usize = buckets.iter().map(|b| b.len()).sum();
            nodes = Vec::with_capacity(total_nodes);
            for b in buckets.iter() {
                nodes.extend(b.iter().cloned());
            }
        }

        if nodes.len() <= K {
            return Ok(nodes);
        }
        let (slice, _, _) = nodes.select_nth_unstable_by_key(K, |n| n.id ^ key);
        Ok(slice.to_owned())
    }

    async fn retire(&self, node: Node) -> Result<(), TokadError> {
        if node.id == self.id || node.id == 0 {
            return Ok(());
        }
        let dist = node.id ^ self.id;
        let bid = (dist.leading_zeros() - ID_MASK.leading_zeros()) as usize;
        if bid >= ID_BITS {
            return Ok(());
        }
        {
            let mut buckets = self.buckets.lock()?;
            if let Some(i) = buckets[bid].iter().position(|n| n.id == node.id) {
                buckets[bid].remove(i);
            }
        }
        Ok(())
    }

    #[instrument(level = "debug", skip_all)]
    async fn refresh(&self, node: Node) -> Result<(), Status> {
        debug!("{}", node);
        if node.id == self.id || node.id == 0 {
            return Ok(());
        }
        let dist = node.id ^ self.id;
        let bid = (dist.leading_zeros() - ID_MASK.leading_zeros()) as usize;
        let test;
        {
            let mut buckets = self.buckets.lock().map_err(|_| TokadError::StorageLockPoisoned)?;

            if let Some(i) = buckets[bid].iter().position(|n| n.id == node.id) {
                let n = buckets[bid].remove(i);
                buckets[bid].push(n);
                return Ok(());
            }

            if buckets[bid].len() < K {
                buckets[bid].push(node);
                return Ok(());
            }

            test = buckets[bid][0].clone();
            buckets[bid].rotate_left(1);
        }

        let mut alive = false;
        if test.raw_ping(self).await.is_ok() {
            alive = true;
        }

        {
            let mut buckets = self.buckets.lock().map_err(|_| TokadError::StorageLockPoisoned)?;
            if let Some(i) = buckets[bid].iter().position(|n| n.id == test.id) {
                buckets[bid].remove(i);
                if alive {
                    buckets[bid].push(test);
                } else {
                    buckets[bid].push(node);
                }
            } else if buckets[bid].len() < K {
                buckets[bid].push(node);
            }
        }

        Ok(())
    }
}

pub type ServerResult = Result<(), tonic::transport::Error>;

impl StateRef {
    #[instrument(level = "debug", skip(self))]
    pub async fn lookup_node(&self, key: u128) -> Result<Vec<Node>, TokadError> {
        let mut queue: Vec<Node> = self.nearest(key).await?;
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
                    // debug!(...)
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

    #[instrument(level = "debug", skip(self))]
    pub async fn lookup_value(&self, key: u128) -> Result<StoreOrNodes, TokadError> {
        if let Some(value) = self.store.lock()?.get(&key) {
            return Ok(Store {
                key,
                value: value.data.to_vec(),
            }
            .or_nodes());
        }

        let mut queue: Vec<Node> = self.nearest(key).await?;
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
                    //debug!(...)
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
            node.store(*self, StoreRequest{
                store: Store{
                    key,
                    value: value.to_vec()
                },
                publish_time,
                publish
            }).await?;
        }

        Ok(())
    }

    pub async fn raw_publish(&self, key: u128, value: &[u8]) -> Result<(), TokadError> {
        self.store
            .lock()?
            .insert(key, Data::publish(value.to_vec()));
        self.lookup_and_store(key, value, SystemTime::now(), true)
            .await
    }

    #[instrument(level = "info", skip(self))]
    pub async fn publish(&self, value: &[u8]) -> Result<u128, TokadError> {
        let key = key_hash(value);
        self.raw_publish(key, value).await?;
        Ok(key)
    }

    async fn refresh_bucket(&self, bid: usize) -> Result<(), TokadError> {
        if self.buckets.lock()?[bid].is_empty() {
            return Ok(());
        }
        debug!("bucket {} refresh", bid);
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
    #[instrument(level = "info", skip_all)]
    async fn ping(&self, request: Request<proto::Ping>) -> Result<Response<proto::Pong>, Status> {
        let stub = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = stub;
        if let Some(loc) = request.remote_addr() {
            info!("source: {} {} {}", id, loc.ip(), port);
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            info!("source: {} ? {}", id, port);
        }

        Ok(Response::new(proto::Pong {
            source: Some(self.stub().rep()),
        })) // Send back our formatted greeting
    }

    #[instrument(level = "info", skip_all)]
    async fn store(
        &self,
        request: Request<proto::StoreRequest>,
    ) -> Result<Response<proto::Pong>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            info!("source: {} {} {}", id, loc.ip(), port);
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            info!("source: {} ? {}", id, port);
        }

        let StoreRequest {
            store: Store { key, value },
            publish_time,
            ..
        } = request.into_inner().unrep()?;

        let head_count = min(40, value.len());
        let head = if let Ok(string_value) = String::from_utf8(value[..head_count].to_vec()) {
            if head_count == 40 {
                string_value + "..."
            } else {
                string_value
            }
        } else {
            "[unprintable]".to_owned()
        };
        info!("{}: {}", key, head);

        {
            let mut store = self.store.lock().map_err(|_| TokadError::StorageLockPoisoned)?;
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

    #[instrument(level = "info", skip_all)]
    async fn find_node(
        &self,
        request: Request<proto::Key>,
    ) -> Result<Response<proto::Nodes>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            info!("source: {} {} {}", id, loc.ip(), port);
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            info!("source: {} ? {}", id, port);
        }

        let Key { key } = request.into_inner().unrep()?;

        info!("target: {}", key);

        Ok(Response::new(
            Nodes {
                nodes: self.nearest(key).await?,
            }
            .rep(self.stub()),
        ))
    }

    #[instrument(level = "info", skip_all)]
    async fn find_value(
        &self,
        request: Request<proto::Key>,
    ) -> Result<Response<proto::StoreOrNodes>, Status> {
        let source = Stub::try_from(request.get_ref())?;
        let Stub { id, port } = source;
        if let Some(loc) = request.remote_addr() {
            info!("source: {} {} {}", id, loc.ip(), port);
            let node = Node {
                id,
                ip: loc.ip().to_string(),
                port,
            };
            tokio::spawn(self.state.refresh(node));
        } else {
            info!("source: {} ? {}", id, port);
        }

        let Key { key } = request.into_inner().unrep()?;

        info!("target: {}", key);

        {
            let store = self.store.lock().map_err(|_| TokadError::StorageLockPoisoned)?;
            if store.contains_key(&key) {
                debug!("value found!");
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
                nodes: self.nearest(key).await?,
            }
            .or_store()
            .rep(self.stub()),
        ))
    }
}

pub fn start_server(
    port: u16,
    seed: Option<SocketAddr>,
) -> Result<(StateRef, JoinHandle<ServerResult>, JoinHandle<()>), TokadError> {
    let bind_ip = "::".parse()?;
    let bind_addr = SocketAddr::new(bind_ip, port);
    let state = Box::leak(Box::new(State::default()));
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
                        error!("Seed refresh failed: {}", err);
                    } else if let Err(err) = state_ref.lookup_node(state_ref.id).await {
                        error!("Seed lookup failed: {}", err);
                    };
                    // let mut last = 0;
                    // {
                    //     let buckets = state_ref.buckets.lock()?;
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
                Err(err) => {
                    error!("Seed ping error: {}", err);
                }
            }
        });
    }

    let time_loop_handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(JIFFY);
        info!("id: {}", state_ref.id);
        loop {
            ticker.tick().await;

            // refresh
            {
                let mut to_refresh = vec![];
                let now = SystemTime::now();
                {
                    let mut refresh = state_ref.refresh.lock().expect("refresh lock poisoned");
                    for i in 0..ID_BITS {
                        if refresh[i] + REFRESH < now {
                            refresh[i] = now;
                            to_refresh.push(i);
                        }
                    }
                }
                // if !to_refresh.is_empty() {
                //         debug!(
                //             "bucket refresh: {}",
                //             to_refresh
                //                 .iter()
                //                 .map(|i| i.to_string())
                //                 .collect::<Vec<_>>()
                //                 .join(" ")
                //         );
                // }
                for i in to_refresh {
                    tokio::spawn(async move {
                        if let Err(err) = state_ref.refresh_bucket(i).await {
                            error!("bucket refresh error: {}", err)
                        }
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
                    let mut store = state_ref.store.lock().expect("store lock poisoned");
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
                if !to_republish.is_empty() {
                        debug!(
                            "republishing: {}",
                            to_republish
                                .iter()
                                .map(|(k, _)| k.to_string())
                                .collect::<Vec<_>>()
                                .join(" ")
                        );
                }
                for (key, data) in to_republish {
                    tokio::spawn(async move {
                        if let Err(err) = state_ref
                            .lookup_and_store(key, &data.data, data.publish_time, true)
                            .await {
                            error!("Republish error: {}", err)
                        }
                    });
                }
                for (key, data) in to_replicate {
                    tokio::spawn(async move {
                        if let Err(err) = state_ref
                            .lookup_and_store(key, &data.data, data.publish_time, false)
                            .await {
                            error!("Replicate error: {}", err)
                        }
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
