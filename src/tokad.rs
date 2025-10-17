use std::array;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::interval;
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
pub use proto::{Node, Stub};

const K: usize = 4;
const ALPHA: usize = 3;

const JIFFY: Duration = Duration::from_secs(1);
const EXPIRE: Duration = Duration::from_secs(240);
const REFRESH: Duration = Duration::from_secs(30);
const REPLICATE: Duration = Duration::from_secs(10);
const REPUBLISH: Duration = Duration::from_secs(120);

#[derive(Debug, Clone)]
pub struct Key {
    key: u32,
}

#[derive(Debug, Clone)]
pub struct Store {
    key: u32,
    value: Vec<u8>,
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

impl Nodes {
    fn or_store(self) -> StoreOrNodes {
        StoreOrNodes::Nodes(self)
    }
    fn rep(self, stub: Option<Stub>) -> proto::Nodes {
        proto::Nodes {
            source: stub,
            nodes: self.nodes,
        }
    }
}

impl proto::Nodes {
    fn unrep(self) -> (Option<Stub>, Nodes) {
        (self.source, Nodes { nodes: self.nodes })
    }
}

impl Store {
    fn or_nodes(self) -> StoreOrNodes {
        StoreOrNodes::Store(self)
    }
    fn rep(self, stub: Option<Stub>) -> proto::Store {
        proto::Store {
            source: stub,
            key: self.key,
            value: self.value,
        }
    }
}

impl proto::Store {
    fn req(self, publish: bool) -> proto::StoreRequest {
        proto::StoreRequest {
            source: self.source,
            key: self.key,
            value: self.value,
            publish,
        }
    }
    fn unrep(self) -> (Option<Stub>, Store) {
        (
            self.source,
            Store {
                key: self.key,
                value: self.value,
            },
        )
    }
}

impl proto::StoreRequest {
    fn unrep(self) -> (Option<Stub>, Store) {
        (
            self.source,
            Store {
                key: self.key,
                value: self.value,
            },
        )
    }
}

impl Key {
    fn rep(self, stub: Option<Stub>) -> proto::Key {
        proto::Key {
            source: stub,
            key: self.key,
        }
    }
}

/*
impl proto::Key {
    fn unrep(self) -> (Option<Stub>, Key) {
        (self.source, Key{key: self.key})
    }
}
*/

impl StoreOrNodes {
    fn rep(self, stub: Option<Stub>) -> proto::StoreOrNodes {
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
    fn unrep(self) -> Option<(Option<Stub>, StoreOrNodes)> {
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

#[derive(Debug, Clone)]
enum DataInfo {
    Publish(Instant), // when to republish
    Expires(Instant), // when to expire
}

#[derive(Debug, Clone)]
pub struct Data {
    info: DataInfo,
    data: Vec<u8>,
}

impl Data {
    pub fn new(data: Vec<u8>) -> Self {
        Data {
            info: DataInfo::Expires(Instant::now() + EXPIRE),
            data,
        }
    }
    pub fn published(data: Vec<u8>) -> Self {
        Data {
            info: DataInfo::Publish(Instant::now() + REPUBLISH),
            data,
        }
    }
    fn refresh(&mut self) {
        if let DataInfo::Expires(ref mut inst) = self.info {
            *inst = Instant::now() + EXPIRE
        }
    }
}

#[derive(Debug)]
pub struct State {
    pub id: u32,
    pub port: u16,
    pub buckets: Mutex<[Vec<Node>; 32]>,
    refresh: Mutex<[Instant; 32]>,
    pub store: Mutex<HashMap<u32, Data>>,
    creation_time: Instant,
    last_replication: AtomicU64,
    console: Option<Sender<String>>,
}

impl Default for State {
    fn default() -> State {
        let id = rand::random_range(1..=u32::MAX);
        let port = 50051;
        let now = Instant::now();
        let buckets: [Vec<Node>; 32] = array::from_fn(|_| Vec::with_capacity(K));
        let refresh: [Instant; 32] = array::from_fn(|_| now + REFRESH.mul_f64(rand::random()));
        let store = HashMap::<u32, Data>::default();
        let console = None;

        State {
            id,
            port,
            buckets: Mutex::new(buckets),
            refresh: Mutex::new(refresh),
            store: Mutex::new(store),
            creation_time: now,
            last_replication: AtomicU64::new(
                REPLICATE
                    .mul_f64(rand::random())
                    .as_millis()
                    .try_into()
                    .unwrap(),
            ),
            console,
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

impl Display for Store {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        if let Ok(s) = str::from_utf8(&self.value) {
            write!(f, "{}: {}", self.key, s)?;
        } else {
            write!(f, "{}: {:?}", self.key, self.value)?;
        }
        Ok(())
    }
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "({} {} {})", self.id, self.ip, self.port)
    }
}

impl Node {
    pub fn sock(&self) -> SocketAddr {
        SocketAddr::new(self.ip.parse().unwrap(), self.port.try_into().unwrap())
    }

    pub fn from_sock(id: u32, sock: SocketAddr) -> Self {
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
        TokadClient::connect(
            Endpoint::from(
                Uri::builder()
                    .scheme("http")
                    .authority(self.sock().to_string())
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

    pub async fn ping(&self, state: StateRef) -> Result<Option<Stub>, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(proto::Ping {
            source: Some(state.stub()),
        });

        let res = con.ping(req).await.map(|p| p.into_inner().source);
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn store(
        &self,
        state: StateRef,
        key: u32,
        value: &[u8],
        publish: bool,
    ) -> Result<Option<Stub>, Status> {
        let mut con = self.connect().await?;
        let req = Request::new(
            Store {
                key,
                value: value.to_vec(),
            }
            .rep(Some(state.stub()))
            .req(publish),
        );

        let res = con.store(req).await.map(|p| p.into_inner().source);
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn find_node(&self, state: StateRef, key: u32) -> Result<(Option<Stub>, Nodes), Status> {
        let mut con = self.connect().await?;
        let req = Request::new(Key { key }.rep(Some(state.stub())));

        let res = con
            .find_node(req)
            .await
            .map(|resp| resp.into_inner().unrep());
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }

    async fn find_value(
        &self,
        state: StateRef,
        key: u32,
    ) -> Result<(Option<Stub>, StoreOrNodes), Status> {
        let mut con = self.connect().await?;
        let req = Request::new(Key { key }.rep(Some(state.stub())));

        let res = con.find_value(req).await.and_then(|resp| {
            resp.into_inner()
                .unrep()
                .ok_or(Status::invalid_argument("No reply content"))
        });
        if res.is_ok() {
            tokio::spawn(state.state.refresh(self.clone()));
        }
        res
    }
}

impl State {
    pub fn log(&self, into_msg: impl Into<String>) {
        let msg = into_msg.into();
        if let Some(console) = &self.console {
            if console.send(msg.clone()).is_err() {
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
                    ));
                }
            }
        }
    }

    pub async fn log_store(&self) {
        {
            let store = self.store.lock().await;
            for (k, v) in store.iter() {
                if let Ok(s) = str::from_utf8(&v.data) {
                    self.log(format!("{}: {}", k, s));
                } else {
                    self.log(format!("{}: {:?}", k, v));
                }
            }
        }
    }

    async fn nearest(&self, key: u32) -> Vec<Node> {
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
        let dist = node.id ^ self.id;
        let bid = dist.leading_zeros() as usize;
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
        let bid = dist.leading_zeros() as usize;
        let mut test = false;
        let mut first = Node::default();
        {
            let buckets = self.buckets.lock().await;
            if !buckets[bid].iter().any(|n| n.id == node.id) && buckets[bid].len() == K {
                test = true;
                first = buckets[bid][0].clone();
            }
        }
        //self.log(format!("{:?}", seen));

        let mut alive = false;
        if test {
            let mut con = first.connect().await?;
            let req = Request::new(proto::Ping {
                source: Some(self.stub()),
            });

            if con.ping(req).await.is_ok() {
                alive = true;
            }
        }

        {
            let mut buckets = self.buckets.lock().await;
            if let Some(i) = buckets[bid].iter().position(|n| n.id == node.id) {
                buckets[bid].remove(i);
            }
            if test && let Some(i) = buckets[bid].iter().position(|n| n.id == first.id) {
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
    pub async fn lookup_node(&self, key: u32) -> Result<Vec<Node>, Box<dyn Error>> {
        let mut queue: Vec<Node> = self.nearest(key).await;
        queue.sort_unstable_by_key(|n| n.id ^ key);
        let mut seen: Vec<Node> = queue.clone();
        seen.push(Node::from_stub(self.stub(), "::1"));
        let mut finished: Vec<Node> = vec![];
        finished.push(Node::from_stub(self.stub(), "::1"));

        let mut futs = JoinSet::new();
        while futs.len() < ALPHA && !queue.is_empty() {
            let node = queue.remove(0);
            let selfc = *self;
            futs.spawn(async move { (node.clone(), node.find_node(selfc, key).await) });
        }

        while !futs.is_empty() {
            let (node, reply) = futs.join_next().await.unwrap()?;
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
                    continue;
                }
                let selfc = *self;
                futs.spawn(async move { (node.clone(), node.find_node(selfc, key).await) });
            }
        }

        //TODO: stop early sometimes?

        finished.truncate(K);
        Ok(finished)
    }

    pub async fn lookup_value(&self, key: u32) -> Result<StoreOrNodes, Box<dyn Error>> {
        if let Some(value) = self.store.lock().await.get(&key) {
            return Ok(Store {
                key,
                value: value.data.clone(),
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
            let selfc = *self;
            futs.spawn(async move { (node.clone(), node.find_value(selfc, key).await) });
        }

        while !futs.is_empty() {
            let (node, reply) = futs.join_next().await.unwrap()?;
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
                    continue;
                }
                let selfc = *self;
                futs.spawn(async move { (node.clone(), node.find_value(selfc, key).await) });
            }
        }

        //TODO: stop early sometimes?

        finished.truncate(K);
        Ok(Nodes { nodes: finished }.or_store())
    }

    pub async fn lookup_and_store(
        &self,
        key: u32,
        value: &[u8],
        publish: bool,
    ) -> Result<(), Box<dyn Error>> {
        let close = self.lookup_node(key).await?;
        for node in close {
            node.store(*self, key, value, publish).await?;
        }

        Ok(())
    }

    pub async fn publish(&self, key: u32, value: &[u8]) -> Result<(), Box<dyn Error>> {
        self.store
            .lock()
            .await
            .insert(key, Data::published(value.to_vec()));
        self.lookup_and_store(key, value, true).await
    }

    async fn refresh_buckets(&self, bid: usize) -> Result<(), Box<dyn Error>> {
        if self.buckets.lock().await[bid].is_empty() {
            return Ok(());
        }
        let keep_mask = if bid == 0 { 0 } else { !0u32 << (32 - bid) };
        let flip_mask = 1 << (31 - bid);
        let first = (self.id & keep_mask) | (!self.id & flip_mask);
        let last = if bid == 31 {
            first
        } else {
            first | (!0u32 >> (bid + 1))
        };
        let key = rand::random_range(first..=last);
        self.lookup_node(key).await?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Tokad for StateRef {
    async fn ping(
        &self,
        request: Request<proto::Ping>, // Accept request of type HelloRequest
    ) -> Result<Response<proto::Pong>, Status> {
        // Return an instance of type HelloReply
        //self.log(format!("Ping: {:?}", request));

        if let Some(Stub { id, port }) = request.get_ref().source {
            if let Some(loc) = request.remote_addr() {
                self.log(format!("Ping: {} {} {}", id, loc.ip(), port));
                let node = Node {
                    id,
                    ip: loc.ip().to_string(),
                    port,
                };
                tokio::spawn(self.state.refresh(node));
            } else {
                self.log(format!("Ping: {} no source addr???", id));
            }
        } else {
            self.log("Ping");
        }

        Ok(Response::new(proto::Pong {
            source: Some(self.stub()),
        })) // Send back our formatted greeting
    }
    async fn store(
        &self,
        request: Request<proto::StoreRequest>, // Accept request of type HelloRequest
    ) -> Result<Response<proto::Pong>, Status> {
        // Return an instance of type HelloReply
        //self.log(format!("Request: {:?}", request));

        if let Some(Stub { id, port }) = request.get_ref().source {
            if let Some(loc) = request.remote_addr() {
                self.log(format!("Source: {} {} {}", id, loc.ip(), port));
                let node = Node {
                    id,
                    ip: loc.ip().to_string(),
                    port,
                };
                tokio::spawn(self.state.refresh(node));
            } else {
                self.log(format!("Source: {} no source addr???", id));
            }
        }

        let publish = request.get_ref().publish;
        let Store { key, value } = request.into_inner().unrep().1;

        if let Ok(string_value) = String::from_utf8(value.clone()) {
            self.log(format!("Store {}: {}", key, string_value));
        } else {
            self.log(format!("Store {}: {:?}", key, value));
        }

        {
            let mut store = self.store.lock().await;
            if let Some(val) = store.get_mut(&key) {
                // TODO: old value check
                if publish {
                    val.refresh();
                }
            } else {
                store.insert(key, Data::new(value));
            }
        }

        Ok(Response::new(proto::Pong {
            source: Some(self.stub()),
        }))
    }

    async fn find_node(
        &self,
        request: Request<proto::Key>, // Accept request of type HelloRequest
    ) -> Result<Response<proto::Nodes>, Status> {
        // Return an instance of type HelloReply
        //self.log(format!("Request: {:?}", request));

        if let Some(Stub { id, port }) = request.get_ref().source {
            if let Some(loc) = request.remote_addr() {
                self.log(format!("Source: {} {} {}", id, loc.ip(), port));
                let node = Node {
                    id,
                    ip: loc.ip().to_string(),
                    port,
                };
                tokio::spawn(self.state.refresh(node));
            } else {
                self.log(format!("Source: {} no source addr???", id));
            }
        }

        let key = request.get_ref().key;

        self.log(format!("Find node {}", key));

        Ok(Response::new(
            Nodes {
                nodes: self.nearest(key).await,
            }
            .rep(Some(self.stub())),
        ))
    }

    async fn find_value(
        &self,
        request: Request<proto::Key>,
    ) -> Result<Response<proto::StoreOrNodes>, Status> {
        //self.log(format!("Request: {:?}", request));

        if let Some(Stub { id, port }) = request.get_ref().source {
            if let Some(loc) = request.remote_addr() {
                self.log(format!("Source: {} {} {}", id, loc.ip(), port));
                let node = Node {
                    id,
                    ip: loc.ip().to_string(),
                    port,
                };
                tokio::spawn(self.state.refresh(node));
            } else {
                self.log(format!("Source: {} no source addr???", id));
            }
        }

        let key = request.get_ref().key;

        self.log(format!("Find value {}", key));

        let key = request.get_ref().key;
        {
            let store = self.store.lock().await;
            if store.contains_key(&key) {
                self.log("Value found!".to_string());
                return Ok(Response::new(
                    Store {
                        key,
                        value: store[&key].data.clone(),
                    }
                    .or_nodes()
                    .rep(Some(self.stub())),
                ));
            }
        }
        Ok(Response::new(
            Nodes {
                nodes: self.nearest(key).await,
            }
            .or_store()
            .rep(Some(self.stub())),
        ))
    }
}

pub fn start_server(
    port: u16,
    console: Option<Sender<String>>,
    seed: Option<SocketAddr>,
) -> Result<
    (
        StateRef,
        JoinHandle<ServerResult>,
        JoinHandle<Box<dyn Error + Send>>,
    ),
    Box<dyn Error>,
> {
    let bind_ip = "::".parse()?;
    let bind_addr = SocketAddr::new(bind_ip, port);
    let state = Box::leak(Box::new(State::default()));
    state.console = console;
    state.port = port;
    state.log(format!("id: {}", state.id));

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
                Ok(Some(source)) => {
                    let seed_node = Node::from_stub(source, seed.ip().to_string());
                    let Ok(_) = state_ref.refresh(seed_node).await else {
                        state_ref.log("init refresh failed");
                        return;
                    };
                    let Ok(_) = state_ref.lookup_node(state_ref.id).await else {
                        state_ref.log("init lookup failed");
                        return;
                    };
                    // let mut last = 0;
                    // {
                    //     let buckets = state_ref.buckets.lock().await;
                    //     for i in 0..32 {
                    //         if !buckets[i].is_empty() {
                    //             last = i;
                    //         }
                    //     }
                    // }
                    // for i in 0..last {
                    //     if let Err(e) = state_ref.refresh_buckets(i).await {
                    //         state_ref.log(format!("init refresh error: {}", e));
                    //     };
                    // }
                    state_ref.log_buckets().await;
                }
                Ok(None) => {
                    state_ref.log("init sus, empty reply");
                }
                Err(e) => {
                    state_ref.log(format!("init error: {}", e));
                }
            }
        });
    }

    let time_loop_handle = tokio::spawn(async move {
        let mut ticker = interval(JIFFY);
        loop {
            ticker.tick().await;

            // refresh
            {
                for i in 0..32 {
                    let now = Instant::now();
                    let mut should_refresh = false;
                    {
                        let mut refresh = state_ref.refresh.lock().await;
                        if refresh[i] + REFRESH < now {
                            refresh[i] = now;
                            should_refresh = true;
                        }
                    }
                    if should_refresh {
                        let _ = state_ref.refresh_buckets(i).await;
                        state_ref.log(format!("REFRESH {}", i));
                    }
                }
            }

            // replicate and republish
            {
                let now = Instant::now();
                let elapsed = now - state_ref.creation_time;
                let to_refr =
                    Duration::from_millis(state_ref.last_replication.load(Ordering::Acquire))
                        + REPLICATE;
                let replicate = elapsed > to_refr;
                if replicate {
                    state_ref.log("REPLICATE");
                }
                let mut expired: Vec<u32> = vec![];
                let keys = state_ref
                    .store
                    .lock()
                    .await
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>();
                for k in keys {
                    let mut expire = false;
                    let mut publish = false;
                    let data: Vec<u8>;
                    {
                        let mut store = state_ref.store.lock().await;
                        let v = store.get_mut(&k).unwrap();
                        data = v.data.clone();
                        let now = Instant::now();
                        match v.info {
                            DataInfo::Publish(ref mut when) => {
                                if *when < now {
                                    *when = now + REPUBLISH;
                                    publish = true;
                                }
                            }
                            DataInfo::Expires(when) => {
                                if when < now {
                                    expire = true;
                                }
                            }
                        }
                    }
                    if expire {
                        expired.push(k);
                    } else if replicate || publish {
                        let _ = state_ref.lookup_and_store(k, &data, publish).await;
                    }
                    if publish {
                        state_ref.log("REPUBLISH");
                    }
                }
                if replicate {
                    state_ref
                        .last_replication
                        .store(elapsed.as_millis().try_into().unwrap(), Ordering::Release);
                }
                {
                    let mut store = state_ref.store.lock().await;
                    for k in expired {
                        store.remove(&k);
                    }
                }
            }
        }
    });

    // let (r4, r6) = tokio::join!(server4, server6);
    // let _ = r4?;
    // let _ = r6?;

    Ok((state_ref, server_handle, time_loop_handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        ()
    }
}
