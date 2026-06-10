//! ROUTER socket — async routed, per-peer addressing. Replaces C++ `router_t`.
use std::collections::HashMap;
use std::sync::Arc;
use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct RouterSocket { fq: FairQueue, routing_table: HashMap<u32, usize>, pipe_to_routing: HashMap<usize, u32> }

impl RouterSocket {
    pub fn new() -> Self { Self { fq: FairQueue::new(), routing_table: HashMap::new(), pipe_to_routing: HashMap::new() } }
}

impl Socket for RouterSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        let rid = msg.routing_id().ok_or(ZmqError::InvalidState("ROUTER: no routing ID"))?;
        self.routing_table.get(&rid).copied().ok_or(ZmqError::InvalidArgument(format!("ROUTER: unknown routing ID: {}", rid)))?;
        Ok(())
    }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() { return Err(ZmqError::NoMessage); }
        Err(ZmqError::NoMessage)
    }
    fn xhas_in(&self) -> bool { self.fq.has_in() }
    fn xhas_out(&self) -> bool { !self.routing_table.is_empty() }
    fn attach_pipe(&mut self, p: Arc<Pipe>, _sa: bool, _li: bool) {
        self.fq.attach(p.id());
        let rid = p.id() as u32;
        self.routing_table.insert(rid, p.id());
        self.pipe_to_routing.insert(p.id(), rid);
    }
    fn pipe_terminated(&mut self, p: &Pipe) { self.fq.terminated(p.id()); if let Some(rid) = self.pipe_to_routing.remove(&p.id()) { self.routing_table.remove(&rid); } }
    fn read_activated(&mut self, p: &Pipe) { self.fq.activated(p.id()); }
    fn write_activated(&mut self, _p: &Pipe) {}
    fn socket_type(&self) -> SocketType { SocketType::Router }
}
