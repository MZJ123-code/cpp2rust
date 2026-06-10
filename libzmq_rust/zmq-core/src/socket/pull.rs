//! PULL socket — fair-queue fan-in. Replaces C++ `pull_t`.
use std::sync::Arc;
use crate::data_structures::fair_queue::FairQueue;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct PullSocket { fq: FairQueue }

impl PullSocket { pub fn new() -> Self { Self { fq: FairQueue::new() } } }

impl Socket for PullSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> { Err(ZmqError::NotSupported("PULL")) }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() { return Err(ZmqError::NoMessage); }
        Err(ZmqError::NoMessage)
    }
    fn xhas_in(&self) -> bool { self.fq.has_in() }
    fn xhas_out(&self) -> bool { false }
    fn attach_pipe(&mut self, p: Arc<Pipe>, _sa: bool, _li: bool) { self.fq.attach(p.id()); }
    fn pipe_terminated(&mut self, p: &Pipe) { self.fq.terminated(p.id()); }
    fn read_activated(&mut self, p: &Pipe) { self.fq.activated(p.id()); }
    fn write_activated(&mut self, _p: &Pipe) {}
    fn socket_type(&self) -> SocketType { SocketType::Pull }
}
