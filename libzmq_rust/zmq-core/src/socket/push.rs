//! PUSH socket — load-balanced fan-out. Replaces C++ `push_t`.
use std::sync::Arc;
use crate::data_structures::load_balancer::LoadBalancer;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct PushSocket { lb: LoadBalancer }

impl PushSocket { pub fn new() -> Self { Self { lb: LoadBalancer::new() } } }

impl Socket for PushSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> {
        if !self.lb.has_out() { return Err(ZmqError::NoPeer); }
        Ok(())
    }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> { Err(ZmqError::NotSupported("PUSH")) }
    fn xhas_in(&self) -> bool { false }
    fn xhas_out(&self) -> bool { self.lb.has_out() }
    fn attach_pipe(&mut self, p: Arc<Pipe>, _sa: bool, _li: bool) { self.lb.attach(p.id()); }
    fn pipe_terminated(&mut self, p: &Pipe) { self.lb.terminated(p.id()); }
    fn read_activated(&mut self, _p: &Pipe) {}
    fn write_activated(&mut self, p: &Pipe) { self.lb.activated(p.id()); }
    fn socket_type(&self) -> SocketType { SocketType::Push }
}
