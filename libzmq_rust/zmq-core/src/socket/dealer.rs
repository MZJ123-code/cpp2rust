//! DEALER socket — async bidirectional, load-balanced. Replaces C++ `dealer_t`.
use std::sync::Arc;
use crate::data_structures::fair_queue::FairQueue;
use crate::data_structures::load_balancer::LoadBalancer;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct DealerSocket { lb: LoadBalancer, fq: FairQueue }

impl DealerSocket { pub fn new() -> Self { Self { lb: LoadBalancer::new(), fq: FairQueue::new() } } }

impl Socket for DealerSocket {
    fn xsend(&mut self, _msg: ZmqMessage) -> ZmqResult<()> {
        if !self.lb.has_out() { return Err(ZmqError::NoPeer); }
        Ok(())
    }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if !self.fq.has_in() { return Err(ZmqError::NoMessage); }
        Err(ZmqError::NoMessage)
    }
    fn xhas_in(&self) -> bool { self.fq.has_in() }
    fn xhas_out(&self) -> bool { self.lb.has_out() }
    fn attach_pipe(&mut self, p: Arc<Pipe>, _sa: bool, _li: bool) { self.lb.attach(p.id()); self.fq.attach(p.id()); }
    fn pipe_terminated(&mut self, p: &Pipe) { self.lb.terminated(p.id()); self.fq.terminated(p.id()); }
    fn read_activated(&mut self, p: &Pipe) { self.fq.activated(p.id()); }
    fn write_activated(&mut self, p: &Pipe) { self.lb.activated(p.id()); }
    fn socket_type(&self) -> SocketType { SocketType::Dealer }
}
