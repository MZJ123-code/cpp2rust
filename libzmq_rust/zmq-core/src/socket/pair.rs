//! PAIR socket — exclusive bidirectional. Replaces C++ `pair_t`.
use std::sync::Arc;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;
use super::base::Socket;

pub struct PairSocket { pipe: Option<Arc<Pipe>> }

impl PairSocket { pub fn new() -> Self { Self { pipe: None } } }

impl Socket for PairSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        match &self.pipe {
            Some(p) if p.is_active() => { p.write_to_session(msg, false); p.flush_to_session(); Ok(()) }
            _ => Err(ZmqError::NoPeer),
        }
    }
    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        match &self.pipe {
            Some(p) if p.check_read_from_session() => p.read_from_session().ok_or(ZmqError::NoMessage),
            _ => Err(ZmqError::NoMessage),
        }
    }
    fn xhas_in(&self) -> bool { self.pipe.as_ref().map(|p| p.check_read_from_session()).unwrap_or(false) }
    fn xhas_out(&self) -> bool { self.pipe.as_ref().map(|p| p.is_active()).unwrap_or(false) }
    fn attach_pipe(&mut self, pipe: Arc<Pipe>, _sa: bool, _li: bool) { if self.pipe.is_none() { self.pipe = Some(pipe); } }
    fn pipe_terminated(&mut self, p: &Pipe) { if self.pipe.as_ref().map(|x| x.id() == p.id()).unwrap_or(false) { self.pipe = None; } }
    fn read_activated(&mut self, _p: &Pipe) {}
    fn write_activated(&mut self, _p: &Pipe) {}
    fn socket_type(&self) -> SocketType { SocketType::Pair }
}
