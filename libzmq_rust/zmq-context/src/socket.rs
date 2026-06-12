//! ZSocket — public socket handle. 1:1 translation of C++ `socket_base_t`.
use std::sync::Arc;
use parking_lot::RwLock;
use zmq_core::error::{ZmqError, ZmqResult};
use zmq_core::message::ZmqMessage;
use zmq_core::socket::base::Socket;
use zmq_core::socket::*;
use zmq_core::socket_type::SocketType;
use zmq_core::pipe::Pipe;
use super::context::ZContextInner;
use super::options::SocketOptions;

bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct SendFlags: i32 { const NONE = 0; const DONTWAIT = 1; const SNDMORE = 2; }
}
bitflags::bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct RecvFlags: i32 { const NONE = 0; const DONTWAIT = 1; }
}

pub struct ZSocket {
    ctx: Arc<ZContextInner>,
    socket_type: SocketType,
    inner: RwLock<Box<dyn Socket>>,
    options: RwLock<SocketOptions>,
    endpoints: RwLock<Vec<String>>,
    pipes: Arc<RwLock<Vec<Arc<Pipe>>>>,
    /// Track how many pipes have been attached to inner (for incremental attach)
    attached_pipe_count: RwLock<usize>,
}

impl ZSocket {
    pub(crate) fn new(ctx: Arc<ZContextInner>, typ: SocketType) -> Self {
        Self { ctx, socket_type: typ, inner: RwLock::new(Self::create_socket_impl(typ)), options: RwLock::new(SocketOptions::default()), endpoints: RwLock::new(Vec::new()), pipes: Arc::new(RwLock::new(Vec::new())), attached_pipe_count: RwLock::new(0) }
    }

    fn create_socket_impl(typ: SocketType) -> Box<dyn Socket> {
        match typ {
            SocketType::Push => Box::new(push::PushSocket::new()),
            SocketType::Pull => Box::new(pull::PullSocket::new()),
            SocketType::Pair => Box::new(pair::PairSocket::new()),
            SocketType::Pub => Box::new(pub_socket::PubSocket::new()),
            SocketType::Sub => Box::new(sub_socket::SubSocket::new()),
            SocketType::Dealer => Box::new(dealer::DealerSocket::new()),
            SocketType::Router => Box::new(router::RouterSocket::new()),
            SocketType::Req => Box::new(req::ReqSocket::new()),
            SocketType::Rep => Box::new(rep::RepSocket::new()),
            SocketType::Xpub => Box::new(xpub::XpubSocket::new()),
            SocketType::Xsub => Box::new(xsub::XsubSocket::new()),
            _ => Box::new(pair::PairSocket::new()),
        }
    }

    pub fn connect(&self, endpoint: &str) -> ZmqResult<()> {
        self.endpoints.write().push(endpoint.to_string());
        if endpoint.starts_with("inproc://") {
            let name = endpoint.strip_prefix("inproc://").unwrap_or("");
            // Create pipe pair
            let (client_pipe, server_pipe) = Pipe::new_pair(0);
            // Attach client pipe to us (connecting side)
            self.pipes.write().push(client_pipe.clone());
            self.inner.write().attach_pipe(client_pipe, false, true);
            // Deliver server pipe to the bound socket, or queue for later
            let bound_sockets = self.ctx.bound_sockets.read();
            if let Some(peer_pipes) = bound_sockets.get(name) {
                peer_pipes.write().push(server_pipe.clone());
            } else {
                // connect-before-bind: queue in registry
                drop(bound_sockets);
                let mut registry = self.ctx.inproc_registry.write();
                registry.queue_pipe(name, server_pipe);
            }
        }
        Ok(())
    }

    pub fn bind(&self, endpoint: &str) -> ZmqResult<()> {
        self.endpoints.write().push(endpoint.to_string());
        if endpoint.starts_with("inproc://") {
            let name = endpoint.strip_prefix("inproc://").unwrap_or("");
            // Register our pipes list so connecting sockets can deliver to us
            self.ctx.bound_sockets.write().insert(name.to_string(), self.pipes.clone());
            // Process any pending pipes from connect-before-bind
            let pending = {
                let registry = self.ctx.inproc_registry.read();
                registry.take_pending(name)
            };
            for pipe in pending {
                self.pipes.write().push(pipe.clone());
                self.inner.write().attach_pipe(pipe, false, false);
            }
        }
        Ok(())
    }

    /// Attach any newly added pipes (from concurrent connect calls) to the inner socket.
    fn sync_pipes(&self) {
        loop {
            let pipe_count = self.pipes.read().len();
            let mut attached = self.attached_pipe_count.write();
            if *attached >= pipe_count {
                return;
            }
            let idx = *attached;
            *attached += 1;
            let pipe = {
                let pipes = self.pipes.read();
                pipes[idx].clone()
            };
            drop(attached);
            self.inner.write().attach_pipe(pipe.clone(), false, false);
            self.inner.write().write_activated(&pipe);
        }
    }

    /// Activate pipes that have data available for reading.
    fn activate_read_pipes(&self) {
        for pipe in self.pipes.read().iter() {
            if pipe.check_read_from_socket() {
                self.inner.write().read_activated(pipe);
            }
        }
    }

    pub fn send(&self, msg: impl Into<ZmqMessage>, flags: SendFlags) -> ZmqResult<()> {
        self.sync_pipes();
        let mut msg = msg.into();
        msg.set_more(flags.contains(SendFlags::SNDMORE));
        if flags.contains(SendFlags::DONTWAIT) && !self.inner.read().xhas_out() { return Err(ZmqError::WouldBlock); }
        self.activate_read_pipes();
        self.inner.write().xsend(msg)
    }
    pub fn recv(&self, flags: RecvFlags) -> ZmqResult<ZmqMessage> {
        self.sync_pipes();
        self.activate_read_pipes();
        if flags.contains(RecvFlags::DONTWAIT) && !self.inner.read().xhas_in() { return Err(ZmqError::WouldBlock); }
        self.inner.write().xrecv()
    }
    pub fn has_in(&self) -> bool { self.sync_pipes(); self.activate_read_pipes(); self.inner.read().xhas_in() }
    pub fn has_out(&self) -> bool { self.sync_pipes(); self.inner.read().xhas_out() }
    pub fn get_options(&self) -> SocketOptions { self.options.read().clone() }
    pub fn socket_type(&self) -> SocketType { self.socket_type }
    pub fn subscribe(&self, prefix: &[u8]) -> ZmqResult<()> {
        self.options.write().subscribe.push(prefix.to_vec());
        // Propagate to inner socket for subscription filtering
        let subs = self.options.read().subscribe.clone();
        self.inner.write().set_subscriptions(&subs);
        Ok(())
    }
    pub fn unsubscribe(&self, prefix: &[u8]) -> ZmqResult<()> {
        self.options.write().subscribe.retain(|s| s.as_slice() != prefix);
        let subs = self.options.read().subscribe.clone();
        self.inner.write().set_subscriptions(&subs);
        Ok(())
    }

    // ── Socket option setters ──────────────────────────────────

    pub fn set_linger(&self, ms: i32) -> ZmqResult<()> { self.options.write().linger = ms; Ok(()) }
    pub fn set_sndhwm(&self, hwm: i32) -> ZmqResult<()> { self.options.write().sndhwm = hwm; Ok(()) }
    pub fn set_rcvhwm(&self, hwm: i32) -> ZmqResult<()> { self.options.write().rcvhwm = hwm; Ok(()) }
    pub fn set_sndtimeo(&self, ms: i32) -> ZmqResult<()> { self.options.write().sndtimeo = ms; Ok(()) }
    pub fn set_rcvtimeo(&self, ms: i32) -> ZmqResult<()> { self.options.write().rcvtimeo = ms; Ok(()) }
    pub fn set_reconnect_ivl(&self, ms: i32) -> ZmqResult<()> { self.options.write().reconnect_ivl = ms; Ok(()) }
    pub fn set_immediate(&self, v: bool) -> ZmqResult<()> { self.options.write().immediate = v; Ok(()) }
    pub fn set_conflate(&self, v: bool) -> ZmqResult<()> { self.options.write().conflate = v; Ok(()) }
    pub fn set_ipv6(&self, v: bool) -> ZmqResult<()> { self.options.write().ipv6 = v; Ok(()) }
    pub fn set_tcp_nodelay(&self, v: bool) -> ZmqResult<()> { self.options.write().tcp_nodelay = v; Ok(()) }
    pub fn set_tcp_keepalive(&self, v: i32) -> ZmqResult<()> { self.options.write().tcp_keepalive = v; Ok(()) }
    pub fn set_mechanism(&self, m: zmq_core::security::SecurityMechanism) -> ZmqResult<()> {
        use zmq_core::security::{NullMechanism, PlainClient};
        self.options.write().mechanism = match m {
            zmq_core::security::SecurityMechanism::Null => super::options::SecurityMechanism::Null,
            zmq_core::security::SecurityMechanism::Plain => super::options::SecurityMechanism::Plain,
            zmq_core::security::SecurityMechanism::Curve => super::options::SecurityMechanism::Curve,
        };
        Ok(())
    }
    pub fn set_plain_username(&self, u: &str) -> ZmqResult<()> { self.options.write().plain_username = u.to_string(); Ok(()) }
    pub fn set_plain_password(&self, p: &str) -> ZmqResult<()> { self.options.write().plain_password = p.to_string(); Ok(()) }
    pub fn set_zap_domain(&self, d: &str) -> ZmqResult<()> { self.options.write().zap_domain = d.to_string(); Ok(()) }
    pub fn set_routing_id(&self, id: &[u8]) -> ZmqResult<()> { self.options.write().routing_id = id.to_vec(); Ok(()) }
    pub fn set_router_mandatory(&self, v: bool) -> ZmqResult<()> { self.options.write().router_mandatory = v; Ok(()) }
    pub fn set_router_handover(&self, v: bool) -> ZmqResult<()> { self.options.write().router_handover = v; Ok(()) }
    pub fn set_req_correlate(&self, v: bool) -> ZmqResult<()> { self.options.write().req_correlate = v; Ok(()) }
    pub fn set_req_relaxed(&self, v: bool) -> ZmqResult<()> { self.options.write().req_relaxed = v; Ok(()) }
    pub fn set_probe_router(&self, v: bool) -> ZmqResult<()> { self.options.write().probe_router = v; Ok(()) }
    pub fn set_xpub_verbose(&self, v: bool) -> ZmqResult<()> { self.options.write().xpub_verbose = v; Ok(()) }
    pub fn set_xpub_verboser(&self, v: bool) -> ZmqResult<()> { self.options.write().xpub_verboser = v; Ok(()) }
    pub fn set_xpub_nodrop(&self, v: bool) -> ZmqResult<()> { self.options.write().xpub_nodrop = v; Ok(()) }
    pub fn set_xpub_manual(&self, v: bool) -> ZmqResult<()> { self.options.write().xpub_manual = v; Ok(()) }
    pub fn set_xpub_manual_last_value(&self, v: bool) -> ZmqResult<()> { self.options.write().xpub_manual_last_value = v; Ok(()) }
    pub fn set_xpub_welcome_msg(&self, msg: &[u8]) -> ZmqResult<()> { self.options.write().xpub_welcome_msg = msg.to_vec(); Ok(()) }
    pub fn set_invert_matching(&self, v: bool) -> ZmqResult<()> { self.options.write().invert_matching = v; Ok(()) }
    pub fn set_xsub_verbose_unsubscribe(&self, v: bool) -> ZmqResult<()> { self.options.write().xsub_verbose_unsubscribe = v; Ok(()) }
    pub fn set_only_first_subscribe(&self, v: bool) -> ZmqResult<()> { self.options.write().only_first_subscribe = v; Ok(()) }
    pub fn set_heartbeat_ivl(&self, ms: i32) -> ZmqResult<()> { self.options.write().heartbeat_ivl = ms; Ok(()) }
    pub fn set_heartbeat_timeout(&self, ms: i32) -> ZmqResult<()> { self.options.write().heartbeat_timeout = ms; Ok(()) }
    pub fn set_heartbeat_ttl(&self, ttl: i32) -> ZmqResult<()> { self.options.write().heartbeat_ttl = ttl; Ok(()) }
    pub fn set_rate(&self, rate: i32) -> ZmqResult<()> { self.options.write().rate = rate; Ok(()) }
    pub fn set_recovery_ivl(&self, ms: i32) -> ZmqResult<()> { self.options.write().recovery_ivl = ms; Ok(()) }
    pub fn set_sndbuf(&self, bytes: i32) -> ZmqResult<()> { self.options.write().sndbuf = bytes; Ok(()) }
    pub fn set_rcvbuf(&self, bytes: i32) -> ZmqResult<()> { self.options.write().rcvbuf = bytes; Ok(()) }
    pub fn set_tos(&self, tos: i32) -> ZmqResult<()> { self.options.write().tos = tos; Ok(()) }
    pub fn set_backlog(&self, n: i32) -> ZmqResult<()> { self.options.write().backlog = n; Ok(()) }
    pub fn set_curve_serverkey(&self, key: &[u8; 32]) -> ZmqResult<()> { self.options.write().curve_serverkey = *key; Ok(()) }
    pub fn set_curve_publickey(&self, key: &[u8; 32]) -> ZmqResult<()> { self.options.write().curve_publickey = *key; Ok(()) }
    pub fn set_curve_secretkey(&self, key: &[u8; 32]) -> ZmqResult<()> { self.options.write().curve_secretkey = *key; Ok(()) }

    // ── Socket option getters ──────────────────────────────────

    pub fn linger(&self) -> i32 { self.options.read().linger }
    pub fn sndhwm(&self) -> i32 { self.options.read().sndhwm }
    pub fn rcvhwm(&self) -> i32 { self.options.read().rcvhwm }
    pub fn sndtimeo(&self) -> i32 { self.options.read().sndtimeo }
    pub fn rcvtimeo(&self) -> i32 { self.options.read().rcvtimeo }
    pub fn reconnect_ivl(&self) -> i32 { self.options.read().reconnect_ivl }
    pub fn immediate(&self) -> bool { self.options.read().immediate }
    pub fn conflate(&self) -> bool { self.options.read().conflate }
    pub fn mechanism(&self) -> super::options::SecurityMechanism { self.options.read().mechanism }
    pub fn routing_id(&self) -> Vec<u8> { self.options.read().routing_id.clone() }
    pub fn heartbeat_ivl(&self) -> i32 { self.options.read().heartbeat_ivl }
    pub fn heartbeat_timeout(&self) -> i32 { self.options.read().heartbeat_timeout }
    pub fn topics_count(&self) -> i32 { self.options.read().subscribe.len() as i32 }

    pub fn close(self) -> ZmqResult<()> { Ok(()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::ZContext;
    use zmq_core::socket::pair::PairSocket;

    #[test]
    fn test_socket_create_close() {
        let ctx = ZContext::new();
        let sock = ctx.socket(SocketType::Push).unwrap();
        assert_eq!(sock.socket_type(), SocketType::Push);
        sock.close().unwrap();
    }

    #[test]
    fn test_pair_socket_create() {
        let mut sock = PairSocket::new();
        assert!(!sock.xhas_in());
        assert!(!sock.xhas_out());
        assert_eq!(sock.socket_type(), SocketType::Pair);
    }
}
