//! TCP transport — connect, bind, accept over TCP/IPv4/IPv6.
//!
//! 1:1 translation of C++ `tcp_listener_t` + `tcp_connecter_t`.
//! Uses `tokio::net` for async I/O.

use std::net::SocketAddr;
use tokio::net::{TcpListener as TokioListener, TcpStream as TokioStream};

use crate::endpoint::Endpoint;
use zmq_core::error::{ZmqError, ZmqResult};

/// A bound TCP listener (replaces `tcp_listener_t`).
pub struct TcpListener {
    inner: TokioListener,
    bound_endpoint: Endpoint,
}

impl TcpListener {
    /// Bind to the given endpoint and start listening.
    pub async fn bind(endpoint: &Endpoint) -> ZmqResult<Self> {
        let addr = endpoint_to_socket_addr(endpoint)?;
        let inner = TokioListener::bind(addr)
            .await
            .map_err(|e| ZmqError::Network(format!("TCP bind failed: {}", e)))?;

        let local = inner
            .local_addr()
            .map_err(|e| ZmqError::Network(format!("getsockname failed: {}", e)))?;

        let bound = socket_addr_to_endpoint(&local, endpoint)?;

        // Disable Nagle's algorithm for low-latency messaging
        // TCP_NODELAY is set per accepted connection in libzmq

        Ok(Self {
            inner,
            bound_endpoint: bound,
        })
    }

    /// Get the actual bound endpoint (for wildcard port resolution).
    pub fn bound_endpoint(&self) -> &Endpoint {
        &self.bound_endpoint
    }

    /// Accept an incoming connection. Returns a `TcpStream` and the peer endpoint.
    pub async fn accept(&self) -> ZmqResult<(TcpStream, Endpoint)> {
        let (stream, peer) = self
            .inner
            .accept()
            .await
            .map_err(|e| ZmqError::Network(format!("TCP accept failed: {}", e)))?;

        // Set TCP_NODELAY for low latency
        stream
            .set_nodelay(true)
            .map_err(|e| ZmqError::Network(format!("set_nodelay failed: {}", e)))?;

        let peer_endpoint = Endpoint::Tcp {
            host: peer.ip().to_string(),
            port: peer.port(),
            is_wildcard: false,
        };

        Ok((TcpStream { inner: stream }, peer_endpoint))
    }
}

/// An established TCP connection (replaces `stream_engine_base_t` I/O handle).
pub struct TcpStream {
    inner: TokioStream,
}

impl TcpStream {
    /// Connect to a remote endpoint.
    pub async fn connect(endpoint: &Endpoint) -> ZmqResult<Self> {
        let addr = endpoint_to_socket_addr(endpoint)?;
        let inner = TokioStream::connect(addr)
            .await
            .map_err(|e| ZmqError::Network(format!("TCP connect failed: {}", e)))?;

        inner
            .set_nodelay(true)
            .map_err(|e| ZmqError::Network(format!("set_nodelay failed: {}", e)))?;

        Ok(Self { inner })
    }

    /// Get the inner Tokio TcpStream for use with engine I/O.
    pub fn into_inner(self) -> TokioStream {
        self.inner
    }

    /// Get a reference to the inner stream.
    pub fn inner(&self) -> &TokioStream {
        &self.inner
    }

    /// Get a mutable reference to the inner stream.
    pub fn inner_mut(&mut self) -> &mut TokioStream {
        &mut self.inner
    }
}

// ─── Address conversion helpers ───────────────────────────────

fn endpoint_to_socket_addr(endpoint: &Endpoint) -> ZmqResult<SocketAddr> {
    match endpoint {
        Endpoint::Tcp { host, port, .. } => {
            let addr_str = format!("{}:{}", host, port);
            addr_str
                .parse::<SocketAddr>()
                .map_err(|e| ZmqError::InvalidEndpoint(format!("{}: {}", addr_str, e)))
        }
        _ => Err(ZmqError::InvalidEndpoint(format!(
            "expected TCP endpoint, got {}",
            endpoint
        ))),
    }
}

fn socket_addr_to_endpoint(addr: &SocketAddr, original: &Endpoint) -> ZmqResult<Endpoint> {
    match original {
        Endpoint::Tcp { port: orig_port, .. } => {
            if *orig_port == 0 {
                // Wildcard port was resolved
                Ok(Endpoint::Tcp {
                    host: addr.ip().to_string(),
                    port: addr.port(),
                    is_wildcard: false,
                })
            } else {
                Ok(original.clone())
            }
        }
        _ => Err(ZmqError::Internal("endpoint type mismatch".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bind_and_accept() {
        let ep: Endpoint = "tcp://127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&ep).await.unwrap();
        let bound = listener.bound_endpoint().clone();
        assert!(matches!(bound, Endpoint::Tcp { port, .. } if port > 0));
    }

    #[tokio::test]
    async fn test_connect_and_accept() {
        let ep: Endpoint = "tcp://127.0.0.1:0".parse().unwrap();
        let listener = TcpListener::bind(&ep).await.unwrap();
        let bound = listener.bound_endpoint().clone();

        // Spawn accept task
        let accept_handle = tokio::spawn(async move { listener.accept().await });

        // Connect
        let _conn = TcpStream::connect(&bound).await.unwrap();
        let peer = accept_handle.await.unwrap().unwrap();
        assert!(peer.0.inner.peer_addr().is_ok());
    }
}
