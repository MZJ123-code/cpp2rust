//! Socket type enumeration — 1:1 mapping with ZMQ_* constants.

/// All ZeroMQ socket types (stable + draft).
///
/// The discriminant values match the ZMQ_* constants from `zmq.h`
/// exactly, enabling 1:1 wire compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(i32)]
pub enum SocketType {
    /// Exclusive pair — bidirectional, exclusive connection
    Pair = 0, // ZMQ_PAIR (stable)

    /// Publisher — broadcast messages to subscribers
    Pub = 1, // ZMQ_PUB (stable)

    /// Subscriber — receive matching messages
    Sub = 2, // ZMQ_SUB (stable)

    /// Request — send request, expect one reply
    Req = 3, // ZMQ_REQ (stable)

    /// Reply — receive request, send one reply
    Rep = 4, // ZMQ_REP (stable)

    /// Dealer — async request/reply, load-balanced
    Dealer = 5, // ZMQ_DEALER (stable)

    /// Router — async request/reply, routed
    Router = 6, // ZMQ_ROUTER (stable)

    /// Pull — fair-queued inbound
    Pull = 7, // ZMQ_PULL (stable)

    /// Push — load-balanced outbound
    Push = 8, // ZMQ_PUSH (stable)

    /// XPUB — publisher with subscription messages
    Xpub = 9, // ZMQ_XPUB (stable)

    /// XSUB — subscriber with subscription messages
    Xsub = 10, // ZMQ_XSUB (stable)

    /// Stream — raw TCP stream
    Stream = 11, // ZMQ_STREAM (stable)

    // Draft API socket types below
    /// Server — draft server socket
    Server = 12, // ZMQ_SERVER

    /// Client — draft client socket
    Client = 13, // ZMQ_CLIENT

    /// Radio — draft radio broadcast
    Radio = 14, // ZMQ_RADIO

    /// Dish — draft dish receiver
    Dish = 15, // ZMQ_DISH

    /// Gather — draft gather receiver
    Gather = 16, // ZMQ_GATHER

    /// Scatter — draft scatter sender
    Scatter = 17, // ZMQ_SCATTER

    /// Datagram — draft unreliable datagram
    Dgram = 18, // ZMQ_DGRAM

    /// Peer — draft peer-to-peer
    Peer = 19, // ZMQ_PEER

    /// Channel — draft bidirectional channel
    Channel = 20, // ZMQ_CHANNEL
}

impl SocketType {
    /// Convert from the C ZMQ_* constant value.
    pub fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Pair),
            1 => Some(Self::Pub),
            2 => Some(Self::Sub),
            3 => Some(Self::Req),
            4 => Some(Self::Rep),
            5 => Some(Self::Dealer),
            6 => Some(Self::Router),
            7 => Some(Self::Pull),
            8 => Some(Self::Push),
            9 => Some(Self::Xpub),
            10 => Some(Self::Xsub),
            11 => Some(Self::Stream),
            12 => Some(Self::Server),
            13 => Some(Self::Client),
            14 => Some(Self::Radio),
            15 => Some(Self::Dish),
            16 => Some(Self::Gather),
            17 => Some(Self::Scatter),
            18 => Some(Self::Dgram),
            19 => Some(Self::Peer),
            20 => Some(Self::Channel),
            _ => None,
        }
    }

    /// Whether this socket type can send messages.
    pub fn can_send(self) -> bool {
        !matches!(self, Self::Pull | Self::Sub | Self::Dish | Self::Gather)
    }

    /// Whether this socket type can receive messages.
    pub fn can_recv(self) -> bool {
        !matches!(self, Self::Push | Self::Pub | Self::Scatter | Self::Radio)
    }

    /// Whether this socket type is part of the draft API.
    pub fn is_draft(self) -> bool {
        matches!(
            self,
            Self::Server
                | Self::Client
                | Self::Radio
                | Self::Dish
                | Self::Gather
                | Self::Scatter
                | Self::Dgram
                | Self::Peer
                | Self::Channel
        )
    }

    /// Whether this socket type is stable.
    pub fn is_stable(self) -> bool {
        !self.is_draft()
    }

    /// Whether this socket type supports routing IDs.
    pub fn has_routing_id(self) -> bool {
        matches!(self, Self::Router | Self::Rep | Self::Stream | Self::Server | Self::Peer)
    }

    /// Whether this socket is peer-to-peer (one connection).
    pub fn is_peer_to_peer(self) -> bool {
        matches!(self, Self::Pair | Self::Channel | Self::Peer)
    }

    /// Human-readable name for this socket type.
    pub fn name(self) -> &'static str {
        match self {
            Self::Pair => "PAIR",
            Self::Pub => "PUB",
            Self::Sub => "SUB",
            Self::Req => "REQ",
            Self::Rep => "REP",
            Self::Dealer => "DEALER",
            Self::Router => "ROUTER",
            Self::Pull => "PULL",
            Self::Push => "PUSH",
            Self::Xpub => "XPUB",
            Self::Xsub => "XSUB",
            Self::Stream => "STREAM",
            Self::Server => "SERVER",
            Self::Client => "CLIENT",
            Self::Radio => "RADIO",
            Self::Dish => "DISH",
            Self::Gather => "GATHER",
            Self::Scatter => "SCATTER",
            Self::Dgram => "DGRAM",
            Self::Peer => "PEER",
            Self::Channel => "CHANNEL",
        }
    }
}

impl From<SocketType> for i32 {
    fn from(t: SocketType) -> i32 {
        t as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_i32_round_trip() {
        for i in 0..=20 {
            let t = SocketType::from_i32(i).unwrap();
            assert_eq!(t as i32, i);
        }
    }

    #[test]
    fn test_send_recv_symmetry() {
        // PUSH can send but not recv
        assert!(SocketType::Push.can_send());
        assert!(!SocketType::Push.can_recv());
        // PULL can recv but not send
        assert!(!SocketType::Pull.can_send());
        assert!(SocketType::Pull.can_recv());
    }
}
