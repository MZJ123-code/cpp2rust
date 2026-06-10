//! Endpoint parsing for ZeroMQ transport addresses.
//! Supports: tcp://, ipc://, inproc://

use std::path::PathBuf;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    Tcp { host: String, port: u16, is_wildcard: bool },
    Ipc { path: PathBuf },
    Inproc { name: String },
}

impl FromStr for Endpoint {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(rest) = s.strip_prefix("tcp://") {
            let (host, port_str) = rest.rsplit_once(':').ok_or("missing port")?;
            let port: u16 = port_str.parse().map_err(|_| "invalid port")?;
            let is_wildcard = port == 0 || host == "*";
            Ok(Endpoint::Tcp { host: host.to_string(), port, is_wildcard })
        } else if let Some(rest) = s.strip_prefix("ipc://") {
            Ok(Endpoint::Ipc { path: PathBuf::from(rest) })
        } else if let Some(rest) = s.strip_prefix("inproc://") {
            Ok(Endpoint::Inproc { name: rest.to_string() })
        } else {
            Err(format!("unsupported protocol in endpoint: {}", s))
        }
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Tcp { host, port, .. } => write!(f, "tcp://{}:{}", host, port),
            Endpoint::Ipc { path } => write!(f, "ipc://{}", path.display()),
            Endpoint::Inproc { name } => write!(f, "inproc://{}", name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_tcp() {
        let ep = "tcp://127.0.0.1:5555".parse::<Endpoint>().unwrap();
        assert_eq!(ep, Endpoint::Tcp { host: "127.0.0.1".into(), port: 5555, is_wildcard: false });
    }

    #[test]
    fn test_parse_tcp_wildcard() {
        let ep = "tcp://*:5555".parse::<Endpoint>().unwrap();
        assert_eq!(ep, Endpoint::Tcp { host: "*".into(), port: 5555, is_wildcard: true });
    }

    #[test]
    fn test_parse_ipc() {
        let ep = "ipc:///tmp/zmq-test".parse::<Endpoint>().unwrap();
        match ep {
            Endpoint::Ipc { path } => assert_eq!(path, PathBuf::from("/tmp/zmq-test")),
            _ => panic!("expected Ipc endpoint"),
        }
    }

    #[test]
    fn test_parse_inproc() {
        let ep = "inproc://my-endpoint".parse::<Endpoint>().unwrap();
        assert_eq!(ep, Endpoint::Inproc { name: "my-endpoint".into() });
    }

    #[test]
    fn test_display_round_trip() {
        let cases = vec![
            "tcp://127.0.0.1:5555",
            "tcp://*:5555",
            "ipc:///tmp/zmq-test",
            "inproc://my-endpoint",
        ];
        for case in cases {
            let ep: Endpoint = case.parse().unwrap();
            assert_eq!(ep.to_string(), case);
        }
    }
}
