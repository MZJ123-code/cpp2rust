//! 1:1 translation of C++ `tests/test_pubsub.cpp`.
mod common;

use zmq_core::message::ZmqMessage;
use zmq_core::pipe::Pipe;
use zmq_core::socket::base::Socket;
use zmq_core::socket::pub_socket::PubSocket;
use zmq_core::socket::sub_socket::SubSocket;

#[test]
fn test_pubsub_basic() {
    let (p1, p2) = Pipe::new_pair(1);
    let mut pub_sock = PubSocket::new();
    let mut sub_sock = SubSocket::new();
    pub_sock.attach_pipe(p1, false, true);
    sub_sock.attach_pipe(p2, true, false); // subscribe_to_all=true

    pub_sock.xsend(ZmqMessage::from_slice(b"hello")).unwrap();
    assert!(sub_sock.xhas_in());
    assert_eq!(sub_sock.xrecv().unwrap().data(), b"hello");
}

#[test]
fn test_pubsub_multiple() {
    let (p1, p2) = Pipe::new_pair(1);
    let mut pub_sock = PubSocket::new();
    let mut sub_sock = SubSocket::new();
    pub_sock.attach_pipe(p1, false, true);
    sub_sock.attach_pipe(p2, true, false);

    for i in 0..5 {
        let msg = format!("msg{}", i);
        pub_sock.xsend(ZmqMessage::from_slice(msg.as_bytes())).unwrap();
        assert!(sub_sock.xhas_in());
        assert_eq!(sub_sock.xrecv().unwrap().data(), msg.as_bytes());
    }
}
