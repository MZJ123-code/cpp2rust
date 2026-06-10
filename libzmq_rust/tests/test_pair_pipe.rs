//! Tests pair socket communication through direct pipe connection.
mod common;
use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::pipe::Pipe;
use zmq_core::socket::base::Socket;
use zmq_core::socket::pair::PairSocket;

#[test]
fn test_pair_via_pipe() {
    // Create pipe pair and two sockets sharing the pipes
    let (p1, p2) = Pipe::new_pair(1);
    let mut sock_a = PairSocket::new();
    let mut sock_b = PairSocket::new();

    // Each socket gets one pipe reference (both are same Arc)
    sock_a.attach_pipe(p1, false, true);
    sock_b.attach_pipe(p2, false, false);

    // A sends to B (via write_to_session → to_session queue)
    let msg = ZmqMessage::from_slice(b"hello");
    sock_a.xsend(msg).unwrap();

    // B receives from A (via read_from_socket → to_session queue)
    assert!(sock_b.xhas_in(), "B should have data from A");
    let received = sock_b.xrecv().unwrap();
    assert_eq!(&received.data(), b"hello");
}

#[test]
fn test_pair_bidirectional() {
    let (p1, p2) = Pipe::new_pair(1);
    let mut sock_a = PairSocket::new();
    let mut sock_b = PairSocket::new();
    sock_a.attach_pipe(p1, false, true);
    sock_b.attach_pipe(p2, false, false);

    // Round-trip
    sock_a.xsend(ZmqMessage::from_slice(b"ping")).unwrap();
    let req = sock_b.xrecv().unwrap();
    assert_eq!(&req.data(), b"ping");

    // Reply back
    sock_b.xsend(ZmqMessage::from_slice(b"pong")).unwrap();
    let rep = sock_a.xrecv().unwrap();
    assert_eq!(&rep.data(), b"pong");
}
