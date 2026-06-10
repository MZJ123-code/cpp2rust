//! 1:1 translation of C++ `tests/test_pair_inproc.cpp`.
mod common;

use common::*;
use zmq_core::message::ZmqMessage;
use zmq_core::pipe::Pipe;
use zmq_core::socket::pair::PairSocket;
use zmq_core::socket::base::Socket;

#[test]
fn test_pair_send_recv() {
    let (p1, p2) = Pipe::new_pair(1);
    let mut bind_sock = PairSocket::new();
    let mut conn_sock = PairSocket::new();
    bind_sock.attach_pipe(p1, false, false);
    conn_sock.attach_pipe(p2, false, true);

    bind_sock.xsend(ZmqMessage::from_slice(b"Hello")).unwrap();
    assert!(conn_sock.xhas_in());
    assert_eq!(conn_sock.xrecv().unwrap().data(), b"Hello");
}

#[test]
fn test_pair_connect_before_bind() {
    let (p1, p2) = Pipe::new_pair(1);
    let mut bind_sock = PairSocket::new();
    let mut conn_sock = PairSocket::new();
    // Connect before bind — both get pipes, order doesn't matter
    conn_sock.attach_pipe(p1, false, true);
    bind_sock.attach_pipe(p2, false, false);

    bind_sock.xsend(ZmqMessage::from_slice(b"World")).unwrap();
    assert!(conn_sock.xhas_in());
    assert_eq!(conn_sock.xrecv().unwrap().data(), b"World");
}
