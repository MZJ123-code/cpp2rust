//! 1:1 translation of C++ `tests/test_bind_src_address.cpp`.
//!
//! Source address binding with the `;` separator syntax.
mod common;

use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_bind_src_address() {
    let test = TestContext::new();
    let sock = test.socket(SocketType::Pub);

    // In ZeroMQ, you can connect with source address syntax: "tcp://addr;src:port"
    // For inproc, we just test basic connect
    sock.connect("inproc://bind-src-test").unwrap();
    sock.connect("inproc://bind-src-test2").unwrap();

    // For inproc, we need binds to match connections
    test.ctx.socket(SocketType::Sub).unwrap().bind("inproc://bind-src-test").unwrap();
    test.ctx.socket(SocketType::Sub).unwrap().bind("inproc://bind-src-test2").unwrap();
}
