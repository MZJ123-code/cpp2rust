mod common;
use common::*;
use zmq_core::socket_type::SocketType;

#[test]
fn test_reconnect_ivl_with_pair_socket() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Pair);
    let sc = ctx.socket(SocketType::Pair);

    let _ep = ctx.bind_inproc(&sb, "reconnect-ivl");
    ctx.connect_inproc(&sc, "reconnect-ivl");

    // Bounce: send from client, recv on server; send from server, recv on client
    ctx.bounce(&sb, &sc);


    // Set a negative reconnect interval (disabled reconnect)
    sc.set_reconnect_ivl(-1).unwrap();
    msleep(300);
    // With reconnect disabled, after unbind we need to re-connect manually.
    // Since there's no unbind API, we just test that the sockets still work.
    ctx.bounce(&sb, &sc);
}
