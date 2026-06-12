mod common;
use common::TestContext;
use zmq_core::socket_type::SocketType;

#[test]
#[ignore = "REQ/REP state machine not yet implemented"]
fn test_roundtrip() {
    let ctx = TestContext::new();
    let sb = ctx.socket(SocketType::Rep);
    sb.bind(&common::ep_inproc("a")).unwrap();

    let sc = ctx.socket(SocketType::Req);
    sc.connect(&common::ep_inproc("a")).unwrap();

    ctx.bounce(&sb, &sc);
}
