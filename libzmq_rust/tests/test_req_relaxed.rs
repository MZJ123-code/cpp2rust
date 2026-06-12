mod common;
use common::{TestContext, SEQ_END};
use zmq_core::message::ZmqMessage;
use zmq_core::socket_type::SocketType;
use zmq_context::socket::{SendFlags, RecvFlags};

fn bounce_echo(socket: &zmq_context::ZSocket) {
    loop {
        let msg = socket.recv(RecvFlags::NONE).unwrap();
        let has_more = msg.more();
        let flags = if has_more { SendFlags::SNDMORE } else { SendFlags::NONE };
        socket.send(msg, flags).unwrap();
        if !has_more {
            break;
        }
    }
}

fn setup_relaxed(ctx: &TestContext, name: &str) -> (Vec<zmq_context::ZSocket>, zmq_context::ZSocket) {
    let req = ctx.socket(SocketType::Req);
    req.set_req_relaxed(true).unwrap();
    req.set_req_correlate(true).unwrap();

    let ep = common::ep_inproc(name);
    req.bind(&ep).unwrap();

    let services = 5usize;
    let mut reps = Vec::new();
    for _ in 0..services {
        let rep = ctx.socket(SocketType::Rep);
        rep.connect(&ep).unwrap();
        common::msleep(300);
        reps.push(rep);
    }
    common::msleep(300);

    (reps, req)
}

#[test]
fn test_case_1() {
    let ctx = TestContext::new();
    let (reps, req) = setup_relaxed(&ctx, "relaxed1");

    let events = common::get_events(&req);
    assert_eq!(events, 1, "expected POLLOUT");

    // Send a request, ensure it arrives, don't send a reply
    common::s_send_seq(&req, &[Some("A"), Some("B"), SEQ_END]);
    common::s_recv_seq(&reps[0], RecvFlags::NONE, &[Some("A"), Some("B"), SEQ_END]);

    let events = common::get_events(&req);
    assert_eq!(events, 1, "expected POLLOUT");

    // Send another request on the REQ socket
    common::s_send_seq(&req, &[Some("C"), Some("D"), SEQ_END]);
    common::s_recv_seq(&reps[1], RecvFlags::NONE, &[Some("C"), Some("D"), SEQ_END]);

    let events = common::get_events(&req);
    assert_eq!(events, 1, "expected POLLOUT");

    // Send a reply to the first request - that should be discarded by the REQ
    common::s_send_seq(&reps[0], &[Some("WRONG"), SEQ_END]);

    // Send the expected reply
    common::s_send_seq(&reps[1], &[Some("OK"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("OK"), SEQ_END]);

    // Another standard req-rep cycle, just to check
    common::s_send_seq(&req, &[Some("E"), SEQ_END]);
    common::s_recv_seq(&reps[2], RecvFlags::NONE, &[Some("E"), SEQ_END]);
    common::s_send_seq(&reps[2], &[Some("F"), Some("G"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("F"), Some("G"), SEQ_END]);

    req.set_linger(0).unwrap();
    for rep in &reps {
        rep.set_linger(0).unwrap();
    }
}

#[test]
fn test_case_2() {
    let ctx = TestContext::new();
    let (reps, req) = setup_relaxed(&ctx, "relaxed2");

    // Reproduce test_case_1 scenario
    common::s_send_seq(&req, &[Some("A"), Some("B"), SEQ_END]);
    common::s_recv_seq(&reps[0], RecvFlags::NONE, &[Some("A"), Some("B"), SEQ_END]);

    common::s_send_seq(&req, &[Some("C"), Some("D"), SEQ_END]);
    common::s_recv_seq(&reps[1], RecvFlags::NONE, &[Some("C"), Some("D"), SEQ_END]);

    common::s_send_seq(&reps[0], &[Some("WRONG"), SEQ_END]);
    common::s_send_seq(&reps[1], &[Some("OK"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("OK"), SEQ_END]);

    common::s_send_seq(&req, &[Some("E"), SEQ_END]);
    common::s_recv_seq(&reps[2], RecvFlags::NONE, &[Some("E"), SEQ_END]);
    common::s_send_seq(&reps[2], &[Some("F"), Some("G"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("F"), Some("G"), SEQ_END]);

    // Case 2: send a request, ensure it arrives, send a reply but don't receive on REQ
    common::s_send_seq(&req, &[Some("H"), SEQ_END]);
    common::s_recv_seq(&reps[3], RecvFlags::NONE, &[Some("H"), SEQ_END]);
    common::s_send_seq(&reps[3], &[Some("BAD"), SEQ_END]);

    // Wait for message to be there
    common::msleep(300);

    // Without receiving that reply, send another request on the REQ socket
    common::s_send_seq(&req, &[Some("I"), SEQ_END]);
    common::s_recv_seq(&reps[4], RecvFlags::NONE, &[Some("I"), SEQ_END]);

    // Send the expected reply
    common::s_send_seq(&reps[4], &[Some("GOOD"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("GOOD"), SEQ_END]);

    req.set_linger(0).unwrap();
    for rep in &reps {
        rep.set_linger(0).unwrap();
    }
}

#[test]
fn test_case_3() {
    let ctx = TestContext::new();
    let (reps, req) = setup_relaxed(&ctx, "relaxed3");

    // Reproduce test_case_1 scenario
    common::s_send_seq(&req, &[Some("A"), Some("B"), SEQ_END]);
    common::s_recv_seq(&reps[0], RecvFlags::NONE, &[Some("A"), Some("B"), SEQ_END]);

    common::s_send_seq(&req, &[Some("C"), Some("D"), SEQ_END]);
    common::s_recv_seq(&reps[1], RecvFlags::NONE, &[Some("C"), Some("D"), SEQ_END]);

    common::s_send_seq(&reps[0], &[Some("WRONG"), SEQ_END]);
    common::s_send_seq(&reps[1], &[Some("OK"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("OK"), SEQ_END]);

    common::s_send_seq(&req, &[Some("E"), SEQ_END]);
    common::s_recv_seq(&reps[2], RecvFlags::NONE, &[Some("E"), SEQ_END]);
    common::s_send_seq(&reps[2], &[Some("F"), Some("G"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("F"), Some("G"), SEQ_END]);

    // Reproduce test_case_2 additions
    common::s_send_seq(&req, &[Some("H"), SEQ_END]);
    common::s_recv_seq(&reps[3], RecvFlags::NONE, &[Some("H"), SEQ_END]);
    common::s_send_seq(&reps[3], &[Some("BAD"), SEQ_END]);
    common::msleep(300);
    common::s_send_seq(&req, &[Some("I"), SEQ_END]);
    common::s_recv_seq(&reps[4], RecvFlags::NONE, &[Some("I"), SEQ_END]);
    common::s_send_seq(&reps[4], &[Some("GOOD"), SEQ_END]);
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("GOOD"), SEQ_END]);

    // Check issue #1690: rep[0] should still be the next to receive, not rep[1]
    common::s_send_seq(&req, &[Some("J"), SEQ_END]);
    common::s_recv_seq(&reps[0], RecvFlags::NONE, &[Some("J"), SEQ_END]);

    req.set_linger(0).unwrap();
    for rep in &reps {
        rep.set_linger(0).unwrap();
    }
}

#[test]
#[ignore = "ROUTER socket xrecv not yet implemented"]
fn test_case_4() {
    // Check issue #1695: responses to messages other than the last sent one
    // are correctly discarded by the REQ pipe
    let ctx = TestContext::new();

    // Setup REQ socket as client (no explicit routing id)
    let req = ctx.socket(SocketType::Req);
    req.set_req_relaxed(true).unwrap();
    req.set_req_correlate(true).unwrap();

    // Connect before server is bound
    let ep = common::ep_inproc("relaxed4");
    req.connect(&ep).unwrap();

    // Setup ROUTER socket as server but do not bind it just yet
    let router = ctx.socket(SocketType::Router);

    // Send two requests before server is bound
    common::s_send_seq(&req, &[Some("TO_BE_DISCARDED"), SEQ_END]);
    common::s_send_seq(&req, &[Some("TO_BE_ANSWERED"), SEQ_END]);

    // Bind server allowing it to receive messages
    router.bind(&ep).unwrap();

    common::msleep(300);

    // Read the two messages and send them back as is
    bounce_echo(&router);
    bounce_echo(&router);

    // Read the expected correlated reply. As REQ_CORRELATE is active,
    // the expected answer is "TO_BE_ANSWERED", not "TO_BE_DISCARDED".
    common::s_recv_seq(&req, RecvFlags::NONE, &[Some("TO_BE_ANSWERED"), SEQ_END]);

    req.set_linger(0).unwrap();
    router.set_linger(0).unwrap();
}
