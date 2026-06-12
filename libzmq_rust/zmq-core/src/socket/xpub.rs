//! XPUB socket — publisher with subscription message forwarding.
//!
//! Replaces C++ `xpub.cpp` / `xpub.hpp`.
//!
//! XPUB is the proxy-aware version of PUB. In addition to broadcasting
//! messages to subscribers, it also receives and forwards subscription
//! messages upstream so an XSUB proxy can subscribe on behalf of its
//! downstream SUB peers.
//!
//! ## Modes (via socket options)
//! - **Manual mode**: subscription messages are silently queued for the
//!   application to handle. The app must call subscribe/unsubscribe via
//!   socket options.
//! - **Nodrop mode**: if no peer is ready, block the publisher (HWM aware).
//! - **Verbose mode**: send a notification for every subscription, not just
//!   the first/last at each topic.
//! - **Welcome message**: initial message sent to new subscribers.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::data_structures::trie::SubscriptionTrie;
use crate::error::{ZmqError, ZmqResult};
use crate::message::ZmqMessage;
use crate::pipe::Pipe;
use crate::socket_type::SocketType;

use super::base::Socket;

/// Flag byte for "subscribe" in old-style subscription messages.
const SUBSCRIBE_FLAG: u8 = 1;
/// Flag byte for "unsubscribe" in old-style subscription messages.
const UNSUBSCRIBE_FLAG: u8 = 0;

// ─── XPUB ──────────────────────────────────────────────────────────

pub struct XpubSocket {
    /// All attached pipes.
    pipes: Vec<Arc<Pipe>>,

    /// Subscription trie: topic prefix → pipe IDs that subscribe.
    subscriptions: SubscriptionTrie,

    /// Manual mode subscriptions — used to track subscriptions that the
    /// application has not explicitly approved yet.
    manual_subscriptions: SubscriptionTrie,

    /// Pending subscription/unsubscription notifications to be returned
    /// via `xrecv`. Each entry is (data, pipe_id), where pipe_id identifies
    /// the originating pipe (for manual mode last_pipe tracking).
    pending_data: VecDeque<Vec<u8>>,
    pending_flags: VecDeque<u8>,
    /// Pending pipe refs — paired with pending_data for manual mode.
    pending_pipes: VecDeque<Option<usize>>,

    /// Whether to send verbose subscription notifications.
    verbose_subs: bool,
    /// Whether to send verbose unsubscription notifications.
    verbose_unsubs: bool,

    /// Whether manual mode is enabled (app must explicitly subscribe/unsubscribe).
    manual: bool,

    /// Whether the socket is lossy (true = PUB-like, false = block on HWM).
    lossy: bool,

    /// Whether to only process the first subscribe message on a multi-part stream.
    only_first_subscribe: bool,

    /// In manual mode, send to the last pipe that sent a subscription.
    send_last_pipe: bool,

    /// Welcome message sent to new subscribers.
    welcome_msg: Option<Vec<u8>>,

    /// In manual mode, the last pipe that sent a subscription (used by xsend).
    last_pipe: Option<usize>,

    /// Whether we are in the middle of a multi-frame send.
    more_send: bool,

    /// Whether we are in the middle of a multi-frame receive.
    more_recv: bool,

    /// Whether to process subsequent frames as subscription data
    /// (for multi-part subscription messages).
    process_subscribe: bool,
}

impl XpubSocket {
    pub fn new() -> Self {
        Self {
            pipes: Vec::new(),
            subscriptions: SubscriptionTrie::new(),
            manual_subscriptions: SubscriptionTrie::new(),
            pending_data: VecDeque::new(),
            pending_flags: VecDeque::new(),
            pending_pipes: VecDeque::new(),
            verbose_subs: false,
            verbose_unsubs: false,
            manual: false,
            lossy: true,
            only_first_subscribe: false,
            send_last_pipe: false,
            welcome_msg: None,
            last_pipe: None,
            more_send: false,
            more_recv: false,
            process_subscribe: false,
        }
    }

    /// Set the welcome message (sent to new peers on connect).
    pub fn set_welcome_msg(&mut self, msg: Vec<u8>) {
        if msg.is_empty() {
            self.welcome_msg = None;
        } else {
            self.welcome_msg = Some(msg);
        }
    }

    /// Enable/disable verbose subscription notifications.
    pub fn set_verbose(&mut self, value: bool) {
        self.verbose_subs = value;
        self.verbose_unsubs = false;
    }

    /// Enable/disable verbose subscription AND unsubscription notifications.
    pub fn set_verboser(&mut self, value: bool) {
        self.verbose_subs = value;
        self.verbose_unsubs = value;
    }

    /// Enable/disable manual subscription mode.
    pub fn set_manual(&mut self, value: bool) {
        self.manual = value;
        if value {
            self.send_last_pipe = true;
        }
    }

    /// Enable/disable manual mode with last-pipe tracking.
    pub fn set_manual_last_value(&mut self, value: bool) {
        self.manual = value;
        self.send_last_pipe = value;
    }

    /// Enable/disable nodrop mode (opposite of lossy).
    /// nodrop=true means lossy=false.
    pub fn set_nodrop(&mut self, value: bool) {
        self.lossy = !value;
    }

    /// Enable/disable only_first_subscribe mode.
    pub fn set_only_first_subscribe(&mut self, value: bool) {
        self.only_first_subscribe = value;
    }

    /// Manually subscribe a topic on the last active pipe.
    pub fn manual_subscribe(&mut self, topic: &[u8]) {
        if let Some(pipe_id) = self.last_pipe {
            self.subscriptions.add(topic, pipe_id);
        }
    }

    /// Manually unsubscribe a topic on the last active pipe.
    pub fn manual_unsubscribe(&mut self, topic: &[u8]) {
        if let Some(pipe_id) = self.last_pipe {
            self.subscriptions.remove(topic, pipe_id);
        }
    }

    /// Number of unique topic prefixes.
    pub fn topics_count(&self) -> usize {
        self.subscriptions.num_prefixes()
    }

    // ─── internal helpers ──────────────────────────────────────

    /// Send a pending subscription/unsubscription notification upstream.
    fn queue_notification(&mut self, subscribe: bool, data: &[u8], pipe_id: Option<usize>) {
        let mut notification = Vec::with_capacity(data.len() + 1);
        if subscribe {
            notification.push(SUBSCRIBE_FLAG);
        } else {
            notification.push(UNSUBSCRIBE_FLAG);
        }
        notification.extend_from_slice(data);

        self.pending_data.push_back(notification);
        self.pending_flags.push_back(0);
        self.pending_pipes.push_back(pipe_id);
    }

    /// Check whether a message is an old-style subscribe command.
    fn is_subscribe_msg(data: &[u8]) -> bool {
        !data.is_empty() && data[0] == SUBSCRIBE_FLAG
    }

    /// Check whether a message is an old-style unsubscribe command.
    fn is_cancel_msg(data: &[u8]) -> bool {
        !data.is_empty() && data[0] == UNSUBSCRIBE_FLAG
    }

    /// Extract the topic data from a subscription message
    /// (skip the leading 0/1 flag byte).
    fn topic_from_msg(data: &[u8]) -> &[u8] {
        if data.len() > 1 { &data[1..] } else { &[] }
    }

    /// Find a pipe by its ID, returning the Arc<Pipe>.
    fn find_pipe(&self, pipe_id: usize) -> Option<&Arc<Pipe>> {
        self.pipes.iter().find(|p| p.id() == pipe_id)
    }

    /// Process a subscription message received on the given pipe.
    fn process_subscribe_cmd(&mut self, pipe_id: usize, data: &[u8], subscribe: bool) {
        let topic = Self::topic_from_msg(data);
        let notify: bool;

        if self.manual {
            // In manual mode, store in manual_subscriptions and queue
            // the pipe so the app can approve/reject.
            if !subscribe {
                self.manual_subscriptions.remove(topic, pipe_id);
            } else {
                self.manual_subscriptions.add(topic, pipe_id);
            }
            self.pending_pipes.push_back(Some(pipe_id));
            // In manual mode we always notify the app.
            self.queue_notification(subscribe, topic, Some(pipe_id));
            return;
        }

        if !subscribe {
            let rm_result = self.subscriptions.remove(topic, pipe_id);
            notify = rm_result != crate::data_structures::trie::RmResult::ValuesRemain
                || self.verbose_unsubs;
        } else {
            let first_added = self.subscriptions.add(topic, pipe_id);
            notify = first_added || self.verbose_subs;
        }

        if notify {
            self.queue_notification(subscribe, topic, Some(pipe_id));
        }
    }

    /// Send an unsubscription notification for a topic that no longer has
    /// any subscribers. Called as a callback from the trie during pipe_terminated.
    fn send_unsubscription(&mut self, data: &[u8]) {
        self.queue_notification(false, data, None);
    }

    /// Match pipes for a topic and send the message to all matching pipes.
    fn send_to_matching(&self, msg: &ZmqMessage, matching: &[usize]) -> ZmqResult<()> {
        if matching.is_empty() {
            // No subscriptions at all, or empty topic — drop
            return Ok(());
        }

        let _data = msg.data();
        let mut sent = false;
        for pipe in &self.pipes {
            if pipe.is_active() && matching.contains(&pipe.id()) {
                pipe.write_to_session(msg.clone(), msg.more());
                pipe.flush_to_session();
                sent = true;
            }
        }

        if !sent && !self.lossy {
            // In nodrop mode, failing to send is an error
            return Err(ZmqError::WouldBlock);
        }

        Ok(())
    }

    /// Match a topic message against the subscription trie.
    /// Returns the pipe IDs that match.
    fn match_topic(&self, data: &[u8]) -> Vec<usize> {
        self.subscriptions.match_all(data)
    }
}

impl Socket for XpubSocket {
    fn xsend(&mut self, msg: ZmqMessage) -> ZmqResult<()> {
        let msg_more = msg.more();

        // For the first part of a multi-part message, find matching pipes.
        if !self.more_send {
            let data = msg.data();

            if self.manual && self.last_pipe.is_some() && self.send_last_pipe {
                // In manual mode with send_last_pipe, only send to the last
                // pipe that sent a subscription (if it matches the topic).
                let pipe_id = self.last_pipe.unwrap();
                if let Some(pipe) = self.find_pipe(pipe_id) {
                    if pipe.is_active() {
                        pipe.write_to_session(msg.clone(), msg_more);
                        pipe.flush_to_session();
                    }
                }
                self.last_pipe = None;
            } else {
                // Match topic against subscription trie.
                let matching = self.match_topic(&data);
                self.send_to_matching(&msg, &matching)?;
            }
        } else {
            // Continuation of multi-part — send to all pipes (PUB behavior).
            for pipe in &self.pipes {
                if pipe.is_active() {
                    pipe.write_to_session(msg.clone(), msg_more);
                    pipe.flush_to_session();
                }
            }
        }

        self.more_send = msg_more;
        Ok(())
    }

    fn xrecv(&mut self) -> ZmqResult<ZmqMessage> {
        if self.pending_data.is_empty() {
            return Err(ZmqError::NoMessage);
        }

        // In manual mode, track the pipe for the subscription so the app
        // can later call manual_subscribe/manual_unsubscribe targeting it.
        if self.manual {
            if let Some(pipe_id) = self.pending_pipes.pop_front() {
                self.last_pipe = pipe_id;
                // If the pipe has been terminated, clear the reference.
                if let Some(pid) = pipe_id {
                    if !self.pipes.iter().any(|p| p.id() == pid) {
                        self.last_pipe = None;
                    }
                }
            }
        } else {
            // Drain the pipe queue even in non-manual mode to keep queues aligned.
            let _ = self.pending_pipes.pop_front();
        }

        let data = self.pending_data.pop_front().unwrap();
        let _flags = self.pending_flags.pop_front();

        Ok(ZmqMessage::from_slice(&data))
    }

    fn xhas_in(&self) -> bool {
        !self.pending_data.is_empty()
    }

    fn xhas_out(&self) -> bool {
        !self.pipes.is_empty()
    }

    fn attach_pipe(
        &mut self,
        pipe: Arc<Pipe>,
        subscribe_to_all: bool,
        _locally_initiated: bool,
    ) {
        let pipe_id = pipe.id();

        // If subscribe_to_all is specified, the caller wants to subscribe
        // to all data on this pipe (empty prefix matches everything).
        if subscribe_to_all {
            self.subscriptions.add(&[], pipe_id);
        }

        // Send welcome message if configured.
        if let Some(ref welcome) = self.welcome_msg {
            let welcome_msg = ZmqMessage::from_slice(welcome);
            pipe.write_to_session(welcome_msg, false);
            pipe.flush_to_session();
        }

        self.pipes.push(pipe);

        // The pipe is active when attached. Read any pending subscriptions.
        // We call read_activated to process any queued subscription commands.
    }

    fn pipe_terminated(&mut self, pipe: &Pipe) {
        let pipe_id = pipe.id();

        if self.manual {
            // Remove the pipe from manual_subscriptions and send corresponding
            // unsubscriptions upstream.
            let mut unsubs: Vec<Vec<u8>> = Vec::new();
            self.manual_subscriptions.remove_by_value(
                pipe_id,
                &mut |data, _len| {
                    unsubs.push(data.to_vec());
                },
                false, // call on every removal
            );
            for topic in &unsubs {
                self.queue_notification(false, topic, None);
            }

            // Also remove from the real subscriptions trie (without callbacks).
            self.subscriptions.remove_by_value(
                pipe_id,
                &mut |_, _| {},
                false,
            );

            // Clear last_pipe if it was this pipe.
            if self.last_pipe == Some(pipe_id) {
                self.last_pipe = None;
            }
        } else {
            // Remove the pipe from the trie. If there are topics that nobody
            // is interested in anymore, send unsubscriptions upstream.
            let mut unsubs: Vec<Vec<u8>> = Vec::new();
            self.subscriptions.remove_by_value(
                pipe_id,
                &mut |data, _len| {
                    unsubs.push(data.to_vec());
                },
                !self.verbose_unsubs, // call_on_unique = !verbose_unsubs
            );
            for topic in &unsubs {
                self.send_unsubscription(topic);
            }
        }

        // Remove from pipes list.
        self.pipes.retain(|p| p.id() != pipe_id);
    }

    fn read_activated(&mut self, pipe: &Pipe) {
        let pipe_id = pipe.id();

        // Read all pending subscription messages from the pipe.
        while let Some(msg) = pipe.read_from_session() {
            let data = msg.data();
            let first_part = !self.more_recv;
            self.more_recv = msg.more();

            let mut subscribe = false;
            let mut is_sub_or_cancel = false;
            let mut _topic: Vec<u8> = Vec::new();

            if first_part || self.process_subscribe {
                // Check for old-style subscription messages (first byte 0 or 1)
                if Self::is_subscribe_msg(&data) {
                    _topic = Self::topic_from_msg(&data).to_vec();
                    subscribe = true;
                    is_sub_or_cancel = true;
                } else if Self::is_cancel_msg(&data) {
                    _topic = Self::topic_from_msg(&data).to_vec();
                    subscribe = false;
                    is_sub_or_cancel = true;
                } else if msg.is_command() {
                    // ZMTP 3.1+ subscription commands would be handled here.
                    // For now, treat non-subscription commands as user messages.
                }
            }

            if first_part {
                self.process_subscribe = !self.only_first_subscribe
                    || is_sub_or_cancel;
            }

            if is_sub_or_cancel {
                self.process_subscribe_cmd(pipe_id, &data, subscribe);
            } else {
                // User message coming upstream from XSUB socket —
                // queue it for xrecv.
                self.pending_data.push_back(data.clone());
                self.pending_flags.push_back(if msg.more() { 1 } else { 0 });
                self.pending_pipes.push_back(Some(pipe_id));
            }
        }
    }

    fn write_activated(&mut self, _pipe: &Pipe) {
        // Pipe is now writable — nothing to do at socket level.
    }

    fn socket_type(&self) -> SocketType {
        SocketType::Xpub
    }
}

// ─── Tests ─────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pipe(id: usize) -> Arc<Pipe> {
        let (a, _b) = Pipe::new_pair(id);
        a
    }

    #[test]
    fn test_xpub_create() {
        let sock = XpubSocket::new();
        assert_eq!(sock.socket_type(), SocketType::Xpub);
        assert!(!sock.xhas_in());
        assert!(!sock.xhas_out());
    }

    #[test]
    fn test_xpub_attach_pipe() {
        let mut sock = XpubSocket::new();
        let pipe = make_pipe(100);
        sock.attach_pipe(pipe, false, true);
        assert!(sock.xhas_out());
    }

    #[test]
    fn test_xpub_attach_pipe_subscribe_to_all() {
        let mut sock = XpubSocket::new();
        let pipe = make_pipe(200);
        let pipe_id = pipe.id();
        sock.attach_pipe(pipe, true, true);
        // subscribe_to_all adds empty prefix → all messages match
        let matching = sock.match_topic(b"anything");
        assert!(matching.contains(&pipe_id));
    }

    #[test]
    fn test_xpub_welcome_message() {
        let mut sock = XpubSocket::new();
        sock.set_welcome_msg(b"WELCOME".to_vec());
        // Welcome msg is sent on attach_pipe when welcome_msg is set
        // (write occurs inline; no verification needed at socket level)
        assert!(sock.welcome_msg.is_some());
    }

    #[test]
    fn test_xpub_send_no_pipes() {
        let mut sock = XpubSocket::new();
        let msg = ZmqMessage::from_slice(b"test");
        // Empty topic won't send to anyone, but should not error
        let result = sock.xsend(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_xpub_recv_empty() {
        let mut sock = XpubSocket::new();
        let result = sock.xrecv();
        assert!(result.is_err());
    }

    #[test]
    fn test_xpub_verbose_mode() {
        let mut sock = XpubSocket::new();
        sock.set_verbose(true);
        assert!(sock.verbose_subs);
        assert!(!sock.verbose_unsubs);
    }

    #[test]
    fn test_xpub_verboser_mode() {
        let mut sock = XpubSocket::new();
        sock.set_verboser(true);
        assert!(sock.verbose_subs);
        assert!(sock.verbose_unsubs);
    }

    #[test]
    fn test_xpub_manual_mode() {
        let mut sock = XpubSocket::new();
        sock.set_manual(true);
        assert!(sock.manual);
        assert!(sock.send_last_pipe);
    }

    #[test]
    fn test_xpub_nodrop_mode() {
        let mut sock = XpubSocket::new();
        assert!(sock.lossy);
        sock.set_nodrop(true);
        assert!(!sock.lossy);
    }

    #[test]
    fn test_xpub_only_first_subscribe() {
        let mut sock = XpubSocket::new();
        sock.set_only_first_subscribe(true);
        assert!(sock.only_first_subscribe);
    }

    #[test]
    fn test_xpub_topics_count() {
        let mut sock = XpubSocket::new();
        assert_eq!(sock.topics_count(), 0);

        let pipe = make_pipe(300);
        let pipe_id = pipe.id();
        sock.attach_pipe(pipe, false, true);

        // Add subscription manually
        sock.subscriptions.add(b"topic1", pipe_id);
        assert_eq!(sock.topics_count(), 1);

        sock.subscriptions.add(b"topic2", pipe_id);
        assert_eq!(sock.topics_count(), 2);

        // Adding same topic again should not increment
        sock.subscriptions.add(b"topic1", pipe_id);
        assert_eq!(sock.topics_count(), 2);
    }

    #[test]
    fn test_xpub_pipe_terminated_cleanup() {
        let mut sock = XpubSocket::new();
        let pipe = make_pipe(400);
        let pipe_id = pipe.id();
        sock.attach_pipe(pipe.clone(), false, true);

        // Add a subscription for this pipe
        sock.subscriptions.add(b"topic", pipe_id);
        assert_eq!(sock.topics_count(), 1);

        // Terminate the pipe
        sock.pipe_terminated(&pipe);
        assert_eq!(sock.topics_count(), 0);
    }

    #[test]
    fn test_xpub_manual_subscribe_unsubscribe() {
        let mut sock = XpubSocket::new();
        let pipe = make_pipe(500);
        let pipe_id = pipe.id();
        sock.attach_pipe(pipe, false, true);

        // Set last_pipe (simulates receiving a sub notification in manual mode)
        sock.last_pipe = Some(pipe_id);

        // Manual subscribe
        sock.manual_subscribe(b"test_topic");
        assert_eq!(sock.topics_count(), 1);

        // Manual unsubscribe
        sock.manual_unsubscribe(b"test_topic");
        assert_eq!(sock.topics_count(), 0);
    }

    #[test]
    fn test_xpub_match_topic() {
        let mut sock = XpubSocket::new();
        let pipe1 = make_pipe(600);
        let pipe2 = make_pipe(601);

        let id1 = pipe1.id();
        let id2 = pipe2.id();

        sock.pipes.push(pipe1);
        sock.pipes.push(pipe2);

        // Subscribe pipe1 to "foo" and pipe2 to "bar"
        sock.subscriptions.add(b"foo", id1);
        sock.subscriptions.add(b"bar", id2);

        let matching_foo = sock.match_topic(b"foo");
        assert!(matching_foo.contains(&id1));
        assert!(!matching_foo.contains(&id2));

        let matching_bar = sock.match_topic(b"bar");
        assert!(matching_bar.contains(&id2));
        assert!(!matching_bar.contains(&id1));

        // No one subscribed to "baz"
        let matching_baz = sock.match_topic(b"baz");
        assert!(matching_baz.is_empty());
    }

    #[test]
    fn test_xpub_prefix_match() {
        let mut sock = XpubSocket::new();
        let pipe = make_pipe(700);
        let pipe_id = pipe.id();
        sock.pipes.push(pipe);

        // Subscribe to "foo" prefix — should match "foo", "foobar", etc.
        sock.subscriptions.add(b"foo", pipe_id);

        let matching = sock.match_topic(b"foobar");
        assert!(matching.contains(&pipe_id));
    }

    #[test]
    fn test_xpub_queue_notification_subscribe() {
        let mut sock = XpubSocket::new();
        sock.queue_notification(true, b"topic", Some(42));
        assert!(sock.xhas_in());

        let msg = sock.xrecv().unwrap();
        let data = msg.data();
        // First byte should be 1 (subscribe), then "topic"
        assert_eq!(data[0], SUBSCRIBE_FLAG);
        assert_eq!(&data[1..], b"topic");
    }

    #[test]
    fn test_xpub_queue_notification_unsubscribe() {
        let mut sock = XpubSocket::new();
        sock.queue_notification(false, b"mytopic", None);
        assert!(sock.xhas_in());

        let msg = sock.xrecv().unwrap();
        let data = msg.data();
        // First byte should be 0 (unsubscribe), then "mytopic"
        assert_eq!(data[0], UNSUBSCRIBE_FLAG);
        assert_eq!(&data[1..], b"mytopic");
    }

    #[test]
    fn test_xpub_multiple_pending_notifications() {
        let mut sock = XpubSocket::new();
        sock.queue_notification(true, b"a", Some(1));
        sock.queue_notification(true, b"b", Some(2));
        sock.queue_notification(false, b"c", Some(3));

        assert!(sock.xhas_in());

        let msg1 = sock.xrecv().unwrap();
        assert_eq!(&msg1.data()[1..], b"a");

        let msg2 = sock.xrecv().unwrap();
        assert_eq!(&msg2.data()[1..], b"b");

        let msg3 = sock.xrecv().unwrap();
        assert_eq!(&msg3.data()[1..], b"c");

        assert!(!sock.xhas_in());
    }

    #[test]
    fn test_xpub_is_subscribe_msg() {
        assert!(XpubSocket::is_subscribe_msg(&[1, b'f', b'o', b'o']));
        assert!(!XpubSocket::is_subscribe_msg(&[0, b'f', b'o', b'o']));
        assert!(!XpubSocket::is_subscribe_msg(&[]));
    }

    #[test]
    fn test_xpub_is_cancel_msg() {
        assert!(XpubSocket::is_cancel_msg(&[0, b'f', b'o', b'o']));
        assert!(!XpubSocket::is_cancel_msg(&[1, b'f', b'o', b'o']));
        assert!(!XpubSocket::is_cancel_msg(&[]));
    }

    #[test]
    fn test_xpub_topic_from_msg() {
        let data = [1, b't', b'e', b's', b't'];
        let topic = XpubSocket::topic_from_msg(&data);
        assert_eq!(topic, b"test");
    }

    #[test]
    fn test_xpub_empty_topic_from_short_msg() {
        let data = [1u8];
        let topic = XpubSocket::topic_from_msg(&data);
        assert_eq!(topic, b"");

        let data = [0u8];
        let topic = XpubSocket::topic_from_msg(&data);
        assert_eq!(topic, b"");
    }
}
