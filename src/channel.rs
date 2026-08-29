use std::sync::{Arc, Weak};

struct Side;

/// One endpoint's ownership token and view of its peer's token.
pub(crate) struct ChannelSide {
    own: Arc<Side>,
    peer: Weak<Side>,
}

impl ChannelSide {
    pub(crate) fn is_peer_connected(&self) -> bool {
        self.peer.strong_count() != 0
    }
}

impl Clone for ChannelSide {
    fn clone(&self) -> Self {
        Self {
            own: Arc::clone(&self.own),
            peer: self.peer.clone(),
        }
    }
}

pub(crate) fn channel_sides() -> (ChannelSide, ChannelSide) {
    let first = Arc::new(Side);
    let second = Arc::new(Side);

    (
        ChannelSide {
            own: Arc::clone(&first),
            peer: Arc::downgrade(&second),
        },
        ChannelSide {
            own: second,
            peer: Arc::downgrade(&first),
        },
    )
}
