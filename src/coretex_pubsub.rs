//! C3 — Pub/Sub: the publishing half, wired to the real write path.
//!
//! `coretex_websocket` has had a complete subscription surface for a long
//! time — subscribe/unsubscribe per collection, a broadcast channel, per
//! connection tracking, heartbeats — and nothing ever called it. This module
//! supplies the missing half: an [`EventBus`] that the write path publishes
//! to, so a data change is an event with no plumbing in between.
//!
//! * [`DataManager::set_event_bus`] attaches a bus; every successful
//!   mutation then announces itself. Without a bus the write path does no
//!   extra work at all.
//! * [`EventBus::subscribe`] hands out a `tokio` broadcast receiver.
//!   Publishing never fails and never blocks: with no subscribers it is a
//!   no-op, and a subscriber that falls behind gets `RecvError::Lagged`
//!   rather than back-pressuring the write that already succeeded.
//! * `WebSocketServer::attach_bus` bridges a bus to existing WebSocket
//!   subscribers, so the notification surface starts working without
//!   rewriting it.
//!
//! The event type is [`DataChangeEvent`] from `coretex_websocket` — one
//! shape for both in-process subscribers and WebSocket clients.

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

pub use crate::coretex_websocket::DataChangeEvent;

/// Receiver of change events. `broadcast::RecvError::Lagged` means this
/// subscriber missed events because it was too slow — the same contract as
/// any `tokio::sync::broadcast` consumer.
pub type EventReceiver = broadcast::Receiver<DataChangeEvent>;

/// How many events a lagging subscriber may fall behind before it starts
/// receiving `Lagged` errors instead of events.
const DEFAULT_CAPACITY: usize = 1024;

/// Fan-out point between the write path and subscribers.
#[derive(Clone)]
pub struct EventBus {
    sender: broadcast::Sender<DataChangeEvent>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new(DEFAULT_CAPACITY)
    }
}

impl EventBus {
    /// A bus buffering up to `capacity` events per subscriber.
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Announce a change. Errors are ignored on purpose: no subscribers, or
    /// every subscriber gone, is not a write failure.
    pub fn publish(&self, event: DataChangeEvent) {
        let _ = self.sender.send(event);
    }

    /// Announce a change with the common fields filled in.
    pub fn publish_change(
        &self,
        collection: &str,
        event_type: &str,
        ids: &[String],
        metadata: Option<serde_json::Value>,
    ) {
        self.publish(DataChangeEvent {
            collection: collection.to_string(),
            event_type: event_type.to_string(),
            ids: ids.to_vec(),
            timestamp: now_secs(),
            event_id: new_event_id(),
            metadata,
        });
    }

    /// Subscribe to everything published after this call. Events are not
    /// filtered here: a subscriber receives every collection and decides what
    /// it cares about, which keeps one slow filter from stalling others.
    pub fn subscribe(&self) -> EventReceiver {
        self.sender.subscribe()
    }

    /// Subscribers currently attached.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn new_event_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// Serialize/Deserialize re-export convenience for transports that carry
/// events as JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub event: DataChangeEvent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_reaches_every_subscriber() {
        let bus = EventBus::new(8);
        let mut a = bus.subscribe();
        let mut b = bus.subscribe();
        assert_eq!(bus.subscriber_count(), 2);

        bus.publish_change("docs", "insert", &["r1".to_string()], None);

        for rx in [&mut a, &mut b] {
            let event = rx.recv().await.unwrap();
            assert_eq!(event.collection, "docs");
            assert_eq!(event.event_type, "insert");
            assert_eq!(event.ids, vec!["r1".to_string()]);
            assert!(!event.event_id.is_empty());
            assert!(event.timestamp > 0);
        }
    }

    #[tokio::test]
    async fn publishing_without_subscribers_is_a_no_op() {
        let bus = EventBus::new(4);
        bus.publish_change("docs", "insert", &[], None);
        assert_eq!(bus.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn slow_subscriber_is_told_it_lagged_rather_than_blocking() {
        // Capacity 2: after three publishes the first subscriber is too far
        // behind and gets Lagged, while publishing never waited for it.
        let bus = EventBus::new(2);
        let mut slow = bus.subscribe();
        for i in 0..3 {
            bus.publish_change("docs", "insert", &[format!("r{i}")], None);
        }
        let first = slow.try_recv();
        assert!(
            matches!(first, Err(broadcast::error::TryRecvError::Lagged(_))),
            "got {first:?}"
        );
    }
}