//! Janus-Kafka
//!
//! Kafka adapter for Janus.
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

mod error;
mod publisher;
#[cfg(feature = "smol-rt")]
mod smol;
mod subscriber;

use std::future::Future;

use futures_util::stream::TryStreamExt;
use janus::{AckHandler, AckMessage, Message};

pub use crate::error::{KafkaError, OffsetError};
pub use crate::publisher::{KafkaPublisher, PublisherAcker, PublisherConfig, PublisherMessage};
pub use crate::subscriber::{KafkaSubscriber, Offset, SubscriberAcker, SubscriberConfig};

pub use rdkafka::util::AsyncRuntime;

#[cfg(feature = "smol-rt")]
pub use crate::smol::SmolRuntime;

#[cfg(feature = "tokio-rt")]
pub use rdkafka::util::TokioRuntime;

/// Message extension methods for Kafka messages
pub trait MessageExt: Message {
    /// The topic of the message.
    fn topic(&self) -> &str;

    /// The key the message is partitioned against.
    fn key(&self) -> Option<&str>;
}

/// Exposes rdkafka's Config
pub type Config = rdkafka::config::ClientConfig;

/// IntoConfig builds a Config
pub trait IntoConfig {
    /// Creates a config
    fn into_config(self) -> Config;
}

impl IntoConfig for std::collections::HashMap<&str, &str> {
    fn into_config(self) -> Config {
        let mut config = Config::new();
        for (key, value) in self {
            config.set(key, value);
        }
        config
    }
}

impl<M: MessageExt> MessageExt for AckMessage<M> {
    fn topic(&self) -> &str {
        self.message().topic()
    }

    fn key(&self) -> Option<&str> {
        self.message().key()
    }
}

/// Awaits each future returned by the ack handler.
pub async fn noop_ack_handler<A: AckHandler>(mut handler: A) -> Result<(), A::Error>
where
    <A as AckHandler>::Output: Future<Output = Result<(), A::Error>>,
{
    while let Some(fut) = handler.try_next().await? {
        fut.await?;
    }
    Ok(())
}
