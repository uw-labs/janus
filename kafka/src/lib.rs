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
mod subscriber;

pub use crate::error::{KafkaError, OffsetError};
pub use crate::publisher::{KafkaPublisher, PublisherAcker, PublisherConfig, PublisherMessage};
pub use crate::subscriber::{KafkaSubscriber, Offset, SubscriberAcker, SubscriberConfig};

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
