use std::fmt;
use std::ops::DerefMut;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::error::{KafkaError, OffsetError};
use crate::MessageExt;

use futures_channel::mpsc::{self, Receiver, Sender};
use futures_core::Stream;
use janus::{AckHandler, AckMessage, Message, Statuser, Subscriber};
use owning_ref::OwningHandle;
use rdkafka::client::Client;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{self, TopicPartitionList};
use rdkafka::types::RDKafkaType;
use rdkafka::util::DefaultRuntime;

/// Consumes messages from Kafka
pub struct KafkaSubscriber {
    upstream: OwningHandle<
        Arc<StreamConsumer>,
        Box<MessageStream<'static, DefaultConsumerContext, DefaultRuntime>>,
    >,
    acks_tx: Sender<SubscriberMessage>,
    config: SubscriberConfig,
}

impl KafkaSubscriber {
    /// Creates a new consumer and acker.
    pub fn new(config: SubscriberConfig) -> Result<(Self, SubscriberAcker), KafkaError> {
        let consumer: StreamConsumer = config.client_config.create()?;

        let topics = config.topics.iter().map(String::as_str).collect::<Vec<_>>();

        consumer.subscribe(&topics)?;

        let consumer = Arc::new(consumer);

        let (acks_tx, acks_rx) = mpsc::channel(config.buffer_size);

        let acker = SubscriberAcker {
            consumer: consumer.clone(),
            acks_rx,
        };

        Ok((
            Self {
                upstream: OwningHandle::new_with_fn(consumer, |c| {
                    let cf = unsafe { &*c };

                    Box::new(cf.stream())
                }),
                acks_tx,
                config,
            },
            acker,
        ))
    }

    /// Creates a KafkaSubscriber healthcheck.
    pub fn status(&self) -> Result<KafkaSubscriberStatus, KafkaError> {
        KafkaSubscriberStatus::new(self.config.clone())
    }
}

impl fmt::Debug for KafkaSubscriber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSubscriber").finish()
    }
}

impl Subscriber for KafkaSubscriber {
    type Message = SubscriberMessage;
    type Error = KafkaError;
}

impl Stream for KafkaSubscriber {
    type Item = Result<AckMessage<SubscriberMessage>, <Self as Subscriber>::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(self.upstream.deref_mut()).poll_next(cx) {
            Poll::Ready(Some(Ok(m))) => Poll::Ready(Some(Ok(AckMessage::new(
                SubscriberMessage {
                    payload: m.payload().unwrap().to_vec(),
                    topic: m.topic().to_owned(),
                    offset: m.offset(),
                    partition: m.partition(),
                },
                self.acks_tx.clone(),
            )))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Allows a healthcheck to be performed on the Kafka subscriber.
#[derive(Clone)]
pub struct KafkaSubscriberStatus {
    client: Arc<Client<DefaultConsumerContext>>,
}

impl KafkaSubscriberStatus {
    fn new(config: SubscriberConfig) -> Result<Self, KafkaError> {
        let native_config = config.client_config.create_native_config()?;

        let client = Client::new(
            &config.client_config,
            native_config,
            RDKafkaType::RD_KAFKA_CONSUMER,
            DefaultConsumerContext,
        )?;

        let client = Arc::new(client);

        Ok(Self { client })
    }
}

impl fmt::Debug for KafkaSubscriberStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSubscriberStatus").finish()
    }
}

impl Statuser for KafkaSubscriberStatus {
    type Error = KafkaError;

    fn status(&self) -> Result<(), Self::Error> {
        self.client
            .fetch_metadata(None, Some(std::time::Duration::from_secs(1)))?;

        Ok(())
    }
}

/// Acknowledges messages from the Subscriber.
pub struct SubscriberAcker {
    consumer: Arc<StreamConsumer>,
    acks_rx: Receiver<SubscriberMessage>,
}

impl fmt::Debug for SubscriberAcker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscriberAcker").finish()
    }
}

impl AckHandler for SubscriberAcker {
    type Output = Result<SubscriberMessage, (SubscriberMessage, KafkaError)>;
    type Error = KafkaError;
}

impl Stream for SubscriberAcker {
    type Item = Result<<Self as AckHandler>::Output, <Self as AckHandler>::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.acks_rx).poll_next(cx) {
            Poll::Ready(Some(m)) => {
                let mut tpl = TopicPartitionList::new();
                if let Err(err) = tpl.add_partition_offset(
                    &m.topic,
                    m.partition,
                    topic_partition_list::Offset::Offset(m.offset + 1),
                ) {
                    return Poll::Ready(Some(Ok(Err((m, err.into())))));
                }

                match self.consumer.store_offsets(&tpl) {
                    Ok(_) => Poll::Ready(Some(Ok(Ok(m)))),
                    Err(e) => Poll::Ready(Some(Ok(Err((m, e.into()))))),
                }
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[derive(Debug)]
pub struct SubscriberMessage {
    payload: Vec<u8>,
    topic: String,
    offset: i64,
    partition: i32,
}

impl Message for SubscriberMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl MessageExt for SubscriberMessage {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn key(&self) -> Option<&str> {
        None
    }
}

/// Configuration options for a Subscriber
#[derive(Clone, Debug)]
pub struct SubscriberConfig {
    client_config: ClientConfig,
    buffer_size: usize,
    topics: Vec<String>,
}

impl Default for SubscriberConfig {
    fn default() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("enable.auto.offset.store", "false");
        client_config.set("auto.commit.interval.ms", "500");

        Self {
            client_config,
            buffer_size: 1,
            topics: Vec::new(),
        }
    }
}

impl SubscriberConfig {
    /// Initial list of brokers as a CSV list of broker host or host:port
    pub fn brokers(mut self, brokers: &str) -> Self {
        self.client_config.set("bootstrap.servers", brokers);
        self
    }

    /// Client group id string. All clients sharing the same group.id belong to the same group.
    pub fn group_id(mut self, group_id: &str) -> Self {
        self.client_config.set("group.id", group_id);
        self
    }

    /// Position for the offset when no initial value.
    pub fn offset(mut self, offset: Offset) -> Self {
        self.client_config
            .set("auto.offset.reset", offset.to_string());
        self
    }

    /// Capacity of ack channel, defaults to 1. Change this to increase consumer throughput.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// List of topics to subscribe to.
    pub fn topics(mut self, topics: &[&str]) -> Self {
        self.topics = topics.into_iter().map(ToString::to_string).collect();
        self
    }

    /// Set a librdkafka config directly, for a list of available configuration, see:
    /// [configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.client_config.set(key, value);
        self
    }
}

/// Position for the offset when no initial value.
#[derive(Clone, Copy, Debug)]
pub enum Offset {
    /// Resets the offset to the smallest offset.
    Earliest,
    /// Resets the offset to the largest offset.
    Latest,
}

impl Default for Offset {
    fn default() -> Self {
        Offset::Latest
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Offset::Earliest => write!(f, "earliest"),
            Offset::Latest => write!(f, "latest"),
        }
    }
}

impl FromStr for Offset {
    type Err = OffsetError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(Offset::Earliest),
            "latest" => Ok(Offset::Latest),
            _ => Err(OffsetError::new(s)),
        }
    }
}
