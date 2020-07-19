use std::fmt;
use std::ops::DerefMut;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::error::{KafkaError, OffsetError};
use crate::{AsyncRuntime, Config, IntoConfig, MessageExt};

use futures_channel::mpsc::{self, Receiver, Sender};
use futures_core::Stream;
use janus::{AckHandler, AckMessage, Message, Statuser, Subscriber};
use owning_ref::OwningHandle;
use rdkafka::client::Client;
use rdkafka::consumer::{Consumer, DefaultConsumerContext, MessageStream, StreamConsumer};
use rdkafka::message::Message as _;
use rdkafka::topic_partition_list::{self, TopicPartitionList};
use rdkafka::types::RDKafkaType;

/// Consumes messages from Kafka
pub struct KafkaSubscriber<
    #[cfg(feature = "tokio-rt")] R = rdkafka::util::TokioRuntime,
    #[cfg(not(feature = "tokio-rt"))] R,
> where
    R: AsyncRuntime,
{
    upstream:
        OwningHandle<Arc<StreamConsumer>, Box<MessageStream<'static, DefaultConsumerContext, R>>>,
    acks_tx: Sender<SubscriberMessage>,
    config: Config,
}

impl<R: AsyncRuntime> KafkaSubscriber<R> {
    /// Creates a new consumer and acker.
    pub fn new<C: IntoConfig>(
        config: C,
        topics: &[&str],
        buffer_size: usize,
    ) -> Result<(Self, SubscriberAcker), KafkaError> {
        let config = config.into_config();

        let consumer: StreamConsumer = config.create()?;

        consumer.subscribe(topics)?;

        let consumer = Arc::new(consumer);

        let (acks_tx, acks_rx) = mpsc::channel(buffer_size);

        let acker = SubscriberAcker {
            consumer: consumer.clone(),
            acks_rx,
        };

        Ok((
            Self {
                upstream: OwningHandle::new_with_fn(consumer, |c| {
                    let cf = unsafe { &*c };

                    Box::new(cf.start_with_runtime(Duration::from_millis(100), false))
                }),
                acks_tx,
                config,
            },
            acker,
        ))
    }

    /// Creates a KafkaSubscriber healthcheck.
    pub fn status(&self) -> KafkaSubscriberStatus {
        KafkaSubscriberStatus::new(self.config.clone())
    }
}

impl<R: AsyncRuntime> fmt::Debug for KafkaSubscriber<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaSubscriber").finish()
    }
}

impl<R: AsyncRuntime> Subscriber for KafkaSubscriber<R> {
    type Message = SubscriberMessage;
    type Error = KafkaError;
}

impl<R: AsyncRuntime> Stream for KafkaSubscriber<R> {
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
#[derive(Clone, Debug)]
pub struct KafkaSubscriberStatus {
    config: Config,
}

impl KafkaSubscriberStatus {
    fn new(config: Config) -> Self {
        Self { config }
    }
}

impl Statuser for KafkaSubscriberStatus {
    type Error = KafkaError;

    fn status(&self) -> Result<(), Self::Error> {
        let native_config = self.config.create_native_config()?;

        let client = Client::new(
            &self.config,
            native_config,
            RDKafkaType::RD_KAFKA_CONSUMER,
            DefaultConsumerContext,
        )?;

        client.fetch_metadata(None, Some(std::time::Duration::from_secs(1)))?;

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
                tpl.add_partition_offset(
                    &m.topic,
                    m.partition,
                    topic_partition_list::Offset::Offset(m.offset + 1),
                );

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
#[derive(Debug, Default)]
pub struct SubscriberConfig<'a> {
    /// Initial list of brokers as a CSV list of broker host or host:port
    pub brokers: &'a str,
    /// Client group id string. All clients sharing the same group.id belong to the same group.
    pub group_id: &'a str,
    /// Position for the offset when no initial value.
    pub offset: Offset,
}

impl<'a> IntoConfig for SubscriberConfig<'a> {
    fn into_config(self) -> Config {
        let mut config = Config::new();

        config.set("bootstrap.servers", self.brokers);
        config.set("group.id", self.group_id);
        config.set("auto.offset.reset", &self.offset.to_string());
        config.set("enable.auto.commit", "true");
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.offset.store", "false");
        config.set("auto.commit.interval.ms", "500");

        config
    }
}

/// Position for the offset when no initial value.
#[derive(Debug)]
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
