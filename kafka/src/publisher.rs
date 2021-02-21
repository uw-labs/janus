use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::error::KafkaError;
use crate::MessageExt;

use futures_channel::mpsc::{self, Receiver, Sender};
use futures_core::Stream;
use futures_sink::Sink;
use janus::{AckHandler, Message, Publisher, Statuser};
use rdkafka::client::Client;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::types::RDKafkaType;

/// Publishes messages to Kafka
#[derive(Clone)]
pub struct KafkaPublisher {
    messages_tx: Sender<PublisherMessage>,
    config: PublisherConfig,
}

impl fmt::Debug for KafkaPublisher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaPublisher")
            .field("messages_tx", &self.messages_tx)
            .finish()
    }
}

impl KafkaPublisher {
    /// Creates a new publisher and acker.
    pub fn new(config: PublisherConfig) -> Result<(Self, PublisherAcker), KafkaError> {
        let producer: FutureProducer = config.client_config.create()?;

        let (messages_tx, messages_rx) = mpsc::channel(config.buffer_size);

        let acker = PublisherAcker::new(producer, messages_rx);

        Ok((
            Self {
                messages_tx,
                config,
            },
            acker,
        ))
    }

    /// Creates a KafkaPublisher healthcheck.
    pub fn status(&self) -> KafkaPublisherStatus {
        KafkaPublisherStatus::new(self.config.clone())
    }
}

impl Publisher for KafkaPublisher {
    type Message = PublisherMessage;
    type Error = KafkaError;
}

impl<M: MessageExt> Sink<M> for KafkaPublisher {
    type Error = <Self as Publisher>::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.messages_tx)
            .poll_ready(cx)
            .map_err(|e| e.into())
    }

    fn start_send(mut self: Pin<&mut Self>, item: M) -> Result<(), Self::Error> {
        Pin::new(&mut self.messages_tx)
            .start_send(PublisherMessage {
                payload: item.payload().to_vec(),
                topic: item.topic().to_owned(),
                key: item.key().map(String::from),
            })
            .map_err(|e| e.into())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.messages_tx)
            .poll_flush(cx)
            .map_err(|e| e.into())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(&mut self.messages_tx)
            .poll_close(cx)
            .map_err(|e| e.into())
    }
}

/// Allows a healthcheck to be performed on the Kafka publisher.
#[derive(Clone, Debug)]
pub struct KafkaPublisherStatus {
    config: PublisherConfig,
}

impl KafkaPublisherStatus {
    fn new(config: PublisherConfig) -> Self {
        Self { config }
    }
}

impl Statuser for KafkaPublisherStatus {
    type Error = KafkaError;

    fn status(&self) -> Result<(), Self::Error> {
        let native_config = self.config.client_config.create_native_config()?;

        let client = Client::new(
            &self.config.client_config,
            native_config,
            RDKafkaType::RD_KAFKA_PRODUCER,
            rdkafka::consumer::DefaultConsumerContext,
        )?;

        client.fetch_metadata(None, Some(std::time::Duration::from_secs(1)))?;

        Ok(())
    }
}

/// Acknowledges messages from the Publisher.
pub struct PublisherAcker {
    producer: FutureProducer,
    messages_rx: Receiver<PublisherMessage>,
}

impl PublisherAcker {
    fn new(producer: FutureProducer, messages_rx: Receiver<PublisherMessage>) -> Self {
        Self {
            producer,
            messages_rx,
        }
    }
}

impl fmt::Debug for PublisherAcker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublisherAcker")
            .field("messages_rx", &self.messages_rx)
            .finish()
    }
}

impl AckHandler for PublisherAcker {
    type Output = DeliveryFuture;
    type Error = KafkaError;
}

impl Stream for PublisherAcker {
    type Item = Result<<Self as AckHandler>::Output, <Self as AckHandler>::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.messages_rx).poll_next(cx) {
            Poll::Ready(Some(m)) => {
                let mut record = FutureRecord::to(m.topic()).payload(m.payload());

                if let Some(key) = m.key() {
                    record = record.key(key);
                }

                let delivery_fut = match self.producer.send_result(record) {
                    Ok(df) => df,
                    Err((e, _)) => return Poll::Ready(Some(Err(e.into()))),
                };

                Poll::Ready(Some(Ok(DeliveryFuture {
                    inner: delivery_fut,
                    message: Some(m),
                })))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for PublisherAcker {
    fn drop(&mut self) {
        self.producer.flush(rdkafka::util::Timeout::Never);
    }
}

/// A future returned when a Kafka message is produced
pub struct DeliveryFuture {
    inner: rdkafka::producer::future_producer::DeliveryFuture,
    message: Option<PublisherMessage>,
}

impl fmt::Debug for DeliveryFuture {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeliveryFuture").finish()
    }
}

impl Future for DeliveryFuture {
    type Output = Result<PublisherMessage, (PublisherMessage, KafkaError)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.inner).poll(cx).map(|res| match res {
            Ok(_) => Ok(self.message.take().unwrap()),
            Err(e) => Err((self.message.take().unwrap(), e.into())),
        })
    }
}

/// A `Message` to be published to Kafka.
#[derive(Debug)]
pub struct PublisherMessage {
    payload: Vec<u8>,
    topic: String,
    key: Option<String>,
}

impl Message for PublisherMessage {
    fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl MessageExt for PublisherMessage {
    fn topic(&self) -> &str {
        &self.topic
    }

    fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }
}

impl PublisherMessage {
    /// Creates a new PublisherMessage
    pub fn new(payload: &[u8], topic: &str, key: Option<&str>) -> Self {
        Self {
            payload: payload.to_vec(),
            topic: topic.to_owned(),
            key: key.map(String::from),
        }
    }
}

/// Configuration options for a Publisher
#[derive(Clone, Debug)]
pub struct PublisherConfig {
    /// Initial list of brokers as a CSV list of broker host or host:port
    client_config: ClientConfig,
    buffer_size: usize,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        let mut client_config = ClientConfig::new();
        client_config.set("partitioner", "fnv1a_random");
        client_config.set("message.send.max.retries", "0");
        client_config.set("queue.buffering.max.ms", "0");

        Self {
            client_config,
            buffer_size: 1,
        }
    }
}

impl PublisherConfig {
    /// Initial list of brokers as a CSV list of broker host or host:port
    pub fn brokers(mut self, brokers: &str) -> Self {
        self.client_config.set("bootstrap.servers", brokers);
        self
    }

    /// Capacity of messages channel, defaults to 1. Change this to increase producer throughput.
    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// Set a librdkafka config directly, for a list of available configuration, see:
    /// [configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
    pub fn set(mut self, key: &str, value: &str) -> Self {
        self.client_config.set(key, value);
        self
    }
}
