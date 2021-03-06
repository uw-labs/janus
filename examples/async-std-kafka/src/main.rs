use anyhow::Error;
use async_std::prelude::*;
use futures::{SinkExt, TryFutureExt, TryStreamExt};
use janus::{Message, Publisher, Subscriber};
use janus_kafka::{
    KafkaPublisher, KafkaSubscriber, Offset, PublisherConfig, PublisherMessage, SubscriberConfig,
};
use structopt::StructOpt;

#[derive(StructOpt)]
enum Opts {
    Publish {
        /// Comma-separated list of brokers.
        #[structopt(short, long, default_value = "localhost:9092")]
        brokers: String,
        /// Topic to produce messages to.
        #[structopt(short, long)]
        topic: String,
        /// Maximum number of messages in-flight.
        #[structopt(short = "s", long, default_value = "1000")]
        buffer_size: usize,
    },
    Subscribe {
        /// Comma-separated list of brokers.
        #[structopt(short, long, default_value = "localhost:9092")]
        brokers: String,
        /// Consumer group name of subscriber.
        #[structopt(short, long, default_value = "janus")]
        group_id: String,
        /// Consumer offset, "earliest" or "latest"
        #[structopt(short, long, default_value)]
        offset: Offset,
        /// Comma-separated list of topics.
        #[structopt(short, long)]
        topics: String,
        /// Maximum number of messages in-flight.
        #[structopt(short = "s", long, default_value = "1000")]
        buffer_size: usize,
    },
}

#[async_std::main]
async fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    match opts {
        Opts::Publish {
            brokers,
            topic,
            buffer_size,
        } => {
            let config = PublisherConfig::default()
                .brokers(&brokers)
                .buffer_size(buffer_size);

            let (publisher, acker) = KafkaPublisher::new(config)?;

            publish_message(publisher, &topic)
                .try_join(janus_kafka::noop_publisher_ack_handler(acker).map_err(Error::new))
                .await?;
        }
        Opts::Subscribe {
            brokers,
            group_id,
            offset,
            topics,
            buffer_size,
        } => {
            let topics = &topics.split(',').collect::<Vec<&str>>();

            let config = SubscriberConfig::default()
                .brokers(&brokers)
                .group_id(&group_id)
                .offset(offset)
                .topics(topics)
                .buffer_size(buffer_size);

            let (subscriber, acker) = KafkaSubscriber::new(config)?;

            message_handler(subscriber)
                .try_join(janus_kafka::noop_subscriber_ack_handler(acker).map_err(Error::new))
                .await?;
        }
    }

    Ok(())
}

async fn publish_message<P>(mut publisher: P, topic: &str) -> Result<(), Error>
where
    P: Publisher<Message = PublisherMessage>,
{
    loop {
        let msg = PublisherMessage::new(b"hello", topic, None);

        publisher.send(msg).await?;
    }
}

async fn message_handler<M, S>(mut subscriber: S) -> Result<(), Error>
where
    M: Message,
    S: Subscriber<Message = M>,
{
    while let Some(m) = subscriber.try_next().await? {
        println!("Got message: {:?}", m.message());
        m.ack().await?;
    }
    Ok(())
}
