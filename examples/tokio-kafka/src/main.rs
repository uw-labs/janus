use std::future::Future;

use anyhow::Error;
use futures::{SinkExt, TryFutureExt, TryStreamExt};
use janus::{AckHandler, Message, Publisher, Subscriber};
use janus_kafka::{
    KafkaPublisher, KafkaSubscriber, Offset, PublisherConfig, PublisherMessage, SubscriberConfig,
    TokioRuntime,
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    match opts {
        Opts::Publish {
            brokers,
            topic,
            buffer_size,
        } => {
            let config = PublisherConfig { brokers: &brokers };

            let (publisher, acker) = KafkaPublisher::new(config, buffer_size)?;

            tokio::try_join!(
                publish_message(publisher, &topic),
                janus_kafka::noop_ack_handler(acker).map_err(Error::new)
            )?;
        }
        Opts::Subscribe {
            brokers,
            group_id,
            offset,
            topics,
            buffer_size,
        } => {
            let config = SubscriberConfig {
                brokers: &brokers,
                group_id: &group_id,
                offset,
            };

            let topics = &topics.split(',').collect::<Vec<&str>>();

            let (subscriber, acker): (KafkaSubscriber<TokioRuntime>, _) =
                KafkaSubscriber::new(config, topics, buffer_size)?;

            tokio::try_join!(
                message_handler(subscriber),
                janus::noop_ack_handler(acker).map_err(Error::new)
            )?;
        }
    }

    Ok(())
}

async fn publish_message<P: Publisher<Message = PublisherMessage>>(
    mut publisher: P,
    topic: &str,
) -> Result<(), Error> {
    loop {
        let msg = PublisherMessage::new(b"hello", topic, None);

        publisher.send(msg).await?;
    }
}

async fn message_handler<M: Message, S: Subscriber<Message = M>>(
    mut subscriber: S,
) -> Result<(), Error> {
    while let Some(m) = subscriber.try_next().await? {
        println!("Got message: {:?}", m.message());
        m.ack().await?;
    }
    Ok(())
}
