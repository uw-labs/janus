use anyhow::Error;
use async_std::prelude::*;
use futures::{SinkExt, TryFutureExt, TryStreamExt};
use janus::{Message, Publisher, Subscriber};
use janus_kafka::{
    KafkaPublisher, KafkaSubscriber, Offset, PublisherConfig, PublisherMessage, SmolRuntime,
    SubscriberConfig,
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
            let config = PublisherConfig { brokers: &brokers };

            let (publisher, acker) = KafkaPublisher::new(config, buffer_size)?;

            publish_message(publisher, &topic)
                .try_join(janus_kafka::noop_ack_handler(acker).map_err(Error::new))
                .await?;
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

            let (subscriber, acker): (KafkaSubscriber<SmolRuntime>, _) =
                KafkaSubscriber::new(config, topics, buffer_size)?;

            message_handler(subscriber)
                .try_join(janus::noop_ack_handler(acker).map_err(Error::new))
                .await?;
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
