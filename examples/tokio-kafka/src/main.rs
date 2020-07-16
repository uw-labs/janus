use anyhow::Error;
use futures::{SinkExt, TryFutureExt, TryStreamExt};
use janus::{Message, Publisher, Subscriber};
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

            let publisher_task = tokio::spawn(publish_message(publisher, topic.to_owned()));
            let acker_task =
                tokio::spawn(janus_kafka::noop_publisher_ack_handler(acker).map_err(Error::new));

            tokio::try_join!(async { publisher_task.await.unwrap() }, async {
                acker_task.await.unwrap()
            })?;
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

            let handler_task = tokio::spawn(message_handler(subscriber));
            let acker_task =
                tokio::spawn(janus_kafka::noop_subscriber_ack_handler(acker).map_err(Error::new));

            tokio::try_join!(async { handler_task.await.unwrap() }, async {
                acker_task.await.unwrap()
            })?;
        }
    }

    Ok(())
}

async fn publish_message<P>(mut publisher: P, topic: String) -> Result<(), Error>
where
    P: Publisher<Message = PublisherMessage>,
{
    let key = "foo";

    for _ in 1..=20i32 {
        let msg = PublisherMessage::new(b"hello", &topic, Some(key));

        publisher.send(msg).await?;
    }
    Ok(())
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
