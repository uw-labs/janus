use std::future::Future;
use std::thread::spawn;

use anyhow::Error;
use futures::executor::{block_on, block_on_stream};
use futures::SinkExt;
use janus::{AckHandler, Message, Publisher, Subscriber};
use janus_kafka::{
    KafkaPublisher, KafkaSubscriber, MessageExt, Offset, PublisherConfig, PublisherMessage,
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

fn main() -> Result<(), Error> {
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

            let threads = vec![
                spawn(move || {
                    let topic = topic.to_owned();

                    publish_message(publisher, &topic).unwrap();
                }),
                spawn(move || {
                    publisher_ack_handler(acker).unwrap();
                }),
            ];

            for thread in threads {
                thread.join().unwrap();
            }
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

            let threads = vec![
                spawn(move || {
                    message_handler(subscriber).unwrap();
                }),
                spawn(move || {
                    ack_handler(acker).unwrap();
                }),
            ];

            for thread in threads {
                thread.join().unwrap();
            }
        }
    }

    Ok(())
}

fn ack_handler<H>(acks: H) -> Result<(), Error>
where
    H: AckHandler,
{
    for ack in block_on_stream(acks) {
        ack?;
    }

    Ok(())
}

fn publish_message<P>(mut publisher: P, topic: &str) -> Result<(), Error>
where
    P: Publisher<Message = PublisherMessage>,
{
    use std::thread::sleep;
    use std::time::Duration;

    loop {
        let msg = PublisherMessage::new(b"hello", &topic, None);

        block_on(publisher.send(msg))?;

        sleep(Duration::from_millis(50));
    }
}

fn message_handler<M, S>(subscriber: S) -> Result<(), Error>
where
    M: Message,
    S: Subscriber<Message = M>,
{
    let messages = block_on_stream(subscriber);

    for message in messages {
        let m = message?;
        println!("Got message: {:?}", m.message());

        block_on(m.ack())?;
    }

    Ok(())
}

fn publisher_ack_handler<M, A>(handler: A) -> Result<(), A::Error>
where
    M: MessageExt,
    A: AckHandler,
    <A as janus::AckHandler>::Output: Future<Output = Result<M, (M, A::Error)>>,
{
    for ack in block_on_stream(handler) {
        let fut = ack?;
        block_on(fut).map_err(|(_, e)| e)?;
    }

    Ok(())
}
