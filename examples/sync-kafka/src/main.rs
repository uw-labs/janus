use std::future::Future;
use std::thread::spawn;

use anyhow::Error;
use futures::executor::{block_on, block_on_stream};
use futures::SinkExt;
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

fn main() -> Result<(), Error> {
    let opts = Opts::from_args();

    match opts {
        Opts::Publish {
            brokers,
            topic,
            buffer_size,
        } => {
            let config = PublisherConfig { brokers: &brokers };

            let (publisher, acker) = KafkaPublisher::new(config, buffer_size)?;

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
            let config = SubscriberConfig {
                brokers: &brokers,
                group_id: &group_id,
                offset,
            };

            let topics = &topics.split(',').collect::<Vec<&str>>();

            let (subscriber, acker): (KafkaSubscriber<TokioRuntime>, _) =
                KafkaSubscriber::new(config, topics, buffer_size).unwrap();

            let threads = vec![
                spawn(move || {
                    let rt = tokio::runtime::Builder::new()
                        .threaded_scheduler()
                        .enable_all()
                        .build()
                        .unwrap();

                    rt.enter(|| message_handler(subscriber).unwrap());
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

fn ack_handler<H: AckHandler>(acks: H) -> Result<(), Error> {
    for ack in block_on_stream(acks) {
        ack?;
    }

    Ok(())
}

fn publish_message<P: Publisher<Message = PublisherMessage>>(
    mut publisher: P,
    topic: &str,
) -> Result<(), Error> {
    use std::thread::sleep;
    use std::time::Duration;

    loop {
        let msg = PublisherMessage::new(b"hello", &topic, None);

        block_on(publisher.send(msg))?;

        sleep(Duration::from_millis(50));
    }
}

fn message_handler<M: Message, S: Subscriber<Message = M>>(subscriber: S) -> Result<(), Error> {
    let messages = block_on_stream(subscriber);

    for message in messages {
        let m = message?;
        println!("Got message: {:?}", m.message());

        block_on(m.ack())?;
    }

    Ok(())
}

fn publisher_ack_handler<A: AckHandler>(handler: A) -> Result<(), A::Error>
where
    <A as janus::AckHandler>::Output: Future<Output = Result<(), A::Error>>,
{
    for ack in block_on_stream(handler) {
        let fut = ack?;
        block_on(fut)?;
    }

    Ok(())
}
