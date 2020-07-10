use std::collections::HashSet;
use std::iter::FromIterator;

use futures_util::{
    sink::SinkExt,
    stream::{StreamExt, TryStreamExt},
};
use janus::{Message, Statuser};
use janus_kafka::{
    KafkaPublisher, KafkaSubscriber, Offset, PublisherConfig, PublisherMessage, SubscriberConfig,
    TokioRuntime,
};

const BROKERS: &str = "localhost:9092";
const TOPIC: &str = "end.to.end";
const GROUP_ID: &str = "janus";

const BUFFER_SIZE: usize = 10;

#[tokio::test]
async fn end_to_end() {
    let (sent_tx, sent_rx) = futures_channel::oneshot::channel();

    let pub_config = PublisherConfig { brokers: BROKERS };

    let (mut publisher, mut pub_acker) = KafkaPublisher::new(pub_config, BUFFER_SIZE).unwrap();

    publisher.status().unwrap();

    let messages = vec![
        String::from("message-1"),
        String::from("message-2"),
        String::from("message-3"),
    ];

    let publish_messages = async {
        for message in &messages {
            let msg = PublisherMessage::new(message.as_bytes(), TOPIC, Some("msg"));

            publisher.send(msg).await.unwrap();
        }
    };

    let handle_pub_acks = async {
        for _ in &messages {
            if let Some(fut) = pub_acker.try_next().await.unwrap() {
                fut.await.unwrap();
            }
        }

        sent_tx.send(()).unwrap();
    };

    let sub_config = SubscriberConfig {
        brokers: BROKERS,
        group_id: GROUP_ID,
        offset: Offset::Earliest,
    };

    let (mut subscriber, mut sub_acker): (KafkaSubscriber<TokioRuntime>, _) =
        KafkaSubscriber::new(sub_config, &[TOPIC], BUFFER_SIZE).unwrap();

    subscriber.status().unwrap();

    let (messages_tx, messages_rx) = futures_channel::mpsc::channel(messages.len());

    let subscribe_messages = async {
        // Wait for messages to be published before subscribing.
        sent_rx.await.unwrap();

        let mut messages_tx = messages_tx.clone();

        for _ in &messages {
            if let Some(msg) = subscriber.try_next().await.unwrap() {
                messages_tx
                    .send(String::from_utf8(msg.payload().to_vec()).unwrap())
                    .await
                    .unwrap();

                msg.ack().await.unwrap();
            }
        }
    };

    let handle_sub_acks = async {
        for _ in &messages {
            if let Some(_) = sub_acker.try_next().await.unwrap() {}
        }
    };

    tokio::join!(
        subscribe_messages,
        handle_sub_acks,
        publish_messages,
        handle_pub_acks
    );

    let messages_from_kafka = messages_rx.take(messages.len()).collect::<Vec<_>>().await;

    let expected_messages: HashSet<_> = HashSet::from_iter(messages.iter().cloned());
    let actual_messages: HashSet<_> =
        HashSet::from_iter(messages_from_kafka.iter().map(|x| x.clone()));

    assert_eq!(expected_messages, actual_messages);
}
