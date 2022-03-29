//! # Janus
//!
//! Janus is a thin abstraction for synchronous/asynchronous messages publishing and consumption.
#![warn(
    missing_debug_implementations,
    missing_docs,
    rust_2018_idioms,
    unreachable_pub
)]

use std::error::Error;
use std::fmt::Debug;

use futures_channel::mpsc::{SendError, Sender};
use futures_core::Stream;
use futures_sink::Sink;
use futures_util::{sink::SinkExt, stream::TryStreamExt};

/// Provides a generic abstraction over a message.
pub trait Message: Debug {
    /// Returns the payload of the message.
    fn payload(&self) -> &[u8];
}

/// Wraps a message with a channel for sending acknowledgments.
#[derive(Debug)]
pub struct AckMessage<M> {
    message: M,
    acks_tx: Sender<M>,
}

impl<M> AckMessage<M> {
    /// Creates a new acknowledged message.
    pub fn new(message: M, acks_tx: Sender<M>) -> Self {
        Self { message, acks_tx }
    }

    /// Returns a reference to the underlying message.
    pub fn message(&self) -> &M {
        &self.message
    }

    /// Sends the message to the `AckHandler`.
    pub async fn ack(mut self) -> Result<(), SendError> {
        self.acks_tx.send(self.message).await?;
        Ok(())
    }
}

impl<M: Message> Message for AckMessage<M> {
    fn payload(&self) -> &[u8] {
        self.message().payload()
    }
}

/// Produces a stream of `Message`s.
pub trait Subscriber:
    Stream<Item = Result<AckMessage<<Self as Subscriber>::Message>, <Self as Subscriber>::Error>>
    + Unpin
{
    /// The type of `Message` that the subscriber will produce when successful.
    type Message;

    /// The type of `Error` that the subscriber will produce when it fails.
    type Error: Error + Send + Sync + 'static;
}

/// Publishes `Message`s via a sink.
pub trait Publisher:
    Sink<<Self as Publisher>::Message, Error = <Self as Publisher>::Error> + Unpin
{
    /// The type of `Message` that the publisher will produce when successful.
    type Message;

    /// The type of `Error` that the publisher will produce when it fails.
    type Error: Error + Send + Sync + 'static;
}

/// Produces a stream of acknowledgments from an associated `Publisher` or `Subscriber`.
pub trait AckHandler:
    Stream<Item = Result<<Self as AckHandler>::Output, <Self as AckHandler>::Error>> + Unpin
{
    /// The type of output that the acknowledhment handler will produce when it fails.
    type Output;

    /// The type of `Error` that the acknowledgment handler will produce when it fails.
    type Error: Error + Send + Sync + 'static;
}

/// Checks the status of an adapter.
pub trait Statuser {
    /// The type of `Error` that the acknowledgment handler will produce when it fails.
    type Error: Error + Send + Sync + 'static;

    /// Determines the status of the adapter.
    fn status(&self) -> Result<(), Self::Error>;
}

/// A convenience function to continuously processes acks until an error is
/// encountered.
pub async fn noop_ack_handler<A: AckHandler<Output = ()>>(mut handler: A) -> Result<(), A::Error> {
    while handler.try_next().await?.is_some() {}
    Ok(())
}
