use std::error::Error;
use std::fmt;

/// Respresents all errors.
#[derive(Debug)]
pub enum KafkaError {
    /// Internal Kafka error
    Internal(rdkafka::error::KafkaError),
    /// Occurs when sending fails.
    Send(futures_channel::mpsc::SendError),
    /// Occurs when a future is canceled.
    Canceled(futures_channel::oneshot::Canceled),
}

impl fmt::Display for KafkaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            KafkaError::Internal(ref err) => err.fmt(f),
            KafkaError::Send(ref err) => err.fmt(f),
            KafkaError::Canceled(ref err) => err.fmt(f),
        }
    }
}

impl Error for KafkaError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match *self {
            KafkaError::Internal(ref err) => Some(err),
            KafkaError::Send(ref err) => Some(err),
            KafkaError::Canceled(ref err) => Some(err),
        }
    }
}

impl From<rdkafka::error::KafkaError> for KafkaError {
    fn from(err: rdkafka::error::KafkaError) -> Self {
        Self::Internal(err)
    }
}

impl From<futures_channel::mpsc::SendError> for KafkaError {
    fn from(err: futures_channel::mpsc::SendError) -> Self {
        Self::Send(err)
    }
}

impl From<futures_channel::oneshot::Canceled> for KafkaError {
    fn from(err: futures_channel::oneshot::Canceled) -> Self {
        Self::Canceled(err)
    }
}

/// OffsetError is raised when an `Offset` can't be created from a `String`.
#[derive(Debug)]
pub struct OffsetError(String);

impl OffsetError {
    pub(crate) fn new(offset: &str) -> Self {
        Self(offset.to_owned())
    }
}

impl fmt::Display for OffsetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Invalid offset: {}", self.0)
    }
}

impl Error for OffsetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}
