use std::future::Future;
use std::time::{Duration, Instant};

use crate::AsyncRuntime;

use futures_util::future::{self, FutureExt};

/// Runtime shim for Smol
#[derive(Debug)]
pub struct SmolRuntime;

impl AsyncRuntime for SmolRuntime {
    type Delay = future::Map<smol::Timer, fn(Instant)>;

    fn spawn<T>(task: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        smol::Task::spawn(task).detach()
    }

    fn delay_for(duration: Duration) -> Self::Delay {
        smol::Timer::after(duration).map(|_| ())
    }
}
