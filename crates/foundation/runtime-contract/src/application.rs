use std::future::Future;
use std::pin::Pin;

use futures_core::Stream;

/// Object-safe async result used by consumer-owned Application Ports.
pub type ApplicationFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Backpressure-aware async subscription used without binding to Tokio or Tonic.
pub type ApplicationStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;
