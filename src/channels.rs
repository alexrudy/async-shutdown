use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tokio::sync::broadcast;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

#[derive(Debug, Clone)]
pub(crate) struct ShutdownSender(broadcast::Sender<()>);

impl ShutdownSender {
    pub(crate) fn send(&self) {
        match self.0.send(()) {
            Ok(n) => {
                tracing::trace!(n, "shutdown notified");
            }
            Err(_) => {
                tracing::warn!("No receivers available for shutdown");
            }
        }
    }

    pub(crate) fn subscribe(&self) -> ShutdownReceiver {
        self.0.subscribe().into()
    }
}

impl From<broadcast::Sender<()>> for ShutdownSender {
    fn from(tx: broadcast::Sender<()>) -> Self {
        ShutdownSender(tx)
    }
}

impl Drop for ShutdownSender {
    fn drop(&mut self) {
        self.send()
    }
}

struct DebugLiteral<'s>(&'s str);

impl<'s> fmt::Debug for DebugLiteral<'s> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Wait for our [broadcast::Receiver] to get a message. We assume that effectively
/// any return from the receiver is "good enough". If an error occured, that usually
/// means that someone tried to send something and we missed it (lag would be difficult
/// to achieve here), or closed all of the senders, which is equivalent to dropping all
/// of the senders and indicates that we'd probably like to shut down.
async fn recv_once(mut rx: broadcast::Receiver<()>) {
    match rx.recv().await {
        Ok(()) => {
            tracing::trace!("shutdown notification rx");
        }
        Err(broadcast::error::RecvError::Closed) => {
            tracing::trace!("shutdown awaited closed handle");
        }
        Err(broadcast::error::RecvError::Lagged(_)) => {
            tracing::trace!("unexpected lag in shutdown rx")
        }
    }
}

pin_project! {

    /// A future which receives a single shutdown signal and from then
    /// on is fused to always return immediately.
    pub(crate) struct ShutdownReceiver {
        #[pin]
        inner: Option<BoxFuture<'static, ()>>,
    }
}

impl fmt::Debug for ShutdownReceiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownReceiver")
            .field(
                "inner",
                &self.inner.as_ref().and(Some(DebugLiteral("<Future>"))),
            )
            .finish()
    }
}

impl From<broadcast::Receiver<()>> for ShutdownReceiver {
    fn from(rx: broadcast::Receiver<()>) -> Self {
        ShutdownReceiver {
            inner: Some(Box::pin(recv_once(rx))),
        }
    }
}

impl From<Option<ShutdownReceiver>> for ShutdownReceiver {
    fn from(maybe: Option<ShutdownReceiver>) -> Self {
        ShutdownReceiver {
            inner: maybe.and_then(|rx| rx.inner),
        }
    }
}

impl Future for ShutdownReceiver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.as_mut().project().inner.as_pin_mut() {
            Some(fut) => match fut.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(_) => {
                    self.project().inner.set(None);
                    Poll::Ready(())
                }
            },
            None => Poll::Ready(()),
        }
    }
}

impl ShutdownReceiver {
    pub(crate) fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }
}

pub(crate) fn channel() -> (ShutdownSender, ShutdownReceiver) {
    let (tx, rx) = tokio::sync::broadcast::channel(1);
    (tx.into(), rx.into())
}

#[cfg(test)]
mod tests {

    use super::*;
    use futures::poll;
    use tokio::pin;

    #[tokio::test]
    async fn send_and_recv() {
        let (tx, rx) = channel();

        pin!(rx);

        assert!(poll!(&mut rx).is_pending(), "not yet ready");

        tx.send();

        assert!(poll!(&mut rx).is_ready(), "immediately ready");
    }

    fn assert_sync<T>(_t: T)
    where
        T: Sync,
    {
    }

    #[test]
    fn shutdown_is_sync() {
        let (tx, rx) = channel();
        assert_sync(tx);
        assert_sync(rx);
    }

    fn assert_send<T>(_t: T)
    where
        T: Send,
    {
    }

    #[test]
    fn shutdown_is_send() {
        let (tx, rx) = channel();
        assert_send(tx);
        assert_send(rx);
    }
}
