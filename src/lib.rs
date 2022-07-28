//! # Single-event signaling for tokio applications with channels
//!
//! This library implements a simple shutdown pattern, similar to that
//! described by tokio's [mini-redis](https://tokio.rs/tokio/topics/shutdown)
//! tutorial. It uses [Arc] (and [Weak]) to reference count handles to
//! the shutdown controller.
//!
//! Shutdown works with controllers which can be created anywhere, and
//! handles, which hold a weak reference to controllers and can be used
//! to wait for a shutdown signal.
//!
//! ```
//! # use async_shutdown::Shutdown;
//! # tokio_test::block_on(async {
//! let controller = Shutdown::new();
//! let handle = controller.handle();
//!
//! // Notify children
//! controller.notify();
//!
//! // Will complete immediately
//! handle.wait().await
//!
//! # })
//! ```
//!
//! Dropping the last controller will cause the handle to complete immediately
//!
//! ```
//! # use async_shutdown::Shutdown;
//! # tokio_test::block_on(async {
//! let controller = Shutdown::new();
//! let handle = controller.handle();
//!
//! // Drop controller
//! drop(controller);
//!
//! // Will complete immediately
//! handle.wait().await
//!
//! # })
//! ```
//!
//! Controllers can wait on themselves, which cause them to complete immediately:
//!
//! ```
//! # use async_shutdown::Shutdown;
//! # tokio_test::block_on(async {
//! let controller = Shutdown::new();
//!
//! // Will complete immediately
//! controller.wait().await
//!
//! # })
//! ```
//!
//! Shutdown controllers hold either strong ([Arc]) or weak ([Weak]) references
//! to the underlying channel. When the last strong reference is dropped, the
//! channel will be considered shutdown. Channels also support explicit notification
//! (see [Shutdown::notify]).

use std::sync::{Arc, Weak};

mod channels;
use channels::{ShutdownReceiver, ShutdownSender};

#[derive(Debug)]
pub struct ShutdownHandle {
    channel: Weak<ShutdownSender>,
    receiver: ShutdownReceiver,
}

impl Clone for ShutdownHandle {
    fn clone(&self) -> Self {
        let channel = self.channel.clone();
        let receiver = self.channel.upgrade().map(|c| c.subscribe());

        Self {
            channel: channel,
            receiver: receiver.into(),
        }
    }
}

impl ShutdownHandle {
    fn new() -> Self {
        ShutdownHandle {
            channel: Weak::new(),
            receiver: None.into(),
        }
    }

    /// Wait for a shutdown signal to be sent.
    pub async fn wait(&mut self) {
        (&mut self.receiver).await
    }

    /// Request that attached objects shut down.
    pub fn notify(&self) {
        if let Some(channel) = self.channel.upgrade() {
            channel.send()
        }
    }

    /// Create a [Shutdown] manager object from this handle
    pub fn promote(self) -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Weak(self),
        }
    }

    pub fn is_shutodwn(&self) -> bool {
        self.receiver.is_terminated() || self.channel.upgrade().is_none()
    }

    pub fn guard(&self) -> Option<ShutdownGuard> {
        self.channel.upgrade().map(|c| ShutdownGuard::from_tx(c))
    }
}

/// A strong reference to a shutdown controller
///
/// There must be at least one strong-referenced controller
/// to keep the shutdown signalling system alive. When
/// the last strong referenced controller drops, all of the
/// connected receivers will also drop.
#[derive(Debug)]
pub struct ShutdownGuard {
    /// Reference-counted access to the broadcast::Sender. It must
    /// be Arc'd so that we can have Weak references, even though [broadcast::Sender] is [Clone].
    channel: Arc<ShutdownSender>,

    /// The receiver, if it has not been used already to get a signal.
    receiver: ShutdownReceiver,
}

impl Clone for ShutdownGuard {
    fn clone(&self) -> Self {
        Self::from_tx(Arc::clone(&self.channel))
    }
}

impl ShutdownGuard {
    pub fn new() -> Self {
        let (tx, rx) = channels::channel();
        Self {
            channel: Arc::new(tx),
            receiver: rx,
        }
    }

    pub fn handle(&self) -> ShutdownHandle {
        let channel = Arc::downgrade(&self.channel);
        let receiver = self.channel.subscribe();

        ShutdownHandle {
            channel: channel,
            receiver: receiver,
        }
    }

    fn into_handle(self) -> ShutdownHandle {
        ShutdownHandle {
            channel: Arc::downgrade(&self.channel),
            receiver: self.receiver,
        }
    }

    fn from_tx(tx: Arc<ShutdownSender>) -> Self {
        let rx = tx.subscribe();
        Self {
            channel: tx,
            receiver: rx,
        }
    }

    pub fn notify(&self) {
        self.channel.send()
    }

    pub fn is_shutodwn(&self) -> bool {
        self.receiver.is_terminated()
    }

    pub fn promote(self) -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Strong(self),
        }
    }
}

#[derive(Debug, Clone)]
enum InnerShutdown {
    Weak(ShutdownHandle),
    Strong(ShutdownGuard),
}

impl InnerShutdown {
    async fn wait(self) {
        match self {
            InnerShutdown::Weak(mut weak) => weak.wait().await,
            InnerShutdown::Strong(strong) => strong.into_handle().wait().await,
        }
    }

    fn notify(&self) {
        match self {
            InnerShutdown::Weak(weak) => weak.notify(),
            InnerShutdown::Strong(strong) => strong.notify(),
        }
    }

    fn weak(&self) -> InnerShutdown {
        match self {
            InnerShutdown::Weak(weak) => InnerShutdown::Weak(weak.clone()),
            InnerShutdown::Strong(strong) => InnerShutdown::Weak(strong.handle()),
        }
    }

    fn strong(&self) -> Option<InnerShutdown> {
        match self {
            InnerShutdown::Weak(weak) => weak.guard().map(|s| InnerShutdown::Strong(s)),
            InnerShutdown::Strong(strong) => Some(InnerShutdown::Strong(strong.clone())),
        }
    }
}

/// A controller for shutdown signals
///
/// This can hold either a strong or weak reference, and can convert between
/// the two reference types at will (see [Shutdown::into_handle] and
/// [Shutdown::into_guard]). When the last strong reference (guard) drops,
/// the shutdown event will be automatically triggered. Events can also be
/// manually triggered with [Shutdown::notify].
#[derive(Debug, Clone)]
pub struct Shutdown {
    inner: InnerShutdown,
}

impl Shutdown {
    /// New, strongly referenced shutdown manager.
    pub fn new() -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Strong(ShutdownGuard::new()),
        }
    }

    /// New, empty shutdown manager which is already shut down.
    pub fn empty() -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Weak(ShutdownHandle::new()),
        }
    }

    /// Wait until we receive a shutdown signal
    ///
    /// If you don't want to consume the [Shutdown] manager, use a [ShutdownHandle]
    /// instead.
    //TODO: This whole stack of objects could impl Future instead since it can only
    // be awaited a single time.
    pub async fn wait(self) {
        self.inner.wait().await
    }

    /// Wait until a sigint / ctrl-c signal occurs
    /// and then shutdown.
    pub async fn sigint(self) {
        let _ = tokio::signal::ctrl_c().await;
        self.inner.notify()
    }

    /// Request that all shutdown objects shut down.
    pub fn notify(self) {
        self.inner.notify()
    }

    /// Create a week reference handle to this shutdown trigger.
    pub fn handle(&self) -> Self {
        Shutdown {
            inner: self.inner.weak(),
        }
    }

    /// Unwrap this shutdown trigger and create a strong reference
    /// [ShutdownGuard]. If this is a weak reference and the trigger
    /// is already shutdown, this method returns [None].
    pub fn into_guard(self) -> Option<ShutdownGuard> {
        match self.inner {
            InnerShutdown::Weak(weak) => weak.guard(),
            InnerShutdown::Strong(strong) => Some(strong),
        }
    }

    /// Unwrap this shutdown trigger and create a weak reference
    /// [ShutdownHandle]. If this was the last strong reference,
    /// then the underlying shutdown notification will be triggered and
    /// all shutdown controllers will return immediately from .await.
    pub fn into_handle(self) -> ShutdownHandle {
        match self.inner {
            InnerShutdown::Weak(weak) => weak,
            InnerShutdown::Strong(strong) => strong.into_handle(),
        }
    }

    /// Convert this shutdown into a strong shutdown guard.
    pub fn guard(&self) -> Option<Self> {
        self.inner.strong().map(|s| Shutdown { inner: s })
    }
}

#[cfg(test)]
mod test {

    use futures::poll;
    use tokio::pin;

    use super::*;

    #[tokio::test]
    async fn wait_cancel_safe() {
        let s = Shutdown::new();
        let mut h = s.clone().into_handle();

        // Poll twice, should still be pending b/c we haven't dropped the receiver.
        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_pending(), "pending first attempt");
        }

        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_pending(), "pending after cancel");
        }

        drop(s);
    }

    #[tokio::test]
    async fn handle_only_returns_immediately() {
        let s = Shutdown::new();
        let mut h = s.into_handle();
        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_ready(), "ready first attempt");
        }
    }

    #[tokio::test]
    async fn shutdown_returns_immediately() {
        let s = Shutdown::new();
        {
            let f = s.wait();
            pin!(f);
            assert!(poll!(f).is_ready(), "ready first attempt");
        }
    }

    #[tokio::test]
    async fn shutdown_empty_returns_immediately() {
        let s = Shutdown::empty();
        {
            let f = s.wait();
            pin!(f);
            assert!(poll!(f).is_ready(), "ready first attempt");
        }
    }

    #[tokio::test]
    async fn ready_immediately_after_notify() {
        let s = Shutdown::new();
        let mut h = s.clone().into_handle();
        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_pending(), "pending first attempt");
        }

        s.notify();

        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_ready(), "ready after notify");
        }

        {
            let f = h.wait();
            pin!(f);
            assert!(poll!(f).is_ready(), "ready immediately");
        }
    }
}
