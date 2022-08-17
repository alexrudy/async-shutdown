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
//! handle.await
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
//! handle.await
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
//! controller.await
//!
//! # })
//! ```
//!
//! Shutdown controllers hold either strong ([Arc]) or weak ([Weak]) references
//! to the underlying channel. When the last strong reference is dropped, the
//! channel will be considered shutdown. Channels also support explicit notification
//! (see [Shutdown::notify]).

use std::{
    pin::Pin,
    sync::{Arc, Weak},
};

mod channels;
use channels::{ShutdownReceiver, ShutdownSender};
use pin_project_lite::pin_project;
use std::future::Future;

pin_project! {

    /// Weak reference to a shutdown guard.
    ///
    /// The handle should be held by tasks which want to shutdown as soon as everything else is done,
    /// such as background tasks. It will automatically resolve when all other references to the shutdown
    /// guard have been dropped, or when the shutdown signal is sent.
    #[derive(Debug)]
    pub struct ShutdownHandle {
        channel: Weak<ShutdownSender>,
        #[pin]
        receiver: ShutdownReceiver,
    }
}
impl Clone for ShutdownHandle {
    fn clone(&self) -> Self {
        let channel = self.channel.clone();
        let receiver = self.channel.upgrade().map(|c| c.subscribe());

        Self {
            channel,
            receiver: receiver.into(),
        }
    }
}

impl ShutdownHandle {
    /// Create a new shutdown handle, which is a weak reference to a shutdown
    /// guard. However, since no guard exists, this is an empty handle and polling it
    /// will immediately return as ready.
    fn new() -> Self {
        ShutdownHandle {
            channel: Weak::new(),
            receiver: None.into(),
        }
    }

    /// Request that attached objects shut down.
    pub fn notify(&self) {
        if let Some(channel) = self.channel.upgrade() {
            channel.send()
        }
    }

    pub async fn sigint(&mut self) {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = self => {},
        };
    }

    /// Check if the underlying guard has already shut down.
    pub fn is_shutodwn(&self) -> bool {
        self.receiver.is_terminated() || self.channel.upgrade().is_none()
    }

    /// Try to convert this into a strong reference guard.
    pub fn guard(&self) -> Option<ShutdownGuard> {
        self.channel.upgrade().map(ShutdownGuard::from_tx)
    }
}

impl Future for ShutdownHandle {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().receiver.poll(cx)
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
    /// Reference-counted access to the [tokio::sync::broadcast::Sender]. It must
    /// be Arc'd so that we can have Weak references, even though [tokio::sync::broadcast::Sender] is [Clone].
    channel: Arc<ShutdownSender>,

    /// The receiver, if it has not been used already to get a signal.
    receiver: ShutdownReceiver,
}

impl Clone for ShutdownGuard {
    fn clone(&self) -> Self {
        Self::from_tx(Arc::clone(&self.channel))
    }
}

impl Default for ShutdownGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl ShutdownGuard {
    /// Create a new, strongly referenced guard, which can be used to produce
    /// other strong guards or weak handles.
    pub fn new() -> Self {
        let (tx, rx) = channels::channel();
        Self {
            channel: Arc::new(tx),
            receiver: rx,
        }
    }

    /// Create a weak reference handle to this shutdown guard.
    pub fn handle(&self) -> ShutdownHandle {
        let channel = Arc::downgrade(&self.channel);
        let receiver = self.channel.subscribe();

        ShutdownHandle { channel, receiver }
    }

    /// Consume this guard and turn it into a weak shutdown handle. This re-uses
    /// the receiver from the shutdown guard, and so may be slightly more efficient.
    fn into_handle(self) -> ShutdownHandle {
        ShutdownHandle {
            channel: Arc::downgrade(&self.channel),
            receiver: self.receiver,
        }
    }

    /// Create a shutdown guard from a transmission channel.
    fn from_tx(tx: Arc<ShutdownSender>) -> Self {
        let rx = tx.subscribe();
        Self {
            channel: tx,
            receiver: rx,
        }
    }

    /// Notify subscribers that they should shut down.
    pub fn notify(&self) {
        self.channel.send()
    }

    /// Check if this receiver is already shut down.
    ///
    /// This will be false until something has awaited this shutdown instance.
    pub fn is_shutdown(&self) -> bool {
        self.receiver.is_terminated()
    }
}

#[derive(Debug, Clone)]
enum InnerShutdown {
    Weak(ShutdownHandle),
    Strong(ShutdownGuard),
}

impl InnerShutdown {
    fn notify(&self) {
        match self {
            InnerShutdown::Weak(weak) => weak.notify(),
            InnerShutdown::Strong(strong) => strong.notify(),
        }
    }

    fn is_shutdown(&self) -> bool {
        match self {
            InnerShutdown::Weak(weak) => weak.is_shutodwn(),
            InnerShutdown::Strong(strong) => strong.is_shutdown(),
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
            InnerShutdown::Weak(weak) => weak.guard().map(InnerShutdown::Strong),
            InnerShutdown::Strong(strong) => Some(InnerShutdown::Strong(strong.clone())),
        }
    }
}

pin_project! {

    /// A controller for shutdown signals
    ///
    /// This can hold either a strong or weak reference, and can convert between
    /// the two reference types at will (see [Shutdown::into_handle] and
    /// [Shutdown::into_guard]). When the last strong reference (guard) drops,
    /// the shutdown event will be automatically triggered. Events can also be
    /// manually triggered with [Shutdown::notify].
    ///
    /// If you want to control whether a shutdown guard is weakly held or strongly held,
    /// use the [ShutdownGuard] and [ShutdownHandle] types instead.
    #[derive(Debug, Clone)]
    pub struct Shutdown {

        inner: InnerShutdown,
    }

}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Shutdown {
    /// New, strongly referenced shutdown manager.
    ///
    /// Note that if this is the only shutdown guard, it will complete immediately (since all other guards
    /// have already shut down)
    ///  
    /// ```
    /// # use async_shutdown::Shutdown;
    /// # tokio_test::block_on(async {
    /// let handle = Shutdown::new();
    ///
    /// // Will complete immediately
    /// handle.await
    ///
    /// # })
    /// ```
    ///
    /// To use this effecitvely, you'll want to retain additional handles:
    /// ```
    /// # use async_shutdown::Shutdown;
    /// # use tokio::pin;
    /// # use futures::poll;
    /// # tokio_test::block_on(async {
    /// let handle = Shutdown::new();
    /// let guard = handle.clone();
    ///
    /// // Will not complete until the guard drops or .notify() is called.
    /// // handle.await
    /// pin!(handle);
    /// assert!(poll!(handle).is_pending());
    ///
    /// # })
    /// ```
    pub fn new() -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Strong(ShutdownGuard::new()),
        }
    }

    /// New, empty shutdown manager which is already shut down.
    ///
    /// When this [Shutdown] guard is polled or awaited, it will
    /// return immediately.
    ///
    /// ```
    /// # use async_shutdown::Shutdown;
    /// # tokio_test::block_on(async {
    /// let handle = Shutdown::empty();
    ///
    /// // Will complete immediately
    /// handle.await
    ///
    /// # })
    /// ```
    pub fn empty() -> Shutdown {
        Shutdown {
            inner: InnerShutdown::Weak(ShutdownHandle::new()),
        }
    }

    /// Wait until a sigint / ctrl-c signal occurs
    /// and then shutdown.
    pub async fn sigint(self) {
        let mut handle = self.into_handle();
        handle.sigint().await;
        handle.notify();
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

    /// Check if this Shutdown has already fired.
    ///
    /// This will be false until something has awaited this shutdown instance.
    pub fn is_shutdown(&self) -> bool {
        self.inner.is_shutdown()
    }

    /// Convert this shutdown into a strong shutdown guard.
    pub fn guard(&self) -> Option<Self> {
        self.inner.strong().map(|s| Shutdown { inner: s })
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let proj = self.project();

        match proj.inner {
            InnerShutdown::Strong(strong) => {
                *proj.inner = InnerShutdown::Weak(strong.handle());
                if let InnerShutdown::Weak(weak) = proj.inner {
                    Pin::new(weak).poll(cx)
                } else {
                    unreachable!("polling a strong handle should always downgrade.");
                }
            }
            InnerShutdown::Weak(weak) => Pin::new(weak).poll(cx),
        }
    }
}

impl From<ShutdownGuard> for Shutdown {
    fn from(guard: ShutdownGuard) -> Self {
        Shutdown {
            inner: InnerShutdown::Strong(guard),
        }
    }
}

impl From<ShutdownHandle> for Shutdown {
    fn from(handle: ShutdownHandle) -> Self {
        Shutdown {
            inner: InnerShutdown::Weak(handle),
        }
    }
}

#[cfg(test)]
mod test {

    use futures::poll;
    use tokio::pin;

    use super::*;

    #[tokio::test]
    async fn shutdown_as_future() {
        let s = Shutdown::new();
        let h = s.clone();

        // Poll twice, should still be pending b/c we haven't dropped the receiver.
        {
            pin!(h);
            assert!(poll!(&mut h).is_pending(), "pending first attempt");
            drop(s);
            assert!(poll!(&mut h).is_ready(), "ready after drop");
        }
    }

    #[tokio::test]
    async fn single_shutdown_as_future() {
        let h = Shutdown::new();

        // Polling shutdown implicitly weakens it and makes it ready instantly.
        {
            pin!(h);
            assert!(poll!(&mut h).is_ready(), "ready for first poll");
        }
    }

    #[tokio::test]
    async fn handle_as_future() {
        let s = Shutdown::new();
        let h = s.handle();
        {
            pin!(h);
            assert!(poll!(&mut h).is_pending(), "pending first attempt");
            drop(s);
            assert!(poll!(&mut h).is_ready(), "ready after drop attempt");
        }
    }

    #[tokio::test]
    async fn handle_only_returns_immediately_as_future() {
        let s = Shutdown::new();
        let h = s.into_handle();
        let h2 = h.clone();
        {
            pin!(h);
            assert!(poll!(&mut h).is_ready(), "ready first attempt");
        }

        // Keeping a second handle shows that they weakly reference.
        drop(h2);
    }

    #[tokio::test]
    async fn promote_handle() {
        let h = ShutdownHandle::new();

        let s: Shutdown = h.into();
        {
            pin!(s);
            assert!(poll!(&mut s).is_ready(), "Immediately ready")
        }
    }

    #[tokio::test]
    async fn handle_clone_alive() {
        let s = Shutdown::new();
        let h = s.clone().into_handle();
        let h2 = h.clone();
        let s2 = s.clone();
        {
            s.notify();
            pin!(h2);
            assert!(poll!(&mut h2).is_ready());
        }
        drop(s2);
    }

    #[tokio::test]
    async fn handle_to_guard() {
        let s = Shutdown::new();
        let h = s.clone().into_handle();
        {
            let g = h.guard();
            assert!(g.is_some());

            pin!(s);
            assert!(
                poll!(&mut s).is_pending(),
                "still waiting, guard was created"
            );
        }
    }

    #[test]
    fn handle_to_no_guard() {
        let h = ShutdownHandle::new();
        let g = h.guard();
        assert!(g.is_none(), "no guard");
    }

    #[tokio::test]
    async fn handle_notify() {
        let s = Shutdown::new();
        let h = s.handle();
        {
            let h = h.into_handle();
            h.notify();
            pin!(s);
            assert!(poll!(&mut s).is_ready(), "ready first attempt");
            // Handle can be kept alive after notify, not a problem.
            drop(h);
        }
    }

    #[tokio::test]
    async fn empty_shutdown_returns_immediately() {
        let s = Shutdown::empty();
        {
            pin!(s);
            assert!(poll!(&mut s).is_ready(), "ready first attempt");
        }
    }

    #[tokio::test]
    async fn shutdown_returns_immediately() {
        let s = Shutdown::new();
        let h = s.handle();
        {
            pin!(s);
            assert!(poll!(&mut s).is_ready(), "ready first attempt");
        }
        {
            pin!(h);
            assert!(
                poll!(&mut h).is_ready(),
                "handle should be ready if shutdown was polled."
            );
        }
    }

    #[tokio::test]
    async fn ready_immediately_after_notify_handle() {
        let s = Shutdown::new();
        let h = s.clone().into_handle();
        {
            pin!(h);
            assert!(poll!(&mut h).is_pending(), "pending first attempt");

            s.notify();

            assert!(poll!(&mut h).is_ready(), "ready after notify");

            assert!(poll!(&mut h).is_ready(), "ready immediately");
        }
    }

    #[tokio::test]
    async fn ready_immediately_after_notify_shutdown() {
        let s = Shutdown::new();
        let h = s.clone();
        {
            pin!(h);
            assert!(poll!(&mut h).is_pending(), "pending first attempt");

            s.notify();

            assert!(poll!(&mut h).is_ready(), "ready after notify");
        }
    }

    #[tokio::test]
    async fn ready_immediately_after_notify_guard() {
        let s = ShutdownGuard::new();
        let h = s.handle();
        {
            pin!(h);
            assert!(poll!(&mut h).is_pending(), "pending first attempt");

            s.notify();
            assert!(!s.is_shutdown());
            assert!(!h.is_shutodwn());

            assert!(poll!(&mut h).is_ready(), "ready after notify");
            assert!(h.is_shutodwn());

            let p: Shutdown = s.into();
            pin!(p);
            assert!(poll!(&mut p).is_ready(), "sender is ready after notify");
            assert!(p.is_shutdown());
        }
    }

    #[tokio::test]
    async fn sigint_stopped_by_notify() {
        let s = Shutdown::new();
        let h = s.clone();

        {
            let f = s.sigint();
            pin!(f);
            assert!(poll!(&mut f).is_pending(), "waiting for signal");

            h.notify();
            assert!(poll!(&mut f).is_ready(), "notified");
        }
    }

    fn assert_sync<T>(_t: T)
    where
        T: Sync,
    {
    }

    #[test]
    fn shutdown_is_sync() {
        let s = Shutdown::new();
        assert_sync(s);
    }

    fn assert_send<T>(_t: T)
    where
        T: Send,
    {
    }

    #[test]
    fn shutdown_is_send() {
        let s = Shutdown::new();
        assert_send(s);
    }
}
