//! Cooperative cancellation token + runtime-agnostic `race` combinator.
//!
//! A [`CancellationToken`] lets one task (e.g. a connection's input loop or a
//! listener reader) wait on a socket `recv` and be woken immediately by
//! `close()` instead of polling on a fixed 100 ms tick — removing the
//! per-idle-connection ~10 Hz timer churn. Built on the permit-storing
//! [`super::Notify`], so cancellation is race-safe with waiter registration.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{Notify, NotifyFuture};

/// Cooperative cancellation token. `cancel()` wakes waiters; each recv loop
/// holds one and races its socket `recv` against [`CancellationToken::cancelled`].
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    cancelled: AtomicBool,
    notify: Notify,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Trigger cancellation: set the flag and wake the waiting `cancelled()`
    /// future. The permit-storing `Notify` makes this race-safe with a waiter
    /// that registers after `cancel` — the stored permit resolves it.
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::Release);
        self.inner.notify.notify_waiters();
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// A future that resolves once the token is cancelled.
    pub fn cancelled(&self) -> Cancelled<'_> {
        Cancelled {
            token: self,
            notified: self.inner.notify.notified(),
        }
    }
}

/// Future that resolves when its token is cancelled.
pub struct Cancelled<'a> {
    token: &'a CancellationToken,
    notified: NotifyFuture<'a>,
}

impl Future for Cancelled<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        if Pin::new(&mut this.notified).poll(cx).is_ready() {
            return Poll::Ready(());
        }
        // Re-check AFTER the waiter slot is published. `notify_waiters`
        // stores a *single* shared backstop permit and early-returns when
        // `waiter_count == 0`; a sibling re-poller racing this same poll
        // (its own `is_cancelled` check landed just before the flag store)
        // can consume that permit via the fast path, so a waiter registering
        // a beat *after* the broadcast sees permit=0 on both checks and
        // parks forever on an already-cancelled token — observed as
        // `cancel_wakes_every_parked_waiter` flaking with
        // `cancelled=true, flags=[true,false,...], notify=(0,0,N)`.
        // The terminal flag, not the permit, is the source of truth for
        // cancellation; re-reading it once the registration is visible
        // closes the window regardless of who consumed the permit. The
        // NotifyFuture deregisters itself via `Drop` on return.
        if this.token.is_cancelled() {
            return Poll::Ready(());
        }
        Poll::Pending
    }
}

/// Which future completed first.
pub enum RaceOutcome<A, B> {
    /// The first future completed.
    First(A),
    /// The second future completed.
    Second(B),
}

/// Wait for two futures and return the first to complete; the loser is dropped.
///
/// Runtime-agnostic: both futures are polled in turn on each wakeup, so it runs
/// on any executor without backend-specific `select!`. Both must be `Unpin`
/// (they are re-created per loop iteration and dropped when the other wins, so
/// no pinned state is lost).
pub struct Race<A, B> {
    a: A,
    b: B,
}

impl<A, B> Race<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<A: Future + Unpin, B: Future + Unpin> Future for Race<A, B> {
    type Output = RaceOutcome<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(out) = Pin::new(&mut this.a).poll(cx) {
            return Poll::Ready(RaceOutcome::First(out));
        }
        if let Poll::Ready(out) = Pin::new(&mut this.b).poll(cx) {
            return Poll::Ready(RaceOutcome::Second(out));
        }
        Poll::Pending
    }
}

/// Await [`Race`]; convenience for `Race::new(a, b).await`.
pub async fn race<A, B>(a: A, b: B) -> RaceOutcome<A::Output, B::Output>
where
    A: Future + Unpin,
    B: Future + Unpin,
{
    Race::new(a, b).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{block_on, spawn_task, yield_now};

    /// Wait (bounded) for the spawned waiter to finish, backend-agnostic: the
    /// `JoinHandle::await` output type differs between tokio (`Result`) and
    /// detached tasks, so we signal via a shared flag instead.
    ///
    /// On failure the panic carries the diagnostic context passed by the
    /// caller (cancelled state / done flags) — a bare "did not complete"
    /// could not distinguish a lost wakeup from a starved budget.
    async fn wait_done(done: &AtomicBool, ctx: &str) {
        for _ in 0..100_000 {
            if done.load(Ordering::Acquire) {
                return;
            }
            yield_now().await;
        }
        panic!("waiter did not complete — {ctx}");
    }

    #[test]
    fn cancelled_resolves_when_cancelled() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        block_on(async move {
            let t = token.clone();
            let done = Arc::new(AtomicBool::new(false));
            let d = done.clone();
            drop(spawn_task(async move {
                t.cancelled().await;
                d.store(true, Ordering::Release);
            }));
            yield_now().await; // let the waiter arm its waker
            token.cancel();
            wait_done(&done, "single-waiter").await;
        });
    }

    #[test]
    fn cancelled_fires_before_registration() {
        // cancel() before cancelled() is ever polled must still resolve it
        // (permit-storing Notify) — no lost wake.
        let token = CancellationToken::new();
        block_on(async move {
            token.cancel();
            token.cancelled().await;
        });
    }

    #[test]
    fn cancel_wakes_every_parked_waiter() {
        // Several tasks race their own recv against the same token (one per
        // reuseport worker / per connection half). A single-waiter `Notify`
        // woke only the last one to register and left the rest parked
        // forever, so `close()` never drained the pool.
        const WAITERS: usize = 4;
        let token = CancellationToken::new();
        block_on(async move {
            let done: Vec<Arc<AtomicBool>> = (0..WAITERS)
                .map(|_| Arc::new(AtomicBool::new(false)))
                .collect();
            for flag in &done {
                let t = token.clone();
                let f = flag.clone();
                drop(spawn_task(async move {
                    match race(std::future::pending::<u32>(), t.cancelled()).await {
                        RaceOutcome::First(_) => {}
                        RaceOutcome::Second(_) => f.store(true, Ordering::Release),
                    }
                }));
            }
            // Let every waiter arm its waker before cancelling.
            for _ in 0..64 {
                yield_now().await;
            }
            token.cancel();
            for (i, flag) in done.iter().enumerate() {
                wait_done(
                    flag,
                    &format!(
                        "waiter {i}: cancelled={} flags={:?} notify={:?}",
                        token.is_cancelled(),
                        done.iter()
                            .map(|f| f.load(Ordering::Acquire))
                            .collect::<Vec<_>>(),
                        token.inner.notify.debug_state()
                    ),
                )
                .await;
            }
        });
    }

    #[test]
    fn race_returns_second_when_cancelled() {
        let token = CancellationToken::new();
        block_on(async move {
            let t = token.clone();
            let done = Arc::new(AtomicBool::new(false));
            let d = done.clone();
            drop(spawn_task(async move {
                // Recv future never completes; only cancellation resolves it.
                match race(std::future::pending::<u32>(), t.cancelled()).await {
                    RaceOutcome::First(_) => {}
                    RaceOutcome::Second(_) => d.store(true, Ordering::Release),
                }
            }));
            yield_now().await;
            token.cancel();
            wait_done(&done, "single-waiter").await;
        });
    }
}
