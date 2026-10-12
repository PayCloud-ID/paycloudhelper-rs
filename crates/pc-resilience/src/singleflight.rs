//! In-flight call de-duplication ("singleflight").
//!
//! mirrors: Go's `golang.org/x/sync/singleflight` usage - concurrent callers
//! sharing a key collapse onto a single execution of the underlying work and
//! all receive the shared result. Implemented with a [`DashMap`] of
//! [`Shared`](futures::future::Shared) futures (the `DashMap<K, Shared<Future>>`
//! option from plan 03 PC-2).
//!
//! A flight is cleared by a drop guard on every exit path of a caller
//! (completion, cancellation, panic), so a cancelled or failed flight is never
//! kept and served to a later caller.

use std::future::Future;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};

/// One in-flight execution for a key. `id` tells it apart from a newer flight
/// registered for the same key; `waiters` counts the callers still awaiting it.
struct Flight<T> {
    id: u64,
    future: Shared<BoxFuture<'static, T>>,
    waiters: usize,
}

/// De-duplicates concurrent calls sharing a key so the guarded future runs
/// exactly once while it is in flight; all concurrent callers receive a clone
/// of its result.
///
/// This de-duplicates only *concurrent* calls: once the shared future
/// completes, its entry is removed and a subsequent call re-runs the work (it
/// is not a result cache). The value type `T` must be `Clone` so every caller
/// can receive the outcome.
pub struct Singleflight<K, T> {
    calls: DashMap<K, Flight<T>>,
    next_id: AtomicU64,
}

impl<K, T> Default for Singleflight<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, T> Singleflight<K, T>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    T: Clone + Send + 'static,
{
    /// Create an empty singleflight group.
    #[must_use]
    pub fn new() -> Self {
        Self {
            calls: DashMap::new(),
            next_id: AtomicU64::new(0),
        }
    }

    #[cfg(test)]
    fn in_flight(&self) -> usize {
        self.calls.len()
    }

    /// Run `fut` de-duplicated by `key`.
    ///
    /// If another call for the same `key` is already in flight, `fut` is not
    /// polled at all; this caller awaits the in-flight future and receives a
    /// clone of its result. Otherwise this call registers `fut` and drives it.
    ///
    /// Whichever caller finishes the flight removes it at once, so a later call
    /// re-runs the work and a failed result is never served again. If a caller
    /// is cancelled (its future is dropped, e.g. by a request budget) the flight
    /// is removed when its last waiter leaves, which drops the abandoned work.
    /// A panic in the shared future panics every waiter and the last one removes
    /// the entry.
    pub async fn do_call<Fut>(&self, key: K, fut: Fut) -> T
    where
        Fut: Future<Output = T> + Send + 'static,
    {
        // Register or join under the shard lock; the lock is dropped before awaiting.
        let (id, future) = match self.calls.entry(key.clone()) {
            Entry::Occupied(mut e) => {
                let flight = e.get_mut();
                flight.waiters += 1;
                (flight.id, flight.future.clone())
            }
            Entry::Vacant(e) => {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let future = fut.boxed().shared();
                e.insert(Flight {
                    id,
                    future: future.clone(),
                    waiters: 1,
                });
                (id, future)
            }
        };
        let mut guard = FlightGuard {
            calls: &self.calls,
            key,
            id,
            finished: false,
        };
        let result = future.await;
        guard.finished = true;
        result
    }
}

/// Leaves a flight on every exit path: completion, cancellation and panic.
struct FlightGuard<'a, K: Eq + Hash, T> {
    calls: &'a DashMap<K, Flight<T>>,
    key: K,
    id: u64,
    finished: bool,
}

impl<K: Eq + Hash, T> Drop for FlightGuard<'_, K, T> {
    fn drop(&mut self) {
        let (id, finished) = (self.id, self.finished);
        // A finished flight is removed at once. An unfinished one is removed when
        // its last waiter leaves. A different id is a newer flight for the same
        // key: never touch it.
        self.calls.remove_if_mut(&self.key, |_, flight| {
            if flight.id != id {
                return false;
            }
            flight.waiters = flight.waiters.saturating_sub(1);
            finished || flight.waiters == 0
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Notify;

    #[tokio::test]
    async fn runs_underlying_future_exactly_once_under_concurrency() {
        let sf: Arc<Singleflight<String, u32>> = Arc::new(Singleflight::new());
        let runs = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..32 {
            let sf = sf.clone();
            let runs = runs.clone();
            handles.push(tokio::spawn(async move {
                sf.do_call("shared-key".to_string(), async move {
                    // Sleep so every spawned caller attaches to the same
                    // in-flight future before it resolves.
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    runs.fetch_add(1, Ordering::SeqCst);
                    42_u32
                })
                .await
            }));
        }

        for h in handles {
            assert_eq!(h.await.unwrap(), 42);
        }
        // The underlying future ran exactly once for all 32 callers.
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn distinct_keys_run_independently() {
        let sf: Singleflight<u32, u32> = Singleflight::new();
        let runs = Arc::new(AtomicUsize::new(0));
        let r = runs.clone();
        let a = sf.do_call(1, async move {
            r.fetch_add(1, Ordering::SeqCst);
            10
        });
        let r = runs.clone();
        let b = sf.do_call(2, async move {
            r.fetch_add(1, Ordering::SeqCst);
            20
        });
        let (ra, rb) = futures::future::join(a, b).await;
        assert_eq!((ra, rb), (10, 20));
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn re_runs_after_completion() {
        let sf: Singleflight<&str, u32> = Singleflight::new();
        let runs = Arc::new(AtomicUsize::new(0));
        for _ in 0..3 {
            let r = runs.clone();
            let v = sf
                .do_call("k", async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    99_u32
                })
                .await;
            assert_eq!(v, 99);
        }
        // Sequential (non-concurrent) calls each re-run: it is not a cache.
        assert_eq!(runs.load(Ordering::SeqCst), 3);
    }

    /// The 2026-10-11 staging incident: the owner is cancelled by its request budget.
    #[tokio::test]
    async fn owner_cancelled_mid_flight_next_caller_starts_a_fresh_fetch() {
        let sf: Singleflight<&str, u32> = Singleflight::new();
        let runs = Arc::new(AtomicUsize::new(0));

        let r = runs.clone();
        let owner = tokio::time::timeout(
            Duration::from_millis(20),
            sf.do_call("k", async move {
                r.fetch_add(1, Ordering::SeqCst);
                std::future::pending::<u32>().await
            }),
        )
        .await;
        assert!(owner.is_err(), "owner must time out");
        assert_eq!(
            sf.in_flight(),
            0,
            "a flight nobody waits on must be dropped"
        );

        let r = runs.clone();
        let v = sf
            .do_call("k", async move {
                r.fetch_add(1, Ordering::SeqCst);
                7_u32
            })
            .await;
        assert_eq!(v, 7);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "the next caller must run its own fetch"
        );
        assert_eq!(sf.in_flight(), 0);
    }

    /// The owner is cancelled while a joiner waits; the joiner completes it with an error.
    /// The error must not be served to anyone who arrives later.
    #[tokio::test]
    async fn error_finished_by_a_joiner_is_not_served_to_a_later_caller() {
        let sf: Arc<Singleflight<&'static str, Result<u32, String>>> =
            Arc::new(Singleflight::new());
        let release = Arc::new(Notify::new());
        let runs = Arc::new(AtomicUsize::new(0));

        let (s, rel, r) = (sf.clone(), release.clone(), runs.clone());
        let owner = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(50),
                s.do_call("k", async move {
                    r.fetch_add(1, Ordering::SeqCst);
                    rel.notified().await;
                    Err::<u32, String>("vendor timeout".into())
                }),
            )
            .await
        });
        while sf.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        let s = sf.clone();
        let joiner = tokio::spawn(async move { s.do_call("k", async { Ok(0) }).await });
        tokio::task::yield_now().await;

        assert!(owner.await.unwrap().is_err(), "owner must time out");
        assert_eq!(
            sf.in_flight(),
            1,
            "the joiner still waits, so the flight stays"
        );
        release.notify_one();
        assert_eq!(joiner.await.unwrap(), Err("vendor timeout".to_string()));
        assert_eq!(
            sf.in_flight(),
            0,
            "a finished flight must be removed by whoever finished it"
        );

        let r = runs.clone();
        let later = sf
            .do_call("k", async move {
                r.fetch_add(1, Ordering::SeqCst);
                Ok(1)
            })
            .await;
        assert_eq!(later, Ok(1), "a later caller must not get the stored error");
        assert_eq!(runs.load(Ordering::SeqCst), 2);
    }

    /// Every caller gives up: no entry may leak and the abandoned work is dropped.
    #[tokio::test]
    async fn all_callers_cancelled_leaves_no_entry_and_drops_the_work() {
        struct DropFlag(Arc<AtomicUsize>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let sf: Arc<Singleflight<&'static str, u32>> = Arc::new(Singleflight::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let flag = DropFlag(dropped.clone());

        let s = sf.clone();
        let a = tokio::spawn(async move {
            tokio::time::timeout(
                Duration::from_millis(20),
                s.do_call("k", async move {
                    let _flag = flag;
                    std::future::pending::<u32>().await
                }),
            )
            .await
        });
        while sf.in_flight() == 0 {
            tokio::task::yield_now().await;
        }
        let s = sf.clone();
        let b = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_millis(40), s.do_call("k", async { 0 })).await
        });
        assert!(a.await.unwrap().is_err());
        assert!(b.await.unwrap().is_err());
        assert_eq!(sf.in_flight(), 0);
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "the abandoned future must be dropped"
        );
    }

    /// A slow waiter of an old flight must never remove the newer flight for the same key.
    #[tokio::test]
    async fn a_late_waiter_never_removes_a_newer_flight() {
        let sf: Singleflight<&str, u32> = Singleflight::new();
        let release = Arc::new(Notify::new());

        let rel = release.clone();
        let mut owner = Box::pin(sf.do_call("k", async move {
            rel.notified().await;
            1
        }));
        let mut joiner = Box::pin(sf.do_call("k", async { 99 }));
        assert!(futures::poll!(owner.as_mut()).is_pending());
        assert!(futures::poll!(joiner.as_mut()).is_pending());

        release.notify_one();
        assert_eq!(owner.await, 1); // the owner finishes and removes flight 1

        let mut newer = Box::pin(sf.do_call("k", std::future::pending::<u32>()));
        assert!(futures::poll!(newer.as_mut()).is_pending());
        assert_eq!(sf.in_flight(), 1, "flight 2 registered");

        assert_eq!(
            joiner.await,
            1,
            "the late joiner still gets flight 1's result"
        );
        assert_eq!(
            sf.in_flight(),
            1,
            "flight 2 must survive the old joiner leaving"
        );
    }
}
