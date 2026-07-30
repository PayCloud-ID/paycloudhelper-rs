//! In-flight call de-duplication ("singleflight").
//!
//! mirrors: Go's `golang.org/x/sync/singleflight` usage — concurrent callers
//! sharing a key collapse onto a single execution of the underlying work and
//! all receive the shared result. Implemented with a [`DashMap`] of
//! [`Shared`](futures::future::Shared) futures (the `DashMap<K, Shared<Future>>`
//! option from plan 03 PC-2).

use std::future::Future;
use std::hash::Hash;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use futures::future::{BoxFuture, FutureExt, Shared};

/// De-duplicates concurrent calls sharing a key so the guarded future runs
/// exactly once while it is in flight; all concurrent callers receive a clone
/// of its result.
///
/// This de-duplicates only *concurrent* calls: once the shared future
/// completes, its entry is removed and a subsequent call re-runs the work (it
/// is not a result cache). The value type `T` must be `Clone` so every caller
/// can receive the outcome.
pub struct Singleflight<K, T> {
    calls: DashMap<K, Shared<BoxFuture<'static, T>>>,
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
        }
    }

    /// Run `fut` de-duplicated by `key`.
    ///
    /// If another call for the same `key` is already in flight, `fut` is not
    /// polled at all; this caller awaits the in-flight future and receives a
    /// clone of its result. Otherwise this call owns the execution: it registers
    /// `fut`, drives it, then clears the entry so later calls can re-run.
    ///
    /// Note: if the owning future panics, the entry is left in place (the
    /// classic shared-future caveat); recreate the group if that is a concern.
    pub async fn do_call<Fut>(&self, key: K, fut: Fut) -> T
    where
        Fut: Future<Output = T> + Send + 'static,
    {
        // Register or join, dropping the shard lock before awaiting.
        let (shared, owner) = {
            match self.calls.entry(key.clone()) {
                Entry::Occupied(e) => (e.get().clone(), false),
                Entry::Vacant(e) => {
                    let shared = fut.boxed().shared();
                    e.insert(shared.clone());
                    (shared, true)
                }
            }
        };

        let result = shared.await;

        // Only the owner removes the entry. While the entry exists no other
        // caller can insert (the key is occupied), so this removal always
        // targets exactly our own in-flight registration — no risk of evicting
        // a newer call's future.
        if owner {
            self.calls.remove(&key);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

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
}
