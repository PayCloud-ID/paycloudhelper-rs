//! DNS-aware endpoint discovery for headless Services.
//!
//! A plain `vec![endpoint; pool]` hands tonic one DNS name; every subchannel
//! resolves it once, at connect time, and then stays on that pod for good. The
//! pure address diff lives here; the refresher built on it is added below.

use std::collections::BTreeSet;
use std::net::SocketAddr;

/// Compare the addresses currently balanced over (`old`) with a fresh lookup
/// (`new`), returning `(to_insert, to_remove)`.
///
/// An **empty** `new` yields no change. A lookup that returns nothing is far
/// more likely a DNS hiccup (or every pod mid-rollout) than "the service has no
/// backends", and dropping the last endpoints would turn a blip into an outage.
pub fn diff_addrs(
    old: &BTreeSet<SocketAddr>,
    new: &BTreeSet<SocketAddr>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    if new.is_empty() {
        return (Vec::new(), Vec::new());
    }
    (
        new.difference(old).copied().collect(),
        old.difference(new).copied().collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(addrs: &[&str]) -> BTreeSet<SocketAddr> {
        addrs.iter().map(|a| a.parse().unwrap()).collect()
    }

    fn a(s: &str) -> SocketAddr {
        s.parse().unwrap()
    }

    #[test]
    fn diff_inserts_new_and_removes_gone() {
        let old = set(&["10.0.0.1:9106"]);
        let new = set(&["10.0.0.1:9106", "10.0.0.2:9106"]);
        assert_eq!(diff_addrs(&old, &new), (vec![a("10.0.0.2:9106")], vec![]));
    }

    #[test]
    fn diff_removes_a_pod_that_left() {
        let old = set(&["10.0.0.1:9106", "10.0.0.2:9106"]);
        let new = set(&["10.0.0.2:9106"]);
        assert_eq!(diff_addrs(&old, &new), (vec![], vec![a("10.0.0.1:9106")]));
    }

    #[test]
    fn an_empty_lookup_keeps_everything() {
        let old = set(&["10.0.0.1:9106"]);
        assert_eq!(diff_addrs(&old, &BTreeSet::new()), (vec![], vec![]));
    }
}
