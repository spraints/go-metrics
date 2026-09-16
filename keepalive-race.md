# KeepAlive can race with set expiration

`Set.KeepAlive()` updates `lastUsed`, but does not coordinate with removal from
its parent's collection. A caller can refresh a set and return it (or one of its
metrics) while a scraper deletes that same set using an earlier expiration
check. Refreshing a set does not cancel a pending deletion or re-register a set
that has already been removed.

This is a logical race: the timestamp, map operations, and counter updates can
all be individually safe for concurrent use while their combined result is
incorrect. Go's race detector need not report it.

## How the race happens

The relevant code is in [set.go](set.go), in `rangeChildrenSets`:

```go
if child.isExpired() {
    // The expiration decision and deletion are separate operations.
    s.setsByHash.Delete(key)
    return true
}
```

`isExpired` first checks the optional activity callback. If it reports active,
`isExpired` calls `KeepAlive` and returns false. Otherwise, it compares the cached
clock with `lastUsed` and the TTL.

Meanwhile, [SetVec.WithLabelValue](set_vec.go) does:

```go
set, ok := sv.s.setsByHash.Load(hash)
if !ok {
    set = sv.s.loadOrStoreSetFromVec(hash, sv.ttl, sv.isActive, sv.label, value)
}
set.KeepAlive()
return set
```

Suppose the set for label value `"a"` is still registered, its counter is zero,
and its last use was more than seven days ago, with a seven-day TTL:

| Step | Scraper: `WritePrometheus` | Application |
| --- | --- | --- |
| 1 | Reads the old set; `isExpired()` returns true. | |
| 2 | Pauses before `Delete(key)`. | `metric.WithLabelValues("a")` loads the same set. |
| 3 | | `WithLabelValue` calls `KeepAlive()` and returns the set; the metric lookup returns its counter. |
| 4 | Deletes the set using the decision from step 1. | |
| 5 | | `Inc()` increments the old counter to 1. |
| 6 | | A later `WithLabelValues("a")` creates a new set and counter. |
| 7 | | `Dec()` decrements the new counter from 0 to `18446744073709551615`. |

Only the lookup needs to overlap the gap between the expiration decision and
deletion. In `metric.WithLabelValues("a").Inc()`, Go can schedule the scraper
between the lookup returning and `Inc()` executing. The increment can also occur
before deletion; neither ordering invalidates the scraper's completed decision.
Deletion can even occur between loading the set and calling `KeepAlive`, leaving
the lookup to refresh and return an already-detached set.

The seven days of inactivity happen **before** this race. Neither goroutine
needs to pause for a TTL. The set must have reached expiration eligibility and
still be present when the lookup overlaps the scraper's removal. A lookup that
refreshes `lastUsed` before the expiration check reads it normally prevents that
check from expiring the set.

Deleting the map entry does not free the Go object while the application holds
it. The old counter remains usable, but is detached from the parent's exported
metrics. Keeping the same counter pointer for both `Inc` and `Dec` avoids the
split-counter underflow, but does not prevent its updates from disappearing
from that parent's scrapes.

## KeepAlive call sites

These are all internal calls to `Set.KeepAlive()` in the current implementation.
`runtime.KeepAlive` in `unique_ident.go` is unrelated.

The classifications below assume a TTL of at least one hour and a time to
return and use a set much shorter than that TTL. They cover expiration of the
set being refreshed, excluding explicit removal and stale deletion of a
replacement under the same key. An existing set can already be old enough to
expire; a freshly created set cannot.

| Caller | What it refreshes and returns | Exposure to concurrent deletion |
| --- | --- | --- |
| [`SetVec.WithLabelValue`](set_vec.go) | Refreshes the set returned by `Load` or `loadOrStoreSetFromVec`, then returns `*Set`. | **At risk** when `Load` returns an existing, expiration-eligible set: a scraper can already have decided to delete it before the refresh. The creation path is not at risk under these timing assumptions. |
| [`Set.loadOrStoreSetFromVec`](set.go) | Refreshes a new candidate before publishing it, then returns the result of `loadOrStoreSet`. | **Not at risk** under these timing assumptions. Its caller has just observed a missing entry. It either publishes its fresh candidate or receives a set created by a competing caller since that miss. Neither can age through an hour-long TTL before return and use. The latter is an existing map entry, but is still newly created for this purpose. |
| [`Set.NewSet`](set.go) | Defers `s.KeepAlive()` on the **parent receiver**, then returns a newly registered child. | **At risk** if the parent receiver `s` is an existing, expiration-eligible TTL set. Its parent may already have decided to delete it, detaching the returned child's whole subtree. The new child itself is not old enough to expire; the refresh here is on the older parent. |
| [`Set.mustStoreMetric`](set.go) | Defers a refresh of the receiver while registering a metric; the public constructor returns that metric. | **At risk** if the receiver is an existing, expiration-eligible TTL set. A pending deletion can detach it despite the refresh, leaving the returned metric in a detached set. Registering on a fresh set is not at risk under these timing assumptions. |
| [`Set.Reset`](set.go) | Clears the receiver's contents and defers a refresh of the receiver; returns no value. | **At risk** if the receiver is an existing, expiration-eligible TTL set. Resetting its contents does not cancel a pending deletion from its parent, so the caller can reuse a detached set. |
| [`Set.isExpired`](set.go), active branch | Refreshes the receiver when `isActive` returns true, then returns false. | **At risk** with concurrent expiration checks: another traversal may already have observed this older set as inactive and expired before it became active. This refresh does not cancel that traversal's pending deletion. This invocation itself returns false and does not delete the set. |

External callers can also call the public `Set.KeepAlive()` directly on a held
pointer. It has the same limitation: it only stores a timestamp when the TTL is
positive, and cannot guarantee that the set remains registered during later use.

## Reproduction

[TestTTLRace](ttlrace_test.go) uses a seven-day TTL and backdates the idle set's
`lastUsed` before starting the scraper. `testHookBeforeSetDelete` pauses the
scraper after `isExpired()` returns true and before deletion. The application
looks up the counter, the test releases and joins the scraper, and then the
application increments the old counter and decrements a newly looked-up one.

```sh
go test -run TTLRace -count=10 .
go test -race -run TTLRace .
```

The test intentionally fails on the pointer mismatch and incorrect counter
value. It forces a valid interleaving; it does not measure its production
frequency. No sleep for the TTL is needed. The cached TTL clock's one-second
granularity explained why the original millisecond-sleep test struggled to
expire a set, but is not the cause of the check/delete race.
