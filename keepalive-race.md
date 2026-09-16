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

| Caller | What it refreshes and returns | Exposure to concurrent deletion |
| --- | --- | --- |
| [`SetVec.WithLabelValue`](set_vec.go) | Refreshes the set returned by `Load` or `loadOrStoreSetFromVec`, then returns `*Set`. | Direct race above: the set can be deleted before, during, or after the refresh and still be returned. |
| [`Set.loadOrStoreSetFromVec`](set.go) | Refreshes a new candidate before publishing it, then returns the result of `loadOrStoreSet`. | `LoadOrStore` can return an existing set instead of the refreshed candidate. That existing set may already have a pending expiration deletion. The outer `WithLabelValue` refreshes the returned set, but does not make it safe from that deletion. A newly published candidate can also be removed by an explicit removal or a stale deletion for the same key. |
| [`Set.NewSet`](set.go) | Defers `s.KeepAlive()` on the **parent receiver**, then returns a newly registered child. | If `s` is itself a TTL set, its parent can already have decided to delete it. The returned child then belongs to a detached subtree despite the deferred refresh of `s`. |
| [`Set.mustStoreMetric`](set.go) | Defers a refresh of the receiver while registering a metric; the public constructor returns that metric. | Registration can succeed on a set that is concurrently removed from its parent. The caller receives a metric in a detached set. |
| [`Set.Reset`](set.go) | Clears the receiver's contents and defers a refresh of the receiver; returns no value. | A caller holding a TTL set can reset and reuse it even though its parent concurrently deletes it. The refresh does not restore parent membership. |
| [`Set.isExpired`](set.go), active branch | Refreshes the receiver when `isActive` returns true, then returns false. | This expiration check preserves the set, but another concurrent traversal may have already decided to delete it. The active result does not cancel that other traversal's deletion. |

External callers can also call the public `Set.KeepAlive()` directly on a held
pointer. It has the same limitation: it only stores a timestamp when the TTL is
positive, and cannot guarantee that the set remains registered during later use.

### APIs that inherit these paths

For vectors created through a `SetVec`, `WithLabelValues` first calls
`SetVec.WithLabelValue` and then accesses the returned set's metrics:

- `Uint64Vec`, `Int64Vec`, and `Float64Vec` in [counter_vec.go](counter_vec.go).
- `HistogramVec` in [histogram_vec.go](histogram_vec.go).
- `FixedHistogramVec` in [fixedhistogram_vec.go](fixedhistogram_vec.go).

The direct `SetVec` constructors `NewUint64`, `NewCounter`, `NewInt64`,
`NewFloat64`, `NewHistogram`, and `NewFixedHistogram` also use
`WithLabelValue`, then register a metric on the returned set.

`mustStoreMetric` is used by the numeric constructors in
[counter.go](counter.go), the function metric constructors in [func.go](func.go),
and the constructors in [histogram.go](histogram.go) and
[fixedhistogram.go](fixedhistogram.go). Each can return a metric whose containing
TTL set has been concurrently detached.

Vectors created directly on a `Set` retain that set pointer. Their metric
lookups do not call `SetVec.WithLabelValue` or refresh its TTL; retaining the
vector or a metric pointer does not keep the set registered. Likewise,
refreshing a child does not recursively refresh its ancestors.

## Removal paths and scope

`rangeChildrenSets` performs expiration and deletion during both Prometheus
write variants and `Collect`. It has the same check-then-delete structure for
`unorderedSets`, although public TTL set creation through `SetVec` uses
`setsByHash`.

There is also a related replacement hazard: `setsByHash.Delete(key)` does not
check that the current value is the child inspected by the traversal. If one
scraper pauses after deciding to expire a set, another removes it, and an
application creates a replacement under the same key, the first scraper can
delete the replacement. `CompareAndDelete(key, child)` would address that
identity mismatch, but by itself would not prevent deletion of the original
child after a successful `KeepAlive`.

Explicit `SetVec.RemoveByLabelValue` and `Set.UnregisterSet` also delete entries
without coordinating with `KeepAlive`. `Set.Reset` clears child collections.
These intentional removals can leave outstanding pointers detached without
requiring an expiration decision. `AppendConstantTags` deletes and reinserts
children, but is explicitly documented as not thread-safe and restricted to
initial setup; concurrent use is outside its supported contract.

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
