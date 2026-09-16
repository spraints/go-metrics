*NOTE: This document is out of date. It describes the state of the world at commit eb5d663cefcd3f48ca600ca0ad8c1a0af50515bd.*

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

## Potential fixes

The goal is to make lookup and expiration agree on whether a set can still be
used, without adding atomic operations or allocations to the ordinary successful
`WithLabelValues` path. Expiration can do extra work. Replacing an existing
atomic store with one atomic read-modify-write meets the operation-count goal,
but still needs benchmarking: the two operations may have different costs under
contention. Retrying after actual expiration can allocate a replacement, as it
does today. Keeping the current implementation and using application workarounds
is also a valid choice.

| Option | Ordinary successful lookup cost | Tradeoff |
| --- | --- | --- |
| 1. Timestamp plus deletion sentinel, updated with CAS | Additional atomic load and possible retries; no new allocation | Preserves timestamp-based TTL behavior, but misses the atomic-operation constraint. |
| **2. `touched`/`deleted` bits — recommended fix** | Replace the timestamp store with one atomic OR; remove the cached-clock read; no new allocation | Moves time tracking to expiration checks and can delay expiration. Requires collector bookkeeping and benchmarks. |
| Leave the library unchanged | Unchanged | Rely on application workarounds. |

### Option 1: timestamp plus deletion sentinel

Reserve a `lastUsed` value meaning permanently deleted. A conditional renewal
operation, such as `tryKeepAlive() bool`, uses compare-and-swap (CAS): write the
new timestamp only if the stored value still equals the value read earlier.
For a positive TTL, the core operation is:

```go
for {
    old := s.lastUsed.Load()
    if old == deleted {
        return false
    }
    now := fastClock().Now()
    if s.lastUsed.CompareAndSwap(old, now) {
        return true
    }
}
```

Expiration snapshots the timestamp, checks activity and TTL eligibility, and
then attempts `CompareAndSwap(observed, deleted)`. It must use the same snapshot
through that decision; it must not reread a newer timestamp after checking
activity and then delete based on the stale activity result. If a lookup
refreshes an expired timestamp before the deletion CAS, the CAS fails. If
deletion wins, renewal fails instead of returning the retired set.

After claiming deletion, remove the entry with `CompareAndDelete(key, set)`.
This prevents a delayed removal from deleting a replacement under the same key.
`SetVec.WithLabelValue` retries when renewal fails; it can help remove the
retired entry with `CompareAndDelete` before retrying rather than waiting for
the original deleting goroutine.

This approach preserves the current meaning of the TTL, but renewal needs an
atomic load plus a CAS, with retries under contention.

### Option 2: activity bit plus permanent deletion bit

Replace the lookup-side timestamp with one atomic word containing two bits:

```go
const (
    touched uint64 = 1 << iota
    deleted
)

// Core renewal operation for a set with a positive TTL.
func (s *Set) tryKeepAlive() bool {
    old := s.state.Or(touched)
    return old&deleted == 0
}
```

Setting `touched` never clears `deleted`. Once marked deleted, the set stays
retired and every subsequent renewal fails.

Each new set starts with `touched` set. Its first expiration check initializes
the `idleSince` timestamp, which is managed by expiration checkers. It records
when a checker last observed activity, not
the actual time of the last `KeepAlive`. Protect this timestamp and the
expiration protocol with a lock shared by all checkers of that set. Lookup and
`KeepAlive` do not take that lock.

On each expiration check, while holding that lock:

1. Atomically clear and inspect `touched`, preserving `deleted`, using
   `old := state.And(^touched)`. If `deleted` was already set, the set is retired.
2. Check the optional `isActive` callback. If it reports active, or the previous
   bits included `touched`, update `idleSince` to the current time and keep the
   set. Do not clear `touched` again: a concurrent renewal may have set it after
   step 1.
3. Otherwise, compare the current time with `idleSince`. Keep the set if a full
   TTL has not yet elapsed.
4. If the TTL has elapsed, attempt `state.CompareAndSwap(0, deleted)`. A renewal
   after step 1 sets `touched` and makes this CAS fail. On failure, preserve the
   set; a subsequent check will observe that activity.
5. On success, remove the entry with `CompareAndDelete(key, set)`.

No new goroutines or timers are needed. Scrapes can perform these checks, as can
any other thread responsible for removing expired sets, provided all checkers
use the same locking protocol. With a one-hour TTL, a check that observes
activity at 12:00 sets `idleSince` to 12:00. Subsequent checks preserve the set
until more than an hour has elapsed without observed activity. If a renewal
occurs at 12:45 and is observed at 12:46, the waiting period restarts at 12:46.

This makes expiration conservative: the TTL starts when activity is observed,
rather than when it occurred. Regular collection adds roughly a collection
interval to the idle period; irregular collection can delay expiration further.
As today, a set is only removed when an expiration check runs.

The final CAS and the lookup's OR decide which operation wins:

- If renewal wins, it sets `touched`, so the deletion CAS fails.
- If deletion wins, renewal sees `deleted` and fails. `WithLabelValue` can help
  remove the retired entry with `CompareAndDelete` and retry the lookup.

Both options require callers to handle failed renewal. `SetVec` has the parent
and key needed to retry transparently; callers holding a `*Set` directly need
an explicit policy for failure. These designs coordinate renewal with
expiration, but do not give retained pointers an unlimited lifetime. They are
proposals, not implemented fixes; activity callbacks, concurrent expiration
checks, retry behavior, and lookup performance need validation before adoption.
