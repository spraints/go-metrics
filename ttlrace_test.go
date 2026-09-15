package metrics

import (
	"io"
	"sync"
	"testing"
	"time"
)

// Check for a race between the prometheus writer and an Inc().
func TestTTLRace(t *testing.T) {
	// Create a top level set with a 1ms TTL on each group of metrics with
	// the same 'set_group_label'.
	set := NewSet()
	sv := set.NewSetVecWithTTL("set_group_label", time.Millisecond)

	// We check a key metric to see if the group is active or idle.
	inactiveRead := make(chan struct{})
	resumeExpiration := make(chan struct{})
	sv.SetIsActive(func(s *Set) bool {
		active, _ := s.GetMetricUint64("count")
		// This is the sync point in RACER 1. At this point, we have a
		// scalar value. In the race case, we get here while the
		// counter is still 0.
		close(inactiveRead)
		<-resumeExpiration
		// After the sync point, we check the (stale) count and
		// incorrectly determine that this metric is NOT active.
		return active > 0
	})
	metric := sv.NewUint64Vec("counter")

	// Use the metric so that it exists.
	original := metric.WithLabelValues("a")
	original.Inc()
	original.Dec()

	// Normally, we don't need to use this directly. But in this case, we
	// will use it later to help syncrhonize our two racing goroutines.
	child := sv.WithLabelValue("a")

	/// RACER 1 - METRICS SCRAPE
	scraped := make(chan error, 1)
	go func() {
		// In our app, this happens during a 'GET /metrics' request.
		_, err := set.WritePrometheus(io.Discard)
		scraped <- err
	}()

	// finishScrape() releases RACER 1.
	finishScrape := sync.OnceValue(func() error {
		close(resumeExpiration)
		return <-scraped
	})
	// Always release and join the scraper, including on a setup failure.
	defer func() {
		if err := finishScrape(); err != nil {
			t.Errorf("WritePrometheus: %v", err)
		}
	}()

	// RACER 2 - Inc the count.

	// Let the scraper get into the IsActive call on our metric SetVec.
	select {
	case <-inactiveRead:
	case <-time.After(5 * time.Second):
		t.Fatal("scraper did not reach the activity check")
	}

	// When we inc the metric here, we get a counter from the set that
	// RACER 1 is deleting.
	m1 := metric.WithLabelValues("a")
	m1.Inc()
	if m1 != original {
		t.Fatal("counter changed before expiration resumed")
	}

	// WithLabelValues refreshed lastUsed. Let that refreshed TTL age while
	// the scraper still holds its stale inactive result. The production
	// fastClock ticks once per second, regardless of the configured TTL.
	deadline := time.Now().Add(5 * time.Second)
	for fastClock().Since(child.lastUsed.Load()) <= child.ttl {
		if time.Now().After(deadline) {
			t.Fatal("cached TTL clock did not advance")
		}
		time.Sleep(time.Millisecond)
	}

	// Let RACER 1 continue.
	if err := finishScrape(); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}

	// Later in RACER 2, we dec and get a new counter, not the one that we
	// previously inc'ed.
	m2 := metric.WithLabelValues("a")
	m2.Dec()
	if m1 != m2 {
		t.Fatalf("active counter expired: Inc used %p (value %d), Dec used %p (value %d)",
			m1, m1.Get(), m2, m2.Get())
	}
}
