package metrics

import (
	"io"
	"sync"
	"testing"
	"time"
)

func TestTTLRace(t *testing.T) {
	set := NewSet()
	sv := set.NewSetVecWithTTL("example_label", time.Millisecond)

	inactiveRead := make(chan struct{})
	resumeExpiration := make(chan struct{})
	sv.SetIsActive(func(s *Set) bool {
		active, _ := s.GetMetricUint64("count")
		close(inactiveRead)
		// Model the scraper being descheduled after reading the counter.
		<-resumeExpiration
		return active > 0
	})
	metric := sv.NewUint64Vec("count")
	original := metric.WithLabelValues("a")
	child := sv.WithLabelValue("a")

	scraped := make(chan error, 1)
	go func() {
		_, err := set.WritePrometheus(io.Discard)
		scraped <- err
	}()
	// Always release and join the scraper, including on a setup failure.
	finishScrape := sync.OnceValue(func() error {
		close(resumeExpiration)
		return <-scraped
	})
	defer func() {
		if err := finishScrape(); err != nil {
			t.Errorf("WritePrometheus: %v", err)
		}
	}()

	select {
	case <-inactiveRead:
	case <-time.After(5 * time.Second):
		t.Fatal("scraper did not reach the activity check")
	}

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

	if err := finishScrape(); err != nil {
		t.Fatalf("WritePrometheus: %v", err)
	}

	m2 := metric.WithLabelValues("a")
	m2.Dec()
	if m1 != m2 {
		t.Fatalf("active counter expired: Inc used %p (value %d), Dec used %p (value %d)",
			m1, m1.Get(), m2, m2.Get())
	}
}
