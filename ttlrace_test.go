package metrics

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"go.withmatt.com/metrics/internal/fasttime"
)

// ageTTLSet models an earlier collection followed by an idle period longer
// than the TTL. Only use it on live sets owned by the test.
func ageTTLSet(s *Set) {
	s.expirationMu.Lock()
	defer s.expirationMu.Unlock()
	s.keepAliveState.Store(0)
	s.idleSince = fastClock().Now() - fasttime.Instant(s.ttl+time.Second)
}

func TestTTLRace(t *testing.T) {
	for _, retired := range []bool{false, true} {
		name := "renewal_wins"
		if retired {
			name = "expiration_wins"
		}
		t.Run(name, func(t *testing.T) {
			set := NewSet()
			sv := set.NewSetVecWithTTL("set_group_label", 7*24*time.Hour)
			sv.SetIsActive(func(s *Set) bool {
				active, _ := s.GetMetricUint64("counter")
				return active > 0
			})
			metric := sv.NewUint64Vec("counter")
			original := metric.WithLabelValues("a")
			child := sv.WithLabelValue("a")
			ageTTLSet(child)

			reached := make(chan struct{})
			resume := make(chan struct{})
			hook := &testHookBeforeSetExpire
			if retired {
				hook = &testHookBeforeSetDelete
			}
			oldHook := *hook
			*hook = func(s *Set) {
				if s == child {
					close(reached)
					<-resume
				}
			}
			defer func() { *hook = oldHook }()

			scraped := make(chan error, 1)
			go func() {
				_, err := set.WritePrometheus(io.Discard)
				scraped <- err
			}()
			finishScrape := sync.OnceValue(func() error {
				close(resume)
				return <-scraped
			})
			defer func() {
				if err := finishScrape(); err != nil {
					t.Errorf("WritePrometheus: %v", err)
				}
			}()

			select {
			case <-reached:
			case <-time.After(5 * time.Second):
				t.Fatal("scraper did not reach the expiration hook")
			}

			// Race several lookups against a paused scraper. If retirement has
			// already won, they must help remove the old set and converge on a
			// single replacement without waiting for the scraper to resume.
			const workers = 16
			lookups := make(chan *Uint64, workers)
			for range workers {
				go func() { lookups <- metric.WithLabelValues("a") }()
			}
			var m1 *Uint64
			for range workers {
				select {
				case m := <-lookups:
					if m1 == nil {
						m1 = m
					}
					if m != m1 {
						t.Error("concurrent lookups returned different counters")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("lookup waited for the paused scraper")
				}
			}
			if (m1 != original) != retired {
				t.Fatalf("replacement = %v, want %v", m1 != original, retired)
			}

			if err := finishScrape(); err != nil {
				t.Fatal(err)
			}

			// The full expression can be descheduled between lookup and Inc.
			m1.Inc()
			m2 := metric.WithLabelValues("a")
			m2.Dec()
			if m1 != m2 || m2.Get() != 0 {
				t.Fatalf("counter lost: Inc used %p, Dec used %p (value %d)", m1, m2, m2.Get())
			}
			if retired && child.KeepAlive() {
				t.Fatal("retired set was revived")
			}
		})
	}
}

func TestTTLKeepAlive(t *testing.T) {
	set := NewSet()
	if !set.KeepAlive() || set.isExpired() {
		t.Fatal("set without TTL should stay alive")
	}
	child := set.NewSetVecWithTTL("group", time.Hour).WithLabelValue("a")
	ageTTLSet(child)
	if !child.KeepAlive() || child.isExpired() {
		t.Fatal("new activity should postpone expiration of an old set")
	}
	if fastClock().Since(child.idleSince) > child.ttl {
		t.Fatal("observed activity did not restart the idle period")
	}
	if child.isExpired() {
		t.Fatal("another immediate check should preserve the set")
	}

	ageTTLSet(child)
	if !child.isExpired() {
		t.Fatal("idle set should expire")
	}
	for range 3 {
		if child.KeepAlive() || !child.isExpired() {
			t.Fatal("expiration must remain permanent after failed renewals")
		}
	}
	child.Reset()
	if child.KeepAlive() {
		t.Fatal("Reset revived an expired set")
	}
	if _, err := child.WritePrometheus(io.Discard); !errors.Is(err, ErrSetExpired) {
		t.Fatalf("WritePrometheus on expired set: %v", err)
	}
	if _, err := child.WritePrometheusUnthrottled(io.Discard); !errors.Is(err, ErrSetExpired) {
		t.Fatalf("WritePrometheusUnthrottled on expired set: %v", err)
	}
}

func TestTTLConcurrentChecks(t *testing.T) {
	set := NewSet()
	sv := set.NewSetVecWithTTL("group", time.Hour)
	reached := make(chan struct{})
	resume := make(chan struct{})
	var once sync.Once
	sv.SetIsActive(func(*Set) bool {
		once.Do(func() {
			close(reached)
			<-resume
		})
		return false
	})
	child := sv.WithLabelValue("a")
	ageTTLSet(child)

	results := make(chan bool, 2)
	go func() { results <- child.isExpired() }()
	finish := sync.OnceFunc(func() { close(resume) })
	defer finish()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		t.Fatal("first check did not reach the activity callback")
	}

	// This touch must survive the first check's final CAS. A second check
	// must not clear it while the first check is still deciding to expire.
	if !child.KeepAlive() {
		t.Fatal("renewal failed before expiration was claimed")
	}
	go func() { results <- child.isExpired() }()
	finish()
	for range 2 {
		if <-results {
			t.Error("concurrent check expired a renewed set")
		}
	}
}
