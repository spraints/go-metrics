package metrics

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"go.withmatt.com/metrics/internal/fasttime"
)

// BenchmarkSetVecLookup measures existing-label lookups, without a counter
// increment that would introduce a second source of contention. Both TTL modes
// go through SetVec so the comparison isolates the renewal cost.
func BenchmarkSetVecLookup(b *testing.B) {
	for _, tc := range []struct {
		name string
		ttl  time.Duration
	}{
		{"no_ttl", 0},
		{"ttl", time.Hour},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.Run("serial", func(b *testing.B) {
				set := NewSet()
				v := set.NewSetVecWithTTL("group", tc.ttl).NewUint64Vec("count")
				v.WithLabelValues("a")

				b.ReportAllocs()
				for b.Loop() {
					v.WithLabelValues("a")
				}
			})

			for _, keyCount := range []int{1, 64, 1024} {
				b.Run("parallel/keys="+strconv.Itoa(keyCount), func(b *testing.B) {
					set := NewSet()
					v := set.NewSetVecWithTTL("group", tc.ttl).NewUint64Vec("count")
					// Every worker samples the same populated key space. Larger
					// spaces model many keys with occasional overlapping lookups.
					values := make([]string, keyCount)
					for i := range values {
						values[i] = strconv.Itoa(i)
						v.WithLabelValues(values[i])
					}

					b.ReportAllocs()
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							// Include the same key-selection cost in both TTL modes.
							v.WithLabelValues(values[rand.IntN(len(values))])
						}
					})
				})
			}
		})
	}
}

// BenchmarkWritePrometheusTTL measures a complete unthrottled scrape of 256
// child sets. Expiration setup is excluded, so expired measures removal rather
// than construction. Keep this setup aligned with the expiration implementation
// when changing how a set records idle time.
func BenchmarkWritePrometheusTTL(b *testing.B) {
	const childCount = 256
	const ttl = time.Hour

	for _, mode := range []string{"no_ttl", "idle", "active", "expired"} {
		b.Run(mode, func(b *testing.B) {
			newFixture := func() *Set {
				set := NewSet()
				setTTL := ttl
				if mode == "no_ttl" {
					setTTL = 0
				}
				sv := set.NewSetVecWithTTL("group", setTTL)
				if mode == "active" {
					sv.SetIsActive(func(s *Set) bool {
						count, _ := s.GetMetricUint64("count")
						return count > 0
					})
				}
				for i := range childCount {
					child := sv.WithLabelValue(strconv.Itoa(i))
					child.NewUint64("count").Inc()
					if mode == "active" || mode == "expired" {
						// Active sets must survive even with an old timestamp.
						child.lastUsed.Store(fastClock().Now() - fasttime.Instant(ttl+time.Second))
					}
				}
				return set
			}

			set := newFixture()
			var buf bytes.Buffer
			if mode != "expired" {
				// Warm the output buffer and metric ordering caches.
				if _, err := set.WritePrometheusUnthrottled(&buf); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			for b.Loop() {
				if mode == "expired" {
					b.StopTimer()
					set = newFixture()
					b.StartTimer()
				}
				buf.Reset()
				if _, err := set.WritePrometheusUnthrottled(&buf); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(childCount, "sets/op")

			// Verify that the benchmark exercised collection or removal, rather
			// than accidentally timing an empty parent throughout the run.
			remaining := 0
			set.setsByHash.Range(func(_ metricHash, _ *Set) bool {
				remaining++
				return true
			})
			want := childCount
			if mode == "expired" {
				want = 0
			}
			if remaining != want {
				b.Fatalf("remaining sets: got %d, want %d", remaining, want)
			}
			if mode == "expired" && buf.Len() != 0 {
				b.Fatal("expired sets were exported")
			}
			if mode != "expired" && buf.Len() == 0 {
				b.Fatal("live sets were not exported")
			}
		})
	}
}
