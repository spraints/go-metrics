package metrics

import (
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"go.withmatt.com/metrics/internal/fasttime"
)

func TestTTLRace(t *testing.T) {
	set := NewSet()
	sv := set.NewSetVecWithTTL("example_label", time.Millisecond)
	sv.SetIsActive(func(s *Set) bool {
		act, _ := s.GetMetricUint64("count")
		fmt.Printf("active = %d\n", act)
		return act > 0
	})
	metric := sv.NewUint64Vec("count")

	var lastM *Uint64
	for i := range 10 {
		fmt.Println("AAA")
		var wg sync.WaitGroup

		//fmt.Println("BBB")
		//_, err := set.WritePrometheus(io.Discard)
		//if err != nil {
		//	t.Fatal(err)
		//}
		wg.Go(func() {
			fmt.Println("CCC")
			set.WritePrometheus(io.Discard)
			fmt.Println("DDD")
		})

		var m1, m2 *Uint64
		wg.Go(func() {
			time.Sleep(10)
			fmt.Println("EEE")
			m1 = metric.WithLabelValues("a")
			m1.Inc()
			time.Sleep(2 * time.Millisecond)
			m2 = metric.WithLabelValues("a")
			m2.Dec()
			fmt.Println("FFF")
		})

		wg.Wait()

		if m1 != m2 {
			t.Errorf("%d: %p != %p", i, m1, m2)
		}
		if m1 != lastM {
			t.Logf("%d: last %p != cur %p", i, lastM, m1)
		}
		lastM = m1
	}

	for i := range 3 {
		time.Sleep(time.Millisecond / 2)
		fmt.Printf("%d // %d // %d\n", i, fasttime.Now(), time.Millisecond)
		set.WritePrometheus(os.Stdout)
	}
}
