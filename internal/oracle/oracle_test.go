package oracle

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock for deterministic tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// Now returns the current fake time.
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// Set moves the fake clock to t.
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

func TestStrictlyIncreasing(t *testing.T) {
	o := New()
	prev := o.Next()
	for i := 0; i < 100_000; i++ {
		ts := o.Next()
		if ts <= prev {
			t.Fatalf("timestamp %d not greater than previous %d", ts, prev)
		}
		prev = ts
	}
}

func TestClockGoingBackwardsStaysMonotonic(t *testing.T) {
	c := &fakeClock{now: time.UnixMilli(1_000_000)}
	o := New(WithClock(c.Now))
	a := o.Next()
	c.Set(time.UnixMilli(500_000)) // clock jumps back 500 s
	b := o.Next()
	if b <= a {
		t.Fatalf("after clock went back: %d <= %d", b, a)
	}
	if Physical(b).UnixMilli() != 1_000_000 {
		t.Fatalf("physical part should stay at the high-water mark, got %d", Physical(b).UnixMilli())
	}
	c.Set(time.UnixMilli(2_000_000))
	d := o.Next()
	if Physical(d).UnixMilli() != 2_000_000 || Logical(d) != 0 {
		t.Fatalf("clock moved forward: want physical 2000000 logical 0, got %d/%d", Physical(d).UnixMilli(), Logical(d))
	}
}

func TestLogicalOverflowCarriesIntoPhysical(t *testing.T) {
	c := &fakeClock{now: time.UnixMilli(42)}
	o := New(WithClock(c.Now))
	var prev uint64
	for i := 0; i < 1<<LogicalBits+10; i++ { // more than one millisecond can hold
		ts := o.Next()
		if ts <= prev {
			t.Fatalf("call %d: %d <= %d", i, ts, prev)
		}
		prev = ts
	}
	if Physical(prev).UnixMilli() != 43 {
		t.Fatalf("expected carry into millisecond 43, got %d", Physical(prev).UnixMilli())
	}
}

func TestConcurrentCallersGetUniqueTimestamps(t *testing.T) {
	o := New()
	const workers, per = 8, 10_000
	out := make([][]uint64, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			got := make([]uint64, per)
			for i := range got {
				got[i] = o.Next()
			}
			out[w] = got
		}(w)
	}
	wg.Wait()
	seen := make(map[uint64]struct{}, workers*per)
	for _, got := range out {
		for i, ts := range got {
			if i > 0 && ts <= got[i-1] {
				t.Fatalf("one caller saw a non-increasing sequence: %d then %d", got[i-1], ts)
			}
			if _, dup := seen[ts]; dup {
				t.Fatalf("timestamp %d issued twice", ts)
			}
			seen[ts] = struct{}{}
		}
	}
}

func TestFloorSurvivesRestart(t *testing.T) {
	c := &fakeClock{now: time.UnixMilli(10)}
	floor := Compose(5_000, 7) // persisted by a previous oracle whose clock was ahead
	o := New(WithClock(c.Now), WithFloor(floor))
	if got := o.Last(); got != floor {
		t.Fatalf("Last before any call = %d, want floor %d", got, floor)
	}
	if ts := o.Next(); ts <= floor {
		t.Fatalf("first timestamp %d not above floor %d", ts, floor)
	}
}

func TestComposeRoundTrip(t *testing.T) {
	ts := Compose(1_700_000_000_000, 12345)
	if Physical(ts).UnixMilli() != 1_700_000_000_000 || Logical(ts) != 12345 {
		t.Fatalf("round trip failed: %d/%d", Physical(ts).UnixMilli(), Logical(ts))
	}
}

func TestComposeKeepsAnOversizedLogicalOutOfThePhysicalBits(t *testing.T) {
	ts := Compose(1_000, 1<<LogicalBits+3)
	if ts>>LogicalBits != 1_000 || Logical(ts) != 3 {
		t.Fatalf("Compose(1000, 2^%d+3) = physical %d logical %d, want 1000 and 3", LogicalBits, ts>>LogicalBits, Logical(ts))
	}
}
