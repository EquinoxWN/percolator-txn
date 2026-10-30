package percolator

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

// load commits a value or fails the test.
func load(t *testing.T, c *Client, k, v string) uint64 {
	t.Helper()
	ts, err := c.Load([]byte(k), []byte(v))
	if err != nil {
		t.Fatalf("Load(%q): %v", k, err)
	}
	return ts
}

// read does a transactional read or fails the test.
func read(t *testing.T, txn *Txn, k string) string {
	t.Helper()
	v, ok, err := txn.Get([]byte(k))
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if !ok {
		return "<none>"
	}
	return string(v)
}

func TestTxnReadsItsStartSnapshot(t *testing.T) {
	c := New(Options{})
	load(t, c, "alice", "100")
	txn := c.Begin()
	commit := load(t, c, "alice", "70") // committed after txn started
	if commit <= txn.StartTS() {
		t.Fatalf("oracle not monotonic: commit %d <= start %d", commit, txn.StartTS())
	}
	if got := read(t, txn, "alice"); got != "100" {
		t.Fatalf("txn saw %q, want its snapshot value 100", got)
	}
	if got := read(t, c.Begin(), "alice"); got != "70" {
		t.Fatalf("new txn saw %q, want 70", got)
	}
	if got := read(t, txn, "bob"); got != "<none>" {
		t.Fatalf("missing key read as %q", got)
	}
}

func TestReadYourOwnWrites(t *testing.T) {
	c := New(Options{})
	load(t, c, "k", "old")
	txn := c.Begin()
	if err := txn.Set([]byte("k"), []byte("new")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, txn, "k"); got != "new" {
		t.Fatalf("got %q, want own write", got)
	}
	if err := txn.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, txn, "k"); got != "<none>" {
		t.Fatalf("got %q after own delete", got)
	}
	if got := read(t, c.Begin(), "k"); got != "old" {
		t.Fatalf("buffered writes leaked to another txn: %q", got)
	}
}

func TestPrewriteLocksEveryKeyWithTheSmallestAsPrimary(t *testing.T) {
	c := New(Options{LockTTL: 5 * time.Second})
	txn := c.Begin()
	for _, k := range []string{"carol", "alice", "bob"} {
		if err := txn.Set([]byte(k), []byte("1")); err != nil {
			t.Fatal(err)
		}
	}
	if err := txn.Delete([]byte("dave")); err != nil {
		t.Fatal(err)
	}
	res, err := txn.Prewrite()
	if err != nil {
		t.Fatal(err)
	}
	if string(res.Primary) != "alice" || res.StartTS != txn.StartTS() {
		t.Fatalf("primary %q start %d", res.Primary, res.StartTS)
	}
	var secs []string
	for _, s := range res.Secondaries {
		secs = append(secs, string(s))
	}
	if fmt.Sprint(secs) != "[bob carol dave]" {
		t.Fatalf("secondaries %v", secs)
	}
	for _, k := range []string{"alice", "bob", "carol", "dave"} {
		l, ok := c.LockOf([]byte(k))
		if !ok || string(l.Primary) != "alice" || l.StartTS != txn.StartTS() || l.TTL != 5*time.Second {
			t.Fatalf("%s: lock %+v present=%v", k, l, ok)
		}
	}
	if l, _ := c.LockOf([]byte("dave")); l.Kind.String() != "delete" {
		t.Fatalf("dave lock kind %v", l.Kind)
	}
	if c.LockCount() != 4 {
		t.Fatalf("want 4 locks, got %d", c.LockCount())
	}
}

func TestReaderMeetsLockOnlyIfItsSnapshotIsLater(t *testing.T) {
	c := New(Options{})
	load(t, c, "k", "v0")
	early := c.Begin()
	w := c.Begin()
	_ = w.Set([]byte("k"), []byte("v1"))
	if _, err := w.Prewrite(); err != nil {
		t.Fatal(err)
	}
	if got := read(t, early, "k"); got != "v0" {
		t.Fatalf("older snapshot should read through the lock, got %q", got)
	}
	_, _, err := c.Begin().Get([]byte("k"))
	var le *KeyLockedError
	if !errors.As(err, &le) || le.Lock.StartTS != w.StartTS() || string(le.Lock.Primary) != "k" {
		t.Fatalf("later snapshot must meet the lock, got %v", err)
	}
}

func TestWriteConflictAbortsAndReleasesPlacedLocks(t *testing.T) {
	c := New(Options{})
	load(t, c, "a", "1")
	load(t, c, "z", "1")
	txn := c.Begin()
	_ = txn.Set([]byte("a"), []byte("2")) // primary, locked first
	_ = txn.Set([]byte("z"), []byte("2"))
	load(t, c, "z", "9") // someone commits z after txn started
	_, err := txn.Prewrite()
	var wc *WriteConflictError
	if !errors.As(err, &wc) || string(wc.Key) != "z" {
		t.Fatalf("want write conflict on z, got %v", err)
	}
	if c.LockCount() != 0 {
		t.Fatalf("aborted txn left %d locks", c.LockCount())
	}
	if _, err := txn.Prewrite(); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("second prewrite: %v", err)
	}
}

func TestLockConflictAbortsTheLaterTxn(t *testing.T) {
	c := New(Options{})
	first, second := c.Begin(), c.Begin()
	_ = first.Set([]byte("m"), []byte("1"))
	_ = second.Set([]byte("a"), []byte("2"))
	_ = second.Set([]byte("m"), []byte("2"))
	if _, err := first.Prewrite(); err != nil {
		t.Fatal(err)
	}
	_, err := second.Prewrite()
	var le *KeyLockedError
	if !errors.As(err, &le) || le.Lock.StartTS != first.StartTS() {
		t.Fatalf("want KeyLockedError held by first, got %v", err)
	}
	if l, ok := c.LockOf([]byte("a")); ok {
		t.Fatalf("second's lock on a was not released: %+v", l)
	}
	if l, ok := c.LockOf([]byte("m")); !ok || l.StartTS != first.StartTS() {
		t.Fatalf("first's lock on m was disturbed: %+v", l)
	}
}

func TestTxnLifecycleAndInputChecks(t *testing.T) {
	c := New(Options{})
	if _, err := c.Begin().Prewrite(); !errors.Is(err, ErrEmptyTxn) {
		t.Fatalf("empty prewrite: %v", err)
	}
	txn := c.Begin()
	if err := txn.Set(nil, []byte("v")); err == nil {
		t.Fatal("empty key accepted")
	}
	if err := txn.Set([]byte("k"), make([]byte, 1<<20+1)); err == nil {
		t.Fatal("oversized value accepted")
	}
	txn.Rollback()
	if err := txn.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("set after rollback: %v", err)
	}
	if _, _, err := txn.Get([]byte("k")); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("get after rollback: %v", err)
	}
	big := c.Begin()
	for i := 0; i < MaxMutations; i++ {
		if err := big.Set([]byte(strconv.Itoa(i)), nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := big.Set([]byte("one-too-many"), nil); !errors.Is(err, ErrTooManyMutations) {
		t.Fatalf("mutation bound not enforced: %v", err)
	}
	if err := big.Set([]byte("0"), []byte("overwrite")); err != nil {
		t.Fatalf("overwriting a buffered key must stay allowed: %v", err)
	}
	if c.LockCount() != 0 {
		t.Fatal("unprewritten transactions must not lock anything")
	}
}

// TestConcurrentTransfersPrewriteAtomically runs random two-account transfers from many
// goroutines. Every transaction either holds all its locks under one primary or none:
// no lock belongs to an aborted transaction, and no two winners share an account.
func TestConcurrentTransfersPrewriteAtomically(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		c := New(Options{})
		const accounts, workers, perWorker = 10, 8, 50
		for i := 0; i < accounts; i++ {
			load(t, c, fmt.Sprintf("acct-%02d", i), "100")
		}
		type outcome struct {
			start uint64
			keys  []string
			ok    bool
		}
		results := make([][]outcome, workers)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed*100 + int64(w)))
				for i := 0; i < perWorker; i++ {
					from, to := rng.Intn(accounts), rng.Intn(accounts)
					if from == to {
						continue
					}
					txn := c.Begin()
					keys := []string{fmt.Sprintf("acct-%02d", from), fmt.Sprintf("acct-%02d", to)}
					for _, k := range keys {
						if _, _, err := txn.Get([]byte(k)); err != nil {
							var le *KeyLockedError
							if !errors.As(err, &le) {
								t.Errorf("unexpected read error: %v", err)
							}
						}
						_ = txn.Set([]byte(k), []byte("changed"))
					}
					_, err := txn.Prewrite()
					sort.Strings(keys)
					results[w] = append(results[w], outcome{start: txn.StartTS(), keys: keys, ok: err == nil})
				}
			}(w)
		}
		wg.Wait()
		owner := map[string]uint64{}
		wins := 0
		for _, rs := range results {
			for _, r := range rs {
				for _, k := range r.keys {
					l, locked := c.LockOf([]byte(k))
					if r.ok {
						if !locked || l.StartTS != r.start || string(l.Primary) != r.keys[0] {
							t.Fatalf("seed %d: winner %d lost its lock on %s: %+v", seed, r.start, k, l)
						}
						if prev, dup := owner[k]; dup {
							t.Fatalf("seed %d: %s won by %d and %d", seed, k, prev, r.start)
						}
						owner[k] = r.start
					} else if locked && l.StartTS == r.start {
						t.Fatalf("seed %d: aborted txn %d still locks %s", seed, r.start, k)
					}
				}
				if r.ok {
					wins++
				}
			}
		}
		if wins == 0 || c.LockCount() != 2*wins {
			t.Fatalf("seed %d: %d winners but %d locks", seed, wins, c.LockCount())
		}
	}
}
