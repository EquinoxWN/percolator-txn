package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// put builds a Put mutation.
func put(k, v string) Mutation { return Mutation{Kind: Put, Key: []byte(k), Value: []byte(v)} }

// del builds a Delete mutation.
func del(k string) Mutation { return Mutation{Kind: Delete, Key: []byte(k)} }

// mustLoad loads a committed version or fails the test.
func mustLoad(t *testing.T, s *Store, m Mutation, start, commit uint64) {
	t.Helper()
	if err := s.Load(m, start, commit); err != nil {
		t.Fatalf("Load(%q, %d, %d): %v", m.Key, start, commit, err)
	}
}

// expect checks a snapshot read.
func expect(t *testing.T, s *Store, key string, ts uint64, want string, wantFound bool) {
	t.Helper()
	v, ok, err := s.Get([]byte(key), ts)
	if err != nil {
		t.Fatalf("Get(%q, %d): %v", key, ts, err)
	}
	if ok != wantFound || string(v) != want {
		t.Fatalf("Get(%q, %d) = %q,%v; want %q,%v", key, ts, v, ok, want, wantFound)
	}
}

func TestGetMissingKey(t *testing.T) {
	expect(t, New(), "nope", 100, "", false)
}

func TestSnapshotReadSeesVersionAtTimestamp(t *testing.T) {
	s := New()
	mustLoad(t, s, put("a", "v1"), 10, 11)
	mustLoad(t, s, put("a", "v2"), 20, 21)
	mustLoad(t, s, put("a", "v3"), 30, 31)
	expect(t, s, "a", 5, "", false)    // before any commit
	expect(t, s, "a", 11, "v1", true)  // exactly at commit ts
	expect(t, s, "a", 20, "v1", true)  // v2 started but not committed yet at 20
	expect(t, s, "a", 25, "v2", true)  // between commits
	expect(t, s, "a", 999, "v3", true) // latest
}

func TestDeleteHidesOlderVersionsOnlyAfterItCommits(t *testing.T) {
	s := New()
	mustLoad(t, s, put("a", "v1"), 10, 11)
	mustLoad(t, s, del("a"), 20, 21)
	expect(t, s, "a", 15, "v1", true)
	expect(t, s, "a", 21, "", false)
}

func TestLockBlocksReadersAtOrAfterItsStart(t *testing.T) {
	s := New()
	mustLoad(t, s, put("a", "old"), 10, 11)
	if err := s.Prewrite(put("a", "new"), []byte("a"), 50, time.Second); err != nil {
		t.Fatal(err)
	}
	for _, ts := range []uint64{50, 51, 1000} {
		_, _, err := s.Get([]byte("a"), ts)
		var le *KeyLockedError
		if !errors.As(err, &le) {
			t.Fatalf("Get at %d: want KeyLockedError, got %v", ts, err)
		}
		if le.Lock.StartTS != 50 || string(le.Lock.Primary) != "a" || le.Lock.Kind != Put || le.Lock.TTL != time.Second {
			t.Fatalf("lock details wrong: %+v", le.Lock)
		}
	}
	// A reader whose snapshot is older than the lock is not blocked: the locking
	// transaction can only commit after ts 50, which that snapshot never sees.
	expect(t, s, "a", 49, "old", true)
}

func TestPrewriteDetectsWriteConflict(t *testing.T) {
	s := New()
	mustLoad(t, s, put("a", "v1"), 10, 30)
	for _, start := range []uint64{20, 30} { // started before, or at, the commit
		err := s.Prewrite(put("a", "x"), []byte("a"), start, 0)
		var wc *WriteConflictError
		if !errors.As(err, &wc) || wc.CommitTS != 30 || wc.StartTS != start {
			t.Fatalf("start %d: want WriteConflictError at 30, got %v", start, err)
		}
	}
	if err := s.Prewrite(put("a", "x"), []byte("a"), 31, 0); err != nil {
		t.Fatalf("start after the commit must succeed: %v", err)
	}
}

func TestPrewriteRefusesKeyLockedByAnotherTxn(t *testing.T) {
	s := New()
	if err := s.Prewrite(put("k", "1"), []byte("p1"), 100, 0); err != nil {
		t.Fatal(err)
	}
	for _, start := range []uint64{90, 110} { // older and younger transactions alike
		err := s.Prewrite(put("k", "2"), []byte("p2"), start, 0)
		var le *KeyLockedError
		if !errors.As(err, &le) || le.Lock.StartTS != 100 || string(le.Lock.Primary) != "p1" {
			t.Fatalf("start %d: want KeyLockedError by txn 100, got %v", start, err)
		}
	}
}

func TestPrewriteRetryIsIdempotent(t *testing.T) {
	s := New()
	m := put("k", "1")
	for i := 0; i < 3; i++ {
		if err := s.Prewrite(m, []byte("k"), 100, 0); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if s.LockCount() != 1 {
		t.Fatalf("want 1 lock, got %d", s.LockCount())
	}
}

func TestReleaseOwnOnlyRemovesTheCallersLock(t *testing.T) {
	s := New()
	mustLoad(t, s, put("k", "committed"), 1, 2)
	if err := s.Prewrite(put("k", "pending"), []byte("k"), 10, 0); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.ReleaseOwn([]byte("k"), 9); ok || err != nil {
		t.Fatalf("another txn's release must be a no-op: %v %v", ok, err)
	}
	if _, locked := s.LockOf([]byte("k")); !locked {
		t.Fatal("lock disappeared after a foreign release")
	}
	if ok, err := s.ReleaseOwn([]byte("k"), 10); !ok || err != nil {
		t.Fatalf("own release: %v %v", ok, err)
	}
	if s.LockCount() != 0 {
		t.Fatal("lock still present")
	}
	expect(t, s, "k", 100, "committed", true) // committed history untouched
}

func TestInputValidation(t *testing.T) {
	s := New()
	big := bytes.Repeat([]byte("x"), MaxKeySize+1)
	cases := []struct {
		name string
		err  error
		want error
	}{
		{"get empty key", func() error { _, _, e := s.Get(nil, 1); return e }(), ErrEmptyKey},
		{"get huge key", func() error { _, _, e := s.Get(big, 1); return e }(), ErrKeyTooLarge},
		{"get ts 0", func() error { _, _, e := s.Get([]byte("a"), 0); return e }(), ErrBadTimestamp},
		{"prewrite bad kind", s.Prewrite(Mutation{Kind: 9, Key: []byte("a")}, []byte("a"), 1, 0), ErrBadKind},
		{"prewrite huge value", s.Prewrite(Mutation{Kind: Put, Key: []byte("a"), Value: make([]byte, MaxValueSize+1)}, []byte("a"), 1, 0), ErrValueTooLarge},
		{"prewrite empty primary", s.Prewrite(put("a", "1"), nil, 1, 0), ErrEmptyKey},
		{"prewrite ts 0", s.Prewrite(put("a", "1"), []byte("a"), 0, 0), ErrBadTimestamp},
		{"load commit before start", s.Load(put("a", "1"), 5, 5), ErrBadTimestamp},
		{"release empty key", func() error { _, e := s.ReleaseOwn(nil, 1); return e }(), ErrEmptyKey},
	}
	for _, c := range cases {
		if !errors.Is(c.err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, c.err, c.want)
		}
	}
	if s.LockCount() != 0 {
		t.Fatal("a refused request left a lock behind")
	}
}

func TestLoadKeepsHistoryOrdered(t *testing.T) {
	s := New()
	mustLoad(t, s, put("a", "v1"), 10, 20)
	var wc *WriteConflictError
	if err := s.Load(put("a", "v0"), 5, 15); !errors.As(err, &wc) {
		t.Fatalf("older commit ts must be refused, got %v", err)
	}
	if err := s.Load(put("a", "dup"), 10, 30); !errors.Is(err, ErrBadTimestamp) {
		t.Fatalf("reused start ts must be refused, got %v", err)
	}
	if err := s.Prewrite(put("a", "p"), []byte("a"), 40, 0); err != nil {
		t.Fatal(err)
	}
	var le *KeyLockedError
	if err := s.Load(put("a", "v2"), 50, 60); !errors.As(err, &le) {
		t.Fatalf("load into a locked row must be refused, got %v", err)
	}
}

func TestValuesAreCopied(t *testing.T) {
	s := New()
	in := []byte("abc")
	mustLoad(t, s, Mutation{Kind: Put, Key: []byte("k"), Value: in}, 1, 2)
	in[0] = 'X'
	out, _, _ := s.Get([]byte("k"), 5)
	if string(out) != "abc" {
		t.Fatalf("store kept a reference to the caller's slice: %q", out)
	}
	out[0] = 'Y'
	expect(t, s, "k", 5, "abc", true)
}

// modelVersion is one committed version in the reference model.
type modelVersion struct {
	commitTS uint64
	value    string
	deleted  bool
}

// TestRandomHistoriesMatchReferenceModel replays random histories of commits, prewrites,
// releases and snapshot reads against a plain-slice model of snapshot isolation.
func TestRandomHistoriesMatchReferenceModel(t *testing.T) {
	const seeds, ops = 200, 400
	keys := []string{"a", "b", "c", "d", "e"}
	for seed := int64(1); seed <= seeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		s := New()
		model := map[string][]modelVersion{} // oldest first
		locks := map[string]uint64{}         // key -> lock start ts
		var ts uint64 = 10                   // leaves room for start timestamps drawn from the past
		for op := 0; op < ops; op++ {
			k := keys[rng.Intn(len(keys))]
			ts += uint64(1 + rng.Intn(3))
			switch rng.Intn(5) {
			case 0: // commit a new version directly
				deleted := rng.Intn(4) == 0
				v := fmt.Sprintf("%s@%d", k, ts)
				m := put(k, v)
				if deleted {
					m = del(k)
				}
				err := s.Load(m, ts, ts+1)
				if _, locked := locks[k]; locked {
					var le *KeyLockedError
					if !errors.As(err, &le) {
						t.Fatalf("seed %d op %d: load into locked row: %v", seed, op, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("seed %d op %d: load: %v", seed, op, err)
				}
				model[k] = append(model[k], modelVersion{commitTS: ts + 1, value: v, deleted: deleted})
				ts++
			case 1: // prewrite with a start ts drawn from the recent past
				start := ts - uint64(rng.Intn(6))
				err := s.Prewrite(put(k, "pending"), []byte(k), start, 0)
				hist := model[k]
				conflict := len(hist) > 0 && hist[len(hist)-1].commitTS >= start
				lockTS, locked := locks[k]
				switch {
				case conflict:
					var wc *WriteConflictError
					if !errors.As(err, &wc) {
						t.Fatalf("seed %d op %d: want write conflict, got %v", seed, op, err)
					}
				case locked && lockTS != start:
					var le *KeyLockedError
					if !errors.As(err, &le) {
						t.Fatalf("seed %d op %d: want key locked, got %v", seed, op, err)
					}
				default:
					if err != nil {
						t.Fatalf("seed %d op %d: prewrite: %v", seed, op, err)
					}
					locks[k] = start
				}
			case 2: // release the current lock
				if lockTS, locked := locks[k]; locked {
					if ok, err := s.ReleaseOwn([]byte(k), lockTS); !ok || err != nil {
						t.Fatalf("seed %d op %d: release: %v %v", seed, op, err, ok)
					}
					delete(locks, k)
				}
			default: // snapshot read at a random point in the past
				readTS := ts - uint64(rng.Intn(int(ts)))
				if readTS == 0 {
					readTS = 1
				}
				v, found, err := s.Get([]byte(k), readTS)
				if lockTS, locked := locks[k]; locked && lockTS <= readTS {
					var le *KeyLockedError
					if !errors.As(err, &le) {
						t.Fatalf("seed %d op %d: read at %d should meet lock %d, got %v", seed, op, readTS, lockTS, err)
					}
					continue
				}
				if err != nil {
					t.Fatalf("seed %d op %d: get: %v", seed, op, err)
				}
				wantV, wantFound := "", false
				for _, mv := range model[k] {
					if mv.commitTS <= readTS {
						wantV, wantFound = mv.value, !mv.deleted
						if mv.deleted {
							wantV = ""
						}
					}
				}
				if found != wantFound || string(v) != wantV {
					t.Fatalf("seed %d op %d: Get(%s,%d)=%q,%v want %q,%v", seed, op, k, readTS, v, found, wantV, wantFound)
				}
			}
		}
		if s.LockCount() != len(locks) {
			t.Fatalf("seed %d: %d locks in store, model has %d", seed, s.LockCount(), len(locks))
		}
	}
}

// TestConcurrentPrewritesNeverShareAKey has many goroutines race for the same rows:
// exactly one prewrite per row wins and every lock names its winner.
func TestConcurrentPrewritesNeverShareAKey(t *testing.T) {
	s := New()
	const workers, rows = 16, 32
	var wg sync.WaitGroup
	winners := make([][]int, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := uint64(1000 + w)
			for r := 0; r < rows; r++ {
				key := []byte(fmt.Sprintf("row-%02d", r))
				if err := s.Prewrite(put(string(key), "v"), key, start, 0); err == nil {
					winners[w] = append(winners[w], r)
				}
			}
		}(w)
	}
	wg.Wait()
	total := 0
	for w, rs := range winners {
		for _, r := range rs {
			l, ok := s.LockOf([]byte(fmt.Sprintf("row-%02d", r)))
			if !ok || l.StartTS != uint64(1000+w) {
				t.Fatalf("row %d: winner %d but lock is %+v", r, w, l)
			}
		}
		total += len(rs)
	}
	if total != rows || s.LockCount() != rows {
		t.Fatalf("want exactly %d winners and locks, got %d winners, %d locks", rows, total, s.LockCount())
	}
}
