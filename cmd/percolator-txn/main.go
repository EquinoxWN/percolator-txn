// Command percolator-txn walks through the M1 protocol: timestamps, snapshot reads and prewrite.
//
//	percolator-txn demo                       # scripted bank scenario with a trace
//	percolator-txn contend -workers 8 -txns 2000 -accounts 50 -seed 1
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	percolator "github.com/EquinoxWN/percolator-txn"
	"github.com/EquinoxWN/percolator-txn/internal/oracle"
)

// main runs the CLI and exits non-zero on error.
func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// run dispatches a subcommand.
func run(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: percolator-txn demo | contend [-workers N] [-txns N] [-accounts N] [-seed N]")
	}
	switch args[0] {
	case "demo":
		return demo(out)
	case "contend":
		fs := flag.NewFlagSet("contend", flag.ContinueOnError)
		fs.SetOutput(out)
		workers := fs.Int("workers", 8, "concurrent clients (1-64)")
		txns := fs.Int("txns", 2000, "transfer transactions in total (1-1000000)")
		accounts := fs.Int("accounts", 50, "accounts (2-100000)")
		seed := fs.Int64("seed", 1, "random seed")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if *workers < 1 || *workers > 64 || *txns < 1 || *txns > 1_000_000 || *accounts < 2 || *accounts > 100_000 {
			return errors.New("contend: flag out of range")
		}
		return contend(out, *workers, *txns, *accounts, *seed)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

// tsString shows a timestamp with its physical and logical parts.
func tsString(ts uint64) string {
	return fmt.Sprintf("%d (ms %d + logical %d)", ts, oracle.Physical(ts).UnixMilli(), oracle.Logical(ts))
}

// demo runs a scripted scenario and prints what each step observes.
func demo(out io.Writer) error {
	c := percolator.New(percolator.Options{})
	p := func(format string, a ...any) { fmt.Fprintf(out, format+"\n", a...) }

	p("== 1. timestamp oracle")
	a, b := c.Timestamp(), c.Timestamp()
	p("next: %s", tsString(a))
	p("next: %s  increasing=%v", tsString(b), b > a)

	p("== 2. initial balances (single-key loads)")
	for _, k := range []string{"alice", "bob"} {
		ts, err := c.Load([]byte(k), []byte("100"))
		if err != nil {
			return err
		}
		p("load %s=100 committed at %d", k, ts)
	}

	p("== 3. snapshot isolation")
	t1 := c.Begin()
	p("T1 begins: start_ts=%d", t1.StartTS())
	ts, err := c.Load([]byte("alice"), []byte("80"))
	if err != nil {
		return err
	}
	p("alice=80 committed at %d (after T1 started)", ts)
	v, _, err := t1.Get([]byte("alice"))
	if err != nil {
		return err
	}
	p("T1 reads alice=%s (its snapshot), a new txn would read 80", v)

	p("== 4. prewrite: T1 moves 30 from alice to bob")
	_ = t1.Set([]byte("alice"), []byte("70"))
	_ = t1.Set([]byte("bob"), []byte("130"))
	_, err = t1.Prewrite()
	var wc *percolator.WriteConflictError
	if !errors.As(err, &wc) {
		return fmt.Errorf("expected a write conflict, got %v", err)
	}
	p("T1 prewrite refused: write conflict on %q (committed at %d >= start %d); locks left: %d",
		wc.Key, wc.CommitTS, wc.StartTS, c.LockCount())

	t2 := c.Begin()
	p("T2 begins: start_ts=%d", t2.StartTS())
	_ = t2.Set([]byte("bob"), []byte("110"))
	_ = t2.Set([]byte("alice"), []byte("60"))
	res, err := t2.Prewrite()
	if err != nil {
		return err
	}
	p("T2 prewrite ok: primary=%q secondaries=%q", res.Primary, res.Secondaries)
	for _, k := range []string{"alice", "bob"} {
		l, _ := c.LockOf([]byte(k))
		p("lock on %-5s -> primary=%q start_ts=%d kind=%s ttl=%s", k, l.Primary, l.StartTS, l.Kind, l.TTL)
	}

	p("== 5. readers and writers meet the locks")
	v, _, err = c.Get([]byte("alice"), t2.StartTS()-1)
	if err != nil {
		return err
	}
	p("read alice at ts %d (before T2 started) = %s, not blocked", t2.StartTS()-1, v)
	_, _, err = c.Begin().Get([]byte("alice"))
	var le *percolator.KeyLockedError
	if !errors.As(err, &le) {
		return fmt.Errorf("expected a lock, got %v", err)
	}
	p("T3 read alice -> locked by txn %d, primary %q (resolution is M2)", le.Lock.StartTS, le.Lock.Primary)
	t4 := c.Begin()
	_ = t4.Set([]byte("bob"), []byte("0"))
	if _, err = t4.Prewrite(); !errors.As(err, &le) {
		return fmt.Errorf("expected a lock conflict, got %v", err)
	}
	p("T4 prewrite bob -> refused, key locked by txn %d", le.Lock.StartTS)
	p("locks held: %d (T2's, waiting for the M2 commit phase)", c.LockCount())
	return nil
}

// contend runs random transfers from many clients and checks the prewrite invariants.
func contend(out io.Writer, workers, txns, accounts int, seed int64) error {
	c := percolator.New(percolator.Options{})
	name := func(i int) []byte { return []byte(fmt.Sprintf("acct-%06d", i)) }
	for i := 0; i < accounts; i++ {
		if _, err := c.Load(name(i), []byte("100")); err != nil {
			return err
		}
	}
	type result struct {
		start uint64
		keys  [][]byte
		err   error
	}
	results := make([][]result, workers)
	var wg sync.WaitGroup
	begin := time.Now()
	for w := 0; w < workers; w++ {
		n := txns / workers
		if w < txns%workers {
			n++
		}
		wg.Add(1)
		go func(w, n int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed*1_000_003 + int64(w)))
			for i := 0; i < n; i++ {
				from := rng.Intn(accounts)
				to := (from + 1 + rng.Intn(accounts-1)) % accounts
				txn := c.Begin()
				keys := [][]byte{name(from), name(to)}
				for _, k := range keys {
					_, _, _ = txn.Get(k) // may meet a lock; prewrite decides
					_ = txn.Set(k, []byte("changed"))
				}
				_, err := txn.Prewrite()
				sort.Slice(keys, func(i, j int) bool { return string(keys[i]) < string(keys[j]) })
				results[w] = append(results[w], result{start: txn.StartTS(), keys: keys, err: err})
			}
		}(w, n)
	}
	wg.Wait()
	elapsed := time.Since(begin)

	var ok, conflicts, locked int
	for _, rs := range results {
		for _, r := range rs {
			var wc *percolator.WriteConflictError
			var le *percolator.KeyLockedError
			switch {
			case r.err == nil:
				ok++
				for _, k := range r.keys {
					l, held := c.LockOf(k)
					if !held || l.StartTS != r.start || string(l.Primary) != string(r.keys[0]) {
						return fmt.Errorf("invariant broken: txn %d does not hold %s under primary %s", r.start, k, r.keys[0])
					}
				}
			case errors.As(r.err, &wc):
				conflicts++
			case errors.As(r.err, &le):
				locked++
			default:
				return r.err
			}
			if r.err != nil {
				for _, k := range r.keys {
					if l, held := c.LockOf(k); held && l.StartTS == r.start {
						return fmt.Errorf("invariant broken: aborted txn %d still locks %s", r.start, k)
					}
				}
			}
		}
	}
	if c.LockCount() != 2*ok {
		return fmt.Errorf("invariant broken: %d locks for %d prewritten txns", c.LockCount(), ok)
	}
	fmt.Fprintf(out, "workers=%d txns=%d accounts=%d seed=%d\n", workers, txns, accounts, seed)
	fmt.Fprintf(out, "prewritten=%d write_conflicts=%d key_locked=%d locks_held=%d\n", ok, conflicts, locked, c.LockCount())
	fmt.Fprintf(out, "elapsed=%s (%.0f txns/s)\n", elapsed.Round(time.Millisecond), float64(txns)/elapsed.Seconds())
	fmt.Fprintln(out, "invariants: every prewritten txn holds all its locks under one primary; aborted txns hold none: OK")
	return nil
}
