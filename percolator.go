// Package percolator implements the client side of Percolator-style distributed transactions.
//
// M1 covers the first half of the protocol: a hybrid timestamp oracle hands every
// transaction a start timestamp, reads see the snapshot as of that timestamp, and the
// prewrite phase locks every written key, naming one key (the smallest) as the primary.
// Committing the primary and resolving locks left by crashed clients are milestone M2.
package percolator

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/EquinoxWN/percolator-txn/internal/mvcc"
	"github.com/EquinoxWN/percolator-txn/internal/oracle"
)

// MaxMutations bounds the write set of one transaction.
const MaxMutations = 10_000

// DefaultLockTTL is the lock lifetime recorded by prewrite when Options.LockTTL is zero.
const DefaultLockTTL = 3 * time.Second

// Errors returned by transactions.
var (
	ErrTxnDone          = errors.New("percolator: transaction already prewritten or aborted")
	ErrEmptyTxn         = errors.New("percolator: nothing to prewrite")
	ErrTooManyMutations = fmt.Errorf("percolator: more than %d mutations in one transaction", MaxMutations)
)

// Re-exported store types so callers can inspect errors and locks with errors.As.
type (
	// KeyLockedError reports a lock that blocks a read or a prewrite.
	KeyLockedError = mvcc.KeyLockedError
	// WriteConflictError reports a version committed after the transaction started.
	WriteConflictError = mvcc.WriteConflictError
	// Lock is the lock column of a row.
	Lock = mvcc.Lock
)

// Options configures a Client.
type Options struct {
	LockTTL time.Duration   // lock lifetime written by prewrite; DefaultLockTTL if zero
	Oracle  []oracle.Option // options for the embedded timestamp oracle
}

// Client owns a timestamp oracle and a store and starts transactions; it is safe for concurrent use.
type Client struct {
	tso   *oracle.Oracle
	store *mvcc.Store
	ttl   time.Duration
}

// New returns a client over a fresh in-memory store.
func New(opts Options) *Client {
	ttl := opts.LockTTL
	if ttl <= 0 {
		ttl = DefaultLockTTL
	}
	return &Client{tso: oracle.New(opts.Oracle...), store: mvcc.New(), ttl: ttl}
}

// Timestamp returns a fresh timestamp from the oracle.
func (c *Client) Timestamp() uint64 { return c.tso.Next() }

// Load commits one key directly (initial data and tests; a nil value commits a deletion)
// and returns the commit timestamp.
// It is a single-row write, not a transaction: multi-key atomic commit is milestone M2.
func (c *Client) Load(key, value []byte) (uint64, error) {
	start := c.tso.Next()
	commit := c.tso.Next()
	m := mvcc.Mutation{Kind: mvcc.Put, Key: key, Value: value}
	if value == nil {
		m.Kind = mvcc.Delete
	}
	if err := c.store.Load(m, start, commit); err != nil {
		return 0, err
	}
	return commit, nil
}

// Get reads key at an explicit snapshot timestamp, outside any transaction.
func (c *Client) Get(key []byte, ts uint64) ([]byte, bool, error) {
	return c.store.Get(key, ts)
}

// LockOf reports the lock currently held on key, if any.
func (c *Client) LockOf(key []byte) (Lock, bool) { return c.store.LockOf(key) }

// LockCount reports how many rows are locked.
func (c *Client) LockCount() int { return c.store.LockCount() }

// state is a transaction's lifecycle stage.
type state uint8

const (
	active state = iota
	prewritten
	aborted
)

// Txn is one snapshot-isolated transaction; it is not safe for concurrent use.
type Txn struct {
	c       *Client
	startTS uint64
	writes  map[string]mvcc.Mutation
	state   state
}

// Begin starts a transaction at a fresh start timestamp.
func (c *Client) Begin() *Txn {
	return &Txn{c: c, startTS: c.tso.Next(), writes: make(map[string]mvcc.Mutation)}
}

// StartTS is the transaction's snapshot timestamp.
func (t *Txn) StartTS() uint64 { return t.startTS }

// Get returns the transaction's own buffered write for key, else the value in its snapshot.
func (t *Txn) Get(key []byte) ([]byte, bool, error) {
	if t.state != active {
		return nil, false, ErrTxnDone
	}
	if m, ok := t.writes[string(key)]; ok {
		if m.Kind == mvcc.Delete {
			return nil, false, nil
		}
		return bytes.Clone(m.Value), true, nil
	}
	return t.c.store.Get(key, t.startTS)
}

// Set buffers a write of value to key.
func (t *Txn) Set(key, value []byte) error {
	if value == nil {
		value = []byte{}
	}
	return t.buffer(mvcc.Mutation{Kind: mvcc.Put, Key: bytes.Clone(key), Value: bytes.Clone(value)})
}

// Delete buffers a deletion of key.
func (t *Txn) Delete(key []byte) error {
	return t.buffer(mvcc.Mutation{Kind: mvcc.Delete, Key: bytes.Clone(key)})
}

// buffer validates and records one mutation.
func (t *Txn) buffer(m mvcc.Mutation) error {
	if t.state != active {
		return ErrTxnDone
	}
	if err := mvcc.ValidateKey(m.Key); err != nil {
		return err
	}
	if len(m.Value) > mvcc.MaxValueSize {
		return mvcc.ErrValueTooLarge
	}
	if _, exists := t.writes[string(m.Key)]; !exists && len(t.writes) >= MaxMutations {
		return ErrTooManyMutations
	}
	t.writes[string(m.Key)] = m
	return nil
}

// PrewriteResult describes a successful prewrite phase.
type PrewriteResult struct {
	StartTS     uint64
	Primary     []byte
	Secondaries [][]byte
}

// Prewrite runs the first phase of two-phase commit: it locks every written key, primary first.
// The primary is the smallest key, so the choice is deterministic. On any conflict the
// transaction releases the locks it already placed, becomes aborted and returns the error
// (*WriteConflictError or *KeyLockedError). After success the locks stay in place until the
// commit phase, which is milestone M2.
func (t *Txn) Prewrite() (PrewriteResult, error) {
	if t.state != active {
		return PrewriteResult{}, ErrTxnDone
	}
	if len(t.writes) == 0 {
		return PrewriteResult{}, ErrEmptyTxn
	}
	keys := make([]string, 0, len(t.writes))
	for k := range t.writes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	primary := []byte(keys[0])
	for i, k := range keys {
		if err := t.c.store.Prewrite(t.writes[k], primary, t.startTS, t.c.ttl); err != nil {
			t.release(keys[:i])
			t.state = aborted
			return PrewriteResult{}, err
		}
	}
	t.state = prewritten
	res := PrewriteResult{StartTS: t.startTS, Primary: primary}
	for _, k := range keys[1:] {
		res.Secondaries = append(res.Secondaries, []byte(k))
	}
	return res, nil
}

// Rollback abandons an active transaction; buffered writes never reach the store.
func (t *Txn) Rollback() {
	if t.state == active {
		t.state = aborted
	}
}

// release removes this transaction's own locks on keys, secondaries before the primary.
func (t *Txn) release(keys []string) {
	for i := len(keys) - 1; i >= 0; i-- {
		// Keys were validated when buffered, so ReleaseOwn cannot fail here.
		_, _ = t.c.store.ReleaseOwn([]byte(keys[i]), t.startTS)
	}
}
