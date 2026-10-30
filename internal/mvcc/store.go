// Package mvcc is an in-memory, multi-version row store with Percolator's three columns.
//
// Each row (key) has:
//   - data:  start_ts -> value, written by prewrite;
//   - lock:  at most one lock, written by prewrite and naming the primary key;
//   - write: commit_ts -> (start_ts, kind), the committed versions readers follow.
//
// Every operation on one row is atomic (a per-shard mutex plays the role of
// Bigtable's single-row transactions). Nothing here spans rows: multi-row
// atomicity is the transaction protocol's job.
package mvcc

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// Input bounds; requests beyond them are refused.
const (
	MaxKeySize   = 4 << 10
	MaxValueSize = 1 << 20
	shardCount   = 64
)

// Kind is the type of a mutation, lock or committed write.
type Kind uint8

// Mutation kinds.
const (
	Put Kind = iota + 1
	Delete
)

// String names the kind.
func (k Kind) String() string {
	switch k {
	case Put:
		return "put"
	case Delete:
		return "delete"
	default:
		return fmt.Sprintf("kind(%d)", uint8(k))
	}
}

// Sentinel errors for invalid input.
var (
	ErrEmptyKey      = errors.New("mvcc: empty key")
	ErrKeyTooLarge   = fmt.Errorf("mvcc: key larger than %d bytes", MaxKeySize)
	ErrValueTooLarge = fmt.Errorf("mvcc: value larger than %d bytes", MaxValueSize)
	ErrBadKind       = errors.New("mvcc: unknown mutation kind")
	ErrBadTimestamp  = errors.New("mvcc: invalid timestamp")
)

// Lock is the lock column of a row, left by a prewrite.
type Lock struct {
	Primary []byte        // the transaction's primary key
	StartTS uint64        // start timestamp of the owning transaction
	Kind    Kind          // mutation this lock protects
	TTL     time.Duration // how long the owner expects to hold it (used by lock resolution, M2)
}

// Mutation is one key change inside a transaction.
type Mutation struct {
	Kind  Kind
	Key   []byte
	Value []byte // nil for Delete
}

// KeyLockedError reports a lock that blocks a read or a prewrite.
type KeyLockedError struct {
	Key  []byte
	Lock Lock
}

// Error describes the blocking lock.
func (e *KeyLockedError) Error() string {
	return fmt.Sprintf("mvcc: key %q locked by txn %d (primary %q)", e.Key, e.Lock.StartTS, e.Lock.Primary)
}

// WriteConflictError reports a version committed at or after the prewriting transaction's start.
type WriteConflictError struct {
	Key      []byte
	StartTS  uint64 // the transaction that lost
	CommitTS uint64 // the committed version it collided with
}

// Error describes the conflict.
func (e *WriteConflictError) Error() string {
	return fmt.Sprintf("mvcc: write conflict on %q: txn %d started before a version committed at %d", e.Key, e.StartTS, e.CommitTS)
}

// write is one entry of the write column.
type write struct {
	commitTS uint64
	startTS  uint64
	kind     Kind
}

// row holds the three columns of one key.
type row struct {
	data   map[uint64][]byte
	lock   *Lock
	writes []write // sorted by commitTS, newest first
}

// shard is a group of rows behind one mutex.
type shard struct {
	mu   sync.Mutex
	rows map[string]*row
}

// Store is the multi-version row store; it is safe for concurrent use.
type Store struct {
	shards [shardCount]shard
}

// New returns an empty store.
func New() *Store {
	s := &Store{}
	for i := range s.shards {
		s.shards[i].rows = make(map[string]*row)
	}
	return s
}

// shardFor picks the shard that owns key.
func (s *Store) shardFor(key []byte) *shard {
	h := fnv.New32a()
	_, _ = h.Write(key)
	return &s.shards[h.Sum32()%shardCount]
}

// rowFor returns the row for key, creating it when create is set (caller holds the shard lock).
func (sh *shard) rowFor(key []byte, create bool) *row {
	r := sh.rows[string(key)]
	if r == nil && create {
		r = &row{data: make(map[uint64][]byte)}
		sh.rows[string(key)] = r
	}
	return r
}

// ValidateKey checks the key bounds.
func ValidateKey(key []byte) error {
	switch {
	case len(key) == 0:
		return ErrEmptyKey
	case len(key) > MaxKeySize:
		return ErrKeyTooLarge
	}
	return nil
}

// validateMutation checks a mutation before it touches the store.
func validateMutation(m Mutation) error {
	if err := ValidateKey(m.Key); err != nil {
		return err
	}
	if m.Kind != Put && m.Kind != Delete {
		return ErrBadKind
	}
	if len(m.Value) > MaxValueSize {
		return ErrValueTooLarge
	}
	return nil
}

// clone copies b so callers can neither change stored bytes nor see later changes.
func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append(make([]byte, 0, len(b)), b...)
}

// cloneLock deep-copies a lock.
func cloneLock(l *Lock) Lock {
	c := *l
	c.Primary = clone(l.Primary)
	return c
}

// Get reads key as of snapshot ts: the newest version committed at or before ts.
// A lock left by a transaction that started at or before ts returns *KeyLockedError,
// because that transaction may still commit with a timestamp the reader must see.
func (s *Store) Get(key []byte, ts uint64) (value []byte, found bool, err error) {
	if err := ValidateKey(key); err != nil {
		return nil, false, err
	}
	if ts == 0 {
		return nil, false, ErrBadTimestamp
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	r := sh.rowFor(key, false)
	if r == nil {
		return nil, false, nil
	}
	if r.lock != nil && r.lock.StartTS <= ts {
		return nil, false, &KeyLockedError{Key: clone(key), Lock: cloneLock(r.lock)}
	}
	for _, w := range r.writes {
		if w.commitTS > ts {
			continue
		}
		if w.kind == Delete {
			return nil, false, nil
		}
		return clone(r.data[w.startTS]), true, nil
	}
	return nil, false, nil
}

// Prewrite writes m's data at startTS and locks the row for the transaction whose primary is primary.
// It fails with *WriteConflictError if a version was committed at or after startTS, and with
// *KeyLockedError if another transaction holds the lock. Repeating a prewrite that already
// succeeded for the same startTS is a no-op, so a retried request is safe.
func (s *Store) Prewrite(m Mutation, primary []byte, startTS uint64, ttl time.Duration) error {
	if err := validateMutation(m); err != nil {
		return err
	}
	if err := ValidateKey(primary); err != nil {
		return fmt.Errorf("primary: %w", err)
	}
	if startTS == 0 {
		return ErrBadTimestamp
	}
	sh := s.shardFor(m.Key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	r := sh.rowFor(m.Key, true)
	if len(r.writes) > 0 && r.writes[0].commitTS >= startTS {
		return &WriteConflictError{Key: clone(m.Key), StartTS: startTS, CommitTS: r.writes[0].commitTS}
	}
	if r.lock != nil {
		if r.lock.StartTS == startTS {
			return nil // our own lock: idempotent retry
		}
		return &KeyLockedError{Key: clone(m.Key), Lock: cloneLock(r.lock)}
	}
	if m.Kind == Put {
		r.data[startTS] = clone(m.Value)
	}
	r.lock = &Lock{Primary: clone(primary), StartTS: startTS, Kind: m.Kind, TTL: ttl}
	return nil
}

// ReleaseOwn removes the lock and data a transaction wrote at startTS, if that lock is still there.
// It is how a client undoes its own prewrites after a failed prewrite phase; it never touches
// another transaction's lock and never removes committed versions. It reports whether a lock was removed.
func (s *Store) ReleaseOwn(key []byte, startTS uint64) (bool, error) {
	if err := ValidateKey(key); err != nil {
		return false, err
	}
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	r := sh.rowFor(key, false)
	if r == nil || r.lock == nil || r.lock.StartTS != startTS {
		return false, nil
	}
	r.lock = nil
	delete(r.data, startTS)
	if len(r.writes) == 0 && len(r.data) == 0 {
		delete(sh.rows, string(key))
	}
	return true, nil
}

// Load installs an already-committed version of one key (initial data, imports and tests).
// It is a single-row write that bypasses two-phase commit; it refuses rows that are locked and
// commit timestamps that are not newer than the row's latest version, so history stays ordered.
func (s *Store) Load(m Mutation, startTS, commitTS uint64) error {
	if err := validateMutation(m); err != nil {
		return err
	}
	if startTS == 0 || commitTS <= startTS {
		return ErrBadTimestamp
	}
	sh := s.shardFor(m.Key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	r := sh.rowFor(m.Key, true)
	if r.lock != nil {
		return &KeyLockedError{Key: clone(m.Key), Lock: cloneLock(r.lock)}
	}
	if len(r.writes) > 0 && r.writes[0].commitTS >= commitTS {
		return &WriteConflictError{Key: clone(m.Key), StartTS: startTS, CommitTS: r.writes[0].commitTS}
	}
	if _, taken := r.data[startTS]; taken {
		return ErrBadTimestamp // another version already owns this start timestamp
	}
	if m.Kind == Put {
		r.data[startTS] = clone(m.Value)
	}
	r.writes = append([]write{{commitTS: commitTS, startTS: startTS, kind: m.Kind}}, r.writes...)
	return nil
}

// LockOf returns a copy of the lock on key, if any.
func (s *Store) LockOf(key []byte) (Lock, bool) {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	r := sh.rowFor(key, false)
	if r == nil || r.lock == nil {
		return Lock{}, false
	}
	return cloneLock(r.lock), true
}

// LockCount returns the number of locked rows (for tests and the demo).
func (s *Store) LockCount() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		for _, r := range sh.rows {
			if r.lock != nil {
				n++
			}
		}
		sh.mu.Unlock()
	}
	return n
}
