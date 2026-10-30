// Package oracle implements a hybrid timestamp oracle (TSO).
//
// A timestamp is the physical time in milliseconds shifted left by LogicalBits,
// plus a logical counter. Every call to Next returns a value strictly greater
// than every value returned before, even if the wall clock goes backwards or
// more than 2^LogicalBits timestamps are requested in one millisecond.
package oracle

import (
	"sync"
	"time"
)

// LogicalBits is the number of low bits used by the logical counter.
const LogicalBits = 18

const logicalMask = 1<<LogicalBits - 1

// Oracle hands out strictly increasing timestamps; it is safe for concurrent use.
type Oracle struct {
	mu       sync.Mutex
	clock    func() time.Time
	physical int64 // milliseconds of the last timestamp issued
	logical  int64 // logical counter of the last timestamp issued
}

// Option configures an Oracle.
type Option func(*Oracle)

// WithClock replaces the wall clock (used by tests).
func WithClock(clock func() time.Time) Option {
	return func(o *Oracle) { o.clock = clock }
}

// WithFloor makes every future timestamp greater than floor (for example the last one persisted).
func WithFloor(floor uint64) Option {
	return func(o *Oracle) {
		o.physical = int64(floor >> LogicalBits)
		o.logical = int64(floor & logicalMask)
	}
}

// New returns an oracle using the system clock unless an option says otherwise.
func New(opts ...Option) *Oracle {
	o := &Oracle{clock: time.Now}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// Next returns a timestamp strictly greater than any returned before.
func (o *Oracle) Next() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock().UnixMilli()
	switch {
	case now > o.physical:
		o.physical, o.logical = now, 0
	case o.logical < logicalMask:
		o.logical++
	default:
		// Logical counter exhausted in this millisecond: borrow the next one.
		o.physical, o.logical = o.physical+1, 0
	}
	return Compose(o.physical, o.logical)
}

// Last returns the most recent timestamp issued (0 if none and no floor).
func (o *Oracle) Last() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return Compose(o.physical, o.logical)
}

// Compose builds a timestamp from its physical and logical parts.
func Compose(physicalMillis, logical int64) uint64 {
	return uint64(physicalMillis)<<LogicalBits | uint64(logical&logicalMask)
}

// Physical returns the wall-clock part of a timestamp.
func Physical(ts uint64) time.Time {
	return time.UnixMilli(int64(ts >> LogicalBits))
}

// Logical returns the logical counter part of a timestamp.
func Logical(ts uint64) int64 {
	return int64(ts & logicalMask)
}
