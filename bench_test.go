package percolator

import (
	"fmt"
	"testing"

	"github.com/EquinoxWN/percolator-txn/internal/oracle"
)

// BenchmarkOracleNext measures one timestamp allocation.
func BenchmarkOracleNext(b *testing.B) {
	o := oracle.New()
	for i := 0; i < b.N; i++ {
		o.Next()
	}
}

// BenchmarkOracleNextParallel measures timestamp allocation under contention.
func BenchmarkOracleNextParallel(b *testing.B) {
	o := oracle.New()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			o.Next()
		}
	})
}

// BenchmarkSnapshotGet reads one key with 10 committed versions, 10,000 keys in the store.
func BenchmarkSnapshotGet(b *testing.B) {
	c := New(Options{})
	const keys = 10_000
	for v := 0; v < 10; v++ {
		for k := 0; k < keys; k++ {
			if _, err := c.Load([]byte(fmt.Sprintf("key-%05d", k)), []byte("value-0123456789")); err != nil {
				b.Fatal(err)
			}
		}
	}
	txn := c.Begin()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := txn.Get([]byte(fmt.Sprintf("key-%05d", i%keys))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrewrite2Keys runs begin, two sets and prewrite on fresh keys (a transfer's write set).
func BenchmarkPrewrite2Keys(b *testing.B) {
	c := New(Options{})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := c.Begin()
		_ = txn.Set([]byte(fmt.Sprintf("from-%09d", i)), []byte("70"))
		_ = txn.Set([]byte(fmt.Sprintf("to-%09d", i)), []byte("130"))
		if _, err := txn.Prewrite(); err != nil {
			b.Fatal(err)
		}
	}
}
