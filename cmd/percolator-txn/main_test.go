package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestDemoShowsEveryM1Step(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"demo"}, &out); err != nil {
		t.Fatalf("demo: %v\n%s", err, out.String())
	}
	for _, want := range []string{
		"increasing=true",
		"T1 reads alice=100 (its snapshot)",
		"T1 prewrite refused: write conflict on \"alice\"",
		"locks left: 0",
		"T2 prewrite ok: primary=\"alice\" secondaries=[\"bob\"]",
		"not blocked",
		"T3 read alice -> locked by txn",
		"T4 prewrite bob -> refused",
		"locks held: 2",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("demo output lacks %q\n%s", want, out.String())
		}
	}
}

func TestContendChecksInvariants(t *testing.T) {
	var out bytes.Buffer
	if err := run([]string{"contend", "-workers", "4", "-txns", "400", "-accounts", "20", "-seed", "7"}, &out); err != nil {
		t.Fatalf("contend: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "invariants:") || !strings.Contains(out.String(), ": OK") {
		t.Fatalf("missing invariant line:\n%s", out.String())
	}
}

func TestBadArgumentsAreRejected(t *testing.T) {
	for _, args := range [][]string{nil, {"nope"}, {"contend", "-workers", "0"}, {"contend", "-accounts", "1"}, {"contend", "-bogus"}} {
		if err := run(args, &bytes.Buffer{}); err == nil {
			t.Errorf("args %q accepted", args)
		}
	}
}
