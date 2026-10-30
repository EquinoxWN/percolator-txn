# ADR 0002: Hybrid physical-logical timestamps from a single oracle

- **Status:** Accepted

## Context

Snapshot isolation needs a total order of start and commit timestamps. The options were:

1. a pure logical counter (an integer incremented per call);
2. a hybrid timestamp, physical milliseconds shifted left 18 bits plus a logical counter
   (the layout TiKV's placement driver uses);
3. per-node hybrid logical clocks (HLC, as in CockroachDB), with no central oracle.

Lock resolution (M2) must decide whether a lock's owner is probably dead by comparing the lock's
age with its TTL, so timestamps should carry wall-clock meaning. Per-node clocks need an
uncertainty window and read restarts, which is a different protocol from Percolator's.

## Decision

Use one oracle that issues hybrid timestamps (option 2). `Next` never returns a value lower than
or equal to a previous one: a clock that goes backwards keeps the old physical part and advances
the logical counter, and an exhausted counter borrows the next millisecond.

## Consequences

- A start timestamp tells roughly when the transaction began, so M2 can compare lock age with
  `Lock.TTL` without a separate field.
- At most 2^18 (262,144) timestamps per millisecond before borrowing ahead; measured `Next` cost
  is about 114 ns, far below that limit.
- The oracle is a single point of serialisation. In-process this costs a mutex; a networked
  oracle will need batched allocation, as TiKV does.
- A restarted oracle must start above the last issued value (`WithFloor`); persisting that floor
  belongs with the commit path.
