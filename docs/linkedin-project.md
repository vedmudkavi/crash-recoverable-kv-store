# LinkedIn project entry

**Project name:** Crash-Recoverable Key-Value Store

**Description:**
Built a persistent key-value storage engine in Python with a checksummed
write-ahead log, disk-backed reads, an in-memory key-to-offset index, atomic
batch writes, conditional compare-and-swap updates, forward/reverse prefix scans, coalesced multi-key reads, log compaction, and consistent backups with verified restore. Added a non-mutating storage inspector and a
failure-testing harness that kills worker processes at six persistence
checkpoints and verifies recovery. Validated behavior with 57 automated tests,
including 1,500 randomized batches checked against a reference model, and
created reproducible benchmarks for commit latency, read throughput, recovery,
and memory use. Uses Python's standard library with no external database engine.

**Skills:** Python · Storage Engines · File I/O · Data Integrity · Concurrency ·
Fault Injection · Performance Benchmarking · Automated Testing

**Project URL:** https://github.com/vedmudkavi/crash-recoverable-kv-store

## A 60–90 second demo

1. Open the README and explain the database in one sentence.
2. Run `python3 -m examples.crash_demo`: show acknowledged writes surviving SIGKILL.
3. Run `python3 -m examples.crash_matrix`: show the six verified crash boundaries.
4. Store a key and run `python3 -m kvstore inspect`: explain offsets and checksums.
5. Show the index in `store.py`: values are read from disk, not retained in memory.
6. Finish with the benchmark output and one tradeoff: larger batches reduce sync
   overhead but require decoding more data when reading a single key.

## Before posting

Run the commands yourself and understand the recovery and compaction ordering.
Use recorded benchmark numbers only with their machine and workload context.
Describe this as a tested educational storage engine. The tests establish process
crash behavior, not universal power-loss safety or production readiness.
