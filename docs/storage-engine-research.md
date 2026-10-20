# Storage-engine research and implemented decisions

Reviewed on 2026-10-20. This is a focused review of primary project documentation,
not an exhaustive audit of every implementation. The changes are independently
implemented in Python; no upstream source code was copied.

| Reference | Lesson | Decision in this project |
|---|---|---|
| [RocksDB MultiGet](https://github.com/facebook/rocksdb/wiki/MultiGet-Performance) | Batching lookups can avoid repeated work on shared storage structures. | Group requested keys by WAL frame; read, checksum, and decode each frame once per request. |
| [RoseDB](https://github.com/rosedblabs/rosedb) | An in-memory index supports forward and backward traversal of disk-backed values. | Cache sorted keys, use binary-search bounds for prefix/cursor selection, and support reverse scans. |
| [Bitcask design](https://github.com/basho/bitcask/blob/develop/doc/bitcask-intro.pdf) | Key-to-offset indexing separates live data from append-only history. | Expose referenced and obsolete frame bytes to make log growth visible. |
| [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html) | Write/sync/publication order and failure assumptions are central to correctness. | Retain the existing durability ordering and run the full crash suite after read-path changes; no SQLite-equivalent durability claim. |

## What changed

`get_many(keys)` preserves input order and duplicates, returning None for missing
keys (stored values are always strings). All lookups share the database mutex, so
one request sees a consistent state. Requested positions are grouped by frame;
only one decoded frame is retained at a time, plus the result and grouping metadata.
Checksums remain enabled. There is no persistent value cache to hide later damage.

Scans now use a lazily built sorted list of key references. Adding/deleting a key
invalidates it; updating an existing value does not. The first scan after a keyset
change costs O(n log n), with O(n) extra references. Subsequent page selection is
O(log n + page size), excluding frame reads/decoding. Writes keep their hash-index
behavior instead of paying O(n) insertion costs on every write. Write-heavy scan
workloads may rebuild often; this is not a B-tree.

`reverse=True` reverses both traversal and cursor comparison. Prefix bounds handle
Unicode's maximum code point without constructing an invalid successor. Scans
use get_many internally so values sharing a frame avoid redundant decoding.

`stats()` counts each referenced frame once. `obsolete_frame_bytes` excludes
frames referenced by any live key. A partly obsolete batch remains referenced;
therefore this is not an exact prediction of space saved by compaction. Rewriting
batches as individual records can also change framing overhead. Computing these
statistics is O(number of live keys).

## Evidence

Run `python3 -m examples.read_efficiency` to reproduce the checked-in
[read-call results](read-efficiency-results.json). For 100 values written in one
batch, 100 individual Gets perform 100 pread calls; one get_many performs one.
Both return the same values. This is a locality-specific operation count, not a
latency benchmark, a physical-disk-I/O count, or a comparison with RocksDB/RoseDB.
When each key occupies a different frame, this optimization cannot eliminate
those frame reads.

New tests cover frame coalescing, corruption after an earlier successful read,
missing and duplicate keys, cache invalidation, reverse cursors, Unicode prefix
bounds, compaction accounting, and randomized keyset changes. Existing recovery,
backup, conditional-update, and crash-injection tests remain enabled.

## Deferred ideas

- Segmented logs and hint files: valuable for bounded log size and faster startup,
  but need a persistent manifest and crash testing across multiple file lifetimes.
- TTL: requires durable expiry timestamps and clear semantics across reads,
  backups, compaction, and conditional updates. A timer alone would be incomplete.
- Bloom filters: limited benefit while all keys are already in an in-memory map;
  revisit if disk-resident sorted tables are introduced.
- Background compaction: requires safe reader/file lifetimes and a stronger
  publication protocol. Current compaction deliberately blocks operations.

These are roadmap items, not implemented features.
