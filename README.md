# Crash-Recoverable Key-Value Store

**A crash-recoverable key-value database built from first principles in Python.**

Disk-backed reads. Checksummed records. Atomic batches. Recovery after SIGKILL.
No database libraries, no runtime dependencies, no TypeScript.

This is a compact systems engineering portfolio project: its central feature is a
failure you can reproduce, inspect, and recover from.

## Run the crash lab

Requires Python 3.10+ on macOS or Linux. From this repository directory:

```sh
python3 -m examples.crash_demo
```

The demo starts a separate writer process, waits for 100 acknowledged writes,
kills it with SIGKILL, and verifies every value after reopening the database.
It then separately injects an incomplete record to demonstrate torn-tail recovery,
updates a key 100 times, compacts the log, and verifies another restart.
All demo data lives in a temporary directory.

```text
CRASH-RECOVERABLE KEY-VALUE STORE / CRASH LAB
  1. Worker acknowledged 100 durable writes.
  2. Sent SIGKILL — no graceful shutdown.
  3. Restart verified every acknowledged value.
  4. Injected torn tail: recovered 44 incomplete bytes.
  5. Compaction: 6,470 → 3,312 bytes.
PASS / process crash, torn tail, and compacted restart verified.
```

## Try the CLI

```sh
python3 -m kvstore put name "Ada Lovelace"
python3 -m kvstore get name
python3 -m kvstore put name "Grace Hopper"
python3 -m kvstore list
python3 -m kvstore stats
python3 -m kvstore inspect
python3 -m kvstore compact
python3 -m kvstore delete name
```

Data defaults to `.kvstore/`. Set a directory before the command with
`python3 -m kvstore --db /tmp/my-kvstore stats`.

Optionally install with `python3 -m pip install -e .` to use the `kvstore` command.
Running directly from the checkout needs no installation or downloads.

## Python API

```python
from kvstore import Database

with Database(".kvstore") as db:
    db.put("account:alice", "100")
    db.batch([
        ("put", "account:alice", "70"),
        ("put", "account:bob", "30"),
        ("delete", "pending:transfer"),
    ])
    print(db.get("account:alice"))
    db.compact()
```

Keys and values are strings. Missing `get` raises `KeyError`; deleting a missing
key is harmless. A batch is recorded as one frame and replayed in full or not at
all after a truncated write. This is atomic batching, not an interactive SQL
transaction system.

## Architecture

```text
put / delete / batch
        │
        ▼
 validate → encode length + CRC32 + JSON operations
        │
        ▼
 append to data.wal → fsync → update in-memory index → acknowledge

open → acquire LOCK → scan and verify WAL → replay complete frames
                                          └→ truncate incomplete final frame

compact → write live keys to compact.tmp → fsync temporary file
        → atomic replace data.wal → fsync directory → reopen WAL
```

Each record has an 8-byte big-endian header: a 4-byte payload length and a 4-byte
CRC32 of the payload. The payload is a JSON list of operations. Records are capped
at 16 MiB. CRC32 detects accidental damage; it is not an authenticity mechanism.

The log is the source of truth. Startup rebuilds an offset index in a Python dictionary.
The dictionary stores keys and `(frame_offset, frame_size, operation_index)` tuples,
not values. Reads use `os.pread`, verify the checksum, and decode the indexed
record. There is no SQLite, Redis, or external storage engine underneath the API. Deletes remain in the log until compaction removes
obsolete history. A separate lock file remains stable across WAL replacement.

An incomplete final header or payload is discarded during recovery. A complete
record with an invalid checksum, format, or length raises `CorruptionError` and
is preserved for inspection. A damaged length that resembles an incomplete tail
cannot be distinguished from a torn write by this format.

Compaction runs under the same mutex as writes. Before replacement, the original
log remains authoritative; after replacement, the complete snapshot is authoritative.
Abandoned `compact.tmp` files are ignored and overwritten on the next compaction.

## Verify and measure

```sh
python3 -m unittest discover -s tests -v
python3 -m examples.crash_matrix
python3 -m examples.benchmark --count 1000 --value-bytes 1024 --repeats 3
```

The 26 tests include a seeded workload of 1,500 batches compared with a reference
dictionary after each batch, with repeated compaction and reopening. A separate
crash matrix kills subprocesses at six append/compaction checkpoints. The original
byte-by-byte torn-batch tests, corruption tests, and locking tests remain included.
GitHub Actions is configured for Python 3.10 and 3.13 on Linux and macOS;
local execution does not imply those CI runs have happened.

The benchmark compares individual commits with batches of 50 across three runs.
It reports commit latency, operation throughput, disk-backed reads, recovery,
compaction, and traced Python memory allocations after reopening. Reads may be
served by the operating system cache. Memory figures are not process RSS.

See [benchmark report](docs/benchmark-report.md), [recorded local benchmark output](docs/benchmark-results.json),
[crash matrix results](docs/crash-matrix-results.json), and
[design decisions](docs/design.md). Results are machine-specific observations,
not comparisons against production databases.

### Inspect storage without changing it

`python3 -m kvstore inspect` reports each valid frame's byte offset, size,
operation counts, and checksum. It detects damaged records or incomplete tails
without repairing the log or displaying stored keys/values. It takes a shared
lock and refuses to run alongside a database writer. It requires an existing
database. Exit status is 0 for healthy storage, 2 for damage/incomplete data,
and 1 for access or locking errors. Opening through `Database` still performs
normal torn-tail recovery.

## Guarantees and limits

- A successful write returns after `os.fsync`. A write that raises an I/O error
  has an uncertain outcome and may appear after recovery; the instance refuses
  further operations until reopened.
- The demo proves recovery from process termination. It does not simulate a
  machine losing power, storage controller failures, or arbitrary filesystem
  corruption. Hardware/filesystem flush semantics still matter; macOS fsync
  is not the stronger hardware-flush primitive `F_FULLFSYNC`.
- One process owns a database directory at a time. Threads on the same instance
  are serialized. Do not share an open instance across `fork()`.
- All keys and their offset metadata must fit in memory; values stay on disk.
  Recovery and reads decode one complete batch at a time (up to 16 MiB encoded).
  `items()` explicitly materializes all values. Startup scans the entire log.
  Compaction blocks reads and writes and needs space for a second copy of live data.
- Local POSIX filesystems only. Windows and network filesystems are not supported.
- No SQL, replication, range index, encryption, MVCC, or production support claim.

## Next engineering milestones

1. Segmented logs and background compaction with safe reader lifetimes.
2. Per-value encoding so reading one key need not decode its entire batch.
3. Group commit with explicit acknowledgement rules and measured throughput.
4. Sorted immutable tables, sparse indexes, and Bloom filters.

Build and benchmark these incrementally. Be ready to explain why a record is
acknowledged only after fsync, why compaction needs atomic replacement, and why a
failed write can still be present after recovery.

## GitHub

Push this directory as the repository root. The implementation, tests, and demos
are Python; there is no frontend source or language-statistics override. Keep
local database directories and generated build artifacts out of version control.

## Portfolio materials

[LinkedIn project description and demo outline](docs/linkedin-project.md).
