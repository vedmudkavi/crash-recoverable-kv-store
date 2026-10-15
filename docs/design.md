# Design decisions and evidence

## Values live on disk; keys live in memory

Version 0.1 rebuilt every value in memory. Version 0.2 retains a tuple pointing
to the latest put operation for each key. Recovery scans one record at a time,
then discards its decoded values after updating the index. A read uses `pread`,
which does not disturb the append file cursor, and verifies CRC32 before returning
the value. The WAL format is unchanged, so existing databases remain readable.

This reduces retained Python memory for large values, but adds disk access,
checksum verification, and JSON decoding to each read. Keys still consume memory.
For batches, fetching one value currently decodes the whole batch. The benchmark
exposes this tradeoff: larger batches amortize fsync but increase read amplification.
No claim of cold-disk throughput is made; the operating system cache remains enabled.

## Compaction replaces both the file and its index

A new index is constructed while writing live values to the temporary file.
The old index continues to reference the old file until the temporary file is
synced, atomically renamed, and its directory synced. Only then is the active
file reopened and the index swapped. The mutex excludes reads/writes throughout.
If an I/O error occurs, the instance refuses subsequent operations until reopen.
The stable LOCK file prevents another process from opening the same database
throughout replacement.

## Failure tests target ordering, not just happy paths

The crash harness subclasses a no-op checkpoint method and waits for a named
checkpoint on a pipe. The parent sends SIGKILL only after that checkpoint has
been reached. Checkpoints cover append written, append synced, compaction written,
compaction synced, log replaced, and directory synced.

An unsynced append is allowed to be either fully present or absent. After a synced
append, the complete batch must be present. At every compaction boundary, the
logical data must be unchanged. Each scenario also writes and reopens after
recovery to catch stale descriptors/indexes. These tests exercise process crashes,
not hardware power loss. Torn writes are separately injected at every byte offset
of a batch to validate the all-or-none replay rule.

## Independent model and inspection

A deterministic random workload applies 1,500 batches of 1–5 operations to both
Crash-Recoverable Key-Value Store and a Python dictionary. Full logical state is compared after every
batch, including repeated compactions and restarts. This tests combinations of
updates and deletes instead of only hand-picked sequences.

The inspection command opens existing files read-only and obtains the lock before
scanning. It reports damage without triggering automatic repair. It does not
output keys or values. A CRC32 cannot detect every possible corruption and is not
cryptographic authentication. A corrupted length can look like an incomplete tail;
the format has no redundant header or footer to disambiguate this case.

## Scope

This remains an educational single-process engine with serialized thread access.
There are no interactive transactions, replication, SQL, lock-free reads, or
background compaction. Adding those responsibly would require new invariants and
failure tests, not just new endpoints.
