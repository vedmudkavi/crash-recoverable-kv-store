# Local benchmark report

Measured with `python3 -m examples.benchmark --count 1000 --value-bytes 1024 --repeats 3`.

Environment: macOS-26.6.2-arm64-arm-64bit-Mach-O; Python 3.13.7.

Each mode writes 1,000 values of 1,024 bytes. Figures below are medians across three runs.

| Measurement | Individual commits | Batches of 50 |
|---|---:|---:|
| Write operations / second | 41,364.691 | 359,453.631 |
| Commit p95 latency (ms) | 0.033 | 0.155 |
| Disk-backed reads / second | 364,431.479 | 20,330.816 |
| Recovery with memory tracing (ms) | 10.831 | 1.949 |
| Retained traced Python bytes after recovery | 139,099.000 | 80,755.000 |
| Peak traced Python bytes during recovery | 144,818.000 | 297,942.000 |

The workload contains 1,024,000 logical value bytes, excluding keys and framing.
Retained traced allocations include the open database and recovered key index; they
exclude operating-system page cache and are not total process memory.

Batching amortizes one fsync over more operations. However, the current JSON frame
format requires reading and decoding a whole batch to fetch one key. This is why
write improvements must be considered together with read amplification.

The OS cache was not cleared. macOS fsync is used without F_FULLFSYNC. These are
local development measurements, not power-loss validation or cross-engine rankings.
Recovery timings include tracemalloc overhead. Raw per-run results are in
[benchmark-results.json](benchmark-results.json).
