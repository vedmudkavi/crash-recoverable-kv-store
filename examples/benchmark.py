"""Reproducible local benchmark; all generated databases are temporary."""
import argparse
import json
import platform
import random
import statistics
import tempfile
import time
import tracemalloc
from kvstore import Database


def measure(count, value_bytes, batch_size):
    with tempfile.TemporaryDirectory() as directory:
        value = 'x' * value_bytes
        commit_latencies = []
        with Database(directory) as db:
            start = time.perf_counter()
            for offset in range(0, count, batch_size):
                ops = [('put', str(i), value) for i in range(offset, min(offset + batch_size, count))]
                commit_start = time.perf_counter()
                db.batch(ops)
                commit_latencies.append(time.perf_counter() - commit_start)
            write_seconds = time.perf_counter() - start
        tracemalloc.start()
        start = time.perf_counter()
        with Database(directory) as db:
            recovery_seconds = time.perf_counter() - start
            retained, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            keys = list(range(count))
            random.Random(42).shuffle(keys)
            start = time.perf_counter()
            for key in keys:
                if db.get(str(key)) != value:
                    raise AssertionError('Read verification failed')
            read_seconds = time.perf_counter() - start
            for i in range(100):
                db.put('hot-key', str(i))
            before = db.stats()['wal_bytes']
            start = time.perf_counter()
            db.compact()
            compact_seconds = time.perf_counter() - start
            after = db.stats()['wal_bytes']
        with Database(directory) as db:
            if db.get('hot-key') != '99' or db.stats()['keys'] != count + 1:
                raise AssertionError('Compaction verification failed')
        return dict(records=count, value_bytes=value_bytes, batch_size=batch_size,
                    commits=len(commit_latencies), write_ops_per_second=count/write_seconds,
                    commit_median_ms=statistics.median(commit_latencies)*1000,
                    commit_p95_ms=sorted(commit_latencies)[min(len(commit_latencies)-1, int(.95*len(commit_latencies)))]*1000,
                    disk_backed_read_ops_per_second=count/read_seconds,
                    recovery_ms=recovery_seconds*1000,
                    recovered_index_traced_bytes=retained, recovery_peak_traced_bytes=peak,
                    logical_value_bytes=count*value_bytes, compact_ms=compact_seconds*1000,
                    wal_bytes_before_compact=before, wal_bytes_after_compact=after)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', type=int, default=1000)
    parser.add_argument('--value-bytes', type=int, default=1024)
    parser.add_argument('--batch-sizes', type=int, nargs='+', default=[1, 50])
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    if min(args.count, args.value_bytes, args.repeats, *args.batch_sizes) < 1:
        parser.error('All parameters must be positive')
    runs = []
    for repeat in range(args.repeats):
        for size in args.batch_sizes:
            runs.append(dict(repeat=repeat + 1, **measure(args.count, args.value_bytes, size)))
    print(json.dumps(dict(platform=platform.platform(), python=platform.python_version(),
        durability='os.fsync per commit; not a power-loss benchmark',
        memory='tracemalloc Python allocations during reopen, not process RSS',
        reads='pread plus CRC32 and JSON decoding; OS cache not cleared', runs=runs), indent=2))


if __name__ == '__main__':
    main()
