"""A single-process, durable, append-only key-value store for POSIX systems."""
from __future__ import annotations

from bisect import bisect_left, bisect_right
import fcntl
import json
import os
from pathlib import Path
import struct
import threading
import zlib

HEADER = struct.Struct('>II')  # payload length, CRC32
MAX_RECORD = 16 * 1024 * 1024


class CorruptionError(Exception):
    """A complete record failed validation; never silently discard it."""


class DatabaseLocked(Exception):
    """Another database instance owns this directory."""


def _encode(ops):
    payload = json.dumps(ops, ensure_ascii=True, separators=(',', ':'), allow_nan=False).encode()
    if len(payload) > MAX_RECORD:
        raise ValueError('Batch exceeds 16 MiB record limit')
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def _validate(ops):
    if not isinstance(ops, list):
        raise ValueError('Operations must be a list')
    for op in ops:
        if not isinstance(op, list) or not op or op[0] not in ('put', 'delete'):
            raise ValueError('Invalid operation')
        if len(op) != (3 if op[0] == 'put' else 2):
            raise ValueError('Invalid operation length')
        if not isinstance(op[1], str):
            raise TypeError('Keys must be strings')
        if op[0] == 'put' and not isinstance(op[2], str):
            raise TypeError('Values must be strings')


class Database:
    """String keys and values. Each successful batch is fsync'd before returning.

    Calls on an instance are serialized. A process-level advisory lock prevents
    concurrent instances. Do not share instances across fork().
    """

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._mutex = threading.RLock()
        self._closed = False
        self._failed = False
        self._index = {}
        self._sorted_keys = None
        self.recovered_records = 0
        self.discarded_tail_bytes = 0
        self._lock = open(self.directory / 'LOCK', 'a+b')
        try:
            fcntl.flock(self._lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self._lock.close()
            raise DatabaseLocked(str(self.directory)) from exc
        self._log = None
        try:
            self._log = open(self.directory / 'data.wal', 'a+b', buffering=0)
            self._sync_directory()
            self._recover()
        except BaseException:
            self.close()
            raise

    def _sync_directory(self):
        fd = os.open(self.directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _check(self):
        if self._closed:
            raise RuntimeError('Database is closed')
        if self._failed:
            raise RuntimeError('I/O failed; close and reopen to recover')

    def _checkpoint(self, name):
        """No-op seam for deterministic subprocess crash tests."""

    def _apply(self, ops, offset, size):
        for position, op in enumerate(ops):
            if op[0] == 'put':
                if op[1] not in self._index:
                    self._sorted_keys = None
                self._index[op[1]] = (offset, size, position)
            else:
                if op[1] in self._index:
                    self._sorted_keys = None
                self._index.pop(op[1], None)

    def _read_frame(self, offset, size):
        # pread does not move the append/recovery file cursor.
        record = os.pread(self._log.fileno(), size, offset)
        if len(record) != size:
            raise CorruptionError(f'Truncated indexed record at byte {offset}')
        length, checksum = HEADER.unpack(record[:HEADER.size])
        payload = record[HEADER.size:]
        if length != len(payload) or zlib.crc32(payload) != checksum:
            raise CorruptionError(f'Damaged indexed record at byte {offset}')
        try:
            ops = json.loads(payload)
            _validate(ops)
            return ops
        except (ValueError, TypeError, IndexError, UnicodeError) as exc:
            raise CorruptionError(f'Invalid indexed record at byte {offset}') from exc

    @staticmethod
    def _indexed_value(ops, position, key):
        try:
            op = ops[position]
            if op[0] != 'put' or op[1] != key:
                raise ValueError('Index does not match record')
            return op[2]
        except (IndexError, ValueError) as exc:
            raise CorruptionError('Index does not match record') from exc

    def _read_value(self, key):
        offset, size, position = self._index[key]
        return self._indexed_value(self._read_frame(offset, size), position, key)

    def get_many(self, keys):
        """Return values in input order (including duplicates), None for missing keys.

        Read each referenced frame once. Decoded frames are not cached across
        requests; at most one frame is decoded at a time in addition to results.
        """
        keys = list(keys)
        if any(not isinstance(key, str) for key in keys):
            raise TypeError('Keys must be strings')
        with self._mutex:
            self._check()
            groups = {}
            result = [None] * len(keys)
            for index, key in enumerate(keys):
                if key in self._index:
                    offset, size, position = self._index[key]
                    groups.setdefault((offset, size), []).append((index, position, key))
            for (offset, size), requests in groups.items():
                ops = self._read_frame(offset, size)
                for index, position, key in requests:
                    result[index] = self._indexed_value(ops, position, key)
            return result

    def _ordered_keys(self):
        if self._sorted_keys is None:
            self._sorted_keys = sorted(self._index)
        return self._sorted_keys

    def _recover(self):
        self._log.seek(0)
        end = os.fstat(self._log.fileno()).st_size
        good = 0
        while good < end:
            header = self._log.read(HEADER.size)
            if len(header) < HEADER.size:
                break
            length, checksum = HEADER.unpack(header)
            if not 0 < length <= MAX_RECORD:
                raise CorruptionError(f'Invalid length at byte {good}')
            payload = self._log.read(length)
            if len(payload) != length:
                break
            if zlib.crc32(payload) != checksum:
                raise CorruptionError(f'Checksum mismatch at byte {good}')
            try:
                ops = json.loads(payload)
                _validate(ops)
            except (ValueError, TypeError, UnicodeError) as exc:
                raise CorruptionError(f'Invalid record at byte {good}') from exc
            self._apply(ops, good, HEADER.size + length)
            self.recovered_records += 1
            good += HEADER.size + length
        if good != end:
            self.discarded_tail_bytes = end - good
            self._log.truncate(good)
            os.fsync(self._log.fileno())
        self._log.seek(0, os.SEEK_END)

    @staticmethod
    def _write_all(file, data):
        view = memoryview(data)
        while view:
            written = file.write(view)
            if written is None or written <= 0:
                raise OSError('Short write without progress')
            view = view[written:]

    def batch(self, operations):
        """Atomically apply [('put', key, value), ('delete', key), ...]."""
        ops = [list(op) for op in operations]
        _validate(ops)
        record = _encode(ops)
        with self._mutex:
            self._check()
            try:
                offset = os.fstat(self._log.fileno()).st_size
                self._write_all(self._log, record)
                self._checkpoint('append_written')
                os.fsync(self._log.fileno())
                self._checkpoint('append_synced')
            except BaseException:
                self._failed = True
                raise
            self._apply(ops, offset, len(record))

    def put(self, key, value):
        self.batch([('put', key, value)])

    def delete(self, key):
        self.batch([('delete', key)])

    def get(self, key):
        with self._mutex:
            self._check()
            return self._read_value(key)

    def compare_and_swap(self, key, expected, value):
        """Durably replace a value only if it matches expected.

        expected=None requires an absent key; value=None deletes the key.
        Returns False without writing on a mismatch. The comparison and commit
        share the instance mutex, preventing lost updates between threads.
        """
        if not isinstance(key, str):
            raise TypeError('Keys must be strings')
        if expected is not None and not isinstance(expected, str):
            raise TypeError('Expected value must be a string or None')
        if value is not None and not isinstance(value, str):
            raise TypeError('New value must be a string or None')
        with self._mutex:
            self._check()
            current = self._read_value(key) if key in self._index else None
            if current != expected:
                return False
            if value is None:
                if current is not None:
                    self.delete(key)
            else:
                self.put(key, value)
            return True

    def scan(self, prefix='', *, after=None, limit=100, reverse=False):
        """Read a page using a cached sorted key list and binary-search bounds.

        after is exclusive in traversal order (less than after when reversing).
        Pages are individually consistent, not snapshots across calls.
        """
        if not isinstance(prefix, str) or (after is not None and not isinstance(after, str)):
            raise TypeError('Prefix and cursor must be strings')
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError('Limit must be a positive integer')
        if not isinstance(reverse, bool):
            raise TypeError('reverse must be a boolean')
        with self._mutex:
            self._check()
            keys = self._ordered_keys()
            lo = bisect_left(keys, prefix)
            # Unicode lexical successor, including prefixes ending in U+10FFFF.
            upper = None
            for i in range(len(prefix) - 1, -1, -1):
                if ord(prefix[i]) < 0x10ffff:
                    upper = prefix[:i] + chr(ord(prefix[i]) + 1)
                    break
            hi = len(keys) if upper is None else bisect_left(keys, upper)
            if reverse:
                if after is not None:
                    hi = min(hi, bisect_left(keys, after))
                selected = list(reversed(keys[max(lo, hi-limit):hi])) if hi > lo else []
            else:
                if after is not None:
                    lo = max(lo, bisect_right(keys, after))
                selected = keys[lo:min(hi, lo+limit)] if lo < hi else []
            return list(zip(selected, self.get_many(selected)))

    def items(self):
        with self._mutex:
            self._check()
            return [(key, self._read_value(key)) for key in sorted(self._index)]

    def stats(self):
        with self._mutex:
            self._check()
            wal_bytes = os.fstat(self._log.fileno()).st_size
            referenced_bytes = sum(size for offset, size in
                                   {(ref[0], ref[1]) for ref in self._index.values()})
            return dict(keys=len(self._index), index_mode='disk-offsets', wal_bytes=wal_bytes,
                        referenced_frame_bytes=referenced_bytes,
                        obsolete_frame_bytes=wal_bytes-referenced_bytes,
                        recovered_records=self.recovered_records,
                        discarded_tail_bytes=self.discarded_tail_bytes)

    def backup(self, destination):
        """Create a consistent portable snapshot; never overwrite an existing file."""
        from .backup import write_backup
        with self._mutex:
            self._check()
            return write_backup(self, destination)

    def compact(self):
        """Atomically replace history with a durable snapshot of live keys."""
        with self._mutex:
            self._check()
            temporary = self.directory / 'compact.tmp'
            try:
                new_index = {}
                offset = 0
                with open(temporary, 'wb', buffering=0) as file:
                    for key in sorted(self._index):
                        record = _encode([['put', key, self._read_value(key)]])
                        self._write_all(file, record)
                        new_index[key] = (offset, len(record), 0)
                        offset += len(record)
                    self._checkpoint('compact_written')
                    os.fsync(file.fileno())
                self._checkpoint('compact_synced')
                os.replace(temporary, self.directory / 'data.wal')
                self._checkpoint('compact_replaced')
                self._sync_directory()
                self._checkpoint('directory_synced')
                self._log.close()
                self._log = open(self.directory / 'data.wal', 'a+b', buffering=0)
                self._index = new_index
            except BaseException:
                self._failed = True
                raise

    def close(self):
        with self._mutex:
            if self._closed:
                return
            self._closed = True
            try:
                if self._log is not None:
                    self._log.close()
            finally:
                self._lock.close()

    def __enter__(self):
        self._check()
        return self

    def __exit__(self, *args):
        self.close()
