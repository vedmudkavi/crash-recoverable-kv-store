"""A single-process, durable, append-only key-value store for POSIX systems."""
from __future__ import annotations

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
                self._index[op[1]] = (offset, size, position)
            else:
                self._index.pop(op[1], None)

    def _read_value(self, key):
        offset, size, position = self._index[key]
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
            op = ops[position]
            if op[0] != 'put' or op[1] != key:
                raise ValueError('Index does not match record')
            return op[2]
        except (ValueError, TypeError, IndexError, UnicodeError) as exc:
            raise CorruptionError(f'Invalid indexed record at byte {offset}') from exc

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

    def items(self):
        with self._mutex:
            self._check()
            return [(key, self._read_value(key)) for key in sorted(self._index)]

    def stats(self):
        with self._mutex:
            self._check()
            return dict(keys=len(self._index), index_mode='disk-offsets', wal_bytes=os.fstat(self._log.fileno()).st_size,
                        recovered_records=self.recovered_records,
                        discarded_tail_bytes=self.discarded_tail_bytes)

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
