"""Versioned, checksummed snapshots with no-overwrite publication."""
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import zlib

from .store import CorruptionError, Database, HEADER, MAX_RECORD, _encode, _validate

MAGIC = b'KVBACK01'
MANIFEST = struct.Struct('>8sQ32s')  # version, key count, SHA-256 of all frames


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write_backup(db, destination):
    """Caller holds the database mutex throughout this consistent snapshot."""
    destination = Path(destination)
    digest = hashlib.sha256()
    count = 0
    # Same filesystem is required for atomic, no-overwrite hard-link publication.
    fd, temporary = tempfile.mkstemp(prefix='.kv-backup-', dir=destination.parent)
    try:
        with os.fdopen(fd, 'w+b', buffering=0) as file:
            Database._write_all(file, bytes(MANIFEST.size))
            for key in sorted(db._index):
                record = _encode([['put', key, db._read_value(key)]])
                Database._write_all(file, record)
                digest.update(record)
                count += 1
            file.seek(0)
            Database._write_all(file, MANIFEST.pack(MAGIC, count, digest.digest()))
            os.fsync(file.fileno())
        os.link(temporary, destination)  # fails if destination already exists
        _sync_directory(destination.parent)
        return dict(keys=count, sha256=digest.hexdigest(), bytes=destination.stat().st_size)
    finally:
        os.unlink(temporary)


def restore_backup(source, destination):
    """Validate the entire snapshot before reserving a new database directory.

    Existing destinations (including empty directories) are never overwritten.
    A crash during publication may leave an empty reserved directory; discard it
    and retry in a fresh directory. A successful return includes directory fsync.
    """
    destination = Path(destination)
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f'Destination already exists: {destination}')
    with tempfile.TemporaryDirectory(prefix='.kv-restore-', dir=destination.parent) as stage:
        staged = Path(stage) / 'data.wal'
        digest = hashlib.sha256()
        count = 0
        previous = None
        with open(source, 'rb') as file, open(staged, 'wb', buffering=0) as output:
            manifest = file.read(MANIFEST.size)
            if len(manifest) != MANIFEST.size:
                raise CorruptionError('Incomplete backup manifest')
            magic, expected_count, expected_digest = MANIFEST.unpack(manifest)
            if magic != MAGIC:
                raise CorruptionError('Unsupported backup format')
            while True:
                header = file.read(HEADER.size)
                if not header:
                    break
                if len(header) != HEADER.size:
                    raise CorruptionError('Incomplete backup record header')
                length, checksum = HEADER.unpack(header)
                if not 0 < length <= MAX_RECORD:
                    raise CorruptionError('Invalid backup record length')
                payload = file.read(length)
                if len(payload) != length or zlib.crc32(payload) != checksum:
                    raise CorruptionError('Incomplete or damaged backup record')
                try:
                    ops = json.loads(payload)
                    _validate(ops)
                    if len(ops) != 1 or ops[0][0] != 'put':
                        raise ValueError('Expected one live value per record')
                    key = ops[0][1]
                    if previous is not None and key <= previous:
                        raise ValueError('Keys must be unique and sorted')
                except (TypeError, ValueError, UnicodeError) as exc:
                    raise CorruptionError('Invalid backup operations') from exc
                previous = key
                record = header + payload
                digest.update(record)
                Database._write_all(output, record)
                count += 1
            if count != expected_count or digest.digest() != expected_digest:
                raise CorruptionError('Backup count or SHA-256 mismatch')
            os.fsync(output.fileno())
        destination.mkdir()  # exclusive reservation; also handles concurrent restores
        published = False
        try:
            os.replace(staged, destination / 'data.wal')
            published = True
            _sync_directory(destination)
            _sync_directory(destination.parent)
        finally:
            if not published:
                destination.rmdir()
        return dict(keys=count, sha256=digest.hexdigest())
