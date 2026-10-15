"""Non-mutating storage inspection, including damaged and incomplete logs."""
import fcntl
import json
from pathlib import Path
import zlib
from .store import HEADER, MAX_RECORD, DatabaseLocked, _validate


def inspect_log(directory):
    """Acquire the existing lock and report frames without repairing the WAL.

    No files are created. A writer holding LOCK makes this operation fail fast.
    Values are deliberately excluded from the report.
    """
    directory = Path(directory)
    with open(directory / 'LOCK', 'rb') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise DatabaseLocked(str(directory)) from exc
        frames = []
        offset = 0
        status = 'healthy'
        reason = None
        with open(directory / 'data.wal', 'rb') as file:
            while True:
                header = file.read(HEADER.size)
                if not header:
                    break
                if len(header) < HEADER.size:
                    status, reason = 'incomplete_tail', 'Partial header'
                    break
                length, checksum = HEADER.unpack(header)
                if not 0 < length <= MAX_RECORD:
                    status, reason = 'corrupt', 'Invalid payload length'
                    break
                payload = file.read(length)
                if len(payload) < length:
                    status, reason = 'incomplete_tail', 'Partial payload'
                    break
                if zlib.crc32(payload) != checksum:
                    status, reason = 'corrupt', 'Checksum mismatch'
                    break
                try:
                    ops = json.loads(payload)
                    _validate(ops)
                except (ValueError, TypeError, UnicodeError):
                    status, reason = 'corrupt', 'Invalid operations'
                    break
                frames.append(dict(offset=offset, bytes=HEADER.size + length,
                                   operations=len(ops), puts=sum(op[0] == 'put' for op in ops),
                                   deletes=sum(op[0] == 'delete' for op in ops),
                                   crc32=f'{checksum:08x}'))
                offset += HEADER.size + length
        return dict(status=status, valid_bytes=offset, problem_offset=None if status == 'healthy' else offset,
                    reason=reason, frames=frames)
