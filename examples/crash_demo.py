"""Run with python -m examples.crash_demo; uses temporary data only."""
from pathlib import Path
import subprocess
import sys
import tempfile
from kvstore import Database
from kvstore.store import _encode


def main():
    print('CRASH-RECOVERABLE KEY-VALUE STORE / CRASH LAB', flush=True)
    with tempfile.TemporaryDirectory() as directory:
        code = '''
import sys
from kvstore import Database
with Database(sys.argv[1]) as db:
    for i in range(100):
        db.put(str(i), f'value:{i}')
    print('ACK', flush=True)
    sys.stdin.readline()
'''
        child = subprocess.Popen([sys.executable, '-c', code, directory], stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, text=True)
        try:
            if child.stdout.readline().strip() != 'ACK':
                raise RuntimeError('Worker failed before acknowledgement')
            print('  1. Worker acknowledged 100 durable writes.')
            child.kill()
            child.wait(timeout=10)
            print('  2. Sent SIGKILL — no graceful shutdown.')
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdin.close()
            child.stdout.close()
        expected = {str(i): f'value:{i}' for i in range(100)}
        with Database(directory) as db:
            assert dict(db.items()) == expected
            print('  3. Restart verified every acknowledged value.')
        with open(Path(directory) / 'data.wal', 'ab') as file:
            file.write(_encode([['put', 'unfinished', 'never acknowledged']])[:-7])
        with Database(directory) as db:
            assert dict(db.items()) == expected
            print(f'  4. Injected torn tail: recovered {db.discarded_tail_bytes} incomplete bytes.')
            for i in range(100):
                db.put('hot-key', str(i))
            before = db.stats()['wal_bytes']
            db.compact()
            print(f"  5. Compaction: {before:,} → {db.stats()['wal_bytes']:,} bytes.")
        with Database(directory) as db:
            expected['hot-key'] = '99'
            assert dict(db.items()) == expected
        print('PASS / process crash, torn tail, and compacted restart verified.')


if __name__ == '__main__':
    main()
