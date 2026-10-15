"""Kill real workers at six storage boundaries and verify recovery."""
import json
import selectors
import subprocess
import sys
import tempfile
from kvstore import Database

POINTS = ('append_written', 'append_synced', 'compact_written', 'compact_synced',
          'compact_replaced', 'directory_synced')
WORKER = '''
import sys
from kvstore import Database
class PausedDatabase(Database):
    def _checkpoint(self, name):
        if name == sys.argv[2]:
            print(name, flush=True)
            sys.stdin.readline()
with PausedDatabase(sys.argv[1]) as db:
    if sys.argv[2].startswith('append'):
        db.batch([('put', 'alice', '70'), ('put', 'bob', '30')])
    else:
        db.compact()
'''


def run_case(point):
    if point not in POINTS:
        raise ValueError(point)
    with tempfile.TemporaryDirectory() as directory:
        with Database(directory) as db:
            db.batch([('put', 'alice', '100'), ('put', 'bob', '0')])
            for i in range(20):
                db.put('version', str(i))
        child = subprocess.Popen([sys.executable, '-c', WORKER, directory, point],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(child.stdout, selectors.EVENT_READ)
                if not selector.select(timeout=10):
                    raise RuntimeError(f'Worker did not reach {point}')
            if child.stdout.readline().strip() != point:
                raise RuntimeError(f'Worker exited before {point}')
            child.kill()
            child.wait(timeout=10)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdin.close()
            child.stdout.close()
        with Database(directory) as db:
            actual = dict(db.items())
            before = dict(alice='100', bob='0', version='19')
            after = dict(alice='70', bob='30', version='19')
            allowed = [before, after] if point == 'append_written' else [after] if point == 'append_synced' else [before]
            if actual not in allowed:
                raise AssertionError((point, actual, allowed))
            db.put('after-recovery', 'works')
        with Database(directory) as db:
            if db.get('after-recovery') != 'works':
                raise AssertionError('Post-recovery write missing')
        return dict(checkpoint=point, passed=True, recovered_keys=len(actual))


def main():
    results = [run_case(point) for point in POINTS]
    print(json.dumps(dict(scenarios=len(results), results=results), indent=2))


if __name__ == '__main__':
    main()
