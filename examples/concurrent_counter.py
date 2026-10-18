"""Demonstrate retrying conditional updates across eight threads."""
from concurrent.futures import ThreadPoolExecutor
import tempfile
from kvstore import Database


def main():
    with tempfile.TemporaryDirectory() as directory:
        with Database(directory) as db:
            db.put('counter', '0')
            def increment(_):
                retries = 0
                for _ in range(100):
                    while True:
                        previous = db.get('counter')
                        if db.compare_and_swap('counter', previous, str(int(previous) + 1)):
                            break
                        retries += 1
                return retries
            with ThreadPoolExecutor(max_workers=8) as pool:
                retries = sum(pool.map(increment, range(8)))
            assert db.get('counter') == '800'
            print(f'8 threads × 100 increments = 800 durable updates; {retries} retries.')
        with Database(directory) as db:
            assert db.get('counter') == '800'
            print('PASS / reopened counter is 800. No lost updates.')


if __name__ == '__main__':
    main()
