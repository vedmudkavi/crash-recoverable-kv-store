import random
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kvstore import Database, CorruptionError, DatabaseLocked
from kvstore.inspect import inspect_log
from kvstore.store import _encode
from examples.crash_matrix import POINTS, run_case


class AdvancedTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def test_disk_index_retains_offsets_not_values(self):
        value = 'large value ' * 10000
        with Database(self.path) as db:
            db.put('a', value)
            self.assertEqual(db._index['a'], (0, (self.path / 'data.wal').stat().st_size, 0))
            with patch('kvstore.store.os.pread', wraps=__import__('os').pread) as read:
                self.assertEqual(db.get('a'), value)
                read.assert_called_once()
        with Database(self.path) as db:
            self.assertEqual(db.get('a'), value)
            db.compact()
            self.assertEqual(db.get('a'), value)

    def test_duplicate_keys_within_batch(self):
        with Database(self.path) as db:
            db.batch([('put', 'a', 'old'), ('delete', 'a'), ('put', 'a', 'last'), ('put', 'b', '2')])
            self.assertEqual(db.get('a'), 'last')
        with Database(self.path) as db:
            self.assertEqual(db.get('a'), 'last')
            db.compact()
            self.assertEqual(db.get('b'), '2')

    def test_read_detects_damage_after_open(self):
        with Database(self.path) as db:
            db.put('a', 'value')
            path = self.path / 'data.wal'
            data = bytearray(path.read_bytes())
            data[-3] ^= 1
            path.write_bytes(data)
            with self.assertRaises(CorruptionError):
                db.get('a')

    def test_model_based_randomized_workload(self):
        rng = random.Random(2026)
        expected = {}
        db = Database(self.path)
        try:
            for step in range(1500):
                ops = []
                for _ in range(rng.randint(1, 5)):
                    key = f'key:{rng.randrange(100)}'
                    if rng.random() < .3:
                        ops.append(('delete', key))
                        expected.pop(key, None)
                    else:
                        value = f'{step}:λ:{rng.randrange(1000)}'
                        ops.append(('put', key, value))
                        expected[key] = value
                db.batch(ops)
                if step % 31 == 0:
                    db.compact()
                if step % 47 == 0:
                    db.close()
                    db = Database(self.path)
                self.assertEqual(dict(db.items()), expected)
        finally:
            db.close()

    def test_six_real_crash_boundaries(self):
        for point in POINTS:
            with self.subTest(point=point):
                self.assertTrue(run_case(point)['passed'])

    def test_inspect_is_non_mutating_and_reports_frames(self):
        with Database(self.path) as db:
            db.batch([('put', 'secret', 'hidden-value'), ('delete', 'gone')])
        path = self.path / 'data.wal'
        original = path.read_bytes()
        report = inspect_log(self.path)
        self.assertEqual(report['status'], 'healthy')
        self.assertEqual(report['frames'][0]['operations'], 2)
        self.assertNotIn('hidden-value', str(report))
        self.assertEqual(path.read_bytes(), original)

    def test_inspect_torn_tail_without_repair(self):
        with Database(self.path) as db:
            db.put('a', '1')
        path = self.path / 'data.wal'
        original = path.read_bytes() + _encode([['put', 'x', 'y']])[:-3]
        path.write_bytes(original)
        self.assertEqual(inspect_log(self.path)['status'], 'incomplete_tail')
        self.assertEqual(path.read_bytes(), original)

    def test_inspect_corruption(self):
        with Database(self.path) as db:
            db.put('a', '1')
        path = self.path / 'data.wal'
        data = bytearray(path.read_bytes())
        data[-3] ^= 1
        path.write_bytes(data)
        self.assertEqual(inspect_log(self.path)['status'], 'corrupt')
        self.assertEqual(path.read_bytes(), data)

    def test_inspect_respects_writer_lock(self):
        with Database(self.path):
            with self.assertRaises(DatabaseLocked):
                inspect_log(self.path)

    def test_inspect_missing_database_creates_nothing(self):
        with self.assertRaises(FileNotFoundError):
            inspect_log(self.path / 'missing')
        self.assertFalse((self.path / 'missing').exists())

    def test_partial_writes_are_completed(self):
        class ShortWriter:
            def __init__(self):
                self.data = bytearray()
            def write(self, data):
                self.data.extend(data[:3])
                return len(data[:3])
        writer = ShortWriter()
        Database._write_all(writer, b'abcdefghijk')
        self.assertEqual(writer.data, b'abcdefghijk')

    def test_zero_progress_write_is_rejected(self):
        class StalledWriter:
            def write(self, data):
                return 0
        with self.assertRaises(OSError):
            Database._write_all(StalledWriter(), b'abc')
