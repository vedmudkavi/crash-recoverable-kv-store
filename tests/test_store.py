from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from kvstore import Database, CorruptionError, DatabaseLocked
from kvstore.store import _encode
from examples.crash_demo import main as crash_demo


class StoreTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def test_restart_updates_deletes_unicode(self):
        with Database(self.path) as db:
            db.put('hello', 'old')
            db.put('hello', 'नमस्ते 🌍')
            db.put('gone', '')
            db.delete('gone')
            db.delete('missing')
        with Database(self.path) as db:
            self.assertEqual(db.items(), [('hello', 'नमस्ते 🌍')])

    def test_every_truncated_batch_boundary_is_atomic(self):
        prefix = _encode([['put', 'existing', 'yes']])
        batch = _encode([['put', 'a', '1'], ['put', 'b', '2'], ['delete', 'existing']])
        for cut in range(len(batch) + 1):
            with self.subTest(cut=cut):
                (self.path / 'data.wal').write_bytes(prefix + batch[:cut])
                with Database(self.path) as db:
                    expected = [('a', '1'), ('b', '2')] if cut == len(batch) else [('existing', 'yes')]
                    self.assertEqual(db.items(), expected)
                    self.assertEqual(db.discarded_tail_bytes, cut if cut < len(batch) else 0)
                    db.put('after', 'recovery')
                with Database(self.path) as db:
                    self.assertEqual(db.get('after'), 'recovery')

    def test_checksum_corruption_preserves_file(self):
        record = bytearray(_encode([['put', 'a', 'b']]))
        record[-3] ^= 1
        (self.path / 'data.wal').write_bytes(record)
        with self.assertRaises(CorruptionError):
            Database(self.path)
        self.assertEqual((self.path / 'data.wal').read_bytes(), record)

    def test_invalid_length_rejected(self):
        (self.path / 'data.wal').write_bytes(b'\xff' * 8)
        with self.assertRaises(CorruptionError):
            Database(self.path)

    def test_exclusive_lock_and_release(self):
        with Database(self.path):
            with self.assertRaises(DatabaseLocked):
                Database(self.path)
        with Database(self.path) as db:
            db.put('lock', 'released')

    def test_compaction_and_subsequent_writes(self):
        with Database(self.path) as db:
            for i in range(25):
                db.put('a', str(i))
            db.put('deleted', 'value')
            db.delete('deleted')
            before = db.stats()['wal_bytes']
            db.compact()
            self.assertLess(db.stats()['wal_bytes'], before)
            db.put('b', 'after')
        with Database(self.path) as db:
            self.assertEqual(db.items(), [('a', '24'), ('b', 'after')])

    def test_empty_compaction(self):
        with Database(self.path) as db:
            db.put('a', '1')
            db.delete('a')
            db.compact()
            self.assertEqual(db.stats()['wal_bytes'], 0)
        with Database(self.path) as db:
            self.assertEqual(db.items(), [])

    def test_failed_compaction_before_rename(self):
        with Database(self.path) as db:
            db.put('a', '1')
            with patch('kvstore.store.os.replace', side_effect=OSError('injected')):
                with self.assertRaises(OSError):
                    db.compact()
        with Database(self.path) as db:
            self.assertEqual(db.get('a'), '1')

    def test_failed_compaction_after_rename(self):
        with Database(self.path) as db:
            db.put('a', '1')
            with patch.object(db, '_sync_directory', side_effect=OSError('injected')):
                with self.assertRaises(OSError):
                    db.compact()
        with Database(self.path) as db:
            self.assertEqual(db.get('a'), '1')

    def test_fsync_failure_requires_reopening(self):
        with Database(self.path) as db:
            with patch('kvstore.store.os.fsync', side_effect=OSError('injected')):
                with self.assertRaises(OSError):
                    db.put('a', 'uncertain')
            with self.assertRaises(RuntimeError):
                db.put('b', 'not allowed')

    def test_validation_before_write(self):
        with Database(self.path) as db:
            with self.assertRaises(TypeError):
                db.batch([('put', 'good', '1'), ('put', 'bad', 3)])
            self.assertEqual(db.items(), [])
            self.assertEqual(db.stats()['wal_bytes'], 0)

    def test_threaded_writes(self):
        with Database(self.path) as db:
            with ThreadPoolExecutor(max_workers=4) as pool:
                list(pool.map(lambda i: db.put(str(i), str(i)), range(80)))
        with Database(self.path) as db:
            self.assertEqual(len(db.items()), 80)

    def test_actual_process_kill(self):
        crash_demo()

    def test_closed_database_rejected(self):
        db = Database(self.path)
        db.close()
        db.close()
        with self.assertRaises(RuntimeError):
            db.get('a')


if __name__ == '__main__':
    unittest.main()
