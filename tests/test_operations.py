import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from kvstore import Database


class OperationTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name) / 'db'

    def cli(self, *args, stdin=None):
        return subprocess.run([sys.executable, '-m', 'kvstore', '--db', str(self.path), *args],
                              input=stdin, capture_output=True, text=True, timeout=10)

    def test_cas_create_update_mismatch_delete_and_restart(self):
        with Database(self.path) as db:
            self.assertTrue(db.compare_and_swap('a', None, '1'))
            before = db.stats()['wal_bytes']
            self.assertFalse(db.compare_and_swap('a', None, '2'))
            self.assertFalse(db.compare_and_swap('a', 'wrong', '2'))
            self.assertEqual(db.stats()['wal_bytes'], before)
            self.assertTrue(db.compare_and_swap('a', '1', '2'))
        with Database(self.path) as db:
            self.assertEqual(db.get('a'), '2')
            self.assertTrue(db.compare_and_swap('a', '2', None))
            before = db.stats()['wal_bytes']
            self.assertTrue(db.compare_and_swap('a', None, None))
            self.assertEqual(db.stats()['wal_bytes'], before)
        with Database(self.path) as db:
            self.assertEqual(db.items(), [])

    def test_cas_concurrent_increment_has_no_lost_updates(self):
        with Database(self.path) as db:
            db.put('counter', '0')
            def increment(_):
                for _ in range(50):
                    while True:
                        old = db.get('counter')
                        if db.compare_and_swap('counter', old, str(int(old) + 1)):
                            break
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(increment, range(8)))
            self.assertEqual(db.get('counter'), '400')
        with Database(self.path) as db:
            self.assertEqual(db.get('counter'), '400')

    def test_cas_empty_string_is_not_absence(self):
        with Database(self.path) as db:
            db.put('a', '')
            self.assertFalse(db.compare_and_swap('a', None, 'new'))
            self.assertTrue(db.compare_and_swap('a', '', 'new'))

    def test_cas_validation_and_closed_instance(self):
        with Database(self.path) as db:
            for args in [(1,None,'v'), ('k',1,'v'), ('k',None,1)]:
                with self.assertRaises(TypeError):
                    db.compare_and_swap(*args)
        with self.assertRaises(RuntimeError):
            db.compare_and_swap('k', None, 'v')

    def test_scan_pagination_and_reads_only_selected_values(self):
        with Database(self.path) as db:
            for key in ['user:α', 'user:3', 'other:1', 'user:1', 'user:2']:
                db.put(key, key)
            with patch.object(db, '_read_frame', wraps=db._read_frame) as read:
                first = db.scan('user:', limit=2)
                self.assertEqual([k for k,v in first], ['user:1','user:2'])
                self.assertEqual(read.call_count, 2)
            second = db.scan('user:', after=first[-1][0], limit=2)
            self.assertEqual([k for k,v in second], ['user:3','user:α'])
            self.assertEqual(db.scan('user:', after=second[-1][0]), [])
            db.delete('user:2')
            db.compact()
        with Database(self.path) as db:
            self.assertEqual([k for k,v in db.scan('user:')], ['user:1','user:3','user:α'])

    def test_scan_validation_empty_and_nonexistent_cursor(self):
        with Database(self.path) as db:
            self.assertEqual(db.scan(), [])
            db.put('b','2')
            self.assertEqual(db.scan(after='a'), [('b','2')])
            for limit in [0,-1,True,1.5]:
                with self.assertRaises(ValueError): db.scan(limit=limit)
            with self.assertRaises(TypeError): db.scan(prefix=None)

    def test_cli_batch_stdin_and_scan(self):
        result = self.cli('batch','-',stdin=json.dumps([['put','a','1'],['put','b','2']]))
        self.assertEqual(result.returncode,0,result.stderr)
        page = self.cli('scan','--limit','1')
        self.assertEqual(json.loads(page.stdout), dict(items=[['a','1']], next_after='a'))
        page = self.cli('scan','--after','a','--limit','2')
        self.assertEqual(json.loads(page.stdout), dict(items=[['b','2']], next_after=None))

    def test_cli_invalid_batch_does_not_create_storage(self):
        for data in ['not json', '{}', '[["put", "a", 1]]']:
            result = self.cli('batch','-',stdin=data)
            self.assertEqual(result.returncode,1)
            self.assertFalse(self.path.exists())

    def test_cli_batch_file(self):
        file = self.path.parent / 'ops.json'
        file.write_text('[["put","key","value"]]')
        result = self.cli('batch',str(file))
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(self.cli('get','key').stdout.strip(),'value')

    def test_cli_cas_exit_codes_and_delete(self):
        self.assertEqual(self.cli('cas','a','--absent','--value','1').returncode,0)
        self.assertEqual(self.cli('cas','a','--expected','wrong','--value','2').returncode,3)
        self.assertEqual(self.cli('cas','a','--expected','1','--delete').returncode,0)
        self.assertEqual(self.cli('get','a').returncode,1)
