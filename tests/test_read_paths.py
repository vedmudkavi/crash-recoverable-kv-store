import os
from pathlib import Path
import random
import tempfile
import unittest
from unittest.mock import patch
from kvstore import Database, CorruptionError


class ReadPathTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.path = Path(temp.name)

    def test_multiget_coalesces_frames_preserves_order_missing_duplicates(self):
        with Database(self.path) as db:
            db.batch([('put','a','1'),('put','b',''),('put','a','last')])
            db.put('c','3')
            with patch('kvstore.store.os.pread', wraps=os.pread) as read:
                self.assertEqual(db.get_many(['b','missing','a','c','a']),['',None,'last','3','last'])
                self.assertEqual(read.call_count,2)
            db.delete('a')
            self.assertEqual(db.get_many(['a','b']),[None,''])
            db.compact()
            self.assertEqual(db.get_many(['c','b']),['3',''])
        with Database(self.path) as db:
            self.assertEqual(db.get_many(['a','c']),[None,'3'])

    def test_multiget_empty_validation_closed(self):
        with Database(self.path) as db:
            self.assertEqual(db.get_many([]),[])
            with self.assertRaises(TypeError): db.get_many(['a',1])
        with self.assertRaises(RuntimeError): db.get_many([])

    def test_multiget_checks_corruption_on_every_request(self):
        with Database(self.path) as db:
            db.put('a','value')
            self.assertEqual(db.get_many(['a']),['value'])
            file = self.path/'data.wal'
            data = bytearray(file.read_bytes()); data[-3] ^= 1; file.write_bytes(data)
            with self.assertRaises(CorruptionError): db.get_many(['a'])

    def test_scan_reuses_key_cache_and_invalidates_on_membership_change(self):
        with Database(self.path) as db:
            db.batch([('put','a','1'),('put','b','2')])
            db.scan(); cached = db._sorted_keys
            db.put('a','updated'); db.scan()
            self.assertIs(db._sorted_keys,cached)
            db.delete('absent'); db.scan()
            self.assertIs(db._sorted_keys,cached)
            db.put('c','3'); db.scan()
            self.assertIsNot(db._sorted_keys,cached)
            db.delete('b'); db.compact()
            self.assertEqual(db.scan(reverse=True),[('c','3'),('a','updated')])

    def test_scan_model_prefix_cursor_reverse_unicode(self):
        keys = ['', 'a', 'aa', 'ab', 'b', 'λ', 'λx', '\U0010ffff', '\U0010ffffx', 'a\U0010ffff']
        with Database(self.path) as db:
            db.batch([('put',k,k) for k in keys])
            for prefix in ['', 'a', 'λ', '\U0010ffff', 'a\U0010ffff', 'missing']:
                for after in [None,'','a','aa','z','\U0010ffffx']:
                    for reverse in [False,True]:
                        expected = sorted((k for k in keys if k.startswith(prefix) and
                            (after is None or (k < after if reverse else k > after))),reverse=reverse)[:2]
                        self.assertEqual(db.scan(prefix,after=after,limit=2,reverse=reverse),[(k,k) for k in expected])

    def test_reverse_pagination_complete_without_duplicates(self):
        with Database(self.path) as db:
            db.batch([('put',f'k:{i:03}',str(i)) for i in range(37)])
            found=[]; after=None
            while True:
                page=db.scan('k:',after=after,limit=5,reverse=True)
                if not page: break
                found.extend(k for k,v in page); after=page[-1][0]
            self.assertEqual(found,[f'k:{i:03}' for i in reversed(range(37))])

    def test_scan_coalesces_one_batch(self):
        with Database(self.path) as db:
            db.batch([('put',str(i),str(i)) for i in range(20)])
            with patch('kvstore.store.os.pread',wraps=os.pread) as read:
                self.assertEqual(len(db.scan()),20)
                self.assertEqual(read.call_count,1)

    def test_frame_accounting_shared_live_records_and_compaction(self):
        with Database(self.path) as db:
            db.batch([('put','a','1'),('put','b','2')])
            first=db.stats()['wal_bytes']
            self.assertEqual(db.stats()['referenced_frame_bytes'],first)
            db.put('a','new')
            self.assertEqual(db.stats()['obsolete_frame_bytes'],0) # b keeps old frame live
            db.put('b','new')
            self.assertEqual(db.stats()['obsolete_frame_bytes'],first)
            db.delete('a'); db.delete('b')
            self.assertEqual(db.stats()['referenced_frame_bytes'],0)
            db.compact()
            self.assertEqual(db.stats()['obsolete_frame_bytes'],0)

    def test_scan_randomized_mutations(self):
        rng=random.Random(42); model={}
        with Database(self.path) as db:
            for i in range(100):
                key=f'{rng.randrange(3)}:{rng.randrange(20)}'
                if rng.random()<.3:
                    db.delete(key); model.pop(key,None)
                else:
                    db.put(key,str(i)); model[key]=str(i)
                for reverse in [True,False]:
                    expected=[(k,model[k]) for k in sorted(model,reverse=reverse) if k.startswith('1:')][:7]
                    self.assertEqual(db.scan('1:',limit=7,reverse=reverse),expected)
