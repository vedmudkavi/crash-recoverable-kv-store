import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor
from kvstore import Database, CorruptionError
from kvstore.backup import restore_backup, MANIFEST, MAGIC
from kvstore.store import _encode


class BackupTests(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.source = self.root / 'source'
        self.snapshot = self.root / 'snapshot.kvb'
        self.target = self.root / 'restored'

    def seed(self):
        with Database(self.source) as db:
            db.batch([('put','a','old'), ('put','gone','x')])
            db.put('a','नमस्ते 🌍')
            db.delete('gone')
            db.put('b','')
            return db.backup(self.snapshot)

    def test_roundtrip_latest_values_and_post_restore_writes(self):
        report = self.seed()
        self.assertEqual(report['keys'],2)
        self.assertEqual(restore_backup(self.snapshot,self.target)['sha256'],report['sha256'])
        with Database(self.target) as db:
            self.assertEqual(db.items(), [('a','नमस्ते 🌍'),('b','')])
            db.put('new','works')
            db.compact()
        with Database(self.target) as db:
            self.assertEqual(db.get('new'),'works')

    def test_empty_backup(self):
        with Database(self.source) as db: db.backup(self.snapshot)
        restore_backup(self.snapshot,self.target)
        with Database(self.target) as db: self.assertEqual(db.items(),[])

    def test_existing_backup_never_overwritten(self):
        self.snapshot.write_bytes(b'keep')
        with Database(self.source) as db:
            with self.assertRaises(FileExistsError): db.backup(self.snapshot)
            db.put('still','usable')
        self.assertEqual(self.snapshot.read_bytes(),b'keep')
        self.assertEqual(list(self.root.glob('.kv-backup-*')),[])

    def test_existing_directory_never_overwritten(self):
        self.seed()
        self.target.mkdir()
        with self.assertRaises(FileExistsError): restore_backup(self.snapshot,self.target)
        self.assertEqual(list(self.target.iterdir()),[])

    def test_truncation_at_every_byte_rejected(self):
        self.seed()
        original = self.snapshot.read_bytes()
        for cut in range(len(original)):
            with self.subTest(cut=cut):
                self.snapshot.write_bytes(original[:cut])
                with self.assertRaises(CorruptionError): restore_backup(self.snapshot,self.target)
                self.assertFalse(self.target.exists())
        self.assertEqual(list(self.root.glob('.kv-restore-*')),[])

    def test_corruption_manifest_and_payload_rejected(self):
        self.seed()
        original = self.snapshot.read_bytes()
        for offset in [0,15,20,len(original)-3]:
            data = bytearray(original)
            data[offset] ^= 1
            self.snapshot.write_bytes(data)
            with self.assertRaises(CorruptionError): restore_backup(self.snapshot,self.target)
            self.assertFalse(self.target.exists())

    def test_unsorted_duplicate_delete_records_rejected(self):
        for ops in [[['put','b','1'],['put','a','2']], [['put','a','1'],['put','a','2']], [['delete','a']]]:
            payload = b''.join(_encode([op]) for op in ops)
            self.snapshot.write_bytes(MANIFEST.pack(MAGIC,len(ops),hashlib.sha256(payload).digest())+payload)
            with self.assertRaises(CorruptionError): restore_backup(self.snapshot,self.target)

    def test_backup_write_failure_leaves_no_published_file(self):
        with Database(self.source) as db:
            db.put('a','1')
            with patch('kvstore.backup.os.fsync',side_effect=OSError('injected')):
                with self.assertRaises(OSError): db.backup(self.snapshot)
            self.assertEqual(db.get('a'),'1')
        self.assertFalse(self.snapshot.exists())
        self.assertEqual(list(self.root.glob('.kv-backup-*')),[])

    def test_restore_failure_before_publication_removes_reservation(self):
        self.seed()
        with patch('kvstore.backup.os.replace',side_effect=OSError('injected')):
            with self.assertRaises(OSError): restore_backup(self.snapshot,self.target)
        self.assertFalse(self.target.exists())

    def test_restore_sync_failure_preserves_complete_data(self):
        self.seed()
        with patch('kvstore.backup._sync_directory',side_effect=OSError('injected')):
            with self.assertRaises(OSError): restore_backup(self.snapshot,self.target)
        with Database(self.target) as db: self.assertEqual(db.stats()['keys'],2)

    def test_snapshot_consistency_during_concurrent_batches(self):
        with Database(self.source) as db:
            db.batch([('put','left','0'),('put','right','0')])
            def writer():
                for i in range(100): db.batch([('put','left',str(i)),('put','right',str(i))])
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(writer)
                db.backup(self.snapshot)
                future.result()
        restore_backup(self.snapshot,self.target)
        with Database(self.target) as db: self.assertEqual(db.get('left'),db.get('right'))

    def test_cli_backup_restore(self):
        self.seed()
        result = subprocess.run([sys.executable,'-m','kvstore','--db',str(self.target),'restore',str(self.snapshot)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['keys'],2)
        result = subprocess.run([sys.executable,'-m','kvstore','--db',str(self.target),'backup',str(self.root/'copy.kvb')],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
