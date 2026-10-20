"""Create a snapshot, change the source, and restore its earlier state."""
from pathlib import Path
import tempfile
from kvstore import Database
from kvstore.backup import restore_backup


def main():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        with Database(root / 'source') as db:
            db.batch([('put','account:alice','100'), ('put','account:bob','50')])
            report = db.backup(root / 'snapshot.kvb')
            db.put('account:alice','0')
            print(f"Snapshot: {report['keys']} keys, SHA-256 {report['sha256']}")
            print('Source changed after backup: alice=0')
        restore_backup(root / 'snapshot.kvb', root / 'restored')
        with Database(root / 'restored') as db:
            assert db.get('account:alice') == '100'
            assert db.get('account:bob') == '50'
            db.put('restore:verified','yes')
        with Database(root / 'restored') as db:
            assert db.get('restore:verified') == 'yes'
        print('PASS / restored original balances and verified new durable writes.')


if __name__ == '__main__':
    main()
