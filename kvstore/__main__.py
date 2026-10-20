import argparse
import json
import sys

from . import CorruptionError, Database, DatabaseLocked
from .inspect import inspect_log
from .backup import restore_backup
from .store import _validate, _encode


def main():
    parser = argparse.ArgumentParser(description='Crash-Recoverable Key-Value Store — durable Python key-value storage')
    parser.add_argument('--db', default='.kvstore', help='database directory')
    commands = parser.add_subparsers(dest='command', required=True)
    put = commands.add_parser('put', help='durably store a string')
    put.add_argument('key')
    put.add_argument('value')
    for name in ('get', 'delete'):
        commands.add_parser(name).add_argument('key')
    for name in ('list', 'stats', 'compact', 'inspect'):
        commands.add_parser(name)
    scan = commands.add_parser('scan', help='read a sorted page of matching keys')
    scan.add_argument('--prefix', default='')
    scan.add_argument('--after', default=None, help='exclusive key cursor')
    scan.add_argument('--limit', type=int, default=100)
    batch = commands.add_parser('batch', help='commit a JSON operation array atomically')
    batch.add_argument('file', help='JSON file path, or - for stdin')
    cas = commands.add_parser('cas', help='compare and swap a value; exit 3 on mismatch')
    cas.add_argument('key')
    expected = cas.add_mutually_exclusive_group(required=True)
    expected.add_argument('--expected')
    expected.add_argument('--absent', action='store_true')
    replacement = cas.add_mutually_exclusive_group(required=True)
    replacement.add_argument('--value')
    replacement.add_argument('--delete', action='store_true')
    commands.add_parser('backup', help='create a consistent snapshot file').add_argument('file')
    commands.add_parser('restore', help='restore a snapshot into a NEW --db directory').add_argument('file')
    args = parser.parse_args()
    try:
        if args.command == 'restore':
            print(json.dumps(restore_backup(args.file, args.db), indent=2))
            return 0
        if args.command == 'inspect':
            report = inspect_log(args.db)
            print(json.dumps(report, indent=2))
            return 0 if report['status'] == 'healthy' else 2
        if args.command == 'batch':
            if args.file == '-':
                operations = json.load(sys.stdin)
            else:
                with open(args.file, encoding='utf-8') as file:
                    operations = json.load(file)
            _validate(operations)
            _encode(operations)  # reject invalid/oversized batches before opening storage
        if args.command == 'scan' and args.limit < 1:
            raise ValueError('Limit must be positive')
        with Database(args.db) as db:
            if args.command == 'put':
                db.put(args.key, args.value)
                print('Stored and synced.')
            elif args.command == 'get':
                print(db.get(args.key))
            elif args.command == 'delete':
                db.delete(args.key)
                print('Deleted and synced.')
            elif args.command == 'list':
                print(json.dumps(dict(db.items()), indent=2, ensure_ascii=False))
            elif args.command == 'batch':
                db.batch(operations)
                print(f'Committed {len(operations)} operations atomically.')
            elif args.command == 'cas':
                changed = db.compare_and_swap(args.key, args.expected, args.value)
                print('Condition matched; commit complete.' if changed else 'Value mismatch; no write performed.')
                return 0 if changed else 3
            elif args.command == 'scan':
                page = db.scan(args.prefix, after=args.after, limit=args.limit)
                print(json.dumps(dict(items=page, next_after=page[-1][0] if len(page) == args.limit else None),
                                 indent=2, ensure_ascii=False))
            elif args.command == 'backup':
                print(json.dumps(db.backup(args.file), indent=2))
            elif args.command == 'stats':
                print(json.dumps(db.stats(), indent=2))
            else:
                before = db.stats()['wal_bytes']
                db.compact()
                print(f"Compacted: {before:,} → {db.stats()['wal_bytes']:,} bytes")
    except (OSError, CorruptionError, DatabaseLocked, KeyError, ValueError, TypeError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
