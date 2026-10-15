import argparse
import json
import sys

from . import CorruptionError, Database, DatabaseLocked
from .inspect import inspect_log


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
    args = parser.parse_args()
    try:
        if args.command == 'inspect':
            report = inspect_log(args.db)
            print(json.dumps(report, indent=2))
            return 0 if report['status'] == 'healthy' else 2
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
            elif args.command == 'stats':
                print(json.dumps(db.stats(), indent=2))
            else:
                before = db.stats()['wal_bytes']
                db.compact()
                print(f"Compacted: {before:,} → {db.stats()['wal_bytes']:,} bytes")
    except (OSError, CorruptionError, DatabaseLocked, KeyError, ValueError) as exc:
        print(f'Error: {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
