"""Count file reads for equivalent requests; no timing or cross-engine claims."""
import json
import os
import tempfile
from unittest.mock import patch
from kvstore import Database


def main():
    keys=[f'key:{i:03}' for i in range(100)]
    with tempfile.TemporaryDirectory() as directory:
        with Database(directory) as db:
            db.batch([('put',key,'value:'+key) for key in keys])
            with patch('kvstore.store.os.pread',wraps=os.pread) as reads:
                individual=[db.get(key) for key in keys]
                individual_reads=reads.call_count
            with patch('kvstore.store.os.pread',wraps=os.pread) as reads:
                grouped=db.get_many(keys)
                grouped_reads=reads.call_count
            assert individual==grouped
            assert individual_reads==100 and grouped_reads==1
            page=db.scan(limit=10); cached=db._sorted_keys
            second=db.scan(after=page[-1][0],limit=10)
            assert db._sorted_keys is cached and len(second)==10
            print(json.dumps(dict(keys=100,wal_frames=1,individual_pread_calls=individual_reads,
                                  get_many_pread_calls=grouped_reads,scan_key_cache_reused=True),indent=2))


if __name__ == '__main__': main()
