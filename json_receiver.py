import argparse
import subprocess

import psycopg2
try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson

from db import json_to_sql
from migrate import transform, register_transform


def add_new_data(item):
    item['columnnames'].append('new_info')
    item['columnvalues'].append('NEW: {}'.format(item['columnvalues'][1]))


def setup_replication_slot(source_db, slot):
    cmd = ['pg_recvlogical', '-d', source_db, '--slot', slot, '--plugin', 'wal2json', '--create-slot', '--start', '-f', '-'] + \
        ['-o', 'include-schemas=f', '-o', 'include-types=f', '-o', 'include-timestamp=f']
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
    return proc


def cleanup_replication_slot(source_db, slot):
    subprocess.call(['pg_recvlogical', '-d', source_db, '--slot', slot, '--drop-slot'])


def main(source_db, dest_db, slot):
    # TODO: load migrations from external file???
    register_transform('data', add_new_data)
    try:
        proc = setup_replication_slot(source_db, slot)
        # parsed = ijson.parse(proc.stdout, multiple_values=True, buf_size=256)
        parsed = ijson.parse(proc.stdout, multiple_values=True, buf_size=1)

        conn = psycopg2.connect(database=dest_db)
        for change_list in ijson.common.items(parsed, 'change'):
            print "New cursor"
            cur = conn.cursor()
            # with conn.cursor() as cur:
            for item in change_list:
                transformed = transform(item)
                json_to_sql(transformed, cur)
            print "Commiting"
            conn.commit()
    finally:
        proc.kill()
        cleanup_replication_slot(source_db, slot)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Logical replication with optional migrations.')
    parser.add_argument('--source',
                        help='databse name to replicate from')
    parser.add_argument('--dest',
                        help='databse name to replicate to')
    parser.add_argument('--slot',
                        help='replication slot name')

    args = parser.parse_args()
    main(source_db=args.source, dest_db=args.dest, slot=args.slot)
