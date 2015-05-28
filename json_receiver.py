import argparse
try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson

from db import JsonWriter
from migrate import Transformer, value_for_key
from receiver import SubprocessReceiver


def add_new_data(item):
    item['columnnames'].append('new_info')
    new_data = 'NEW: {}'.format(value_for_key(item, 'info'))
    item['columnvalues'].append(new_data)
    return item


def filter_out_all_foos(item):
    # TODO: patch wal2json to add this functionality
    if value_for_key(item, 'info').lower().startswith('foo'):
        return []
    else:
        return item


def delete_info_column(item):
    index = item['columnnames'].index('info')
    item['columnnames'].pop(index)
    item['columnvalues'].pop(index)
    return item


def main(source, dest, slot, auto_slot=False):
    # TODO: load migrations from external file???
    transformer = Transformer()
    transformer.register('data', add_new_data)
    receiver = SubprocessReceiver(slot=slot, **source)
    writer = JsonWriter(**dest)

    try:
        if auto_slot:
            receiver.create_replication_slot()

        start_lsn = writer.get_last_lsn(slot)
        data_stream = receiver.start_replication(start_lsn)

        # You can increase buffer size for more efficiency, but no guarantee you get the latest chunk
        # Maybe useful when you are trying to sync 1000's of bakcloged transactions,
        # then restart with buf_size=1 to clean up the last transaction.
        parsed = ijson.parse(data_stream, multiple_values=True, buf_size=1)

        for txn in ijson.common.items(parsed, ''):
            end_lsn = txn.get('nextlsn')
            print "Xid: {}".format(txn.get('xid'))
            print "End Lsn: {}".format(end_lsn)
            change_list = txn['change']
            writer.begin()
            for item in change_list:
                transformed = transformer.transform(item)
                if isinstance(transformed, (list, tuple)):
                    for action in transformed:
                        writer.write_json(action)
                elif transformed is not None:
                    writer.write_json(transformed)
            # record last lsn in lsn_sync_log...
            writer.set_end_lsn(slot, end_lsn)
            writer.commit()
    finally:
        if writer:
            writer.close()
        if receiver:
            receiver.stop_replication()
            if auto_slot:
                receiver.drop_replication_slot()


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Logical replication with optional migrations.')
    parser.add_argument('--src_db',
                        help='databse name to replicate from')
    parser.add_argument('--dest_db',
                        help='databse name to replicate to')
    parser.add_argument('--src_port',
                        help='databse name to replicate from')
    parser.add_argument('--dest_port',
                        help='databse name to replicate to')
    parser.add_argument('--src_host',
                        help='databse host to replicate from')
    parser.add_argument('--dest_host',
                        help='databse host to replicate to')
    parser.add_argument('--src_user',
                        help='databse user to authenticate')
    parser.add_argument('--dest_user',
                        help='databse user to authenticate')
    parser.add_argument('--src_password',
                        help='databse password to authenticate')
    parser.add_argument('--dest_password',
                        help='databse password to authenticate')
    parser.add_argument('--slot',
                        help='replication slot name')

    args = parser.parse_args()
    main(source=dict(database=args.src_db, port=args.src_port, host=args.src_host, user=args.src_user, password=args.src_password), 
         dest=dict(database=args.dest_db, port=args.dest_port, host=args.dest_host, user=args.dest_user, password=args.dest_password), 
         slot=args.slot)

