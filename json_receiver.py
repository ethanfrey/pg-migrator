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


def main(source_db, dest_db, slot):
    # TODO: load migrations from external file???
    transformer = Transformer()
    transformer.register('data', add_new_data)
    receiver = SubprocessReceiver(source_db, slot)

    try:
        receiver.create_replication_slot()
        data_stream = receiver.start_replication()

        # parsed = ijson.parse(proc.stdout, multiple_values=True, buf_size=64)
        parsed = ijson.parse(data_stream, multiple_values=True, buf_size=1)
        writer = JsonWriter(dest_db)

        for change_list in ijson.common.items(parsed, 'change'):
            writer.begin()
            for item in change_list:
                transformed = transformer.transform(item)
                if isinstance(transformed, (list, tuple)):
                    for action in transformed:
                        writer.write_json(action)
                else:
                    writer.write_json(transformed)
            writer.commit()
    finally:
        writer.close()
        receiver.stop_replication()
        receiver.drop_replication_slot()


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
