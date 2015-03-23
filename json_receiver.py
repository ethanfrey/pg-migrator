import sys

# TODO: argparse
import psycopg2

try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson

from db import json_to_sql
from migrate import transform, register_transform


def add_new_data(item):
    item['columnnames'].append('new_data')
    item['columnvalues'].append('NEW: {}'.format(item['columnvalues'][1]))


def main(connect_params):
    # TODO: load migrations from external file???
    register_transform('data', add_new_data)

    conn = psycopg2.connect(connect_params)
    parsed = ijson.parse(sys.stdin, multiple_values=True, buf_size=256)

    for change_list in ijson.common.items(parsed, 'change'):
        with conn.cursor() as cur:
            for item in change_list:
                transformed = transform(item)
                print json_to_sql(transformed, cur)


if __name__ == '__main__':
    # TODO: better arg parsing for db connection string
    connect_params = sys.argv[1]
    main(connect_params)
