import sys

# TODO: argparse
import psycopg2

try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson

from db import json_to_sql


def transform(item):
    # only transform these calls
    if item['kind'] not in set(['update', 'insert']):
        return item
    # and only on the appropriate table
    # TODO: callback to register transforms
    if item['table'] == 'data':
        item['columnnames'].append('new_data')
        item['columnvalues'].append('NEW: {}'.format(item['columnvalues'][1]))
    return item


def main(connect_params):
    conn = psycopg2.connect(connect_params)
    parsed = ijson.parse(sys.stdin, multiple_values=True, buf_size=256)
    for change_list in ijson.common.items(parsed, 'change'):
        with conn.cursor() as cur:
            for item in change_list:
                transformed = transform(item)
                print json_to_sql(transformed, cur)


if __name__ == '__main__':
    connect_params = sys.argv[1]
    main(connect_params)
