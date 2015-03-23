import sys

# TODO: argparse
import psycopg2

try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson


def json_to_sql(item, cur):
    if item['kind'] == 'insert':
        return insert_to_sql(item, cur)
    elif item['kind'] == 'update':
        return update_to_sql(item, cur)
    elif item['kind'] == 'delete':
        return delete_to_sql(item, cur)
    else:
        # TODO: error handling
        print "*** ERROR ***"
        return ''


def _make_pair_placeholder(names):
    return ', '.join('{}=%s'.format(x) for x in names)


def update_to_sql(item, cur):
    query_placeholder = _make_pair_placeholder(item['oldkeys']['keynames'])
    set_placeholder = _make_pair_placeholder(item['columnnames'])
    sql = "UPDATE {} SET {} WHERE {};".format(item['table'], set_placeholder, query_placeholder)
    sql_values = item['columnvalues'] + item['oldkeys']['keyvalues']
    return cur.mogrify(sql, sql_values)


def insert_to_sql(item, cur):
    columns = ', '.join(item['columnnames'])
    value_placeholder = ', '.join(['%s'] * len(item['columnvalues']))
    sql = "INSERT INTO {} ({}) VALUES ({});".format(item['table'], columns, value_placeholder)
    sql_values = item['columnvalues']
    return cur.mogrify(sql, sql_values)


def delete_to_sql(item, cur):
    query_placeholder = _make_pair_placeholder(item['oldkeys']['keynames'])
    sql = "DELETE FROM {} WHERE {};".format(item['table'], query_placeholder)  
    sql_values = item['oldkeys']['keyvalues']
    return cur.mogrify(sql, sql_values)


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
