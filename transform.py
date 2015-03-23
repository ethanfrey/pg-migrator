import sys

# TODO: argparse
import psycopg2

try:
    import ijson.backends.yajl2 as ijson
except ImportError:
    import ijson


def json_to_sql(item):
    if item['kind'] == 'insert':
        return insert_to_sql(item)
    elif item['kind'] == 'update':
        return update_to_sql(item)
    elif item['kind'] == 'delete':
        return delete_to_sql(item)
    else:
        # TODO: error handling
        print "*** ERROR ***"
        return ''


def update_to_sql(item):
    pairs = zip(item['oldkeys']['keynames'], item['oldkeys']['keyvalues'])
    # TODO: only works well with integers, not with strings as primary values
    query = ', '.join('{}={}'.format(name, value) for name, value in pairs)

    val_pairs = zip(item['columnnames'], item['columnvalues'])
    values = ', '.join('{}={}'.format(name, value) for name, value in val_pairs)

    sql = "UPDATE {} SET {} WHERE {}".format(item['table'], values, query)
    return sql


def insert_to_sql(item):
    sql = "INSERT INTO {} ({}) VALUES ({})".format(item['table'], item['columnnames'], item['columnvalues'])
    return sql


def delete_to_sql(item):
    pairs = zip(item['oldkeys']['keynames'], item['oldkeys']['keyvalues'])
    # TODO: only works well with integers, not with strings as primary values
    query = ', '.join('{}={}'.format(name, value) for name, value in pairs)
    sql = "DELETE FROM {} WHERE {}".format(item['table'], query)    
    return sql


def transform(item):
    # TODO: callback to register transforms
    if item['table'] == 'data':
        item['columnnames'].append('new_data')
        item['columnvalues'].append('NEW: {}'.format(item['columnvalues'][1]))
    return item


def main(connect_params):
    conn = psycopg2.connect(connect_params)
    cur = conn.cursor()
    parsed = ijson.parse(sys.stdin, multiple_values=True, buf_size=256)
    for item in ijson.common.items(parsed, 'change.item'):
        # print "GOT ITEM: {}".format(item)
        if item['kind'] in set(['update', 'insert']):
            item = transform(item)
        print json_to_sql(item)


if __name__ == '__main__':
    connect_params = sys.argv[1]
    main(connect_params)
