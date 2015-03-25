import psycopg2


class JsonWriter(object):
    def __init__(self, **conn_args):
        self.conn_args = conn_args
        self.conn = None
        self.cur = None

    def _ensure_connection(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(**self.conn_args)

    def _ensure_cursor(self):
        self._ensure_connection()
        if not self.cur or self.cur.closed:
            self.cur = self.conn.cursor()

    def begin(self):
        print "New cursor"
        self._ensure_cursor()

    def commit(self):
        # TOOD: more than one transaction per cursor????
        # TODO: order of commit, close
        print "Commiting"
        self.conn.commit()
        self.cur.close()
        self.cur = None

    def rollback(self):
        print "Rollback"
        self.conn.rollback()
        self.cur.close()
        self.cur = None

    def close(self):
        if self.cur and not self.cur.closed:
            self.cur.close()
        if self.conn and not self.conn.closed:
            self.conn.close()

    def write_json(self, item):
        # self.begin()
        if item['kind'] == 'insert':
            self.insert_to_sql(item)
        elif item['kind'] == 'update':
            self.update_to_sql(item)
        elif item['kind'] == 'delete':
            self.delete_to_sql(item)
        else:
            # TODO: error handling
            print "*** ERROR ***"

    def _make_pair_placeholder(self, names):
        return ', '.join('{}=%s'.format(x) for x in names)

    def update_to_sql(self, item):
        query_placeholder = self._make_pair_placeholder(item['oldkeys']['keynames'])
        set_placeholder = self._make_pair_placeholder(item['columnnames'])
        sql = "UPDATE {} SET {} WHERE {};".format(item['table'], set_placeholder, query_placeholder)
        sql_values = item['columnvalues'] + item['oldkeys']['keyvalues']
        print self.cur.mogrify(sql, sql_values)
        self.cur.execute(sql, sql_values)

    def insert_to_sql(self, item):
        columns = ', '.join(item['columnnames'])
        value_placeholder = ', '.join(['%s'] * len(item['columnvalues']))
        sql = "INSERT INTO {} ({}) VALUES ({});".format(item['table'], columns, value_placeholder)
        sql_values = item['columnvalues']
        print self.cur.mogrify(sql, sql_values)
        self.cur.execute(sql, sql_values)

    def delete_to_sql(self, item):
        query_placeholder = self._make_pair_placeholder(item['oldkeys']['keynames'])
        sql = "DELETE FROM {} WHERE {};".format(item['table'], query_placeholder)
        sql_values = item['oldkeys']['keyvalues']
        print self.cur.mogrify(sql, sql_values)
        self.cur.execute(sql, sql_values)
