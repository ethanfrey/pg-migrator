from collections import defaultdict

import psycopg2

from migrate import value_for_key


class JsonWriter(object):
    def __init__(self, **conn_args):
        self.conn_args = conn_args
        self.conn = None
        self.cur = None
        self._init_sequence_mapper()

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
        # TODO: table -> pk_name, sequence mapper, then update sequence on insert
        self.increment_sequences_for_item(item)

    def delete_to_sql(self, item):
        query_placeholder = self._make_pair_placeholder(item['oldkeys']['keynames'])
        sql = "DELETE FROM {} WHERE {};".format(item['table'], query_placeholder)
        sql_values = item['oldkeys']['keyvalues']
        print self.cur.mogrify(sql, sql_values)
        self.cur.execute(sql, sql_values)

    def _init_sequence_mapper(self):
        self.sequence_map = defaultdict(lambda: [])
        self._ensure_cursor()
        get_all_seq_sql = """SELECT d.nspname,
           b.oid,
           c.oid,
           b.relname as table_name,
           c.relname as seq_name,
           e.attname
        FROM   pg_depend AS a,
           pg_class AS b,
           pg_class AS c,
           pg_namespace AS d,
           pg_attribute AS e
        WHERE  ( refobjid IN (SELECT oid
                              FROM   pg_class
                              WHERE  relkind = 'r')
                  OR refobjsubid IN (SELECT oid
                                 FROM   pg_class
                                 WHERE  relkind = 'r') )
           AND b.relkind = 'r'
           AND b.oid = a.refobjid
           AND c.relkind = 'S'
           AND c.oid = a.objid
           AND d.oid = b.relnamespace
           AND e.attrelid = b.oid
           AND e.attnum = a.refobjsubid;"""
        self.cur.execute(get_all_seq_sql)
        for (schema, _oid1, _oid2, table_name, seq_name, attr_name) in self.cur.fetchall():
            key = "{}.{}".format(schema, table_name)
            self.sequence_map[key].append((attr_name, seq_name))
        # debug
        print self.sequence_map

    def increment_sequences_for_item(self, item):
        """
        when we replication insertions through logical replication, sequences aren't automatically incremented...
        this code fixes that!
        """
        key = "{}.{}".format(item['schema'], item['table'])
        for attr, seq in self.sequence_map[key]:
            val = value_for_key(item, attr)
            print "updating seq {} to {}".format(seq, val)
            # this may increment too much, but never go down
            update_sql = "select case when nextval(%s) < %s THEN setval(%s, %s) end;"
            sql_vals = (seq, val, seq, val)
            # this doesn't do extra increments, but may decend...
            # update_sql = "select setval(%s, %s);"
            # sql_vals = (seq, val)
            print self.cur.mogrify(update_sql, sql_vals)
            self.cur.execute(update_sql, sql_vals)

    def get_last_lsn(self, slot):
        create_sql = "CREATE TABLE IF NOT EXISTS lsn_sync_log(slot text, lsn pg_lsn);"
        self.cur.execute(create_sql)
        query_sql = "SELECT lsn FROM lsn_sync_log WHERE slot = %s;"
        self.cur.execute(query_sql, (slot,))
        lsn = self.cur.fetchone()
        if lsn:
            return lsn[0]
        else:
            insert_sql = "INSERT INTO lsn_sync_log (slot, lsn) VALUES (%s, %s);"
            self.cur.execute(insert_sql, (slot, "0/0"))
            return None

    def set_end_lsn(self, slot, end_lsn):
        # we have to add 0x40 to this to move beyond the "end commit" wal block
        lsn1, lsn2 = end_lsn.split('/')
        lsn2 = hex(int(lsn2, 16) + 64).upper()[2:]
        start_lsn = '{}/{}'.format(lsn1, lsn2)
        insert_sql = "UPDATE lsn_sync_log SET lsn = %s WHERE slot = %s and lsn < %s;"
        self.cur.execute(insert_sql, (start_lsn, slot, start_lsn))


