import psycopg2
import subprocess


class Receiver(object):
    def __init__(self, slot, **conn_args):
        self.conn_args = conn_args
        self.slot = slot
        self.proc = None

        # TODO: allow configuration???
        self.plugin = 'wal2json'
        self.options = {
            'include-schemas': True,
            'include-types': False,
            'include-timestamp': False,
            'include-xids': True,
            'include-lsn': True
            }


class SubprocessReceiver(Receiver):
    def _make_conn_args(self):
        result = []
        if 'database' in self.conn_args:
            result += ['-d', self.conn_args['database']]
        if 'host' in self.conn_args:
            result += ['-h', self.conn_args['host']]
        if 'port' in self.conn_args:
            result += ['-p', self.conn_args['port']]
        if 'user' in self.conn_args:
            result += ['-U', self.conn_args['user']]
        return result

    def _make_environment(self):
        env = dict()
        if 'password' in self.conn_args:
            env['PGPASSWORD'] = self.conn_args['password']
        return env

    def _make_plugin_options(self):
        # TODO: build from self.options
        return ['-o', 'include-schemas=t', '-o', 'include-types=f', '-o', 'include-timestamp=f', '-o', 'include-lsn=t']

    def create_replication_slot(self):
        cmd = ['pg_recvlogical', '--slot', self.slot, '--plugin', self.plugin, '--create-slot'] + self._make_conn_args()
        print cmd
        print self._make_environment()
        subprocess.call(cmd, env=self._make_environment())

    def drop_replication_slot(self):
        cmd = ['pg_recvlogical', '--slot', self.slot, '--drop-slot'] + self._make_conn_args()
        subprocess.call(cmd, env=self._make_environment())

    def start_replication(self, start_lsn=None):
        if self.proc:
            raise Exception('Process {} already running'.format(self.proc.pid))
        cmd = ['pg_recvlogical', '--slot', self.slot, '--start', '-f', '-'] + \
            self._make_conn_args() + self._make_plugin_options()
        if start_lsn:
            cmd += ['--startpos', start_lsn]
        print ' '.join(cmd)
        print self._make_environment()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, env=self._make_environment())
        return self.proc.stdout

    def stop_replication(self):
        if self.proc:
            self.proc.kill()


class PsycopgReceiver(Receiver):
    def __init__(self, *args, **kwargs):
        super(PsycopgReceiver, self).__init__(*args, **kwargs)
        self.conn = None
        self.cur = None

    def _ensure_connection(self):
        if not self.conn or self.conn.closed:
            self.conn = psycopg2.connect(**self.conn_args)

    def _ensure_cursor(self):
        self._ensure_connection()
        if not self.cur or self.cur.closed:
            self.cur = self.conn.cursor()

    def create_replication_slot(self):
        self._ensure_cursor()
        sql = "SELECT * FROM pg_create_logical_replication_slot(%s, %s);"
        sql_values = (self.slot, self.plugin)
        self.cur.execute(sql, sql_values)

    def drop_replication_slot(self):
        self._ensure_cursor()
        sql = "SELECT * FROM pg_drop_replication_slot(%s);"
        sql_values = (self.slot,)
        self.cur.execute(sql, sql_values)

    def get_slot_location(self):
        self._ensure_cursor()
        sql = "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = %s;"
        sql_values = (self.slot,)
        self.cur.execute(sql, sql_values)
        result = self.cur.fetchone()[0]
        return result

    def consume_slot_changes(self, to_lsn):
        sql = "SELECT * FROM pg_logical_slot_get_changes(%s, %s, NULL);"
        sql_values = (self.slot, to_lsn)
        self.cur.execute(sql, sql_values)

    def check_physical_xlog_replay(self):
        """This is to run on the slave..."""
        sql_xlog = "SELECT * FROM pg_last_xlog_replay_location();"
        self.cur.execute(sql_xlog)
        xlog = self.cur.fetchone()[0]
        return xlog

    def is_slave(self):
        self._ensure_cursor()
        sql_check = "SELECT * FROM pg_is_in_recovery();"
        self.cur.execute(sql_check)
        return self.cur.fetchone()[0]

    def promote_slave(self, trigger_file):
        if not self.is_slave():
            raise Exception("Target is already promoted, cannot promote")
        sql = "COPY (SELECT 1) TO %s;"
        sql_values = (trigger_file, )
        self.cur.execute(sql, sql_values)

    def start_replication(self):
        raise NotImplementedError()

    def stop_replication(self):
        raise NotImplementedError()
