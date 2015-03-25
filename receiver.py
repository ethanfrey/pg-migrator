import subprocess


class Receiver(object):
    def __init__(self, slot, **conn_args):
        self.conn_args = conn_args
        self.slot = slot
        self.proc = None

        # TODO: allow configuration???
        self.plugin = 'wal2json'
        self.options = {
            'include-schemas': False,
            'include-types': False,
            'include-timestamp': False,
            'include-xids': True
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
        return ['-o', 'include-schemas=f', '-o', 'include-types=f', '-o', 'include-timestamp=f']

    def create_replication_slot(self):
        cmd = ['pg_recvlogical', '--slot', self.slot, '--plugin', self.plugin, '--create-slot'] + self._make_conn_args()
        print cmd
        print self._make_environment()
        subprocess.call(cmd, env=self._make_environment())

    def drop_replication_slot(self):
        cmd = ['pg_recvlogical', '--slot', self.slot, '--drop-slot'] + self._make_conn_args()
        subprocess.call(cmd, env=self._make_environment())

    def start_replication(self):
        if self.proc:
            raise Exception('Process {} already running'.format(self.proc.pid))
        cmd = ['pg_recvlogical', '--slot', self.slot, '--start', '-f', '-'] + \
            self._make_conn_args() + self._make_plugin_options()
        print ' '.join(cmd)
        print self._make_environment()
        self.proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1, env=self._make_environment())
        return self.proc.stdout

    def stop_replication(self):
        if self.proc:
            self.proc.kill()


# TODO: psycopg receiver
