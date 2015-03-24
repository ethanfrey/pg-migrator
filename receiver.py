import subprocess


class Receiver(object):
    def __init__(self, db_name, slot):
        self.db_name = db_name
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
    def create_replication_slot(self):
        cmd = ['pg_recvlogical', '-d', self.db_name, '--slot', self.slot, '--plugin', self.plugin, '--create-slot']
        subprocess.call(cmd)

    def drop_replication_slot(self):
        cmd = ['pg_recvlogical', '-d', self.db_name, '--slot', self.slot, '--drop-slot']
        subprocess.call(cmd)

    def start_replication(self):
        if self.proc:
            raise Exception('Process {} already running'.format(self.proc.pid))
        cmd = ['pg_recvlogical', '-d', self.db_name, '--slot', self.slot, '--start', '-f', '-']
        # TODO: build from self.options
        options = ['-o', 'include-schemas=f', '-o', 'include-types=f', '-o', 'include-timestamp=f']

        self.proc = subprocess.Popen(cmd + options, stdout=subprocess.PIPE, bufsize=1)
        return self.proc.stdout

    def stop_replication(self):
        self.proc.kill()


# TODO: psycopg receiver
